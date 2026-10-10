"""Bounded, append-only closed-bar catch-up for the controlled runtime schema.

This runner deliberately has no bus, dispatcher, Redis, strategy, execution, or
paid-provider integration. It reuses only the V1 module's stateless bar
validation/recovery primitives; V1's schema profile and orchestration remain
unchanged.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import time
from typing import Any

import aiohttp
from kairos_core.topics import Topics
from kairos_persistence import OfflineDurableWriter
from kairos_persistence.config import PersistenceSettings
from kairos_persistence.database import Database, MigrationProfile
from kairos_persistence.operator_control import OperatorControlRepository

from .config import QuantSettings
from .long_gap_recovery import (
    MINUTE,
    SOURCE,
    SYMBOLS,
    RecoveryError,
    RecoveryStatus,
    load_anchor,
    recover_symbol,
)

MAXIMUM_BARS = 150_000
CONTROLLED_RUNTIME_SCHEMA_VERSIONS = (
    "001_audit_and_idempotency.sql",
    "002_durable_runtime.sql",
    "003_execution_effect_journal.sql",
    "004_execution_recovery_delay.sql",
    "005_source_state_and_usage.sql",
    "006_paper_trade_lifecycle.sql",
    "007_execution_runtime_health.sql",
    "008_public_execution_events.sql",
    "009_paper_canary_arms.sql",
    "010_runtime_compensation_reserve.sql",
    "011_execution_mutation_budget.sql",
    "012_outbox_producer_order.sql",
    "013_campaign_source_budgets.sql",
    "014_bounded_canary_sessions.sql",
    "015_canary_dispatch_claims.sql",
    "016_global_canary_session_guard.sql",
    "018_offline_outbox_reconciliation.sql",
    "026_operator_control.sql",
)


def _validate_profile_manifest() -> None:
    if Database.migration_names(MigrationProfile.CONTROLLED_RUNTIME) != CONTROLLED_RUNTIME_SCHEMA_VERSIONS:
        raise RecoveryError(
            "installed persistence controlled-runtime manifest differs from the reviewed profile"
        )


async def _verify_runtime_identity(writer: OfflineDurableWriter) -> None:
    """Require the provisioned runtime login and its independently verified grants."""
    async with writer.database.pool.acquire() as connection:
        identity = await connection.fetchrow(
            "SELECT current_user AS current_actor, session_user AS session_actor"
        )
    if (
        identity is None
        or identity["current_actor"] != "kairos_runtime"
        or identity["session_actor"] != "kairos_runtime"
    ):
        raise RecoveryError("controlled-runtime catch-up requires the exact kairos_runtime identity")
    await OperatorControlRepository(writer.database.pool).verify_runtime_access()


async def run_recovery(
    end_exclusive: int,
    maximum_bars: int,
    *,
    expected_database_name: str,
    maximum_append_bars: int | None = None,
    status: RecoveryStatus | None = None,
) -> None:
    """Append at most the explicit global cap, resuming only from audit anchors."""
    status = status or RecoveryStatus(lambda item: print(json.dumps(item), flush=True))
    status.end_exclusive_ms = end_exclusive
    status.maximum_append_bars = maximum_append_bars
    status.record("STARTED", end_exclusive_ms=end_exclusive, maximum_bars=maximum_bars)
    try:
        await _run_recovery(
            end_exclusive,
            maximum_bars,
            status,
            expected_database_name=expected_database_name,
            maximum_append_bars=maximum_append_bars,
        )
        remaining = status.remaining_bars
        if remaining is None or remaining < 0:
            raise RecoveryError("recovery finished without an exact bounded remaining count")
    except BaseException as exc:
        if status.first_failure_phase is None:
            status.failed(exc)
        raise
    status.enter("FINISHED")
    status.record("PAUSED_LIMIT" if remaining else "COMPLETED", end_exclusive_ms=end_exclusive)


async def _run_recovery(
    end_exclusive: int,
    maximum_bars: int,
    status: RecoveryStatus,
    *,
    expected_database_name: str,
    maximum_append_bars: int | None = None,
) -> None:
    if maximum_append_bars is not None and (
        type(maximum_append_bars) is not int
        or not 1 <= maximum_append_bars <= MAXIMUM_BARS
        or type(maximum_bars) is not int
        or maximum_append_bars > maximum_bars
    ):
        raise RecoveryError("recovery append limit must be an integer from 1 to the explicit bar budget")
    if not isinstance(expected_database_name, str) or not expected_database_name.strip():
        raise RecoveryError("recovery requires a non-empty expected database name")
    if type(maximum_bars) is not int or not 1 <= maximum_bars <= MAXIMUM_BARS:
        raise RecoveryError("recovery deadline must be closed and budget at most 150000 bars")
    _validate_profile_manifest()
    settings = QuantSettings()
    if (
        settings.environment != "paper"
        or settings.bus_backend != "redis"
        or settings.service_name != SOURCE
        or set(settings.symbols) != set(SYMBOLS)
        or settings.binance_rest_base != "https://fapi.binance.com"
    ):
        raise RecoveryError("recovery requires the isolated PAPER five-symbol producer profile")
    finality_ms = max(5_000, int(settings.kline_finality_delay_s * 1_000))
    if end_exclusive % MINUTE or end_exclusive > int(time.time() * 1_000) - finality_ms:
        raise RecoveryError("recovery deadline must be closed and behind the configured finality window")

    status.enter("BUILD_OFFLINE_WRITER")
    persistence_settings = PersistenceSettings(migration_profile=MigrationProfile.CONTROLLED_RUNTIME.value)
    writer = OfflineDurableWriter(
        service_name=SOURCE,
        expected_database_name=expected_database_name,
        expected_schema_versions=CONTROLLED_RUNTIME_SCHEMA_VERSIONS,
        settings=persistence_settings,
    )
    try:
        status.enter("OFFLINE_WRITER_START")
        await writer.start()
        try:
            status.enter("VERIFY_RUNTIME_IDENTITY")
            await _verify_runtime_identity(writer)
            await _recover_with_lease(
                writer,
                settings,
                end_exclusive,
                maximum_bars,
                status,
                maximum_append_bars=maximum_append_bars,
            )
        except BaseException as exc:
            status.failed(exc)
            raise
    except BaseException as exc:
        if status.first_failure_phase is None:
            status.failed(exc)
        raise
    finally:
        status.enter("OFFLINE_WRITER_CLOSE")
        try:
            await writer.close()
        except BaseException as exc:
            previous_failure = status.first_failure_phase is not None
            status.failed(exc)
            if not previous_failure:
                raise


async def _recover_with_lease(
    writer: OfflineDurableWriter,
    settings: QuantSettings,
    end_exclusive: int,
    maximum_bars: int,
    status: RecoveryStatus,
    *,
    maximum_append_bars: int | None = None,
) -> None:
    anchors = [await load_anchor(writer, symbol, status=status) for symbol in SYMBOLS]
    status.enter("VALIDATE_BUDGET")
    if any(anchor.close_time_ms + 1 > end_exclusive for anchor in anchors):
        raise RecoveryError("recovery deadline precedes committed history")
    required = sum((end_exclusive - anchor.close_time_ms - 1) // MINUTE for anchor in anchors)
    status.planned_remaining_bars = required
    if required > maximum_bars:
        raise RecoveryError("recovery exceeds its explicit bar budget")
    status.record("PROGRESS", bars_required=required, end_exclusive_ms=end_exclusive)
    status.enter("REST_SESSION_OPEN")
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:

        async def fetch(symbol: str, start: int, end: int) -> object:
            async with session.get(
                settings.binance_rest_base + "/fapi/v1/klines",
                params={
                    "symbol": symbol,
                    "interval": "1m",
                    "startTime": start,
                    "endTime": end - 1,
                    "limit": (end - start) // MINUTE,
                },
            ) as response:
                response.raise_for_status()
                return await response.json()

        async def publish(event: Any) -> None:
            await writer.append(Topics.CLOSED_BAR, event)

        async def pause() -> None:
            await asyncio.sleep(max(1.0, settings.kline_finality_delay_s))

        append_limit = maximum_bars if maximum_append_bars is None else maximum_append_bars
        for anchor in anchors:
            available = append_limit - status.total_appended_bars
            if available <= 0:
                break
            symbol_end = min(end_exclusive, anchor.close_time_ms + 1 + available * MINUTE)
            await recover_symbol(
                anchor,
                end_exclusive=symbol_end,
                fetch=fetch,
                publish=publish,
                pause=pause,
                progress=status.emit,
                status=status,
            )
        status.enter("REST_SESSION_CLOSE")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--end-exclusive-ms", type=int, required=True)
    parser.add_argument("--maximum-bars", type=int, required=True)
    parser.add_argument("--expected-database-name", required=True)
    parser.add_argument("--maximum-append-bars", type=int)
    parser.add_argument("--offline-consumers-confirmed", action="store_true", required=True)
    args = parser.parse_args()
    try:
        asyncio.run(
            run_recovery(
                args.end_exclusive_ms,
                args.maximum_bars,
                expected_database_name=args.expected_database_name,
                maximum_append_bars=args.maximum_append_bars,
            )
        )
    except Exception:
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
