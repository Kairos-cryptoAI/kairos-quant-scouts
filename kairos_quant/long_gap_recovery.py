"""Explicit, offline PAPER bar repair through an isolated durable writer.

Run only with strategy/risk/execution consumers stopped. No venue mutation or
paid API exists here. Restart resumes from committed event_audit, not RAM.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import aiohttp
from kairos_core.contracts import ClosedBarEventV1
from kairos_core.topics import Topics
from kairos_persistence import OfflineDurableWriter

from .config import QuantSettings

MINUTE = 60_000
SOURCE = "kairos-quant-scouts"
SYMBOLS = ("BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT")
RECOVERY_SCHEMA_VERSIONS = (
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
)
FetchPage = Callable[[str, int, int], Awaitable[object]]
Publish = Callable[[ClosedBarEventV1], Awaitable[None]]


class RecoveryError(ValueError):
    """Safe operational reason that contains no payload or secret values."""


@dataclass
class RecoveryStatus:
    """Operational state only; never store REST bodies, DSNs or exception text."""

    emit: Callable[[dict[str, Any]], None]
    phase: str = "VALIDATE_SETTINGS"
    symbol: str | None = None
    window_start_ms: int | None = None
    window_end_exclusive_ms: int | None = None
    last_progress_at_utc: str | None = None
    confirmed_through_exclusive_ms: dict[str, int] = field(default_factory=dict)
    total_appended_bars: int = 0
    first_failure_phase: str | None = None
    end_exclusive_ms: int | None = None
    maximum_append_bars: int | None = None
    planned_remaining_bars: int | None = None
    publish_outcome_unknown: bool = False

    @property
    def remaining_bars(self) -> int | None:
        if self.planned_remaining_bars is None or self.publish_outcome_unknown:
            return None
        return self.planned_remaining_bars - self.total_appended_bars

    def enter(
        self, phase: str, *, symbol: str | None = None, start: int | None = None, end: int | None = None
    ) -> None:
        self.phase, self.symbol = phase, symbol
        self.window_start_ms, self.window_end_exclusive_ms = start, end

    def confirmed(self, event: ClosedBarEventV1, *, appended: bool = False) -> None:
        self.confirmed_through_exclusive_ms[event.symbol] = event.close_time_ms + 1
        self.last_progress_at_utc = datetime.now(UTC).isoformat()
        if appended:
            self.total_appended_bars += 1

    def record(self, state: str, **details: Any) -> None:
        self.emit(
            {
                "state": state,
                "observed_at_utc": datetime.now(UTC).isoformat(),
                "phase": self.phase,
                "symbol": self.symbol,
                "window_start_ms": self.window_start_ms,
                "window_end_exclusive_ms": self.window_end_exclusive_ms,
                "last_progress_at_utc": self.last_progress_at_utc,
                "confirmed_through_exclusive_ms": dict(self.confirmed_through_exclusive_ms),
                "total_appended_bars": self.total_appended_bars,
                "actual_appended_bars": None if self.publish_outcome_unknown else self.total_appended_bars,
                "end_exclusive_ms": self.end_exclusive_ms,
                "maximum_append_bars": self.maximum_append_bars,
                "planned_remaining_bars": self.planned_remaining_bars,
                "remaining_bars": self.remaining_bars,
                **details,
            }
        )

    def failed(self, exc: BaseException) -> None:
        self.first_failure_phase = self.first_failure_phase or self.phase
        self.publish_outcome_unknown = self.publish_outcome_unknown or self.phase == "DURABLE_PUBLISH"
        self.record(
            "FAILED",
            error_type=type(exc).__name__,
            reason=str(exc) if isinstance(exc, RecoveryError) else "operation failed; payload withheld",
            first_failure_phase=self.first_failure_phase,
            publish_outcome="UNKNOWN" if self.phase == "DURABLE_PUBLISH" else "NOT_ATTEMPTED_IN_PHASE",
        )


def parse_page(raw: object, *, symbol: str, start: int, end: int) -> tuple[ClosedBarEventV1, ...]:
    if not isinstance(raw, list) or len(raw) != (end - start) // MINUTE:
        raise RecoveryError("recovery REST page is incomplete")
    result = []
    for index, row in enumerate(raw):
        if not isinstance(row, list) or len(row) < 11:
            raise RecoveryError("recovery REST row is malformed")
        if type(row[0]) is not int or type(row[6]) is not int or row[0] != start + index * MINUTE:
            raise RecoveryError("recovery bars are duplicate, reordered or gapped")
        numeric = (row[1], row[2], row[3], row[4], row[5], row[7], row[9], row[10])
        if any(isinstance(value, bool) for value in numeric):
            raise RecoveryError("boolean bar values are invalid")
        result.append(
            ClosedBarEventV1(
                source=SOURCE,
                symbol=symbol,
                open_time_ms=row[0],
                close_time_ms=row[6],
                open=float(row[1]),
                high=float(row[2]),
                low=float(row[3]),
                close=float(row[4]),
                base_volume=float(row[5]),
                quote_volume=float(row[7]),
                taker_buy_base_volume=float(row[9]),
                taker_buy_quote_volume=float(row[10]),
            )
        )
    return tuple(result)


async def recover_symbol(
    anchor: ClosedBarEventV1,
    *,
    end_exclusive: int,
    fetch: FetchPage,
    publish: Publish,
    page_size: int = 200,
    pause: Callable[[], Awaitable[None]],
    progress: Callable[[dict[str, Any]], None],
    status: RecoveryStatus | None = None,
) -> int:
    status = status or RecoveryStatus(progress)
    status.enter("VALIDATE_INTERVAL", symbol=anchor.symbol, start=anchor.open_time_ms, end=end_exclusive)
    if not 2 <= page_size <= 200 or end_exclusive % MINUTE or end_exclusive <= anchor.open_time_ms:
        raise RecoveryError("invalid bounded recovery interval")
    appended = 0
    current = anchor
    status.confirmed(anchor)
    while current.close_time_ms + 1 < end_exclusive:
        start = current.open_time_ms
        end = min(end_exclusive, start + page_size * MINUTE)
        status.enter("REST_GET_FIRST", symbol=current.symbol, start=start, end=end)
        raw = await fetch(current.symbol, start, end)
        status.enter("REST_VALIDATE_FIRST", symbol=current.symbol, start=start, end=end)
        first = parse_page(raw, symbol=current.symbol, start=start, end=end)
        status.enter("CONFIRMATION_DELAY", symbol=current.symbol, start=start, end=end)
        await pause()
        status.enter("REST_GET_SECOND", symbol=current.symbol, start=start, end=end)
        raw = await fetch(current.symbol, start, end)
        status.enter("REST_VALIDATE_SECOND", symbol=current.symbol, start=start, end=end)
        second = parse_page(raw, symbol=current.symbol, start=start, end=end)
        status.enter("CONFIRMATION_COMPARE", symbol=current.symbol, start=start, end=end)
        if first != second or first[0].canonical_bar_bytes() != current.canonical_bar_bytes():
            raise RecoveryError("recovery REST confirmation or persisted anchor conflict")
        # Validate the entire page before publishing any row. Every publish
        # atomically inserts event_audit and outbox; no ACK/cursor can outrun it.
        for event in first[1:]:
            status.enter(
                "DURABLE_PUBLISH", symbol=event.symbol, start=event.open_time_ms, end=event.close_time_ms + 1
            )
            await publish(event)
            current = event
            appended += 1
            status.confirmed(event, appended=True)
        status.enter("PAGE_COMMITTED", symbol=current.symbol, start=start, end=end)
        status.record(
            "PROGRESS",
            through_exclusive_ms=current.close_time_ms + 1,
            appended_bars=appended,
            retrieved_at_utc=datetime.now(UTC).isoformat(),
        )
    return appended


async def load_anchor(
    writer: OfflineDurableWriter, symbol: str, *, status: RecoveryStatus | None = None
) -> ClosedBarEventV1:
    if status is not None:
        status.enter("RESTORE_READ", symbol=symbol)
    if writer.repository is None:
        raise RuntimeError("recovery repository unavailable")
    # Validate the persisted timeline without reading strategy/trade performance.
    rows = await writer.repository.pool.fetch(
        """SELECT payload FROM event_audit WHERE topic=$1 AND source=$2
             AND payload->>'symbol'=$3 AND payload->>'venue'='BINANCE_UM'
             ORDER BY (payload->>'open_time_ms')::bigint""",
        Topics.CLOSED_BAR,
        SOURCE,
        symbol,
    )
    previous = None
    if status is not None:
        status.enter("RESTORE_VALIDATE", symbol=symbol)
    for record in rows:
        raw = record["payload"]
        event = ClosedBarEventV1.model_validate(json.loads(raw) if isinstance(raw, str) else raw)
        if previous is not None and event.open_time_ms != previous.close_time_ms + 1:
            raise RecoveryError("persisted producer history is not a unique contiguous prefix")
        previous = event
    if previous is None:
        raise RecoveryError("recovery requires an existing authoritative anchor for every symbol")
    if status is not None:
        status.confirmed(previous)
    return previous


async def run_recovery(
    end_exclusive: int,
    maximum_bars: int,
    *,
    expected_database_name: str,
    maximum_append_bars: int | None = None,
    status: RecoveryStatus | None = None,
) -> None:
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
        or not 1 <= maximum_append_bars <= 150_000
        or type(maximum_bars) is not int
        or maximum_append_bars > maximum_bars
    ):
        raise RecoveryError("recovery append limit must be an integer from 1 to the explicit bar budget")
    if not isinstance(expected_database_name, str) or not expected_database_name.strip():
        raise RecoveryError("recovery requires a non-empty expected database name")
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
    if (
        end_exclusive % MINUTE
        or end_exclusive > int(time.time() * 1_000) - finality_ms
        or not 1 <= maximum_bars <= 150_000
    ):
        raise RecoveryError("recovery deadline must be closed and budget at most 150000 bars")
    status.enter("BUILD_OFFLINE_WRITER")
    writer = OfflineDurableWriter(
        service_name=SOURCE,
        expected_database_name=expected_database_name,
        expected_schema_versions=RECOVERY_SCHEMA_VERSIONS,
    )
    try:
        status.enter("OFFLINE_WRITER_START")
        await writer.start()
        try:
            await _recover_with_lease(
                writer,
                settings,
                end_exclusive,
                maximum_bars,
                status,
                maximum_append_bars=maximum_append_bars,
            )
        except BaseException as exc:
            # Capture the failing operation before writer cleanup can change
            # phase or itself fail. No append is ever retried in this run.
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

        async def publish(event: ClosedBarEventV1) -> None:
            await writer.append(Topics.CLOSED_BAR, event)

        async def pause() -> None:
            await asyncio.sleep(max(1.0, settings.kline_finality_delay_s))

        append_limit = maximum_bars if maximum_append_bars is None else maximum_append_bars
        for anchor in anchors:
            available = append_limit - status.total_appended_bars
            if available <= 0:
                break
            # Bound the requested interval before fetching. recover_symbol still
            # validates both complete pages, including their authoritative anchor;
            # never fetch a larger page and validate/publish only a chosen prefix.
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
        # run_recovery already emitted a sanitized failure with exact phase.
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
