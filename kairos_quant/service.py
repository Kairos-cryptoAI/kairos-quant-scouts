"""Quant Scouts service: collector -> SnapshotBuilder -> bus."""

from __future__ import annotations

import asyncio
import json
import math
import time
from collections.abc import Iterable, Mapping
from dataclasses import asdict
from typing import Any

import aiohttp
from kairos_core.bus import build_bus
from kairos_core.contracts import ClosedBarEventV1, VenueQualityV1, canonical_sha256
from kairos_core.logging import configure_logging, get_logger
from kairos_core.topics import Topics
from kairos_persistence import DurableMessageBus

from .collectors import BinanceFuturesCollector, ClosedKline
from .config import QuantSettings
from .runtime_venue_gate import (
    VenueGatePolicy,
    evedex_dev_symbol,
    fetch_venue_quality,
)
from .snapshot import SnapshotBuilder
from .venue_poll import (
    VENUE_POLL_TOPIC,
    VenuePollStatus,
    build_venue_poll_fact,
)

log = get_logger("quant-scouts")

_ONE_MINUTE_MS = 60_000


def _advance_fixed_rate_deadline(
    *,
    previous_deadline: float,
    interval_s: float,
    now: float,
) -> tuple[float, int]:
    """Advance a start-to-start schedule without latency drift or catch-up bursts."""

    next_deadline = previous_deadline + interval_s
    skipped = 0
    if next_deadline <= now:
        skipped = math.floor((now - next_deadline) / interval_s) + 1
        next_deadline += skipped * interval_s
    return next_deadline, skipped


class QuantScoutsService:
    def __init__(self, settings: QuantSettings | None = None) -> None:
        self.settings = settings or QuantSettings()
        transport = build_bus(self.settings)
        self.bus = (
            transport
            if self.settings.bus_backend == "memory"
            else DurableMessageBus(transport, service_name=self.settings.service_name)
        )
        self.builder = SnapshotBuilder(
            self.settings.service_name, self.settings.price_window, self.settings.depth_levels
        )
        self.collector = BinanceFuturesCollector(
            self.settings.symbols,
            self.settings.binance_ws_base,
            self.settings.binance_rest_base,
            reconnect_initial_s=self.settings.ws_reconnect_initial_s,
            reconnect_max_s=self.settings.ws_reconnect_max_s,
            kline_buffer_size=self.settings.price_window,
        )
        self._last_kline_close_time_ms: dict[str, int] = {}
        self.venue_gate_policy = VenueGatePolicy(
            assessed_notional_usd=self.settings.venue_quality_notional_usd,
            maximum_abs_basis_bps=self.settings.maximum_abs_basis_bps,
            maximum_spread_bps=self.settings.maximum_evedex_spread_bps,
            maximum_slippage_bps=self.settings.maximum_evedex_slippage_bps,
            maximum_book_age_ms=self.settings.maximum_venue_book_age_ms,
            maximum_timestamp_skew_ms=self.settings.maximum_venue_timestamp_skew_ms,
            maximum_latency_ms=self.settings.maximum_venue_latency_ms,
            measurement_ttl_ms=self.settings.venue_quality_ttl_ms,
            taker_fee_bps=self.settings.venue_taker_fee_bps,
        )
        self._venue_pairs = tuple(
            sorted(
                ((symbol.upper(), evedex_dev_symbol(symbol)) for symbol in self.settings.symbols),
                key=lambda pair: pair[1],
            )
        )
        self._venue_expected_symbols = tuple(pair[1] for pair in self._venue_pairs)
        self._venue_interval_ms = math.ceil(self.settings.venue_quality_interval_s * 1_000)
        self._venue_config_fingerprint = canonical_sha256(
            {
                "contract_version": "venue-poll-config.v1",
                "source": self.settings.service_name,
                "pairs": self._venue_pairs,
                "interval_ms": self._venue_interval_ms,
                "request_timeout_ms": math.ceil(self.settings.venue_request_timeout_s * 1_000),
                "binance_base_url": self.settings.binance_rest_base.rstrip("/"),
                "evedex_base_url": self.settings.evedex_dev_base_url.rstrip("/"),
                "policy": asdict(self.venue_gate_policy),
            }
        )
        self._venue_monotonic = time.monotonic
        self._venue_wall_clock_ms = lambda: int(time.time() * 1_000)

    def _restore_closed_bar_payloads(self, payloads: Iterable[object]) -> None:
        """Restore the producer's last immutable window without republishing it."""

        for raw_payload in payloads:
            payload: Any = raw_payload
            if isinstance(payload, str):
                payload = json.loads(payload)
            if not isinstance(payload, Mapping):
                raise TypeError("event_audit closed-bar payload must be a JSON object")
            event = ClosedBarEventV1.model_validate(dict(payload))
            if event.symbol.upper() not in {symbol.upper() for symbol in self.settings.symbols}:
                continue
            candle = ClosedKline(
                symbol=event.symbol,
                timeframe=event.timeframe,
                open_time_ms=event.open_time_ms,
                close_time_ms=event.close_time_ms,
                open=event.open,
                high=event.high,
                low=event.low,
                close=event.close,
                base_volume=event.base_volume,
                quote_volume=event.quote_volume,
                taker_buy_base_volume=event.taker_buy_base_volume,
                taker_buy_quote_volume=event.taker_buy_quote_volume,
            )
            if self.collector.restore_closed_kline(candle):
                self.builder.push_candle(
                    event.symbol.upper(), high=event.high, low=event.low, close=event.close
                )
                self._last_kline_close_time_ms[event.symbol.lower()] = event.close_time_ms

    async def _restore_closed_bars(self) -> None:
        if not isinstance(self.bus, DurableMessageBus):
            return
        await self.bus.start()
        if self.bus.repository is None:  # defensive: start() establishes it
            raise RuntimeError("durable quant bus has no audit repository")
        rows = await self.bus.repository.pool.fetch(
            """SELECT payload
                 FROM (
                     SELECT payload,
                            produced_at,
                            row_number() OVER (
                                PARTITION BY payload->>'symbol'
                                ORDER BY (payload->>'open_time_ms')::bigint DESC, produced_at DESC
                            ) AS row_number
                       FROM event_audit
                      WHERE topic=$1
                        AND source=$2
                        AND payload->>'venue'='BINANCE_UM'
                 ) AS ranked
                WHERE row_number <= $3
                ORDER BY payload->>'symbol',
                         (payload->>'open_time_ms')::bigint,
                         produced_at""",
            Topics.CLOSED_BAR,
            self.settings.service_name,
            self.settings.price_window,
        )
        self._restore_closed_bar_payloads(row["payload"] for row in rows)
        log.info(
            "closed_bar.producer_state_restored",
            symbols=len(self._last_kline_close_time_ms),
            bars=sum(len(values) for values in self.collector._known_klines.values()),
            blocked_symbols=sorted(
                symbol.upper()
                for symbol in self.collector.symbols
                if not self.collector.is_kline_stream_safe(symbol)
            ),
        )

    async def _emit_loop(self) -> None:
        while True:
            await asyncio.sleep(self.settings.snapshot_interval_s)
            await self._emit_once()

    async def _emit_once(self) -> None:
        for configured_symbol in self.settings.symbols:
            symbol = configured_symbol.upper()
            key = configured_symbol.lower()

            # A gap can be repaired by REST backfill, but a conflicting/reordered
            # final candle is an immutable-data violation.  In either case no
            # strategy consumer may advance until the collector is safe again.
            if not self.collector.is_kline_stream_safe(key):
                log.warning(
                    "closed_bar.blocked_integrity",
                    symbol=symbol,
                    reason=self.collector.kline_integrity_reason(key),
                )
                continue

            # Publish every complete bar before acknowledging the local queue.
            # Snapshot/book freshness is deliberately separate: strategy parity is
            # driven by final bars, while entry permission is decided later by the
            # EVEDEX venue-quality gate.
            for kline in self.collector.pending_closed_klines(key):
                event = ClosedBarEventV1(
                    source=self.settings.service_name,
                    symbol=kline.symbol,
                    timeframe=kline.timeframe,
                    open_time_ms=kline.open_time_ms,
                    close_time_ms=kline.close_time_ms,
                    open=kline.open,
                    high=kline.high,
                    low=kline.low,
                    close=kline.close,
                    base_volume=kline.base_volume,
                    quote_volume=kline.quote_volume,
                    taker_buy_base_volume=kline.taker_buy_base_volume,
                    taker_buy_quote_volume=kline.taker_buy_quote_volume,
                )
                await self.bus.publish(Topics.CLOSED_BAR, event)
                self.collector.acknowledge_closed_klines(key, 1)
                previous_close_time_ms = self._last_kline_close_time_ms.get(key)
                if previous_close_time_ms == kline.close_time_ms:
                    continue
                if (
                    previous_close_time_ms is not None
                    and kline.close_time_ms - previous_close_time_ms != _ONE_MINUTE_MS
                ):
                    self.builder.reset(symbol)
                self.builder.push_candle(symbol, high=kline.high, low=kline.low, close=kline.close)
                self._last_kline_close_time_ms[key] = kline.close_time_ms

            book = self.collector.books.get(key, {"bids": [], "asks": []})
            if (
                not book["bids"]
                or not book["asks"]
                or not self.collector.is_book_fresh(key, self.settings.book_stale_after_s)
                or not self.collector.is_kline_fresh(key, self.settings.kline_stale_after_s)
                or not self.collector.is_funding_fresh(key, self.settings.derivatives_stale_after_s)
                or not self.collector.is_open_interest_fresh(
                    key,
                    self.settings.derivatives_stale_after_s,
                )
            ):
                log.warning("snapshot.skipped_stale", symbol=symbol)
                continue

            liquidations = self.collector.liquidation_totals(key)
            snapshot = self.builder.build(
                symbol,
                bids=book["bids"],
                asks=book["asks"],
                funding_rate=self.collector.funding.get(key, 0.0),
                open_interest=self.collector.open_interest.get(key, 0.0),
                oi_change_pct_1h=self.collector.oi_change_pct_1h.get(key, 0.0),
                long_liq_usd=liquidations.long_usd,
                short_liq_usd=liquidations.short_usd,
                volume_usd=self.collector.volume_usd.get(key, 0.0),
            )
            await self.bus.publish(Topics.MARKET_SNAPSHOT, snapshot)
            self.collector.acknowledge_liquidations(key, liquidations)
            log.info(
                "snapshot",
                symbol=snapshot.symbol,
                bias=snapshot.quant_bias.value,
                rsi=round(snapshot.indicators.rsi_14, 1),
            )

    async def _emit_venue_quality_once(
        self,
        session: aiohttp.ClientSession,
        *,
        scheduled_at_ms: int | None = None,
    ) -> None:
        """Persist each attempt before concurrent public reads and its outcome after.

        A success is recorded only after the corresponding ``VenueQualityV1``
        event is durable. Missing attempts and missing terminal outcomes therefore
        reduce the 24-hour availability ratio instead of disappearing from it.
        """

        if scheduled_at_ms is None:
            now_ms = self._venue_wall_clock_ms()
            scheduled_at_ms = now_ms - (now_ms % self._venue_interval_ms)
        attempted_at_ms = max(scheduled_at_ms, self._venue_wall_clock_ms())
        attempts = tuple(
            build_venue_poll_fact(
                source=self.settings.service_name,
                config_fingerprint=self._venue_config_fingerprint,
                status=VenuePollStatus.ATTEMPTED,
                binance_symbol=binance_symbol,
                venue_symbol=evedex_symbol,
                expected_symbols=self._venue_expected_symbols,
                interval_ms=self._venue_interval_ms,
                scheduled_at_ms=scheduled_at_ms,
                attempted_at_ms=attempted_at_ms,
            )
            for binance_symbol, evedex_symbol in self._venue_pairs
        )
        await asyncio.gather(*(self.bus.publish(VENUE_POLL_TOPIC, fact) for fact in attempts))

        results = await asyncio.gather(
            *(
                fetch_venue_quality(
                    session,
                    binance_symbol=binance_symbol,
                    evedex_symbol=evedex_symbol,
                    binance_base_url=self.settings.binance_rest_base,
                    evedex_base_url=self.settings.evedex_dev_base_url,
                    policy=self.venue_gate_policy,
                    source=self.settings.service_name,
                )
                for binance_symbol, evedex_symbol in self._venue_pairs
            ),
            return_exceptions=True,
        )

        async def persist_outcome(
            binance_symbol: str,
            evedex_symbol: str,
            result: VenueQualityV1 | BaseException,
        ) -> None:
            completed_at_ms = max(attempted_at_ms, self._venue_wall_clock_ms())
            if isinstance(result, BaseException):
                failure = build_venue_poll_fact(
                    source=self.settings.service_name,
                    config_fingerprint=self._venue_config_fingerprint,
                    status=VenuePollStatus.FAILED,
                    binance_symbol=binance_symbol,
                    venue_symbol=evedex_symbol,
                    expected_symbols=self._venue_expected_symbols,
                    interval_ms=self._venue_interval_ms,
                    scheduled_at_ms=scheduled_at_ms,
                    attempted_at_ms=attempted_at_ms,
                    completed_at_ms=completed_at_ms,
                    failure_code=type(result).__name__,
                )
                await self.bus.publish(VENUE_POLL_TOPIC, failure)
                log.warning(
                    "venue_quality.unavailable",
                    binance_symbol=binance_symbol,
                    evedex_symbol=evedex_symbol,
                    error=type(result).__name__,
                )
                return
            await self.bus.publish(Topics.VENUE_QUALITY, result)
            success = build_venue_poll_fact(
                source=self.settings.service_name,
                config_fingerprint=self._venue_config_fingerprint,
                status=VenuePollStatus.SUCCEEDED,
                binance_symbol=binance_symbol,
                venue_symbol=evedex_symbol,
                expected_symbols=self._venue_expected_symbols,
                interval_ms=self._venue_interval_ms,
                scheduled_at_ms=scheduled_at_ms,
                attempted_at_ms=attempted_at_ms,
                completed_at_ms=completed_at_ms,
            )
            await self.bus.publish(VENUE_POLL_TOPIC, success)
            log.info(
                "venue_quality",
                symbol=evedex_symbol,
                entry_allowed=result.entry_allowed,
                reasons=list(result.reason_codes),
                basis_bps=round(result.basis_bps, 3),
                spread_bps=round(result.spread_bps, 3),
            )

        await asyncio.gather(
            *(
                persist_outcome(binance_symbol, evedex_symbol, result)
                for (binance_symbol, evedex_symbol), result in zip(
                    self._venue_pairs,
                    results,
                    strict=True,
                )
            )
        )

    async def _venue_quality_loop(self) -> None:
        timeout = aiohttp.ClientTimeout(total=self.settings.venue_request_timeout_s)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            next_deadline = self._venue_monotonic()
            now_ms = self._venue_wall_clock_ms()
            scheduled_at_ms = now_ms - (now_ms % self._venue_interval_ms)
            while True:
                delay = next_deadline - self._venue_monotonic()
                if delay > 0:
                    await asyncio.sleep(delay)
                try:
                    await self._emit_venue_quality_once(session, scheduled_at_ms=scheduled_at_ms)
                except asyncio.CancelledError:
                    raise
                except (TimeoutError, aiohttp.ClientError, ValueError):
                    log.exception("venue_quality.poll_failed")
                next_deadline, skipped = _advance_fixed_rate_deadline(
                    previous_deadline=next_deadline,
                    interval_s=self.settings.venue_quality_interval_s,
                    now=self._venue_monotonic(),
                )
                if skipped:
                    log.warning("venue_quality.poll_slots_skipped", count=skipped)
                scheduled_at_ms += (skipped + 1) * self._venue_interval_ms

    async def run(self) -> None:  # pragma: no cover - requires network
        configure_logging(
            self.settings.log_level, json_logs=self.settings.log_json, service=self.settings.service_name
        )
        await self._restore_closed_bars()
        log.info("quant.start", symbols=self.settings.symbols)
        try:
            async with asyncio.TaskGroup() as tasks:
                tasks.create_task(self.collector.run())
                tasks.create_task(
                    self.collector.run_open_interest_loop(self.settings.open_interest_interval_s)
                )
                tasks.create_task(
                    self.collector.run_kline_reconciliation_loop(
                        self.settings.kline_reconciliation_interval_s
                    ),
                    name="binance-kline-reconciliation",
                )
                tasks.create_task(self._emit_loop())
                if self.settings.enable_venue_quality_gate:
                    tasks.create_task(self._venue_quality_loop(), name="evedex-dev-venue-quality")
        finally:
            await self.bus.close()


def main() -> None:  # pragma: no cover
    asyncio.run(QuantScoutsService().run())


if __name__ == "__main__":
    main()
