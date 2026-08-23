"""Quant Scouts service: collector -> SnapshotBuilder -> bus."""

from __future__ import annotations

import asyncio

import aiohttp
from kairos_core.bus import build_bus
from kairos_core.contracts import ClosedBarEventV1
from kairos_core.logging import configure_logging, get_logger
from kairos_core.topics import Topics
from kairos_persistence import DurableMessageBus

from .collectors import BinanceFuturesCollector
from .config import QuantSettings
from .runtime_venue_gate import (
    VenueGatePolicy,
    evedex_dev_symbol,
    fetch_venue_quality,
)
from .snapshot import SnapshotBuilder

log = get_logger("quant-scouts")

_ONE_MINUTE_MS = 60_000


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

    async def _emit_venue_quality_once(self, session: aiohttp.ClientSession) -> None:
        pairs = [(symbol.upper(), evedex_dev_symbol(symbol)) for symbol in self.settings.symbols]
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
                for binance_symbol, evedex_symbol in pairs
            ),
            return_exceptions=True,
        )
        for (binance_symbol, evedex_symbol), result in zip(pairs, results, strict=True):
            if isinstance(result, BaseException):
                log.warning(
                    "venue_quality.unavailable",
                    binance_symbol=binance_symbol,
                    evedex_symbol=evedex_symbol,
                    error=type(result).__name__,
                )
                continue
            await self.bus.publish(Topics.VENUE_QUALITY, result)
            log.info(
                "venue_quality",
                symbol=evedex_symbol,
                entry_allowed=result.entry_allowed,
                reasons=list(result.reason_codes),
                basis_bps=round(result.basis_bps, 3),
                spread_bps=round(result.spread_bps, 3),
            )

    async def _venue_quality_loop(self) -> None:
        timeout = aiohttp.ClientTimeout(total=self.settings.venue_request_timeout_s)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            while True:
                try:
                    await self._emit_venue_quality_once(session)
                except asyncio.CancelledError:
                    raise
                except (TimeoutError, aiohttp.ClientError, ValueError):
                    log.exception("venue_quality.poll_failed")
                await asyncio.sleep(self.settings.venue_quality_interval_s)

    async def run(self) -> None:  # pragma: no cover - requires network
        configure_logging(
            self.settings.log_level, json_logs=self.settings.log_json, service=self.settings.service_name
        )
        log.info("quant.start", symbols=self.settings.symbols)
        async with asyncio.TaskGroup() as tasks:
            tasks.create_task(self.collector.run())
            tasks.create_task(self.collector.run_open_interest_loop(self.settings.open_interest_interval_s))
            tasks.create_task(self._emit_loop())
            if self.settings.enable_venue_quality_gate:
                tasks.create_task(self._venue_quality_loop(), name="evedex-dev-venue-quality")


def main() -> None:  # pragma: no cover
    asyncio.run(QuantScoutsService().run())


if __name__ == "__main__":
    main()
