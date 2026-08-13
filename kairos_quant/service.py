"""Quant Scouts service: collector -> SnapshotBuilder -> bus."""

from __future__ import annotations

import asyncio

from kairos_core.bus import build_bus
from kairos_core.logging import configure_logging, get_logger
from kairos_core.topics import Topics

from .collectors import BinanceFuturesCollector
from .config import QuantSettings
from .snapshot import SnapshotBuilder

log = get_logger("quant-scouts")

_ONE_MINUTE_MS = 60_000


class QuantScoutsService:
    def __init__(self, settings: QuantSettings | None = None) -> None:
        self.settings = settings or QuantSettings()
        self.bus = build_bus(self.settings)
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

    async def _emit_loop(self) -> None:
        while True:
            await asyncio.sleep(self.settings.snapshot_interval_s)
            await self._emit_once()

    async def _emit_once(self) -> None:
        for configured_symbol in self.settings.symbols:
            symbol = configured_symbol.upper()
            key = configured_symbol.lower()

            # Drain every closed candle exactly once, even if the book is temporarily
            # unavailable. Live mid-prices must never enter the indicator history.
            for kline in self.collector.drain_closed_klines(key):
                previous_close_time_ms = self._last_kline_close_time_ms.get(key)
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

    async def run(self) -> None:  # pragma: no cover - requires network
        configure_logging(
            self.settings.log_level, json_logs=self.settings.log_json, service=self.settings.service_name
        )
        log.info("quant.start", symbols=self.settings.symbols)
        async with asyncio.TaskGroup() as tasks:
            tasks.create_task(self.collector.run())
            tasks.create_task(self.collector.run_open_interest_loop(self.settings.open_interest_interval_s))
            tasks.create_task(self._emit_loop())


def main() -> None:  # pragma: no cover
    asyncio.run(QuantScoutsService().run())


if __name__ == "__main__":
    main()
