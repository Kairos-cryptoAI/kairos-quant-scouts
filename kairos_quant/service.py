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


class QuantScoutsService:
    def __init__(self, settings: QuantSettings | None = None) -> None:
        self.settings = settings or QuantSettings()
        self.bus = build_bus(self.settings)
        self.builder = SnapshotBuilder(self.settings.service_name, self.settings.price_window,
                                       self.settings.depth_levels)
        self.collector = BinanceFuturesCollector(
            self.settings.symbols, self.settings.binance_ws_base, self.settings.binance_rest_base
        )

    async def _emit_loop(self) -> None:
        while True:
            await asyncio.sleep(self.settings.snapshot_interval_s)
            for sym in self.settings.symbols:
                book = self.collector.books.get(sym.lower(), {"bids": [], "asks": []})
                # Binance depth updates can temporarily expose only one side while
                # reconnecting/resynchronising. Never index or publish an incomplete book.
                if not book["bids"] or not book["asks"]:
                    continue
                mid = (book["bids"][0][0] + book["asks"][0][0]) / 2.0
                self.builder.push_close(sym.upper(), mid)
                snap = self.builder.build(
                    sym.upper(), bids=book["bids"], asks=book["asks"],
                    funding_rate=self.collector.funding.get(sym.lower(), 0.0),
                    open_interest=self.collector.open_interest.get(sym.lower(), 0.0),
                )
                await self.bus.publish(Topics.MARKET_SNAPSHOT, snap)
                log.info("snapshot", symbol=snap.symbol, bias=snap.quant_bias.value,
                        rsi=round(snap.indicators.rsi_14, 1))

    async def run(self) -> None:  # pragma: no cover - requires network
        configure_logging(self.settings.log_level, json_logs=self.settings.log_json,
                          service=self.settings.service_name)
        log.info("quant.start", symbols=self.settings.symbols)
        await asyncio.gather(self.collector.run(), self._emit_loop())


def main() -> None:  # pragma: no cover
    asyncio.run(QuantScoutsService().run())


if __name__ == "__main__":
    main()
