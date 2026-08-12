"""Quant Scouts configuration."""

from __future__ import annotations

from kairos_core.config import CoreSettings


class QuantSettings(CoreSettings):
    service_name: str = "kairos-quant-scouts"

    @property
    def symbols(self) -> list[str]:
        """Compatibility alias; the universe is owned by CoreSettings."""
        return self.trading_symbols

    snapshot_interval_s: float = 60.0
    depth_levels: int = 10
    price_window: int = 200
    open_interest_interval_s: float = 60.0
    book_stale_after_s: float = 10.0
    kline_stale_after_s: float = 90.0
    ws_reconnect_initial_s: float = 1.0
    ws_reconnect_max_s: float = 30.0

    # Binance USD-M Futures endpoints (used for tests / dev; EVEDEX feed lives in the execution repo).
    binance_ws_base: str = "wss://fstream.binance.com/stream"
    binance_rest_base: str = "https://fapi.binance.com"
