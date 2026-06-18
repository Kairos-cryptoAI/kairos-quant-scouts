"""Quant Scouts configuration."""
from __future__ import annotations

from typing import List

from kairos_core.config import CoreSettings


class QuantSettings(CoreSettings):
    service_name: str = "kairos-quant-scouts"
    symbols: List[str] = ["BTCUSDT", "ETHUSDT"]
    snapshot_interval_s: float = 60.0
    depth_levels: int = 10
    price_window: int = 200

    # Binance USD-M Futures endpoints (used for tests / dev; EVEDEX feed lives in the execution repo).
    binance_ws_base: str = "wss://fstream.binance.com/stream"
    binance_rest_base: str = "https://fapi.binance.com"
