"""Quant Scouts configuration."""

from __future__ import annotations

from typing import Self

from kairos_core.config import CoreSettings
from pydantic import Field, model_validator

from .indicators import MACD_MIN_SAMPLES


class QuantSettings(CoreSettings):
    service_name: str = "kairos-quant-scouts"

    @property
    def symbols(self) -> list[str]:
        """Compatibility alias; the universe is owned by CoreSettings."""
        return self.trading_symbols

    snapshot_interval_s: float = Field(default=60.0, gt=0)
    depth_levels: int = Field(default=10, gt=0)
    price_window: int = Field(default=200, ge=MACD_MIN_SAMPLES)
    open_interest_interval_s: float = Field(default=60.0, gt=0)
    book_stale_after_s: float = Field(default=10.0, gt=0)
    kline_stale_after_s: float = Field(default=90.0, gt=0)
    derivatives_stale_after_s: float = Field(default=180.0, gt=0)
    ws_reconnect_initial_s: float = Field(default=1.0, gt=0)
    ws_reconnect_max_s: float = Field(default=30.0, gt=0)

    @model_validator(mode="after")
    def _validate_reconnect_window(self) -> Self:
        if self.ws_reconnect_max_s < self.ws_reconnect_initial_s:
            raise ValueError("maximum reconnect delay cannot be below its initial delay")
        return self

    # Binance USD-M Futures endpoints (used for tests / dev; EVEDEX feed lives in the execution repo).
    binance_ws_base: str = "wss://fstream.binance.com/stream"
    binance_rest_base: str = "https://fapi.binance.com"
