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

    snapshot_interval_s: float = Field(default=5.0, gt=0)
    depth_levels: int = Field(default=10, gt=0)
    price_window: int = Field(default=200, ge=MACD_MIN_SAMPLES)
    open_interest_interval_s: float = Field(default=60.0, gt=0)
    funding_interval_s: float = Field(default=30.0, gt=0)
    kline_reconciliation_interval_s: float = Field(default=5.0, gt=0)
    kline_finality_delay_s: float = Field(default=5.0, ge=0)
    book_stale_after_s: float = Field(default=10.0, gt=0)
    kline_stale_after_s: float = Field(default=90.0, gt=0)
    derivatives_stale_after_s: float = Field(default=180.0, gt=0)
    maximum_binance_future_skew_ms: int = Field(default=2_000, ge=0)
    ws_reconnect_initial_s: float = Field(default=1.0, gt=0)
    ws_reconnect_max_s: float = Field(default=30.0, gt=0)

    # Public read-only EVEDEX DEV quality monitor.  PAPER deployment enables
    # this explicitly; ordinary unit tests and legacy DRY_RUN remain network-free.
    enable_venue_quality_gate: bool = False
    venue_quality_interval_s: float = Field(default=30.0, gt=0)
    venue_quality_notional_usd: float = Field(default=1_000.0, gt=0)
    venue_quality_ttl_ms: int = Field(default=5_000, gt=0)
    venue_taker_fee_bps: float = Field(default=5.0, ge=0)
    maximum_abs_basis_bps: float = Field(default=25.0, gt=0)
    maximum_evedex_spread_bps: float = Field(default=25.0, gt=0)
    maximum_evedex_slippage_bps: float = Field(default=25.0, gt=0)
    maximum_venue_book_age_ms: int = Field(default=5_000, gt=0)
    maximum_venue_timestamp_skew_ms: int = Field(default=2_000, gt=0)
    maximum_venue_latency_ms: int = Field(default=5_000, gt=0)
    venue_request_timeout_s: float = Field(default=10.0, gt=0)
    evedex_dev_base_url: str = "https://trading-api.evedex.tech"

    @model_validator(mode="after")
    def _validate_reconnect_window(self) -> Self:
        if self.ws_reconnect_max_s < self.ws_reconnect_initial_s:
            raise ValueError("maximum reconnect delay cannot be below its initial delay")
        if self.enable_venue_quality_gate and self.evedex_dev_base_url != ("https://trading-api.evedex.tech"):
            raise ValueError("runtime venue gate requires the exact official EVEDEX DEV URL")
        return self

    # Binance USD-M Futures endpoints (used for tests / dev; EVEDEX feed lives in the execution repo).
    binance_ws_base: str = "wss://fstream.binance.com/stream"
    binance_rest_base: str = "https://fapi.binance.com"
