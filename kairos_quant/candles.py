from __future__ import annotations

import math
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Candle:
    symbol: str
    timeframe: str
    open_time_ms: int
    close_time_ms: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    quote_volume: float = 0.0
    taker_buy_volume: float = 0.0

    def __post_init__(self) -> None:
        if not self.symbol.strip() or not self.timeframe.strip():
            raise ValueError("candle symbol and timeframe are required")
        if (
            isinstance(self.open_time_ms, bool)
            or isinstance(self.close_time_ms, bool)
            or not isinstance(self.open_time_ms, int)
            or not isinstance(self.close_time_ms, int)
            or self.open_time_ms < 0
            or self.close_time_ms <= self.open_time_ms
        ):
            raise ValueError("candle timestamps must be non-negative ordered integers")
        prices = (self.open, self.high, self.low, self.close)
        volumes = (self.volume, self.quote_volume, self.taker_buy_volume)
        if not all(math.isfinite(value) for value in (*prices, *volumes)):
            raise ValueError("candle values must be finite")
        if min(prices) <= 0:
            raise ValueError("OHLC prices must be positive")
        if self.high < max(self.open, self.close) or self.low > min(self.open, self.close):
            raise ValueError("invalid OHLC bounds")
        if min(volumes) < 0:
            raise ValueError("volumes cannot be negative")
        if self.taker_buy_volume > self.volume:
            raise ValueError("taker-buy volume cannot exceed total volume")
