from __future__ import annotations

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
        if self.close_time_ms <= self.open_time_ms:
            raise ValueError("candle close must follow open")
        if min(self.open, self.high, self.low, self.close) <= 0:
            raise ValueError("OHLC prices must be positive")
        if self.high < max(self.open, self.close) or self.low > min(self.open, self.close):
            raise ValueError("invalid OHLC bounds")
        if self.volume < 0 or self.quote_volume < 0:
            raise ValueError("volumes cannot be negative")
