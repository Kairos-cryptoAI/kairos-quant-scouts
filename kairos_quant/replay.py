from __future__ import annotations

from dataclasses import dataclass

from kairos_core.enums import Side

from .bias import derive_bias
from .candles import Candle
from .indicators import macd, rsi


@dataclass(frozen=True, slots=True)
class ReplayPoint:
    timestamp_ms: int
    bias: Side


@dataclass(frozen=True, slots=True)
class ReplayResult:
    points: tuple[ReplayPoint, ...]


def replay_candles(candles: list[Candle], warmup: int = 26) -> ReplayResult:
    ordered = sorted(candles, key=lambda candle: candle.open_time_ms)
    closes: list[float] = []
    points = []
    for candle in ordered:
        closes.append(candle.close)
        if len(closes) < warmup:
            side = Side.FLAT
        else:
            _, _, histogram = macd(closes)
            side = derive_bias(rsi_14=rsi(closes), macd_hist=histogram, ob_imbalance=0.0)
        points.append(ReplayPoint(candle.close_time_ms, side))
    return ReplayResult(tuple(points))
