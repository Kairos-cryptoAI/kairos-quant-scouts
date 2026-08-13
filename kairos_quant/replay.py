from __future__ import annotations

from dataclasses import dataclass

from kairos_core.enums import Side

from .bias import derive_bias
from .candles import Candle
from .indicators import MACD_MIN_SAMPLES, macd, rsi


@dataclass(frozen=True, slots=True)
class ReplayPoint:
    timestamp_ms: int
    bias: Side


@dataclass(frozen=True, slots=True)
class ReplayResult:
    points: tuple[ReplayPoint, ...]


def replay_candles(candles: list[Candle], warmup: int = MACD_MIN_SAMPLES) -> ReplayResult:
    if isinstance(warmup, bool) or not isinstance(warmup, int) or warmup <= 0:
        raise ValueError("warmup must be a positive integer")
    ordered = sorted(candles, key=lambda candle: candle.open_time_ms)
    if ordered:
        symbol = ordered[0].symbol
        timeframe = ordered[0].timeframe
        for previous, candle in zip(ordered, ordered[1:], strict=False):
            if candle.symbol != symbol or candle.timeframe != timeframe:
                raise ValueError("a replay must contain one symbol and timeframe")
            if candle.open_time_ms != previous.close_time_ms + 1:
                raise ValueError("replay candles must be unique and contiguous")

    closes: list[float] = []
    points: list[ReplayPoint] = []
    required_samples = max(warmup, MACD_MIN_SAMPLES)
    for candle in ordered:
        closes.append(candle.close)
        if len(closes) < required_samples:
            side = Side.FLAT
        else:
            _, _, histogram = macd(closes)
            side = derive_bias(rsi_14=rsi(closes), macd_hist=histogram, ob_imbalance=0.0)
        points.append(ReplayPoint(candle.close_time_ms, side))
    return ReplayResult(tuple(points))
