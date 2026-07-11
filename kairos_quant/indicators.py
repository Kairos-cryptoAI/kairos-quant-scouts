"""Technical indicators — Wilder's RSI and classic MACD.

Implemented on plain sequences (numpy under the hood) so they are easy to unit
test against known properties and reference values.
"""
from __future__ import annotations

from typing import Sequence, Tuple

import numpy as np


def ema(values: Sequence[float], period: int) -> np.ndarray:
    """Exponential moving average, seeded with the SMA of the first ``period``."""
    arr = np.asarray(values, dtype=float)
    if arr.size == 0:
        return arr
    alpha = 2.0 / (period + 1.0)
    out = np.empty_like(arr)
    if arr.size < period:
        out[:] = np.cumsum(arr) / (np.arange(arr.size) + 1)
        return out
    seed = arr[:period].mean()
    out[:period] = seed
    prev = seed
    for i in range(period, arr.size):
        prev = alpha * arr[i] + (1 - alpha) * prev
        out[i] = prev
    return out


def rsi(values: Sequence[float], period: int = 14) -> float:
    """Wilder's RSI over the last ``period`` deltas. Returns the latest value."""
    arr = np.asarray(values, dtype=float)
    if arr.size <= period:
        return 50.0
    deltas = np.diff(arr)
    gains = np.where(deltas > 0, deltas, 0.0)
    losses = np.where(deltas < 0, -deltas, 0.0)
    avg_gain = gains[:period].mean()
    avg_loss = losses[:period].mean()
    for i in range(period, deltas.size):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return float(100.0 - 100.0 / (1.0 + rs))


def macd(
    values: Sequence[float], fast: int = 12, slow: int = 26, signal: int = 9
) -> Tuple[float, float, float]:
    """Return the latest ``(macd, signal, histogram)`` triple."""
    arr = np.asarray(values, dtype=float)
    if arr.size < slow:
        return 0.0, 0.0, 0.0
    macd_line = ema(arr, fast) - ema(arr, slow)
    signal_line = ema(macd_line, signal)
    m = float(macd_line[-1])
    s = float(signal_line[-1])
    return m, s, m - s


def atr(highs: Sequence[float], lows: Sequence[float], closes: Sequence[float], period: int = 14) -> float:
    """Average True Range over ``period`` (absolute price units)."""
    h, low, c = (np.asarray(x, dtype=float) for x in (highs, lows, closes))
    n = min(h.size, low.size, c.size)
    if n < 2:
        return 0.0
    h, low, c = h[-n:], low[-n:], c[-n:]
    prev_close = c[:-1]
    tr = np.maximum.reduce([h[1:] - low[1:], np.abs(h[1:] - prev_close), np.abs(low[1:] - prev_close)])
    if tr.size < period:
        return float(tr.mean()) if tr.size else 0.0
    return float(tr[-period:].mean())
