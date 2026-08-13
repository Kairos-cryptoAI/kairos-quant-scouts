"""Technical indicators — Wilder's RSI and classic MACD.

Implemented on plain sequences (numpy under the hood) so they are easy to unit
test against known properties and reference values.
"""

from __future__ import annotations

from collections.abc import Sequence

import numpy as np

RSI_PERIOD = 14
MACD_FAST_PERIOD = 12
MACD_SLOW_PERIOD = 26
MACD_SIGNAL_PERIOD = 9
MACD_MIN_SAMPLES = MACD_SLOW_PERIOD + MACD_SIGNAL_PERIOD - 1
ATR_PERIOD = 14
ATR_MIN_SAMPLES = ATR_PERIOD + 1


def _finite_array(values: Sequence[float] | np.ndarray, *, name: str) -> np.ndarray:
    arr = np.asarray(values, dtype=float)
    if arr.ndim != 1:
        raise ValueError(f"{name} must be one-dimensional")
    if not np.all(np.isfinite(arr)):
        raise ValueError(f"{name} must contain only finite values")
    return arr


def _positive_period(period: int, *, name: str) -> None:
    if isinstance(period, bool) or not isinstance(period, (int, np.integer)) or period <= 0:
        raise ValueError(f"{name} must be a positive integer")


def _stable_mean(values: np.ndarray, *, name: str) -> float:
    scale = float(np.max(np.abs(values)))
    if scale == 0:
        return 0.0
    result = float(np.mean(values / scale) * scale)
    if not np.isfinite(result):
        raise ValueError(f"{name} arithmetic exceeds the finite numeric range")
    return result


def ema(values: Sequence[float] | np.ndarray, period: int) -> np.ndarray:
    """EMA seeded by the first full-period SMA; warm-up entries are ``NaN``.

    A leading ``NaN`` prefix is preserved so causal indicator series can be
    composed (for example, a MACD line feeding its signal EMA). Once finite
    observations start, every remaining value must be finite.
    """
    _positive_period(period, name="EMA period")
    arr = np.asarray(values, dtype=float)
    if arr.ndim != 1:
        raise ValueError("EMA values must be one-dimensional")
    if arr.size == 0:
        return arr.copy()
    if np.any(np.isinf(arr)):
        raise ValueError("EMA values must not contain infinite values")
    finite_indices = np.flatnonzero(np.isfinite(arr))
    if finite_indices.size == 0:
        return np.full_like(arr, np.nan)
    start = int(finite_indices[0])
    if not np.all(np.isfinite(arr[start:])):
        raise ValueError("EMA values must be finite after any leading NaN warm-up prefix")

    alpha = 2.0 / (period + 1.0)
    out = np.full_like(arr, np.nan)
    finite_values = arr[start:]
    if finite_values.size < period:
        return out
    seed = _stable_mean(finite_values[:period], name="EMA")
    seed_index = start + period - 1
    out[seed_index] = seed
    prev = seed
    for i in range(seed_index + 1, arr.size):
        prev = alpha * arr[i] + (1 - alpha) * prev
        out[i] = prev
    return out


def rsi(values: Sequence[float], period: int = RSI_PERIOD) -> float:
    """Wilder's RSI over the last ``period`` deltas. Returns the latest value."""
    _positive_period(period, name="RSI period")
    arr = _finite_array(values, name="RSI values")
    if arr.size <= period:
        return 50.0
    deltas = np.diff(arr)
    if not np.all(np.isfinite(deltas)):
        raise ValueError("RSI arithmetic exceeds the finite numeric range")
    gains = np.where(deltas > 0, deltas, 0.0)
    losses = np.where(deltas < 0, -deltas, 0.0)
    avg_gain = _stable_mean(gains[:period], name="RSI")
    avg_loss = _stable_mean(losses[:period], name="RSI")
    for i in range(period, deltas.size):
        avg_gain += (gains[i] - avg_gain) / period
        avg_loss += (losses[i] - avg_loss) / period
    if avg_gain == 0 and avg_loss == 0:
        return 50.0
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return float(100.0 - 100.0 / (1.0 + rs))


def macd(
    values: Sequence[float],
    fast: int = MACD_FAST_PERIOD,
    slow: int = MACD_SLOW_PERIOD,
    signal: int = MACD_SIGNAL_PERIOD,
) -> tuple[float, float, float]:
    """Return the latest fully warmed ``(MACD, signal, histogram)`` triple."""
    for period, name in ((fast, "fast"), (slow, "slow"), (signal, "signal")):
        _positive_period(period, name=f"MACD {name} period")
    if fast >= slow:
        raise ValueError("MACD fast period must be smaller than slow period")

    arr = _finite_array(values, name="MACD values")
    if arr.size < slow + signal - 1:
        return 0.0, 0.0, 0.0
    macd_line = ema(arr, fast) - ema(arr, slow)
    valid_macd = macd_line[slow - 1 :]
    if not np.all(np.isfinite(valid_macd)):
        raise ValueError("MACD arithmetic exceeds the finite numeric range")
    signal_line = ema(valid_macd, signal)
    m = float(macd_line[-1])
    s = float(signal_line[-1])
    return m, s, m - s


def atr(
    highs: Sequence[float],
    lows: Sequence[float],
    closes: Sequence[float],
    period: int = ATR_PERIOD,
) -> float:
    """Latest Wilder-smoothed ATR in absolute price units."""
    _positive_period(period, name="ATR period")
    h = _finite_array(highs, name="ATR highs")
    low = _finite_array(lows, name="ATR lows")
    c = _finite_array(closes, name="ATR closes")
    if not (h.size == low.size == c.size):
        raise ValueError("ATR inputs must have equal lengths")
    if (
        np.any(h <= 0)
        or np.any(low <= 0)
        or np.any(c <= 0)
        or np.any(h < low)
        or np.any(c > h)
        or np.any(c < low)
    ):
        raise ValueError("ATR inputs must contain valid positive price ranges")
    if h.size < period + 1:
        return 0.0
    prev_close = c[:-1]
    tr = np.maximum.reduce([h[1:] - low[1:], np.abs(h[1:] - prev_close), np.abs(low[1:] - prev_close)])
    value = _stable_mean(tr[:period], name="ATR")
    for current_range in tr[period:]:
        value += (float(current_range) - value) / period
    return value
