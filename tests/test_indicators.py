import numpy as np
from kairos_quant.indicators import ema, rsi, macd, atr


def test_rsi_bounds_and_extremes():
    up = list(range(1, 60))           # strictly increasing -> RSI ~ 100
    down = list(range(60, 1, -1))     # strictly decreasing -> RSI ~ 0
    assert rsi(up, 14) > 99.0
    assert rsi(down, 14) < 1.0
    assert 0.0 <= rsi([1, 2, 1, 2, 1, 2] * 5, 14) <= 100.0


def test_rsi_short_series_neutral():
    assert rsi([1, 2, 3], 14) == 50.0


def test_macd_sign_follows_trend():
    up = list(np.linspace(100, 200, 80))
    m, s, hist = macd(up)
    assert m > 0  # fast EMA above slow EMA in an uptrend


def test_ema_converges_to_constant():
    out = ema([5.0] * 50, 10)
    assert abs(out[-1] - 5.0) < 1e-9


def test_atr_non_negative():
    highs = [10, 11, 12, 11, 13]
    lows = [9, 9, 10, 10, 11]
    closes = [9.5, 10.5, 11.5, 10.5, 12]
    assert atr(highs, lows, closes, 3) >= 0
