import numpy as np
import pytest

from kairos_quant.indicators import MACD_MIN_SAMPLES, atr, ema, macd, rsi


def test_rsi_bounds_and_extremes():
    up = list(range(1, 60))  # strictly increasing -> RSI ~ 100
    down = list(range(60, 1, -1))  # strictly decreasing -> RSI ~ 0
    assert rsi(up, 14) > 99.0
    assert rsi(down, 14) < 1.0
    assert 0.0 <= rsi([1, 2, 1, 2, 1, 2] * 5, 14) <= 100.0


def test_rsi_short_series_neutral():
    assert rsi([1, 2, 3], 14) == 50.0


def test_rsi_flat_series_is_neutral():
    assert rsi([42.0] * 50, 14) == 50.0


def test_rsi_matches_wilder_reference_seed():
    closes = [
        44.34,
        44.09,
        44.15,
        43.61,
        44.33,
        44.83,
        45.10,
        45.42,
        45.84,
        46.08,
        45.89,
        46.03,
        45.61,
        46.28,
        46.28,
    ]

    assert rsi(closes, 14) == pytest.approx(70.464135, rel=1e-6)


def test_macd_sign_follows_trend():
    up = list(np.linspace(100, 200, 80))
    m, s, hist = macd(up)
    assert m > 0  # fast EMA above slow EMA in an uptrend


def test_ema_converges_to_constant():
    out = ema([5.0] * 50, 10)
    assert abs(out[-1] - 5.0) < 1e-9


def test_indicators_do_not_overflow_on_large_finite_constant_prices():
    values = [1e308] * 50

    assert ema(values, 12)[-1] == pytest.approx(1e308)
    assert rsi(values) == 50.0
    assert macd(values) == pytest.approx((0.0, 0.0, 0.0))


def test_ema_exposes_undefined_warmup_instead_of_backfilling_future_seed():
    out = ema([1.0, 2.0, 3.0, 4.0], 3)

    assert np.isnan(out[:2]).all()
    assert out[2] == 2.0
    assert out[3] == 3.0


def test_ema_preserves_a_leading_warmup_prefix_for_causal_composition():
    out = ema([np.nan, np.nan, 1.0, 2.0, 3.0, 4.0], 3)

    assert np.isnan(out[:4]).all()
    assert out[4:].tolist() == pytest.approx([2.0, 3.0])


def test_public_ema_supports_the_backtest_macd_series_composition():
    closes = np.linspace(100.0, 150.0, 50) ** 2
    macd_line = ema(closes, 12) - ema(closes, 26)
    histogram = macd_line - ema(macd_line, 9)

    assert np.isnan(histogram[: MACD_MIN_SAMPLES - 1]).all()
    assert np.isfinite(histogram[MACD_MIN_SAMPLES - 1 :]).all()
    assert histogram[-1] == pytest.approx(macd(closes)[2])


def test_macd_is_neutral_until_slow_and_signal_windows_are_full():
    trend = [100 + index**2 / 10 for index in range(MACD_MIN_SAMPLES)]

    assert macd(trend[:-1]) == (0.0, 0.0, 0.0)
    value, signal, histogram = macd(trend)
    assert value > 0
    assert signal > 0
    assert histogram > 0


def test_macd_is_translation_invariant_and_scale_equivariant():
    closes = list(np.linspace(100, 140, 80) + np.sin(np.arange(80)))
    baseline = np.asarray(macd(closes))

    assert np.asarray(macd([value + 1_000 for value in closes])) == pytest.approx(baseline)
    assert np.asarray(macd([value * 3 for value in closes])) == pytest.approx(baseline * 3)


def test_atr_uses_wilder_smoothing_after_the_seed():
    highs = [11, 12, 12, 12, 14]
    lows = [9, 9, 9, 9, 8]
    closes = [10, 10, 10, 10, 10]

    # True ranges are [3, 3, 3, 6]: seed=3, then (3*2 + 6)/3 = 4.
    assert atr(highs, lows, closes, 3) == 4.0


def test_atr_is_zero_until_a_full_period_of_true_ranges_exists():
    assert atr([11, 12, 12], [9, 9, 9], [10, 10, 10], 3) == 0.0


@pytest.mark.parametrize("indicator", [ema, rsi])
def test_univariate_indicators_reject_non_finite_input(indicator):
    with pytest.raises(ValueError, match="finite"):
        indicator([1.0, float("nan"), 2.0], 2)


def test_macd_rejects_non_finite_input_even_during_warmup():
    with pytest.raises(ValueError, match="finite"):
        macd([1.0, float("inf")])


def test_atr_rejects_misaligned_or_non_finite_inputs():
    with pytest.raises(ValueError, match="equal lengths"):
        atr([2, 3], [1], [1.5, 2.5], 1)
    with pytest.raises(ValueError, match="finite"):
        atr([2, float("nan")], [1, 1], [1.5, 1.5], 1)
    with pytest.raises(ValueError, match="price ranges"):
        atr([2, 2], [1, 1], [1.5, 2.5], 1)


@pytest.mark.parametrize(
    ("call", "match"),
    [
        (lambda: ema([1, 2], 0), "positive integer"),
        (lambda: rsi([1, 2], -1), "positive integer"),
        (lambda: macd([1] * 40, fast=26, slow=12), "smaller"),
        (lambda: atr([2, 2], [1, 1], [1.5, 1.5], 0), "positive integer"),
    ],
)
def test_indicator_parameters_are_validated(call, match):
    with pytest.raises(ValueError, match=match):
        call()
