import math

import numpy as np
import pytest
from kairos_core.enums import Side

from kairos_quant.bias import derive_bias
from kairos_quant.indicators import MACD_MIN_SAMPLES, atr, macd, rsi
from kairos_quant.snapshot import SnapshotBuilder


def test_build_snapshot_round_trip():
    b = SnapshotBuilder()
    for px in range(100, 200):
        b.push_candle("BTCUSD", high=float(px + 2), low=float(px - 2), close=float(px))
    snap = b.build(
        "BTCUSD", bids=[(199.9, 5)], asks=[(200.1, 1)], funding_rate=0.0001, open_interest=1e9, volume_usd=1e6
    )
    assert snap.symbol == "BTCUSD"
    assert snap.order_book.imbalance > 0
    assert 0 <= snap.indicators.rsi_14 <= 100
    assert snap.indicators.atr_pct is not None
    assert snap.indicators.atr_pct > 0
    # round-trips through JSON contract
    assert snap.from_json(snap.to_json()).symbol == "BTCUSD"


def _build(builder: SnapshotBuilder, *, bids=None, asks=None, volume_usd=0.0):
    return builder.build(
        "btcusdt",
        bids=[(101, 5)] if bids is None else bids,
        asks=[(102, 1)] if asks is None else asks,
        funding_rate=0.0,
        open_interest=1_000.0,
        volume_usd=volume_usd,
    )


def test_snapshot_is_fail_neutral_until_macd_is_fully_warmed():
    builder = SnapshotBuilder()
    closes = [100 + index**2 / 10 for index in range(MACD_MIN_SAMPLES)]
    for close in closes[:-1]:
        builder.push_close("BTCUSDT", close)

    cold = _build(builder)
    assert cold.indicators.macd == 0.0
    assert cold.indicators.macd_signal == 0.0
    assert cold.indicators.macd_hist == 0.0
    assert cold.quant_bias is Side.FLAT

    builder.push_close("BTCUSDT", closes[-1])
    warm = _build(builder)
    assert warm.indicators.macd > 0
    assert warm.indicators.macd_signal > 0
    assert warm.quant_bias is Side.LONG


def test_atr_pct_is_absent_until_fourteen_true_ranges_exist():
    builder = SnapshotBuilder()
    for close in range(100, 114):
        builder.push_candle("BTCUSDT", high=close + 2, low=close - 2, close=close)
    assert _build(builder).indicators.atr_pct is None

    builder.push_candle("BTCUSDT", high=116, low=112, close=114)
    assert _build(builder).indicators.atr_pct == pytest.approx(4 / 114)


def test_constant_prices_and_zero_volume_are_finite_and_neutral():
    builder = SnapshotBuilder()
    for _ in range(MACD_MIN_SAMPLES):
        builder.push_close("BTCUSDT", 100.0)

    snapshot = _build(
        builder,
        bids=[(99, 1)],
        asks=[(101, 1)],
        volume_usd=0,
    )

    assert snapshot.volume_usd == 0
    assert snapshot.indicators.rsi_14 == 50
    assert snapshot.indicators.macd_hist == 0
    assert snapshot.order_book.imbalance == 0
    assert snapshot.order_book.depth_usd == 200
    assert snapshot.quant_bias is Side.FLAT


def test_snapshot_rejects_a_book_without_effective_liquidity():
    builder = SnapshotBuilder()
    builder.push_close("BTCUSDT", 100)

    with pytest.raises(ValueError, match="both sides"):
        _build(builder, bids=[(99, 0)], asks=[(101, 0)])


def test_recursive_indicator_state_does_not_reseed_when_raw_window_evicts():
    builder = SnapshotBuilder(window=MACD_MIN_SAMPLES)
    closes = [100.0 + index * 0.15 + 3.0 * math.sin(index / 4) for index in range(90)]
    highs = [close + 1.0 + (index % 3) * 0.1 for index, close in enumerate(closes)]
    lows = [close - 1.0 - (index % 2) * 0.1 for index, close in enumerate(closes)]
    for high, low, close in zip(highs, lows, closes, strict=True):
        builder.push_candle("BTCUSDT", high=high, low=low, close=close)

    snapshot = _build(builder, bids=[(99, 1)], asks=[(101, 1)])
    expected_macd = macd(closes)

    assert len(builder._closes["BTCUSDT"]) == MACD_MIN_SAMPLES
    assert snapshot.indicators.rsi_14 == pytest.approx(rsi(closes))
    assert np.asarray(
        [
            snapshot.indicators.macd,
            snapshot.indicators.macd_signal,
            snapshot.indicators.macd_hist,
        ]
    ) == pytest.approx(expected_macd)
    assert snapshot.indicators.atr_pct == pytest.approx(atr(highs, lows, closes) / closes[-1])
    assert snapshot.quant_bias is derive_bias(
        rsi_14=rsi(closes),
        macd_hist=expected_macd[2],
        ob_imbalance=0.0,
    )


def test_snapshot_normalizes_symbol_and_book_order():
    builder = SnapshotBuilder()
    builder.push_close(" btcusdt ", 100)

    snapshot = _build(
        builder,
        bids=[(99, 100), (101, 1)],
        asks=[(103, 100), (102, 4)],
    )

    assert snapshot.symbol == "BTCUSDT"
    assert snapshot.order_book.best_bid == 101
    assert snapshot.order_book.best_ask == 102
    assert snapshot.mid_price == 101.5


@pytest.mark.parametrize(
    "candle",
    [
        {"high": 99, "low": 98, "close": 100},
        {"high": 101, "low": 99, "close": math.nan},
        {"high": math.inf, "low": 99, "close": 100},
    ],
)
def test_invalid_candles_are_rejected(candle):
    with pytest.raises(ValueError, match="invalid candle"):
        SnapshotBuilder().push_candle("BTCUSDT", **candle)


def test_snapshot_rejects_missing_crossed_or_non_finite_market_inputs():
    builder = SnapshotBuilder()
    builder.push_close("BTCUSDT", 100)

    with pytest.raises(ValueError, match="both sides"):
        _build(builder, bids=[], asks=[(101, 1)])
    with pytest.raises(ValueError, match="crossed"):
        _build(builder, bids=[(102, 1)], asks=[(101, 1)])
    with pytest.raises(ValueError, match="finite"):
        builder.build(
            "BTCUSDT",
            bids=[(100, 1)],
            asks=[(101, 1)],
            funding_rate=math.nan,
            open_interest=1_000,
        )


def test_builder_rejects_a_window_that_can_never_warm_macd():
    with pytest.raises(ValueError, match=str(MACD_MIN_SAMPLES)):
        SnapshotBuilder(window=MACD_MIN_SAMPLES - 1)


@pytest.mark.parametrize("kwargs", [{"window": 34.5}, {"depth_levels": True}, {"depth_levels": 1.5}])
def test_builder_rejects_non_integer_window_configuration(kwargs):
    with pytest.raises(ValueError):
        SnapshotBuilder(**kwargs)
