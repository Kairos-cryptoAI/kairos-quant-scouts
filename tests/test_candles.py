import math

import pytest

from kairos_quant.candles import Candle


def _candle(**overrides) -> Candle:
    values = {
        "symbol": "BTCUSDT",
        "timeframe": "1m",
        "open_time_ms": 0,
        "close_time_ms": 59_999,
        "open": 100.0,
        "high": 102.0,
        "low": 99.0,
        "close": 101.0,
        "volume": 10.0,
        "quote_volume": 1_000.0,
        "taker_buy_volume": 4.0,
    }
    values.update(overrides)
    return Candle(**values)


def test_zero_volume_candle_is_valid():
    candle = _candle(volume=0, quote_volume=0, taker_buy_volume=0)

    assert candle.volume == 0


@pytest.mark.parametrize(
    "overrides",
    [
        {"close": math.nan},
        {"high": math.inf},
        {"quote_volume": math.inf},
        {"volume": -1},
        {"volume": 1, "taker_buy_volume": 2},
        {"high": 100, "close": 101},
        {"low": 102, "close": 101},
        {"open_time_ms": -1},
        {"open_time_ms": 0.5},
        {"close_time_ms": True},
    ],
)
def test_invalid_candle_boundaries_are_rejected(overrides):
    with pytest.raises(ValueError):
        _candle(**overrides)
