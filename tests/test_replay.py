import pytest
from kairos_core.enums import Side

from kairos_quant.candles import Candle
from kairos_quant.indicators import MACD_MIN_SAMPLES
from kairos_quant.replay import replay_candles


def _candles(count: int, *, symbol: str = "BTCUSDT", start: int = 0) -> list[Candle]:
    candles = []
    for index in range(count):
        open_time_ms = start + index * 60_000
        close = 100.0 + index**2 / 10
        candles.append(
            Candle(
                symbol=symbol,
                timeframe="1m",
                open_time_ms=open_time_ms,
                close_time_ms=open_time_ms + 59_999,
                open=close - 0.5,
                high=close + 1,
                low=close - 1,
                close=close,
                volume=1,
            )
        )
    return candles


def test_replay_stays_flat_until_full_macd_warmup():
    result = replay_candles(_candles(MACD_MIN_SAMPLES))

    assert all(point.bias is Side.FLAT for point in result.points[:-1])
    assert result.points[-1].bias is Side.LONG


def test_future_candles_do_not_change_past_replay_points():
    prefix = _candles(40)
    future = _candles(10, start=40 * 60_000)
    future = [
        Candle(
            symbol=item.symbol,
            timeframe=item.timeframe,
            open_time_ms=item.open_time_ms,
            close_time_ms=item.close_time_ms,
            open=50 - index + 0.5,
            high=50 - index + 1,
            low=50 - index - 1,
            close=50 - index,
            volume=1,
        )
        for index, item in enumerate(future)
    ]

    assert replay_candles(prefix).points == replay_candles(prefix + future).points[: len(prefix)]


@pytest.mark.parametrize("mutation", ["duplicate", "gap", "symbol"])
def test_replay_rejects_ambiguous_or_non_contiguous_series(mutation):
    candles = _candles(3)
    if mutation == "duplicate":
        candles.insert(1, candles[0])
    elif mutation == "gap":
        candles.pop(1)
    else:
        candles[1] = _candles(1, symbol="ETHUSDT", start=60_000)[0]

    with pytest.raises(ValueError):
        replay_candles(candles)


@pytest.mark.parametrize("warmup", [True, 1.5])
def test_replay_warmup_must_be_a_positive_integer(warmup):
    with pytest.raises(ValueError, match="positive integer"):
        replay_candles(_candles(3), warmup=warmup)
