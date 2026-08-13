from kairos_core.enums import Side

from kairos_quant.bias import derive_bias


def test_long_bias():
    assert derive_bias(rsi_14=60, macd_hist=1.2, ob_imbalance=0.3) is Side.LONG


def test_short_bias():
    assert derive_bias(rsi_14=40, macd_hist=-1.2, ob_imbalance=-0.3) is Side.SHORT


def test_flat_when_mixed():
    assert derive_bias(rsi_14=50, macd_hist=0.0, ob_imbalance=0.0) is Side.FLAT
