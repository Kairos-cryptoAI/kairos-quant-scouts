import math

import pytest
from kairos_core.enums import Side

from kairos_quant.bias import derive_bias


def test_long_bias():
    assert derive_bias(rsi_14=60, macd_hist=1.2, ob_imbalance=0.3) is Side.LONG


def test_short_bias():
    assert derive_bias(rsi_14=40, macd_hist=-1.2, ob_imbalance=-0.3) is Side.SHORT


def test_flat_when_mixed():
    assert derive_bias(rsi_14=50, macd_hist=0.0, ob_imbalance=0.0) is Side.FLAT


@pytest.mark.parametrize("invalid", [math.nan, math.inf, -math.inf])
def test_non_finite_factor_fails_neutral(invalid):
    assert derive_bias(rsi_14=60, macd_hist=1, ob_imbalance=invalid) is Side.FLAT


def test_out_of_domain_normalized_factors_fail_neutral():
    assert derive_bias(rsi_14=101, macd_hist=1, ob_imbalance=0.5) is Side.FLAT
    assert derive_bias(rsi_14=60, macd_hist=1, ob_imbalance=1.1) is Side.FLAT


def test_direction_is_symmetric_under_mirrored_factors():
    assert derive_bias(rsi_14=60, macd_hist=2, ob_imbalance=0.3) is Side.LONG
    assert derive_bias(rsi_14=40, macd_hist=-2, ob_imbalance=-0.3) is Side.SHORT


def test_invalid_threshold_configuration_is_rejected():
    with pytest.raises(ValueError, match="threshold"):
        derive_bias(rsi_14=50, macd_hist=0, ob_imbalance=0, rsi_low=60, rsi_high=55)
