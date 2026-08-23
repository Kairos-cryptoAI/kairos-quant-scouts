import pytest
from pydantic import ValidationError

from kairos_quant.config import QuantSettings
from kairos_quant.indicators import MACD_MIN_SAMPLES


@pytest.mark.parametrize(
    "override",
    [
        {"snapshot_interval_s": 0},
        {"depth_levels": 0},
        {"price_window": MACD_MIN_SAMPLES - 1},
        {"open_interest_interval_s": 0},
        {"book_stale_after_s": 0},
        {"kline_stale_after_s": 0},
        {"derivatives_stale_after_s": 0},
        {"ws_reconnect_initial_s": 2, "ws_reconnect_max_s": 1},
    ],
)
def test_math_and_freshness_boundaries_are_validated_at_configuration(override):
    with pytest.raises(ValidationError):
        QuantSettings(**override)


def test_enabled_runtime_gate_rejects_non_dev_evedex_url():
    with pytest.raises(ValidationError, match="exact official EVEDEX DEV URL"):
        QuantSettings(
            bus_backend="memory",
            enable_venue_quality_gate=True,
            evedex_dev_base_url="https://trading-api.evedex.com",
        )
