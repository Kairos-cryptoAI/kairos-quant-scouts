import math

import pytest

from kairos_quant.orderbook import depth_usd, order_book_imbalance, spread_bps


def test_imbalance_sign():
    bids = [(100, 5)] * 10
    asks = [(101, 1)] * 10
    assert order_book_imbalance(bids, asks, 10) > 0
    assert order_book_imbalance([(100, 1)] * 10, [(101, 5)] * 10, 10) < 0


def test_spread_bps():
    assert round(spread_bps(100.0, 100.1), 2) == round((0.1 / 100.05) * 10000, 2)


def test_depth_usd_positive():
    assert depth_usd([(100, 2)], [(101, 2)], 10) > 0


def test_top_levels_are_price_sorted_before_factors_are_computed():
    bids = [(99, 100), (101, 1)]
    asks = [(103, 100), (102, 4)]

    assert order_book_imbalance(bids, asks, levels=1) == pytest.approx(-0.6)
    assert depth_usd(bids, asks, levels=1) == 509


def test_zero_size_book_is_neutral_and_has_zero_depth():
    bids = [(100, 0)]
    asks = [(101, 0)]

    assert order_book_imbalance(bids, asks) == 0.0
    assert depth_usd(bids, asks) == 0.0


def test_zero_size_levels_do_not_hide_the_effective_top_of_book():
    bids = [(105, 0), (100, 1)]
    asks = [(96, 0), (101, 1)]

    assert order_book_imbalance(bids, asks, levels=1) == 0.0
    assert depth_usd(bids, asks, levels=1) == 201.0


def test_factors_handle_or_reject_finite_inputs_that_overflow_arithmetic():
    assert math.isfinite(order_book_imbalance([(100, 1e308), (99, 1e308)], [(101, 1e308), (102, 1e308)]))
    with pytest.raises(ValueError, match="numeric range"):
        depth_usd([(1e308, 2)], [(1e308, 2)])


def test_locked_book_has_zero_spread():
    assert spread_bps(100, 100) == 0.0


@pytest.mark.parametrize(
    "call",
    [
        lambda: spread_bps(101, 100),
        lambda: spread_bps(float("inf"), 101),
        lambda: order_book_imbalance([(100, float("nan"))], [(101, 1)]),
        lambda: depth_usd([(float("inf"), 1)], [(101, 1)]),
        lambda: order_book_imbalance([(100, 1)], [(101, 1)], levels=0),
    ],
)
def test_invalid_order_book_inputs_are_rejected(call):
    with pytest.raises(ValueError):
        call()


@pytest.mark.parametrize("levels", [True, 1.5])
def test_level_count_must_be_a_positive_integer(levels):
    with pytest.raises(ValueError, match="positive integer"):
        order_book_imbalance([(100, 1)], [(101, 1)], levels=levels)
