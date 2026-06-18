from kairos_quant.orderbook import order_book_imbalance, spread_bps, depth_usd


def test_imbalance_sign():
    bids = [(100, 5)] * 10
    asks = [(101, 1)] * 10
    assert order_book_imbalance(bids, asks, 10) > 0
    assert order_book_imbalance(asks_to_bids := [(100, 1)] * 10, [(101, 5)] * 10, 10) < 0


def test_spread_bps():
    assert round(spread_bps(100.0, 100.1), 2) == round((0.1 / 100.05) * 10000, 2)


def test_depth_usd_positive():
    assert depth_usd([(100, 2)], [(101, 2)], 10) > 0
