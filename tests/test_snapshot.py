from kairos_quant.snapshot import SnapshotBuilder


def test_build_snapshot_round_trip():
    b = SnapshotBuilder()
    for px in range(100, 200):
        b.push_close("BTCUSD", float(px))
    snap = b.build("BTCUSD", bids=[(199.9, 5)], asks=[(200.1, 1)],
                   funding_rate=0.0001, open_interest=1e9, volume_usd=1e6)
    assert snap.symbol == "BTCUSD"
    assert snap.order_book.imbalance > 0
    assert 0 <= snap.indicators.rsi_14 <= 100
    # round-trips through JSON contract
    assert snap.from_json(snap.to_json()).symbol == "BTCUSD"
