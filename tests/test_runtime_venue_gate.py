import pytest
from kairos_core.enums import EvedexProfile

from kairos_quant.runtime_venue_gate import (
    VenueGatePolicy,
    build_venue_quality,
    evedex_dev_symbol,
)
from kairos_quant.venue_comparison import (
    BookLevel,
    BookSnapshot,
    parse_binance_book,
    parse_evedex_book,
)


def _book(
    source: str,
    *,
    bid: float,
    ask: float,
    quantity: float = 20.0,
    timestamp_ms: int = 1_000_000,
) -> BookSnapshot:
    return BookSnapshot(
        source=source,
        symbol="BTCUSDT" if source == "binance" else "BTCUSD:DEV",
        timestamp_ms=timestamp_ms,
        bids=(BookLevel(bid, quantity),),
        asks=(BookLevel(ask, quantity),),
        latency_ms=50.0,
    )


def test_builds_allowed_dev_measurement_with_executable_economics():
    quality = build_venue_quality(
        binance=_book("binance", bid=99.95, ask=100.05),
        evedex=_book("evedex", bid=99.95, ask=100.05),
        venue_symbol="BTCUSD:DEV",
        observed_at_ms=1_001_000,
        latency_ms=100,
        policy=VenueGatePolicy(),
    )

    assert quality.entry_allowed
    assert quality.reason_codes == ()
    assert quality.profile.value == "DEV"
    assert quality.basis_bps == pytest.approx(0)
    assert quality.spread_bps == pytest.approx(10)
    assert quality.buy_slippage_bps == pytest.approx(5)
    assert quality.sell_slippage_bps == pytest.approx(5)
    assert quality.depth_usd >= quality.assessed_notional_usd
    assert quality.measurement_id == quality.message_id


def test_blocks_basis_spread_and_insufficient_depth():
    quality = build_venue_quality(
        binance=_book("binance", bid=99.95, ask=100.05),
        evedex=_book("evedex", bid=100.5, ask=101.5, quantity=1.0),
        venue_symbol="BTCUSD:DEV",
        observed_at_ms=1_001_000,
        latency_ms=100,
        policy=VenueGatePolicy(),
    )

    assert not quality.entry_allowed
    assert set(quality.reason_codes) >= {
        "basis_exceeds_limit",
        "spread_exceeds_limit",
        "insufficient_depth",
    }
    assert quality.buy_slippage_bps == 26.0
    assert quality.sell_slippage_bps == 26.0


def test_blocks_stale_skewed_or_high_latency_measurement():
    quality = build_venue_quality(
        binance=_book("binance", bid=99.95, ask=100.05, timestamp_ms=1_000_000),
        evedex=_book("evedex", bid=99.95, ask=100.05, timestamp_ms=1_010_000),
        venue_symbol="BTCUSD:DEV",
        observed_at_ms=1_020_000,
        latency_ms=5_001,
        policy=VenueGatePolicy(),
    )

    assert not quality.entry_allowed
    assert set(quality.reason_codes) >= {
        "stale_order_book",
        "timestamp_skew_exceeds_limit",
        "latency_exceeds_limit",
    }


@pytest.mark.parametrize(
    ("binance", "evedex"),
    [
        ("BTCUSDT", "BTCUSD:DEV"),
        ("ETHUSDT", "ETHUSD:DEV"),
        ("SOLUSDT", "SOLUSD:DEV"),
        ("BNBUSDT", "BNBUSD:DEV"),
        ("XRPUSDT", "XRPUSD:DEV"),
    ],
)
def test_maps_the_fixed_five_symbol_universe(binance, evedex):
    assert evedex_dev_symbol(binance) == evedex


@pytest.mark.parametrize("symbol", ["", "BTCUSD"])
def test_rejects_non_usdt_binance_symbols(symbol):
    with pytest.raises(ValueError):
        evedex_dev_symbol(symbol)


def test_symbol_mapping_normalizes_transport_case_and_whitespace():
    assert evedex_dev_symbol(" btcusdt ") == "BTCUSD:DEV"


def test_default_dev_builder_preserves_legacy_non_five_symbol_pairs():
    binance = BookSnapshot(
        source="binance",
        symbol="LTCUSDT",
        timestamp_ms=1_000_000,
        bids=(BookLevel(99.95, 20),),
        asks=(BookLevel(100.05, 20),),
        latency_ms=35,
    )
    evedex = BookSnapshot(
        source="evedex",
        symbol="LTCUSD:DEV",
        timestamp_ms=1_000_000,
        bids=(BookLevel(99.95, 20),),
        asks=(BookLevel(100.05, 20),),
        latency_ms=45,
    )

    quality = build_venue_quality(
        binance=binance,
        evedex=evedex,
        venue_symbol="LTCUSD:DEV",
        observed_at_ms=1_001_000,
        latency_ms=45,
        policy=VenueGatePolicy(),
    )

    assert quality.profile is EvedexProfile.DEV
    assert quality.symbol == "LTCUSD:DEV"


def _captured_prod_books():
    """Normalize already-captured response bodies without opening a session."""
    binance = parse_binance_book(
        "BTCUSDT",
        {"T": 1_000_000, "bids": [["99.95", "20"]], "asks": [["100.05", "20"]]},
        latency_ms=35,
    )
    evedex = parse_evedex_book(
        "BTCUSD",
        {
            "t": 1_000_000,
            "bids": [{"price": "99.95", "quantity": "20"}],
            "asks": [{"price": "100.05", "quantity": "20"}],
        },
        latency_ms=45,
    )
    return binance, evedex


def test_builds_prod_measurement_from_actual_normalized_captured_books_without_network():
    binance, evedex = _captured_prod_books()

    quality = build_venue_quality(
        binance=binance,
        evedex=evedex,
        venue_symbol="BTCUSD",
        observed_at_ms=1_001_000,
        latency_ms=45,
        policy=VenueGatePolicy(),
        profile=EvedexProfile.PROD,
    )

    assert quality.profile is EvedexProfile.PROD
    assert quality.symbol == "BTCUSD"
    assert not quality.symbol.endswith(":DEV")
    assert quality.entry_allowed
    assert quality.basis_bps == pytest.approx(0)


@pytest.mark.parametrize(
    ("binance_symbol", "evedex_symbol", "venue_symbol", "profile"),
    [
        ("BTCUSDT", "BTCUSD:DEV", "BTCUSD:DEV", EvedexProfile.PROD),
        ("BTCUSDT", "BTCUSD", "BTCUSD:DEV", EvedexProfile.PROD),
        ("ETHUSDT", "BTCUSD", "BTCUSD", EvedexProfile.PROD),
        ("BTCUSDT", "BTCUSD", "ETHUSD", EvedexProfile.PROD),
        ("BTCUSDT", "BTCUSD:DEV", "BTCUSD:DEV", "PROD"),
        ("BTCUSDT", "BTCUSD", "BTCUSD", EvedexProfile.DEMO),
    ],
)
def test_rejects_dev_books_or_mismatched_pair_for_prod(binance_symbol, evedex_symbol, venue_symbol, profile):
    binance = BookSnapshot(
        source="binance",
        symbol=binance_symbol,
        timestamp_ms=1_000_000,
        bids=(BookLevel(99.95, 20),),
        asks=(BookLevel(100.05, 20),),
        latency_ms=35,
    )
    evedex = BookSnapshot(
        source="evedex",
        symbol=evedex_symbol,
        timestamp_ms=1_000_000,
        bids=(BookLevel(99.95, 20),),
        asks=(BookLevel(100.05, 20),),
        latency_ms=45,
    )

    with pytest.raises(ValueError):
        build_venue_quality(
            binance=binance,
            evedex=evedex,
            venue_symbol=venue_symbol,
            observed_at_ms=1_001_000,
            latency_ms=45,
            policy=VenueGatePolicy(),
            profile=profile,
        )
