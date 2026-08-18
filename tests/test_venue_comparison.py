import json
from datetime import UTC, datetime

import pytest

from kairos_quant.venue_comparison import (
    DEFAULT_THRESHOLDS,
    BookLevel,
    BookSnapshot,
    ComparisonStatus,
    _write_report,
    compare_venues,
    market_slippage_bps,
    parse_binance_book,
    parse_evedex_book,
)

NOW = datetime(2026, 8, 18, 0, tzinfo=UTC)
NOW_MS = int(NOW.timestamp() * 1000)


def _book(source: str, symbol: str, *, mid: float = 100.0, timestamp_ms: int = NOW_MS):
    return BookSnapshot(
        source=source,
        symbol=symbol,
        timestamp_ms=timestamp_ms,
        bids=(BookLevel(mid - 0.05, 100), BookLevel(mid - 0.1, 100)),
        asks=(BookLevel(mid + 0.05, 100), BookLevel(mid + 0.1, 100)),
        latency_ms=2.0,
    )


def test_book_parsers_validate_shape_order_and_transaction_time():
    evedex = parse_evedex_book(
        "BTCUSD",
        {
            "t": NOW_MS,
            "bids": [{"price": "100", "quantity": "2"}, {"price": "99", "quantity": "3"}],
            "asks": [{"price": "101", "quantity": "4"}, {"price": "102", "quantity": "5"}],
        },
        latency_ms=1,
    )
    binance = parse_binance_book(
        "BTCUSDT",
        {
            "T": NOW_MS,
            "bids": [["100", "2"], ["99", "3"]],
            "asks": [["101", "4"], ["102", "5"]],
        },
        latency_ms=1,
    )

    assert evedex.mid == binance.mid == 100.5
    with pytest.raises(ValueError, match="malformed level"):
        parse_evedex_book(
            "BTCUSD",
            {"t": NOW_MS, "bids": [{"price": 100, "quantity": 1}, []], "asks": []},
            latency_ms=1,
        )
    with pytest.raises(ValueError, match="not sorted"):
        parse_binance_book(
            "BTCUSDT",
            {"T": NOW_MS, "bids": [[99, 1], [100, 1]], "asks": [[101, 1]]},
            latency_ms=1,
        )


def test_market_slippage_uses_executable_depth_and_reports_insufficient_book():
    book = BookSnapshot(
        source="evedex",
        symbol="BTCUSD",
        timestamp_ms=NOW_MS,
        bids=(BookLevel(99, 5), BookLevel(98, 5)),
        asks=(BookLevel(101, 5), BookLevel(102, 5)),
        latency_ms=1,
    )

    assert market_slippage_bps(book, buy=True, notional_usd=101) == pytest.approx(100)
    assert market_slippage_bps(book, buy=False, notional_usd=99) == pytest.approx(100)
    assert market_slippage_bps(book, buy=True, notional_usd=10_000) is None


@pytest.mark.asyncio
async def test_comparison_passes_only_with_complete_synchronized_executable_samples():
    symbol_map = {"BTCUSDT": "BTCUSD", "ETHUSDT": "ETHUSD"}

    async def fetch_pair(logical, venue):
        return _book("evedex", venue), _book("binance", logical)

    async def no_sleep(_seconds):
        return None

    report = await compare_venues(
        fetch_pair=fetch_pair,
        symbol_map=symbol_map,
        samples=30,
        interval_s=0,
        notional_usd=1_000,
        clock=lambda: NOW,
        sleep=no_sleep,
    )

    assert report.status is ComparisonStatus.PASS
    assert report.live_orders_allowed is False
    assert len(report.samples) == 60
    assert all(item.p95_abs_basis_bps == 0 for item in report.symbols)


@pytest.mark.asyncio
async def test_comparison_blocks_transport_gaps_basis_and_insufficient_depth():
    symbol_map = {"BTCUSDT": "BTCUSD", "ETHUSDT": "ETHUSD"}
    calls = 0

    async def fetch_pair(logical, venue):
        nonlocal calls
        calls += 1
        if logical == "ETHUSDT" and calls % 4 == 0:
            raise TimeoutError("simulated public feed timeout")
        evedex = _book("evedex", venue, mid=101)
        if logical == "BTCUSDT":
            evedex = BookSnapshot(
                source="evedex",
                symbol=venue,
                timestamp_ms=NOW_MS,
                bids=(BookLevel(100.95, 0.01),),
                asks=(BookLevel(101.05, 0.01),),
                latency_ms=2,
            )
        return evedex, _book("binance", logical, mid=100)

    async def no_sleep(_seconds):
        return None

    report = await compare_venues(
        fetch_pair=fetch_pair,
        symbol_map=symbol_map,
        samples=30,
        interval_s=0,
        notional_usd=1_000,
        clock=lambda: NOW,
        sleep=no_sleep,
    )

    assert report.status is ComparisonStatus.BLOCKED
    btc = next(item for item in report.symbols if item.logical_symbol == "BTCUSDT")
    eth = next(item for item in report.symbols if item.logical_symbol == "ETHUSDT")
    assert "basis_exceeds_limit" in btc.reasons
    assert "insufficient_evedex_depth" in btc.reasons
    assert "insufficient_availability" in eth.reasons


@pytest.mark.asyncio
async def test_comparison_rejects_modified_or_invalid_policy():
    async def fetch_pair(logical, venue):
        return _book("evedex", venue), _book("binance", logical)

    invalid = dict(DEFAULT_THRESHOLDS)
    invalid.pop("maximum_book_age_ms")
    with pytest.raises(ValueError, match="exact registered"):
        await compare_venues(
            fetch_pair=fetch_pair,
            symbol_map={"BTCUSDT": "BTCUSD"},
            thresholds=invalid,
        )


@pytest.mark.asyncio
async def test_report_writer_refuses_overwrite(tmp_path):
    async def fetch_pair(logical, venue):
        return _book("evedex", venue), _book("binance", logical)

    async def no_sleep(_seconds):
        return None

    report = await compare_venues(
        fetch_pair=fetch_pair,
        symbol_map={"BTCUSDT": "BTCUSD"},
        samples=1,
        interval_s=0,
        clock=lambda: NOW,
        sleep=no_sleep,
    )
    destination = tmp_path / "comparison.json"

    _write_report(destination, report, overwrite=False)

    persisted = json.loads(destination.read_text(encoding="utf-8"))
    assert persisted["live_orders_allowed"] is False
    with pytest.raises(FileExistsError):
        _write_report(destination, report, overwrite=False)
