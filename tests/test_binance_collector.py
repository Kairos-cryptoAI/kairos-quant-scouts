import asyncio

import pytest

from kairos_quant.collectors.binance_ws import BinanceFuturesCollector


def _collector(*, clock=lambda: 100.0, wall_clock=lambda: 1_000.0) -> BinanceFuturesCollector:
    return BinanceFuturesCollector(
        ["BTCUSDT"],
        "wss://example.invalid/stream",
        "https://example.invalid",
        clock=clock,
        wall_clock=wall_clock,
    )


def test_subscribes_to_depth_funding_closed_klines_and_liquidations():
    streams = _collector()._streams().split("/")

    assert streams == [
        "btcusdt@depth10@100ms",
        "btcusdt@markPrice@1s",
        "btcusdt@kline_1m",
        "btcusdt@forceOrder",
    ]


def test_only_closed_unique_klines_are_buffered():
    collector = _collector()
    open_kline = {
        "stream": "btcusdt@kline_1m",
        "data": {"k": {"x": False, "T": 60_000, "c": "101.5", "q": "2500"}},
    }
    collector._on_message(open_kline)
    assert collector.drain_closed_klines("btcusdt") == []

    closed_kline = {
        "stream": "btcusdt@kline_1m",
        "data": {
            "k": {
                "x": True,
                "T": 60_000,
                "h": "104",
                "l": "99",
                "c": "102.5",
                "q": "2750",
            }
        },
    }
    collector._on_message(closed_kline)
    collector._on_message(closed_kline)  # reconnect replay must not duplicate a candle

    klines = collector.drain_closed_klines("BTCUSDT")
    assert [(item.close_time_ms, item.high, item.low, item.close, item.quote_volume) for item in klines] == [
        (60_000, 104.0, 99.0, 102.5, 2750.0)
    ]
    assert collector.volume_usd["btcusdt"] == 2750.0


def test_force_orders_are_aggregated_by_liquidated_position_side():
    collector = _collector()
    collector._on_message(
        {
            "stream": "btcusdt@forceOrder",
            "data": {"o": {"S": "SELL", "ap": "100", "z": "2"}},
        }
    )
    collector._on_message(
        {
            "stream": "btcusdt@forceOrder",
            "data": {"o": {"S": "BUY", "p": "90", "q": "3"}},
        }
    )

    totals = collector.drain_liquidations("btcusdt")
    assert totals.long_usd == 200.0
    assert totals.short_usd == 270.0
    assert collector.drain_liquidations("btcusdt").long_usd == 0.0


def test_market_data_freshness_uses_receive_time():
    now = [100.0]
    collector = _collector(clock=lambda: now[0])
    collector._on_message(
        {
            "stream": "btcusdt@depth10@100ms",
            "data": {"b": [["100", "2"]], "a": [["101", "3"]]},
        }
    )
    collector._on_message(
        {
            "stream": "btcusdt@kline_1m",
            "data": {"k": {"x": True, "T": 60_000, "c": "100.5", "q": "1000"}},
        }
    )

    assert collector.is_book_fresh("btcusdt", 10)
    assert collector.is_kline_fresh("btcusdt", 90)
    now[0] = 111.0
    assert not collector.is_book_fresh("btcusdt", 10)
    assert collector.is_kline_fresh("btcusdt", 90)


async def test_open_interest_loop_refreshes_periodically(monkeypatch):
    collector = _collector()
    refreshed_twice = asyncio.Event()
    calls = 0

    async def fake_refresh(session=None):
        nonlocal calls
        calls += 1
        if calls == 2:
            refreshed_twice.set()

    monkeypatch.setattr(collector, "refresh_open_interest", fake_refresh)
    task = asyncio.create_task(collector.run_open_interest_loop(0.001))
    await asyncio.wait_for(refreshed_twice.wait(), timeout=1)
    task.cancel()

    try:
        await task
    except asyncio.CancelledError:
        pass

    assert calls >= 2


class _OpenInterestResponse:
    status = 200

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return None

    async def json(self):
        return [
            {"sumOpenInterestValue": "10000"},
            {"sumOpenInterestValue": "12345.5"},
        ]


class _OpenInterestSession:
    def get(self, url, *, params):
        assert url == "https://example.invalid/futures/data/openInterestHist"
        assert params == {"symbol": "BTCUSDT", "period": "5m", "limit": 13}
        return _OpenInterestResponse()


def test_open_interest_response_updates_symbol_without_network():
    collector = _collector()

    asyncio.run(collector.refresh_open_interest(_OpenInterestSession()))

    assert collector.open_interest["btcusdt"] == 12345.5
    assert collector.oi_change_pct_1h["btcusdt"] == pytest.approx(23.455)


class _KlineResponse:
    status = 200

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return None

    async def json(self):
        return [
            [0, "90", "110", "80", "100", "1", 59_999, "500", 1, "0", "0", "0"],
            [60_000, "100", "120", "90", "110", "1", 119_999, "600", 1, "0", "0", "0"],
            [120_000, "110", "130", "100", "120", "1", 1_000_000, "700", 1, "0", "0", "0"],
        ]


class _KlineSession:
    def get(self, url, *, params):
        assert url == "https://example.invalid/fapi/v1/klines"
        assert params == {"symbol": "BTCUSDT", "interval": "1m", "limit": 1000}
        return _KlineResponse()


def test_kline_backfill_excludes_open_candle_and_deduplicates_stream_replay():
    collector = _collector()

    asyncio.run(collector.refresh_klines(_KlineSession()))
    collector._on_message(
        {
            "stream": "btcusdt@kline_1m",
            "data": {"k": {"x": True, "T": 119_999, "h": "120", "l": "90", "c": "110"}},
        }
    )

    klines = collector.drain_closed_klines("btcusdt")
    assert [item.close_time_ms for item in klines] == [59_999, 119_999]
    assert klines[-1].high == 120.0
