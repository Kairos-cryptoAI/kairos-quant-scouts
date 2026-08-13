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
                "T": 59_999,
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
        (59_999, 104.0, 99.0, 102.5, 2750.0)
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
    wall_now = [60.0]
    collector = _collector(clock=lambda: now[0], wall_clock=lambda: wall_now[0])
    collector._on_message(
        {
            "stream": "btcusdt@depth10@100ms",
            "data": {"E": 60_000, "u": 1, "b": [["100", "2"]], "a": [["101", "3"]]},
        }
    )
    collector._on_message(
        {
            "stream": "btcusdt@kline_1m",
            "data": {
                "k": {
                    "x": True,
                    "T": 59_999,
                    "h": "101",
                    "l": "99",
                    "c": "100.5",
                    "q": "1000",
                }
            },
        }
    )

    assert collector.is_book_fresh("btcusdt", 10)
    assert collector.is_kline_fresh("btcusdt", 90)
    now[0] = 111.0
    wall_now[0] = 71.0
    assert not collector.is_book_fresh("btcusdt", 10)
    assert collector.is_kline_fresh("btcusdt", 90)
    wall_now[0] = 151.0
    assert not collector.is_kline_fresh("btcusdt", 90)


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
            {
                "sumOpenInterestValue": str(10_000 + index * (2_345.5 / 12)),
                "timestamp": 1_000_000 + index * 300_000,
            }
            for index in range(13)
        ]


class _OpenInterestSession:
    def get(self, url, *, params):
        assert url == "https://example.invalid/futures/data/openInterestHist"
        assert params == {"symbol": "BTCUSDT", "period": "5m", "limit": 13}
        return _OpenInterestResponse()


def test_open_interest_response_updates_symbol_without_network():
    collector = _collector(wall_clock=lambda: 4_600.0)

    asyncio.run(collector.refresh_open_interest(_OpenInterestSession()))

    assert collector.open_interest["btcusdt"] == 12345.5
    assert collector.oi_change_pct_1h["btcusdt"] == pytest.approx(23.455)
    assert collector.is_open_interest_fresh("btcusdt", 1)


def test_open_interest_orders_a_complete_hour_by_exchange_timestamp():
    class ReversedResponse(_OpenInterestResponse):
        async def json(self):
            return list(reversed(await super().json()))

    class ReversedSession(_OpenInterestSession):
        def get(self, url, *, params):
            return ReversedResponse()

    collector = _collector(wall_clock=lambda: 4_600.0)
    asyncio.run(collector.refresh_open_interest(ReversedSession()))

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
            [60_000, "100", "120", "90", "110", "1", 119_999, "600", 1, "0", "0", "0"],
            [30_000, "bad", "row"],
            [0, "90", "110", "80", "100", "1", 59_999, "500", 1, "0", "0", "0"],
            [120_000, "110", "130", "100", "120", "1", 1_000_000, "700", 1, "0", "0", "0"],
        ]


class _KlineSession:
    def get(self, url, *, params):
        assert url == "https://example.invalid/fapi/v1/klines"
        assert params == {"symbol": "BTCUSDT", "interval": "1m", "limit": 1000}
        return _KlineResponse()


def test_kline_backfill_excludes_open_candle_and_deduplicates_stream_replay():
    collector = _collector(wall_clock=lambda: 4_600.0)

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
    assert not collector.is_kline_fresh("btcusdt", 90)


def _closed_kline(close_time_ms: int, *, close: str = "100", quote_volume: str = "1000") -> dict:
    return {
        "stream": "btcusdt@kline_1m",
        "data": {
            "k": {
                "x": True,
                "T": close_time_ms,
                "h": str(float(close) + 1),
                "l": str(float(close) - 1),
                "c": close,
                "q": quote_volume,
            }
        },
    }


def test_gap_is_held_until_missing_candle_arrives_then_emitted_in_order():
    wall_now = [180.0]
    collector = _collector(wall_clock=lambda: wall_now[0])
    collector._on_message(_closed_kline(59_999, close="100"))
    collector._on_message(_closed_kline(179_999, close="102"))

    assert [item.close_time_ms for item in collector.drain_closed_klines("btcusdt")] == [59_999]
    assert "btcusdt" in collector._needs_kline_backfill

    collector._on_message(_closed_kline(119_999, close="101"))

    assert [item.close_time_ms for item in collector.drain_closed_klines("btcusdt")] == [
        119_999,
        179_999,
    ]
    assert collector._needs_kline_backfill == set()


def test_duplicate_final_candle_does_not_extend_freshness():
    now = [100.0]
    wall_now = [60.0]
    collector = _collector(clock=lambda: now[0], wall_clock=lambda: wall_now[0])
    message = _closed_kline(59_999)
    assert collector._on_message(message)
    now[0] = 111.0
    wall_now[0] = 71.0
    assert not collector._on_message(message)

    assert not collector.is_kline_fresh("btcusdt", 10)


@pytest.mark.parametrize(
    "kline",
    [
        {"x": True, "T": 59_999, "c": "100", "q": "1000"},
        {"x": 1, "T": 59_999, "h": "101", "l": "99", "c": "100", "q": "1000"},
        {"x": True, "T": 59_999, "h": "101", "l": "99", "c": "100", "q": "-1"},
        {"x": True, "T": 59_999, "h": "inf", "l": "99", "c": "100", "q": "1"},
        {"x": True, "T": 60_000, "h": "101", "l": "99", "c": "100", "q": "1"},
    ],
)
def test_malformed_closed_kline_is_not_buffered_or_marked_fresh(kline):
    collector = _collector(wall_clock=lambda: 60.0)

    assert not collector._on_message({"stream": "btcusdt@kline_1m", "data": {"k": kline}})
    assert collector.drain_closed_klines("btcusdt") == []
    assert not collector.is_kline_fresh("btcusdt", 90)


def test_depth_updates_are_sorted_and_cannot_regress_exchange_sequence():
    now = [100.0]
    collector = _collector(clock=lambda: now[0])
    assert collector._on_message(
        {
            "stream": "btcusdt@depth10@100ms",
            "data": {
                "E": 1_000_000,
                "u": 20,
                "b": [["99", "5"], ["100", "1"]],
                "a": [["102", "1"], ["101", "2"]],
            },
        }
    )
    now[0] = 101.0
    assert not collector._on_message(
        {
            "stream": "btcusdt@depth10@100ms",
            "data": {"E": 1_000_001, "u": 10, "b": [["90", "1"]], "a": [["91", "1"]]},
        }
    )

    assert collector.books["btcusdt"] == {
        "bids": [(100.0, 1.0), (99.0, 5.0)],
        "asks": [(101.0, 2.0), (102.0, 1.0)],
    }
    now[0] = 110.5
    assert not collector.is_book_fresh("btcusdt", 10)


def test_depth_without_a_valid_exchange_sequence_is_rejected():
    collector = _collector()

    assert not collector._on_message(
        {
            "stream": "btcusdt@depth10@100ms",
            "data": {"E": 1_000_000, "b": [["100", "1"]], "a": [["101", "1"]]},
        }
    )
    assert not collector.is_book_fresh("btcusdt", 10)


def test_invalid_crossed_book_does_not_replace_or_freshen_last_valid_book():
    now = [100.0]
    collector = _collector(clock=lambda: now[0])
    collector._on_message(
        {
            "stream": "btcusdt@depth10@100ms",
            "data": {"E": 1_000_000, "u": 1, "b": [["100", "1"]], "a": [["101", "1"]]},
        }
    )
    now[0] = 105.0
    assert not collector._on_message(
        {
            "stream": "btcusdt@depth10@100ms",
            "data": {"E": 1_000_001, "u": 2, "b": [["102", "1"]], "a": [["101", "1"]]},
        }
    )

    assert collector.books["btcusdt"]["bids"][0][0] == 100
    now[0] = 111.0
    assert not collector.is_book_fresh("btcusdt", 10)


def test_funding_freshness_requires_a_valid_observation():
    collector = _collector()
    assert not collector._on_message({"stream": "btcusdt@markPrice@1s", "data": {"E": 1_000_000, "r": "nan"}})
    assert not collector.is_funding_fresh("btcusdt", 10)

    assert collector._on_message({"stream": "btcusdt@markPrice@1s", "data": {"E": 1_000_000, "r": "0"}})
    assert collector.is_funding_fresh("btcusdt", 10)


def test_old_funding_event_cannot_regress_or_refresh_the_current_value():
    now = [100.0]
    collector = _collector(clock=lambda: now[0])
    assert collector._on_message({"stream": "btcusdt@markPrice@1s", "data": {"E": 1_000_000, "r": "0.001"}})
    now[0] = 101.0

    assert not collector._on_message({"stream": "btcusdt@markPrice@1s", "data": {"E": 999_999, "r": "0.002"}})
    assert collector.funding["btcusdt"] == 0.001
    now[0] = 110.5
    assert not collector.is_funding_fresh("btcusdt", 10)


def test_open_interest_requires_thirteen_five_minute_observations():
    class ShortResponse(_OpenInterestResponse):
        async def json(self):
            return [{"sumOpenInterestValue": "10000"}, {"sumOpenInterestValue": "11000"}]

    class ShortSession(_OpenInterestSession):
        def get(self, url, *, params):
            return ShortResponse()

    collector = _collector()
    asyncio.run(collector.refresh_open_interest(ShortSession()))

    assert collector.open_interest["btcusdt"] == 0
    assert collector.oi_change_pct_1h["btcusdt"] == 0
    assert not collector.is_open_interest_fresh("btcusdt", 10)


@pytest.mark.parametrize("bad_index", [None, 6])
def test_open_interest_rejects_missing_or_irregular_five_minute_grid(bad_index):
    class IrregularResponse(_OpenInterestResponse):
        async def json(self):
            records = await super().json()
            if bad_index is None:
                records[-1].pop("timestamp")
            else:
                records[bad_index]["timestamp"] += 1
            return records

    class IrregularSession(_OpenInterestSession):
        def get(self, url, *, params):
            return IrregularResponse()

    collector = _collector(wall_clock=lambda: 4_600.0)
    asyncio.run(collector.refresh_open_interest(IrregularSession()))

    assert collector.open_interest["btcusdt"] == 0
    assert collector.oi_change_pct_1h["btcusdt"] == 0
    assert not collector.is_open_interest_fresh("btcusdt", 10)


def test_open_interest_rejects_a_stale_hour_window_even_when_just_received():
    collector = _collector(wall_clock=lambda: 5_201.0)

    asyncio.run(collector.refresh_open_interest(_OpenInterestSession()))

    assert collector.open_interest["btcusdt"] == 0
    assert collector.oi_change_pct_1h["btcusdt"] == 0
    assert not collector.is_open_interest_fresh("btcusdt", 10)


def test_liquidation_replay_is_deduplicated_by_exchange_event_identity():
    collector = _collector()
    first = {
        "stream": "btcusdt@forceOrder",
        "data": {"E": 1_000, "o": {"S": "SELL", "ap": "100", "z": "2"}},
    }
    second = {
        "stream": "btcusdt@forceOrder",
        "data": {"E": 1_001, "o": {"S": "SELL", "ap": "100", "z": "2"}},
    }

    assert collector._on_message(first)
    assert not collector._on_message(first)
    assert collector._on_message(second)
    assert collector.liquidation_totals("btcusdt").long_usd == 400


class _BoundaryResponse(_KlineResponse):
    def __init__(self, wall_now):
        self.wall_now = wall_now

    async def json(self):
        self.wall_now[0] = 60.1
        return [
            [0, "99", "101", "99", "100", "1", 59_999, "1000"],
            [60_000, "100", "102", "100", "101", "1", 119_999, "1000"],
        ]


class _BoundarySession(_KlineSession):
    def __init__(self, wall_now):
        self.wall_now = wall_now

    def get(self, url, *, params):
        return _BoundaryResponse(self.wall_now)


def test_backfill_uses_post_response_time_at_minute_boundary():
    wall_now = [59.5]
    collector = _collector(wall_clock=lambda: wall_now[0])

    asyncio.run(collector.refresh_klines(_BoundarySession(wall_now)))

    assert [item.close_time_ms for item in collector.drain_closed_klines("btcusdt")] == [59_999]


def test_gap_backfill_attempts_are_throttled_by_monotonic_time():
    now = [100.0]
    collector = _collector(clock=lambda: now[0])
    collector._needs_kline_backfill.add("btcusdt")

    assert collector._gap_backfill_due()
    collector._last_kline_backfill_at["btcusdt"] = now[0]
    assert not collector._gap_backfill_due()

    now[0] += collector.reconnect_initial_s
    assert collector._gap_backfill_due()
