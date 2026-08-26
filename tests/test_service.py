"""Regression tests for Quant Scouts service lifecycle."""

import asyncio

from kairos_core.contracts import ClosedBarEventV1
from kairos_core.topics import Topics

from kairos_quant.config import QuantSettings
from kairos_quant.service import QuantScoutsService


class _RecordingBus:
    def __init__(self) -> None:
        self.messages = []

    async def publish(self, topic, message):
        self.messages.append((topic, message))


class _FailingBus:
    async def publish(self, topic, message):
        raise RuntimeError("bus unavailable")


class _InjectingBus:
    def __init__(self, collector) -> None:
        self.collector = collector

    async def publish(self, topic, message):
        self.collector._on_message(
            {
                "stream": "btcusdt@forceOrder",
                "data": {"o": {"S": "SELL", "ap": "50", "z": "1"}},
            }
        )


def _service() -> QuantScoutsService:
    settings = QuantSettings(
        bus_backend="memory",
        trading_symbols=["BTCUSDT"],
        book_stale_after_s=10,
        kline_stale_after_s=90,
    )
    service = QuantScoutsService(settings)
    service.bus = _RecordingBus()
    service.collector._clock = lambda: 100.0
    service.collector._wall_clock = lambda: 60.0
    service.collector._on_message({"stream": "btcusdt@markPrice@1s", "data": {"E": 60_000, "r": "0.0001"}})
    service.collector.open_interest["btcusdt"] = 1_000.0
    service.collector._open_interest_updated_at["btcusdt"] = 100.0
    return service


def _depth_message(*, bid: str = "100", ask: str = "102", event_time_ms: int = 60_000) -> dict:
    return {
        "stream": "btcusdt@depth10@100ms",
        "data": {"E": event_time_ms, "u": 1, "b": [[bid, "2"]], "a": [[ask, "3"]]},
    }


def _kline_message(
    *,
    closed: bool,
    close_time_ms: int = 59_999,
    close: str = "95",
    high: str = "100",
    low: str = "90",
    quote_volume: str = "2500",
) -> dict:
    return {
        "stream": "btcusdt@kline_1m",
        "data": {
            "k": {
                "x": closed,
                "t": close_time_ms - 59_999,
                "T": close_time_ms,
                "o": close,
                "h": high,
                "l": low,
                "c": close,
                "v": "25",
                "q": quote_volume,
                "V": "10",
                "Q": str(float(quote_volume) * 0.4),
            }
        },
    }


def _confirm_kline(service: QuantScoutsService, message: dict) -> bool:
    service.collector._on_message(message)
    kline = message["data"]["k"]
    arguments = (
        "btcusdt",
        int(kline["t"]),
        int(kline["T"]),
        float(kline["o"]),
        float(kline["h"]),
        float(kline["l"]),
        float(kline["c"]),
        float(kline["v"]),
        float(kline["q"]),
        float(kline["V"]),
        float(kline["Q"]),
    )
    assert not service.collector._observe_rest_closed_kline(*arguments)
    return service.collector._observe_rest_closed_kline(*arguments)


def test_emit_skips_incomplete_order_book():
    service = _service()
    _confirm_kline(service, _kline_message(closed=True))
    service.collector.books["btcusdt"] = {"bids": [(100.0, 1.0)], "asks": []}

    asyncio.run(service._emit_once())

    assert len(service.bus.messages) == 1
    topic, bar = service.bus.messages[0]
    assert topic == Topics.CLOSED_BAR
    assert isinstance(bar, ClosedBarEventV1)


def test_emit_requires_a_closed_kline():
    service = _service()
    service.collector._on_message(_depth_message())
    service.collector._on_message(_kline_message(closed=False))

    asyncio.run(service._emit_once())

    assert service.bus.messages == []


def test_emit_blocks_entire_symbol_until_gap_is_backfilled():
    service = _service()
    service.collector._wall_clock = lambda: 180.0
    _confirm_kline(service, _kline_message(closed=True, close_time_ms=59_999, close="95"))
    _confirm_kline(service, _kline_message(closed=True, close_time_ms=179_999, close="97"))

    asyncio.run(service._emit_once())

    assert service.bus.messages == []
    assert len(service.collector.pending_closed_klines("btcusdt")) == 1

    _confirm_kline(service, _kline_message(closed=True, close_time_ms=119_999, close="96"))
    asyncio.run(service._emit_once())

    assert [topic for topic, _ in service.bus.messages] == [Topics.CLOSED_BAR] * 3
    assert [message.close_time_ms for _, message in service.bus.messages] == [59_999, 119_999, 179_999]


def test_emit_blocks_conflicting_final_bar():
    service = _service()
    service.collector._wall_clock = lambda: 120.0
    _confirm_kline(service, _kline_message(closed=True, close="95"))
    _confirm_kline(service, _kline_message(closed=True, close="96"))

    asyncio.run(service._emit_once())

    assert service.bus.messages == []
    assert service.collector.kline_integrity_reason("btcusdt") == "conflicting_closed_bar"


def test_emit_uses_closed_kline_for_indicators_and_current_book_for_mid_price():
    service = _service()
    service.collector._on_message(_depth_message(bid="100", ask="102"))
    _confirm_kline(service, _kline_message(closed=True, close="95", quote_volume="3000"))
    service.collector.open_interest["btcusdt"] = 12345.0
    service.collector.oi_change_pct_1h["btcusdt"] = 2.5
    service.collector._on_message(
        {
            "stream": "btcusdt@forceOrder",
            "data": {"o": {"S": "SELL", "ap": "100", "z": "2"}},
        }
    )

    asyncio.run(service._emit_once())

    assert list(service.builder._closes["BTCUSDT"]) == [95.0]
    assert [topic for topic, _ in service.bus.messages] == [Topics.CLOSED_BAR, Topics.MARKET_SNAPSHOT]
    bar = service.bus.messages[0][1]
    assert isinstance(bar, ClosedBarEventV1)
    assert bar.open == 95.0
    assert bar.base_volume == 25.0
    assert bar.taker_buy_base_volume == 10.0
    assert bar.bar_sha256 == bar.message_id
    snapshot = service.bus.messages[1][1]
    assert snapshot.mid_price == 101.0
    assert snapshot.volume_usd == 3000.0
    assert snapshot.derivatives.open_interest == 12345.0
    assert snapshot.derivatives.oi_change_pct_1h == 2.5
    assert snapshot.indicators.atr_pct is None
    assert snapshot.derivatives.long_liquidations_usd == 200.0
    assert snapshot.derivatives.short_liquidations_usd == 0.0


def test_emit_rejects_stale_book_without_losing_closed_kline():
    now = [100.0]
    service = _service()
    service.collector._clock = lambda: now[0]
    service.collector._on_message(_depth_message())
    _confirm_kline(service, _kline_message(closed=True))
    now[0] = 111.0

    asyncio.run(service._emit_once())

    assert [topic for topic, _ in service.bus.messages] == [Topics.CLOSED_BAR]
    assert list(service.builder._closes["BTCUSDT"]) == [95.0]


def test_emit_rejects_stale_derivatives_even_with_fresh_book_and_kline():
    now = [281.0]
    service = _service()
    service.collector._clock = lambda: now[0]
    service.collector._wall_clock = lambda: 180.0
    service.collector._on_message(_depth_message(event_time_ms=180_000))
    _confirm_kline(
        service,
        {
            "stream": "btcusdt@kline_1m",
            "data": {
                "k": {
                    "x": True,
                    "t": 120_000,
                    "T": 179_999,
                    "o": "100",
                    "h": "101",
                    "l": "99",
                    "c": "100",
                    "v": "10",
                    "q": "1000",
                    "V": "5",
                    "Q": "500",
                }
            },
        },
    )

    asyncio.run(service._emit_once())

    assert [topic for topic, _ in service.bus.messages] == [Topics.CLOSED_BAR]


def test_emit_resumes_after_derivative_observations_are_refreshed():
    now = [281.0]
    service = _service()
    service.collector._clock = lambda: now[0]
    service.collector._wall_clock = lambda: 180.0
    service.collector._on_message(_depth_message(event_time_ms=180_000))
    _confirm_kline(
        service,
        {
            "stream": "btcusdt@kline_1m",
            "data": {
                "k": {
                    "x": True,
                    "t": 120_000,
                    "T": 179_999,
                    "o": "100",
                    "h": "101",
                    "l": "99",
                    "c": "100",
                    "v": "10",
                    "q": "1000",
                    "V": "5",
                    "Q": "500",
                }
            },
        },
    )
    service.collector._on_message({"stream": "btcusdt@markPrice@1s", "data": {"E": 180_000, "r": "0.0002"}})
    service.collector._open_interest_updated_at["btcusdt"] = now[0]

    asyncio.run(service._emit_once())

    assert [topic for topic, _ in service.bus.messages] == [Topics.CLOSED_BAR, Topics.MARKET_SNAPSHOT]


def test_emit_keeps_liquidations_when_publish_fails():
    service = _service()
    service.bus = _FailingBus()
    service.collector._on_message(_depth_message())
    _confirm_kline(service, _kline_message(closed=True))
    service.collector._on_message(
        {
            "stream": "btcusdt@forceOrder",
            "data": {"o": {"S": "SELL", "ap": "100", "z": "2"}},
        }
    )

    try:
        asyncio.run(service._emit_once())
    except RuntimeError as exc:
        assert str(exc) == "bus unavailable"
    else:
        raise AssertionError("publish failure was not propagated")

    assert service.collector.liquidation_totals("btcusdt").long_usd == 200.0
    assert [item.close_time_ms for item in service.collector.pending_closed_klines("btcusdt")] == [59_999]


def test_emit_does_not_clear_liquidations_arriving_during_publish():
    service = _service()
    service.bus = _InjectingBus(service.collector)
    service.collector._on_message(_depth_message())
    _confirm_kline(service, _kline_message(closed=True))
    service.collector._on_message(
        {
            "stream": "btcusdt@forceOrder",
            "data": {"o": {"S": "SELL", "ap": "100", "z": "2"}},
        }
    )

    asyncio.run(service._emit_once())

    assert service.collector.liquidation_totals("btcusdt").long_usd == 50.0


def test_restore_rebuilds_indicator_window_without_duplicate_publish():
    service = _service()
    event = ClosedBarEventV1(
        source=service.settings.service_name,
        symbol="BTCUSDT",
        timeframe="1m",
        open_time_ms=0,
        close_time_ms=59_999,
        open=95.0,
        high=100.0,
        low=90.0,
        close=95.0,
        base_volume=25.0,
        quote_volume=2_500.0,
        taker_buy_base_volume=10.0,
        taker_buy_quote_volume=1_000.0,
    )

    service._restore_closed_bar_payloads([event.model_dump(mode="json")])

    assert service.collector.pending_closed_klines("btcusdt") == ()
    assert list(service.builder._closes["BTCUSDT"]) == [95.0]
    assert service._last_kline_close_time_ms == {"btcusdt": 59_999}
    assert not _confirm_kline(service, _kline_message(closed=True))
