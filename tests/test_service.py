"""Regression tests for Quant Scouts service lifecycle."""

import asyncio

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
                "T": 59_999,
                "h": high,
                "l": low,
                "c": close,
                "q": quote_volume,
            }
        },
    }


def test_emit_skips_incomplete_order_book():
    service = _service()
    service.collector._on_message(_kline_message(closed=True))
    service.collector.books["btcusdt"] = {"bids": [(100.0, 1.0)], "asks": []}

    asyncio.run(service._emit_once())

    assert service.bus.messages == []


def test_emit_requires_a_closed_kline():
    service = _service()
    service.collector._on_message(_depth_message())
    service.collector._on_message(_kline_message(closed=False))

    asyncio.run(service._emit_once())

    assert service.bus.messages == []


def test_emit_uses_closed_kline_for_indicators_and_current_book_for_mid_price():
    service = _service()
    service.collector._on_message(_depth_message(bid="100", ask="102"))
    service.collector._on_message(_kline_message(closed=True, close="95", quote_volume="3000"))
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
    assert len(service.bus.messages) == 1
    snapshot = service.bus.messages[0][1]
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
    service.collector._on_message(_kline_message(closed=True))
    now[0] = 111.0

    asyncio.run(service._emit_once())

    assert service.bus.messages == []
    assert list(service.builder._closes["BTCUSDT"]) == [95.0]


def test_emit_rejects_stale_derivatives_even_with_fresh_book_and_kline():
    now = [281.0]
    service = _service()
    service.collector._clock = lambda: now[0]
    service.collector._wall_clock = lambda: 180.0
    service.collector._on_message(_depth_message(event_time_ms=180_000))
    service.collector._on_message(
        {
            "stream": "btcusdt@kline_1m",
            "data": {
                "k": {
                    "x": True,
                    "T": 179_999,
                    "h": "101",
                    "l": "99",
                    "c": "100",
                    "q": "1000",
                }
            },
        }
    )

    asyncio.run(service._emit_once())

    assert service.bus.messages == []


def test_emit_resumes_after_derivative_observations_are_refreshed():
    now = [281.0]
    service = _service()
    service.collector._clock = lambda: now[0]
    service.collector._wall_clock = lambda: 180.0
    service.collector._on_message(_depth_message(event_time_ms=180_000))
    service.collector._on_message(
        {
            "stream": "btcusdt@kline_1m",
            "data": {
                "k": {
                    "x": True,
                    "T": 179_999,
                    "h": "101",
                    "l": "99",
                    "c": "100",
                    "q": "1000",
                }
            },
        }
    )
    service.collector._on_message({"stream": "btcusdt@markPrice@1s", "data": {"E": 180_000, "r": "0.0002"}})
    service.collector._open_interest_updated_at["btcusdt"] = now[0]

    asyncio.run(service._emit_once())

    assert len(service.bus.messages) == 1


def test_emit_keeps_liquidations_when_publish_fails():
    service = _service()
    service.bus = _FailingBus()
    service.collector._on_message(_depth_message())
    service.collector._on_message(_kline_message(closed=True))
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


def test_emit_does_not_clear_liquidations_arriving_during_publish():
    service = _service()
    service.bus = _InjectingBus(service.collector)
    service.collector._on_message(_depth_message())
    service.collector._on_message(_kline_message(closed=True))
    service.collector._on_message(
        {
            "stream": "btcusdt@forceOrder",
            "data": {"o": {"S": "SELL", "ap": "100", "z": "2"}},
        }
    )

    asyncio.run(service._emit_once())

    assert service.collector.liquidation_totals("btcusdt").long_usd == 50.0
