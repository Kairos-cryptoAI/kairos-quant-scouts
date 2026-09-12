from copy import deepcopy

import pytest
from pydantic import ValidationError

from kairos_quant.collectors.binance_ws import BinanceFuturesCollector
from kairos_quant.config import QuantSettings
from kairos_quant.stream_routes import websocket_root


@pytest.mark.parametrize("suffix", ["", "/", "/stream", "/stream/"])
def test_legacy_configuration_always_dials_explicit_split_routes(suffix):
    collector = BinanceFuturesCollector(
        ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT"],
        "wss://fstream.binance.com" + suffix,
        "https://fapi.binance.com",
    )
    urls = collector._stream_urls()
    assert set(urls) == {"public", "market"}
    assert urls["public"].startswith("wss://fstream.binance.com/public/stream?streams=")
    assert urls["market"].startswith("wss://fstream.binance.com/market/stream?streams=")
    assert len(collector._streams("public").split("/")) == 5
    assert len(collector._streams("market").split("/")) == 15
    assert not collector._subscriptions["public"] & collector._subscriptions["market"]


@pytest.mark.parametrize(
    "value",
    [
        "",
        "fstream.binance.com",
        "https://fstream.binance.com",
        "ws://fstream.binance.com",
        "wss://fstream.binance.com/public/stream",
        "wss://fstream.binance.com/market",
        "wss://fstream.binance.com/private",
        "wss://fstream.binance.com/stream?streams=x",
        "wss://fstream.binance.com/#x",
        "wss://user:password@example.invalid",
        "wss://example.invalid:99999",
        "wss://example.invalid:bad",
        "wss://",
        " wss://example.invalid",
        "wss://example.invalid/str\neam",
    ],
)
def test_invalid_base_rejected_before_startup(value):
    with pytest.raises(ValueError):
        websocket_root(value)
    with pytest.raises(ValidationError):
        QuantSettings(bus_backend="memory", binance_ws_base=value)


def test_loopback_transport_is_available_for_isolated_integration():
    assert websocket_root("ws://127.0.0.1:12345/stream") == "ws://127.0.0.1:12345"
    assert QuantSettings(bus_backend="memory").binance_ws_base == "wss://fstream.binance.com"


@pytest.mark.parametrize("symbol", ["", "btcusdt@depth", "btc/usdt", "btc?usdt", "btc-usdt", "btcσ"])
def test_symbol_cannot_inject_additional_subscriptions(symbol):
    with pytest.raises(ValueError):
        BinanceFuturesCollector([symbol], "wss://example.invalid", "https://example.invalid")


def _book():
    return {
        "stream": "btcusdt@depth10@100ms",
        "data": {
            "s": "BTCUSDT",
            "E": 60_000,
            "u": 1,
            "b": [["100", "2"]],
            "a": [["101", "3"]],
        },
    }


@pytest.mark.parametrize(
    "stream",
    [
        "btcusdt@depth",
        "btcusdt@depth20@100ms",
        "btcusdt@depth10@100ms-extra",
        "ethusdt@depth10@100ms",
        None,
        [],
        {},
    ],
)
def test_unsubscribed_stream_cannot_be_treated_as_top_n_snapshot(stream):
    collector = BinanceFuturesCollector(["BTCUSDT"], "wss://example.invalid", "https://example.invalid")
    message = _book()
    message["stream"] = stream
    assert not collector._on_message(message)
    assert collector.books["btcusdt"] == {"bids": [], "asks": []}


def test_payload_symbol_and_route_cannot_cross_contaminate_the_book():
    collector = BinanceFuturesCollector(["BTCUSDT"], "wss://example.invalid", "https://example.invalid")
    message = _book()
    assert not collector._on_message(message, route="market")
    wrong = deepcopy(message)
    wrong["data"]["s"] = "ETHUSDT"
    assert not collector._on_message(wrong, route="public")
    assert collector._on_message(message, route="public")


@pytest.mark.parametrize("stream,body", [("btcusdt@kline_1m", "k"), ("btcusdt@forceOrder", "o")])
def test_nested_symbol_conflict_is_rejected(stream, body):
    collector = BinanceFuturesCollector(["BTCUSDT"], "wss://example.invalid", "https://example.invalid")
    assert not collector._on_message({"stream": stream, "data": {body: {"s": "ETHUSDT"}}}, route="market")


def test_invalidation_retains_book_id_and_requires_new_snapshot():
    collector = BinanceFuturesCollector(
        ["BTCUSDT"],
        "wss://example.invalid",
        "https://example.invalid",
        clock=lambda: 100.0,
        wall_clock=lambda: 60.0,
    )
    message = _book()
    assert collector._on_message(message)
    assert collector.is_book_fresh("BTCUSDT", 10)
    collector._invalidate_books()
    assert not collector.is_book_fresh("BTCUSDT", 10)
    assert collector.books["btcusdt"] == {"bids": [], "asks": []}
    assert not collector._on_message(message)
    assert not collector.is_book_fresh("BTCUSDT", 10)
    message["data"]["u"] = 37  # Coalesced update IDs need not increment by one.
    assert collector._on_message(message)
    assert collector.is_book_fresh("BTCUSDT", 10)
