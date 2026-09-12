"""Network-free lifecycle coverage for the public/market WebSocket split."""

import asyncio
import json
from collections import defaultdict
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit

import aiohttp
import pytest

from kairos_quant.collectors import binance_ws
from kairos_quant.collectors.binance_ws import BinanceFuturesCollector


class FakeWebSocket:
    def __init__(self):
        self.messages = asyncio.Queue()
        self.closed = asyncio.Event()
        self.close_started = asyncio.Event()
        self.close_release = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        self.close_started.set()
        try:
            if self.close_release is not None:
                await self.close_release.wait()
        finally:
            self.closed.set()

    def __aiter__(self):
        return self

    async def __anext__(self):
        item = await self.messages.get()
        if isinstance(item, BaseException):
            raise item
        return item

    def send(self, payload):
        self.messages.put_nowait(SimpleNamespace(type=aiohttp.WSMsgType.TEXT, data=json.dumps(payload)))

    def disconnect(self):
        self.messages.put_nowait(SimpleNamespace(type=aiohttp.WSMsgType.CLOSE, data=None))


class FakeSession:
    def __init__(self, factory):
        self.factory = factory
        self.closed = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        self.closed = True

    def ws_connect(self, url, **kwargs):
        route = urlsplit(url).path.split("/")[-2]
        assert route in {"public", "market"}, f"Unexpected WebSocket route: {url}"
        socket = FakeWebSocket()
        self.factory.urls.append(url)
        self.factory.sockets[route].append(socket)
        self.factory.connected[(route, len(self.factory.sockets[route]))].set()
        return socket

    def get(self, *args, **kwargs):
        raise AssertionError("Unexpected REST request in a network-free WebSocket test")


class FakeSessions:
    def __init__(self):
        self.instances = []
        self.urls = []
        self.sockets = defaultdict(list)
        self.connected = defaultdict(asyncio.Event)

    def __call__(self, **kwargs):
        session = FakeSession(self)
        self.instances.append(session)
        return session

    async def connection(self, route, attempt=1):
        await asyncio.wait_for(self.connected[(route, attempt)].wait(), timeout=2)
        return self.sockets[route][attempt - 1]

    def assert_closed(self):
        assert self.instances
        assert all(session.closed for session in self.instances)
        assert all(socket.closed.is_set() for sockets in self.sockets.values() for socket in sockets)


def collector(**overrides):
    return BinanceFuturesCollector(
        ["BTCUSDT"],
        "wss://example.invalid/stream",
        "https://example.invalid",
        reconnect_initial_s=0.001,
        reconnect_max_s=0.004,
        clock=lambda: 100.0,
        wall_clock=lambda: 1_000.0,
        **overrides,
    )


async def no_backfill(session):
    return None


def install_fakes(monkeypatch, subject):
    sessions = FakeSessions()
    monkeypatch.setattr(binance_ws.aiohttp, "ClientSession", sessions)
    monkeypatch.setattr(subject, "refresh_klines", no_backfill)
    return sessions


async def cancel_and_join(task):
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=2)


def depth_message(update_id=10):
    return {
        "stream": "btcusdt@depth10@100ms",
        "data": {"s": "BTCUSDT", "E": 1_000_000, "u": update_id, "b": [["100", "2"]], "a": [["101", "3"]]},
    }


def track_messages(monkeypatch, subject, count):
    original = subject._on_message
    processed = []
    completed = asyncio.Event()

    def observe(message, *, route=None):
        accepted = original(message, route=route)
        processed.append((route, accepted))
        if len(processed) >= count:
            completed.set()
        return accepted

    monkeypatch.setattr(subject, "_on_message", observe)
    return completed, processed


@pytest.mark.parametrize("base", ["wss://example.invalid", "wss://example.invalid/stream"])
def test_stream_urls_partition_exact_subscriptions_for_root_and_legacy_base(base):
    subject = BinanceFuturesCollector(["BTCUSDT", "ETHUSDT"], base, "https://example.invalid")
    urls = subject._stream_urls()
    assert set(urls) == {"public", "market"}
    expected = {
        "public": {"btcusdt@depth10@100ms", "ethusdt@depth10@100ms"},
        "market": {
            f"{symbol}@{stream}"
            for symbol in ("btcusdt", "ethusdt")
            for stream in ("markPrice@1s", "kline_1m", "forceOrder")
        },
    }
    for route, url in urls.items():
        parsed = urlsplit(url)
        assert (parsed.scheme, parsed.netloc, parsed.path) == (
            "wss",
            "example.invalid",
            f"/{route}/stream",
        )
        streams = parse_qs(parsed.query)["streams"][0].split("/")
        assert len(streams) == len(expected[route])
        assert set(streams) == expected[route]
        assert set(subject._streams(route).split("/")) == expected[route]


async def test_both_routes_connect_and_cancellation_closes_every_session(monkeypatch):
    subject = collector()
    sessions = install_fakes(monkeypatch, subject)
    task = asyncio.create_task(subject.run())
    try:
        await sessions.connection("public")
        await sessions.connection("market")
        assert len(sessions.instances) == 2
        assert set(sessions.urls) == set(subject._stream_urls().values())
    finally:
        await cancel_and_join(task)
    sessions.assert_closed()


async def test_public_depth_continues_while_market_startup_backfill_waits(monkeypatch):
    subject = collector()
    sessions = install_fakes(monkeypatch, subject)
    backfill_entered = asyncio.Event()
    backfill_cancelled = asyncio.Event()
    never_finished = asyncio.Event()
    calls = []

    async def blocked_backfill(session):
        calls.append(session)
        backfill_entered.set()
        try:
            await never_finished.wait()
        finally:
            backfill_cancelled.set()

    monkeypatch.setattr(subject, "refresh_klines", blocked_backfill)
    processed, observations = track_messages(monkeypatch, subject, 1)
    task = asyncio.create_task(subject.run())
    try:
        public = await sessions.connection("public")
        await sessions.connection("market")
        await asyncio.wait_for(backfill_entered.wait(), timeout=2)
        public.send(depth_message())
        await asyncio.wait_for(processed.wait(), timeout=2)
        assert observations == [("public", True)]
        assert subject.is_book_fresh("btcusdt", 10)
        assert len(calls) == 1
    finally:
        await cancel_and_join(task)
    assert backfill_cancelled.is_set()
    sessions.assert_closed()


@pytest.mark.parametrize("broken_route", ["public", "market"])
async def test_only_disconnected_route_reconnects(monkeypatch, broken_route):
    subject = collector()
    sessions = install_fakes(monkeypatch, subject)
    task = asyncio.create_task(subject.run())
    try:
        public = await sessions.connection("public")
        market = await sessions.connection("market")
        assert subject._on_message(depth_message(), route="public")
        peer_route = "market" if broken_route == "public" else "public"
        peer = market if broken_route == "public" else public
        sessions.sockets[broken_route][0].disconnect()
        await sessions.connection(broken_route, 2)
        assert len(sessions.sockets[peer_route]) == 1
        assert not peer.closed.is_set()
        assert sessions.sockets[broken_route][0].closed.is_set()
        if broken_route == "market":
            assert subject.is_book_fresh("btcusdt", 10)
            assert subject.books["btcusdt"]["bids"] == [(100.0, 2.0)]
    finally:
        await cancel_and_join(task)
    sessions.assert_closed()


async def test_public_disconnect_invalidates_book_until_new_nonreplayed_snapshot(monkeypatch):
    subject = collector()
    sessions = install_fakes(monkeypatch, subject)
    task = asyncio.create_task(subject.run())
    try:
        public = await sessions.connection("public")
        await sessions.connection("market")
        assert subject._on_message(depth_message(), route="public")
        assert subject.is_book_fresh("btcusdt", 10)
        public.disconnect()
        await asyncio.wait_for(public.closed.wait(), timeout=2)
        assert subject.books["btcusdt"] == {"bids": [], "asks": []}
        assert not subject.is_book_fresh("btcusdt", 10)
        assert subject._last_book_update_id["btcusdt"] == 10
        await sessions.connection("public", 2)
        assert not subject.is_book_fresh("btcusdt", 10)
        assert not subject._on_message(depth_message(), route="public")
        assert not subject.is_book_fresh("btcusdt", 10)
        assert subject._on_message(depth_message(11), route="public")
        assert subject.is_book_fresh("btcusdt", 10)
    finally:
        await cancel_and_join(task)
    sessions.assert_closed()


async def test_cancellation_invalidates_public_book_and_retains_sequence(monkeypatch):
    subject = collector()
    sessions = install_fakes(monkeypatch, subject)
    task = asyncio.create_task(subject.run())
    try:
        await sessions.connection("public")
        await sessions.connection("market")
        assert subject._on_message(depth_message(), route="public")
    finally:
        await cancel_and_join(task)
    assert subject.books["btcusdt"] == {"bids": [], "asks": []}
    assert subject._book_updated_at["btcusdt"] is None
    assert subject._book_event_time_ms["btcusdt"] is None
    assert subject._last_book_update_id["btcusdt"] == 10
    assert not subject.is_book_fresh("btcusdt", 10)
    sessions.assert_closed()


@pytest.mark.parametrize("fatal", [False, True])
async def test_public_book_is_invalid_before_slow_socket_close_finishes(monkeypatch, fatal):
    subject = collector()
    sessions = install_fakes(monkeypatch, subject)
    close_release = asyncio.Event()
    fatal_dispatched = False
    task = asyncio.create_task(subject.run())
    try:
        public = await sessions.connection("public")
        await sessions.connection("market")
        assert subject._on_message(depth_message(), route="public")
        assert subject.is_book_fresh("btcusdt", 10)
        public.close_release = close_release
        if fatal:
            public.messages.put_nowait(RuntimeError("synthetic fatal before slow close"))
            fatal_dispatched = True
        else:
            public.disconnect()
        await asyncio.wait_for(public.close_started.wait(), timeout=2)
        assert not public.closed.is_set()
        assert not task.done()
        assert subject.books["btcusdt"] == {"bids": [], "asks": []}
        assert subject._book_updated_at["btcusdt"] is None
        assert subject._book_event_time_ms["btcusdt"] is None
        assert subject._last_book_update_id["btcusdt"] == 10
        assert not subject.is_book_fresh("btcusdt", 10)
        close_release.set()
        if fatal:
            with pytest.raises(ExceptionGroup) as raised:
                await asyncio.wait_for(task, timeout=2)
            assert any(
                isinstance(error, RuntimeError) and str(error) == "synthetic fatal before slow close"
                for error in raised.value.exceptions
            )
        else:
            await sessions.connection("public", 2)
    finally:
        close_release.set()
        if not task.done():
            if fatal_dispatched:
                with pytest.raises(ExceptionGroup):
                    await asyncio.wait_for(task, timeout=2)
            else:
                await cancel_and_join(task)
    sessions.assert_closed()


async def test_market_payloads_are_consumed_without_promoting_provisional_kline(monkeypatch):
    subject = collector()
    sessions = install_fakes(monkeypatch, subject)
    processed, observations = track_messages(monkeypatch, subject, 3)
    task = asyncio.create_task(subject.run())
    try:
        await sessions.connection("public")
        market = await sessions.connection("market")
        market.send(
            {"stream": "btcusdt@markPrice@1s", "data": {"s": "BTCUSDT", "E": 1_000_000, "r": "0.001"}}
        )
        market.send(
            {
                "stream": "btcusdt@forceOrder",
                "data": {"E": 1_000_000, "o": {"s": "BTCUSDT", "S": "SELL", "ap": "100", "z": "2"}},
            }
        )
        market.send(
            {
                "stream": "btcusdt@kline_1m",
                "data": {
                    "s": "BTCUSDT",
                    "k": {
                        "s": "BTCUSDT",
                        "x": True,
                        "t": 900_000,
                        "T": 959_999,
                        "o": "100",
                        "h": "101",
                        "l": "99",
                        "c": "100",
                        "v": "1",
                        "q": "100",
                        "V": "0.5",
                        "Q": "50",
                    },
                },
            }
        )
        await asyncio.wait_for(processed.wait(), timeout=2)
        assert observations == [("market", True)] * 3
        assert subject.funding["btcusdt"] == 0.001
        assert subject.liquidation_totals("btcusdt").long_usd == 200
        assert 959_999 in subject._ws_closed_candidates["btcusdt"]
        assert subject.pending_closed_klines("btcusdt") == ()
    finally:
        await cancel_and_join(task)
    sessions.assert_closed()


async def test_wrong_route_payload_is_ignored_by_running_worker(monkeypatch):
    subject = collector()
    sessions = install_fakes(monkeypatch, subject)
    processed, observations = track_messages(monkeypatch, subject, 1)
    task = asyncio.create_task(subject.run())
    try:
        await sessions.connection("public")
        market = await sessions.connection("market")
        market.send(depth_message())
        await asyncio.wait_for(processed.wait(), timeout=2)
        assert observations == [("market", False)]
        assert subject.books["btcusdt"] == {"bids": [], "asks": []}
        assert subject._last_book_update_id["btcusdt"] is None
    finally:
        await cancel_and_join(task)
    sessions.assert_closed()


@pytest.mark.parametrize("fatal_route", ["public", "market"])
async def test_fatal_worker_error_cancels_peer_and_closes_sessions(monkeypatch, fatal_route):
    subject = collector()
    sessions = install_fakes(monkeypatch, subject)
    task = asyncio.create_task(subject.run())
    try:
        await sessions.connection("public")
        await sessions.connection("market")
        sessions.sockets[fatal_route][0].messages.put_nowait(RuntimeError("synthetic fatal WS failure"))
        with pytest.raises(ExceptionGroup) as raised:
            await asyncio.wait_for(task, timeout=2)
        assert any(
            isinstance(error, RuntimeError) and str(error) == "synthetic fatal WS failure"
            for error in raised.value.exceptions
        )
    finally:
        if not task.done():
            await cancel_and_join(task)
    sessions.assert_closed()
