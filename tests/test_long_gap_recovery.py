"""Long-gap repair is lossless, bounded, fail-closed and transport independent."""

import asyncio
import json
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest

from kairos_quant import long_gap_recovery as recovery
from kairos_quant.long_gap_recovery import MINUTE, load_anchor, parse_page, recover_symbol


def rows(start, end):
    return [
        [t, "100", "102", "99", "101", "20", t + 59999, "2000", 1, "10", "1000"]
        for t in range(start, end, MINUTE)
    ]


def anchor(t=0):
    return parse_page(rows(t, t + MINUTE), symbol="BTCUSDT", start=t, end=t + MINUTE)[0]


async def no_pause():
    return None


def test_many_pages_keep_every_bar_and_match_contract_ids():
    published, requests, progress = [], [], []

    async def fetch(symbol, start, end):
        requests.append((start, end))
        return rows(start, end)

    async def publish(event):
        published.append(event)

    total = asyncio.run(
        recover_symbol(
            anchor(),
            end_exclusive=17 * MINUTE,
            fetch=fetch,
            publish=publish,
            page_size=4,
            pause=no_pause,
            progress=progress.append,
        )
    )
    assert total == 16
    assert [event.open_time_ms for event in published] == list(range(MINUTE, 17 * MINUTE, MINUTE))
    assert [event.message_id for event in published] == [
        anchor(t).message_id for t in range(MINUTE, 17 * MINUTE, MINUTE)
    ]
    assert all(requests[i] == requests[i + 1] for i in range(0, len(requests), 2))
    assert all(end - start <= 4 * MINUTE for start, end in requests)
    assert progress[-1]["through_exclusive_ms"] == 17 * MINUTE


@pytest.mark.parametrize("change", ["mismatch", "gap", "duplicate", "reorder", "nan", "boolean"])
def test_bad_confirmation_publishes_nothing(change):
    calls, published = [], []

    async def fetch(symbol, start, end):
        page = rows(start, end)
        calls.append(1)
        if len(calls) == 2:
            if change == "mismatch":
                page[-1][4] = "100.5"
            if change == "gap":
                page.pop()
            if change == "duplicate":
                page[-1] = page[0]
            if change == "reorder":
                page.reverse()
            if change == "nan":
                page[-1][4] = "nan"
            if change == "boolean":
                page[-1][5] = True
        return page

    async def publish(event):
        published.append(event)

    with pytest.raises(ValueError):
        asyncio.run(
            recover_symbol(
                anchor(),
                end_exclusive=4 * MINUTE,
                fetch=fetch,
                publish=publish,
                pause=no_pause,
                progress=lambda _: None,
            )
        )
    assert published == []


def test_anchor_conflict_is_not_overwritten():
    async def fetch(symbol, start, end):
        page = rows(start, end)
        page[0][4] = "100.5"
        return page

    async def publish(event):
        pytest.fail("anchor conflict must prevent all writes")

    with pytest.raises(ValueError, match="anchor conflict"):
        asyncio.run(
            recover_symbol(
                anchor(),
                end_exclusive=2 * MINUTE,
                fetch=fetch,
                publish=publish,
                pause=no_pause,
                progress=lambda _: None,
            )
        )


def test_restart_after_unknown_publish_ack_uses_durable_anchor():
    committed = {0: anchor()}
    fail = True

    async def fetch(symbol, start, end):
        return rows(start, end)

    async def publish(event):
        nonlocal fail
        assert event.open_time_ms not in committed
        committed[event.open_time_ms] = event
        if fail and event.open_time_ms == 3 * MINUTE:
            fail = False
            raise RuntimeError("commit succeeded, caller lost ACK")

    async def run():
        await recover_symbol(
            committed[max(committed)],
            end_exclusive=10 * MINUTE,
            fetch=fetch,
            publish=publish,
            page_size=4,
            pause=no_pause,
            progress=lambda _: None,
        )

    with pytest.raises(RuntimeError, match="ACK"):
        asyncio.run(run())
    asyncio.run(run())
    assert sorted(committed) == list(range(0, 10 * MINUTE, MINUTE))


def test_read_only_anchor_rejects_preexisting_gap():
    async def fetch(*args):
        return [{"payload": anchor(t).model_dump_json()} for t in (0, 2 * MINUTE)]

    bus = SimpleNamespace(repository=SimpleNamespace(pool=SimpleNamespace(fetch=fetch)))
    with pytest.raises(ValueError, match="contiguous"):
        asyncio.run(load_anchor(bus, "BTCUSDT"))


@pytest.mark.parametrize("acquired", [False, True])
def test_producer_lease_excludes_second_writer_and_releases_on_error(monkeypatch, acquired):
    from kairos_quant import producer_lease as module

    events = []

    class Connection:
        async def fetchval(self, *args):
            events.append("try_lock")
            return acquired

        async def execute(self, *args):
            events.append("unlock")

    class Pool:
        @asynccontextmanager
        async def acquire(self):
            yield Connection()

    class Bus:
        service_name = "kairos-quant-scouts"
        repository = SimpleNamespace(pool=Pool())

        async def start(self):
            events.append("start")

    monkeypatch.setattr(module, "DurableMessageBus", Bus)

    async def run():
        async with module.producer_lease(Bus()):
            events.append("body")
            raise RuntimeError("simulated failure")

    with pytest.raises(RuntimeError):
        asyncio.run(run())
    assert events == (["start", "try_lock", "body", "unlock"] if acquired else ["start", "try_lock"])


class RecoveryHarness:
    """Real orchestration with fake transport/store; no DB or network access."""

    def __init__(self, monkeypatch, fault=None, *, close_fault=False):
        self.fault = fault
        self.close_fault = close_fault
        self.requests = []
        self.publications = []
        self.closes = 0
        self.committed = {
            symbol: {0: parse_page(rows(0, MINUTE), symbol=symbol, start=0, end=MINUTE)[0]}
            for symbol in recovery.SYMBOLS
        }
        harness = self

        class Bus:
            repository = SimpleNamespace(pool=SimpleNamespace(fetch=self.restore))

            async def publish(self, topic, event):
                assert topic == recovery.Topics.CLOSED_BAR
                harness.publications.append((event.symbol, event.open_time_ms))
                assert event.open_time_ms not in harness.committed[event.symbol]
                harness.committed[event.symbol][event.open_time_ms] = event
                if harness.fault == "publish" and event.open_time_ms == 2 * MINUTE:
                    raise TimeoutError("secret://publish-result-unknown")

            async def close(self):
                harness.closes += 1
                if harness.close_fault:
                    raise TimeoutError("secret://close-timeout")

        self.bus = Bus()

        class Response:
            def __init__(self, params):
                self.params = params

            async def __aenter__(self):
                if harness.fault == "fetch_first":
                    raise TimeoutError("secret://first-REST-timeout")
                if harness.fault == "fetch_second" and len(harness.requests) == 2:
                    raise TimeoutError("secret://second-REST-timeout")
                return self

            async def __aexit__(self, *args):
                return False

            def raise_for_status(self):
                pass

            async def json(self):
                page = rows(self.params["startTime"], self.params["endTime"] + 1)
                if harness.fault == "incomplete":
                    page.pop()
                if harness.fault == "conflict" and len(harness.requests) == 2:
                    page[-1][4] = "100.5"
                return page

        class Session:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                return False

            def get(self, url, *, params):
                harness.requests.append(params)
                return Response(params)

        @asynccontextmanager
        async def lease(bus):
            if harness.fault == "lease":
                raise TimeoutError("secret://lease-timeout")
            yield
            if harness.fault == "lease_release":
                raise TimeoutError("secret://lease-release-timeout")

        async def pause(_seconds):
            if harness.fault == "pause":
                raise TimeoutError("secret://pause-timeout")

        monkeypatch.setattr(
            recovery,
            "QuantSettings",
            lambda: SimpleNamespace(
                environment="paper",
                bus_backend="redis",
                service_name=recovery.SOURCE,
                symbols=recovery.SYMBOLS,
                binance_rest_base="https://fapi.binance.com",
                kline_finality_delay_s=5,
            ),
        )
        monkeypatch.setattr(recovery, "build_bus", lambda settings: object())
        monkeypatch.setattr(recovery, "DurableMessageBus", lambda *args, **kwargs: self.bus)
        monkeypatch.setattr(recovery, "producer_lease", lease)
        monkeypatch.setattr(recovery.aiohttp, "ClientSession", lambda **kwargs: Session())
        monkeypatch.setattr(recovery.asyncio, "sleep", pause)

    async def restore(self, _sql, _topic, _source, symbol):
        if self.fault == "restore_read":
            raise TimeoutError("secret://restore-timeout")
        return [{"payload": event.model_dump_json()} for event in self.committed[symbol].values()]

    def run(self, messages):
        status = recovery.RecoveryStatus(messages.append)
        asyncio.run(recovery.run_recovery(4 * MINUTE, 100, status=status))


@pytest.mark.parametrize(
    ("fault", "expected_phase", "expected_requests"),
    [
        ("lease", "PRODUCER_LEASE_ACQUIRE", 0),
        ("restore_read", "RESTORE_READ", 0),
        ("fetch_first", "REST_GET_FIRST", 1),
        ("pause", "CONFIRMATION_DELAY", 1),
        ("fetch_second", "REST_GET_SECOND", 2),
    ],
)
def test_timeout_reports_exact_operation_before_publish_without_retry(
    monkeypatch, fault, expected_phase, expected_requests
):
    harness = RecoveryHarness(monkeypatch, fault)
    messages = []
    with pytest.raises(TimeoutError):
        harness.run(messages)
    assert messages[0]["state"] == "STARTED"
    failure = messages[-1]
    assert failure["state"] == "FAILED"
    assert failure["phase"] == failure["first_failure_phase"] == expected_phase
    assert failure["error_type"] == "TimeoutError"
    assert failure["total_appended_bars"] == 0
    assert len(harness.requests) == expected_requests
    assert harness.publications == []
    assert harness.closes == 1
    assert "secret://" not in json.dumps(messages)
    if fault in {"fetch_first", "pause", "fetch_second"}:
        assert failure["symbol"] == "BTCUSDT"
        assert failure["window_start_ms"] == 0
        assert failure["window_end_exclusive_ms"] == 4 * MINUTE
        assert failure["last_progress_at_utc"] is not None


def test_publish_timeout_reports_unknown_ack_and_exact_confirmed_prefix_then_resumes(monkeypatch):
    harness = RecoveryHarness(monkeypatch, "publish")
    messages = []
    with pytest.raises(TimeoutError):
        harness.run(messages)
    failure = messages[-1]
    assert failure["phase"] == "DURABLE_PUBLISH"
    assert failure["publish_outcome"] == "UNKNOWN"
    assert failure["window_start_ms"] == 2 * MINUTE
    assert failure["confirmed_through_exclusive_ms"]["BTCUSDT"] == 2 * MINUTE
    assert failure["total_appended_bars"] == 1
    assert failure["last_progress_at_utc"] is not None
    assert harness.publications == [("BTCUSDT", MINUTE), ("BTCUSDT", 2 * MINUTE)]
    assert "secret://" not in json.dumps(messages)

    # The unknown result actually committed. Only a new, explicitly invoked
    # run reads that durable prefix; the failed invocation never retries it.
    harness.fault = None
    resumed = []
    harness.run(resumed)
    assert resumed[-1]["state"] == "COMPLETED"
    assert resumed[-1]["phase"] == "FINISHED"
    assert resumed[-1]["total_appended_bars"] == 13
    assert resumed[-1]["confirmed_through_exclusive_ms"] == {
        symbol: 4 * MINUTE for symbol in recovery.SYMBOLS
    }
    assert len(harness.publications) == len(set(harness.publications)) == 15
    assert all(
        sorted(history) == list(range(0, 4 * MINUTE, MINUTE)) for history in harness.committed.values()
    )
    assert all("phase" in item and "last_progress_at_utc" in item for item in resumed)
    assert any(item["state"] == "PROGRESS" and item["phase"] == "PAGE_COMMITTED" for item in resumed)


def test_restore_validation_failure_never_fetches_or_changes_the_existing_prefix(monkeypatch):
    harness = RecoveryHarness(monkeypatch)
    harness.committed["BTCUSDT"][2 * MINUTE] = anchor(2 * MINUTE)
    before = dict(harness.committed["BTCUSDT"])
    messages = []
    with pytest.raises(recovery.RecoveryError, match="contiguous"):
        harness.run(messages)
    assert messages[-1]["phase"] == "RESTORE_VALIDATE"
    assert messages[-1]["symbol"] == "BTCUSDT"
    assert harness.requests == harness.publications == []
    assert harness.committed["BTCUSDT"] == before


@pytest.mark.parametrize(
    ("fault", "phase", "request_count"),
    [("incomplete", "REST_VALIDATE_FIRST", 1), ("conflict", "CONFIRMATION_COMPARE", 2)],
)
def test_fatal_integrity_error_is_not_retried_or_published(monkeypatch, fault, phase, request_count):
    harness = RecoveryHarness(monkeypatch, fault)
    messages = []
    with pytest.raises(recovery.RecoveryError):
        harness.run(messages)
    assert messages[-1]["phase"] == phase
    assert messages[-1]["state"] == "FAILED"
    assert len(harness.requests) == request_count
    assert harness.publications == []
    assert not any(item["state"] == "COMPLETED" for item in messages)


def test_close_failure_does_not_replace_the_original_failure_phase(monkeypatch):
    harness = RecoveryHarness(monkeypatch, "fetch_first", close_fault=True)
    messages = []
    with pytest.raises(TimeoutError, match="first-REST"):
        harness.run(messages)
    failures = [item for item in messages if item["state"] == "FAILED"]
    assert [item["phase"] for item in failures] == ["REST_GET_FIRST", "BUS_CLOSE"]
    assert all(item["first_failure_phase"] == "REST_GET_FIRST" for item in failures)
    assert "secret://" not in json.dumps(messages)


@pytest.mark.parametrize("fault", ["lease_release", "close"])
def test_cleanup_must_finish_before_success_is_reported(monkeypatch, fault):
    harness = RecoveryHarness(monkeypatch, fault, close_fault=fault == "close")
    messages = []
    with pytest.raises(TimeoutError):
        harness.run(messages)
    assert not any(item["state"] == "COMPLETED" for item in messages)
    expected = "BUS_CLOSE" if fault == "close" else "PRODUCER_LEASE_RELEASE"
    assert messages[-1]["phase"] == expected
    assert messages[-1]["total_appended_bars"] == 15
    assert len(harness.publications) == 15
