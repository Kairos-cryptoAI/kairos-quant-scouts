"""Long-gap repair is lossless, bounded, fail-closed and transport independent."""

import asyncio
import json
from contextlib import asynccontextmanager
from datetime import UTC, datetime
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


def test_failure_observation_time_is_not_the_last_confirmed_progress_time(monkeypatch):
    times = iter(
        [
            datetime(2026, 9, 12, 13, 52, 14, tzinfo=UTC),
            datetime(2026, 9, 12, 13, 53, 4, tzinfo=UTC),
        ]
    )

    class Clock:
        @staticmethod
        def now(tz):
            assert tz is UTC
            return next(times)

    monkeypatch.setattr(recovery, "datetime", Clock)
    messages = []
    status = recovery.RecoveryStatus(messages.append)
    status.confirmed(anchor(), appended=True)
    status.enter("DURABLE_PUBLISH", symbol="BTCUSDT", start=MINUTE, end=2 * MINUTE)
    status.failed(TimeoutError("secret://unknown-commit"))
    assert messages[0]["last_progress_at_utc"] == "2026-09-12T13:52:14+00:00"
    assert messages[0]["observed_at_utc"] == "2026-09-12T13:53:04+00:00"
    assert messages[0]["publish_outcome"] == "UNKNOWN"
    assert messages[0]["confirmed_through_exclusive_ms"] == {"BTCUSDT": MINUTE}
    assert "secret://" not in json.dumps(messages)


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
        self.lifecycle = []
        self.writer_args = None
        self.committed = {
            symbol: {0: parse_page(rows(0, MINUTE), symbol=symbol, start=0, end=MINUTE)[0]}
            for symbol in recovery.SYMBOLS
        }
        harness = self

        class Writer:
            repository = SimpleNamespace(pool=SimpleNamespace(fetch=self.restore))

            async def start(self):
                harness.lifecycle.append("writer_start")
                if harness.fault == "writer_start":
                    raise TimeoutError("secret://writer-start-timeout")

            async def append(self, topic, event):
                assert topic == recovery.Topics.CLOSED_BAR
                harness.publications.append((event.symbol, event.open_time_ms))
                assert event.open_time_ms not in harness.committed[event.symbol]
                harness.committed[event.symbol][event.open_time_ms] = event
                if harness.fault == "publish" and event.open_time_ms == 2 * MINUTE:
                    raise TimeoutError("secret://publish-result-unknown")
                return True

            async def close(self):
                harness.closes += 1
                harness.lifecycle.append("writer_close")
                if harness.close_fault or harness.fault == "writer_close":
                    raise TimeoutError("secret://close-timeout")

        self.writer = Writer()

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
                harness.lifecycle.append("session_open")
                return self

            async def __aexit__(self, *args):
                harness.lifecycle.append("session_close")
                if harness.fault == "session_close":
                    raise TimeoutError("secret://session-close-timeout")
                return False

            def get(self, url, *, params):
                harness.requests.append(params)
                return Response(params)

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

        def offline_writer(*args, **kwargs):
            harness.writer_args = (args, kwargs)
            return self.writer

        monkeypatch.setattr(recovery, "OfflineDurableWriter", offline_writer, raising=False)
        monkeypatch.setattr(
            recovery,
            "build_bus",
            lambda _settings: pytest.fail("offline recovery must not build a bus"),
            raising=False,
        )
        monkeypatch.setattr(
            recovery,
            "DurableMessageBus",
            lambda *_args, **_kwargs: pytest.fail("offline recovery must not create a durable bus"),
            raising=False,
        )
        monkeypatch.setattr(
            recovery,
            "producer_lease",
            lambda *_args, **_kwargs: pytest.fail("offline recovery must not start a producer lease"),
            raising=False,
        )
        monkeypatch.setattr(recovery.aiohttp, "ClientSession", lambda **kwargs: Session())
        monkeypatch.setattr(recovery.asyncio, "sleep", pause)

    async def restore(self, _sql, _topic, _source, symbol):
        if self.fault == "restore_read":
            raise TimeoutError("secret://restore-timeout")
        return [{"payload": event.model_dump_json()} for event in self.committed[symbol].values()]

    def run(
        self,
        messages,
        *,
        end_exclusive=4 * MINUTE,
        maximum_bars=100,
        maximum_append_bars=None,
        expected_database_name="kairos",
    ):
        status = recovery.RecoveryStatus(messages.append)
        asyncio.run(
            recovery.run_recovery(
                end_exclusive,
                maximum_bars,
                expected_database_name=expected_database_name,
                maximum_append_bars=maximum_append_bars,
                status=status,
            )
        )


def test_recovery_uses_exact_offline_writer_profile_without_redis_dispatcher(monkeypatch):
    """A future refactor must not turn an offline repair into live bus startup."""
    harness = RecoveryHarness(monkeypatch)
    messages = []
    expected_versions = (
        "001_audit_and_idempotency.sql",
        "002_durable_runtime.sql",
        "003_execution_effect_journal.sql",
        "004_execution_recovery_delay.sql",
        "005_source_state_and_usage.sql",
        "006_paper_trade_lifecycle.sql",
        "007_execution_runtime_health.sql",
        "008_public_execution_events.sql",
        "009_paper_canary_arms.sql",
        "010_runtime_compensation_reserve.sql",
        "011_execution_mutation_budget.sql",
        "012_outbox_producer_order.sql",
    )

    harness.run(
        messages,
        maximum_append_bars=1,
        expected_database_name="kairos_recovery_primary",
    )

    assert harness.writer_args == (
        (),
        {
            "service_name": recovery.SOURCE,
            "expected_database_name": "kairos_recovery_primary",
            "expected_schema_versions": expected_versions,
        },
    )
    assert messages[-1]["state"] == "PAUSED_LIMIT"
    assert harness.lifecycle[0] == "writer_start"
    assert harness.lifecycle[-2:] == ["session_close", "writer_close"]


@pytest.mark.parametrize(
    ("fault", "expected_phase", "expected_requests"),
    [
        ("writer_start", "OFFLINE_WRITER_START", 0),
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
    assert [item["phase"] for item in failures] == ["REST_GET_FIRST", "OFFLINE_WRITER_CLOSE"]
    assert all(item["first_failure_phase"] == "REST_GET_FIRST" for item in failures)
    assert "secret://" not in json.dumps(messages)


@pytest.mark.parametrize("fault", ["writer_close"])
def test_cleanup_must_finish_before_success_is_reported(monkeypatch, fault):
    harness = RecoveryHarness(monkeypatch, fault)
    messages = []
    with pytest.raises(TimeoutError):
        harness.run(messages)
    assert not any(item["state"] == "COMPLETED" for item in messages)
    assert messages[-1]["phase"] == "OFFLINE_WRITER_CLOSE"
    assert messages[-1]["total_appended_bars"] == 15
    assert len(harness.publications) == 15


@pytest.mark.parametrize("cap", [1, 199, 200, 201, 399])
def test_append_cap_bounds_full_confirmed_pages_before_fetch(monkeypatch, cap):
    harness = RecoveryHarness(monkeypatch)
    messages = []
    fixed_end = 501 * MINUTE
    harness.run(messages, end_exclusive=fixed_end, maximum_bars=2_500, maximum_append_bars=cap)
    final = messages[-1]
    assert final["state"] == "PAUSED_LIMIT"
    assert final["phase"] == "FINISHED"
    assert final["end_exclusive_ms"] == fixed_end
    assert final["maximum_append_bars"] == cap
    assert final["planned_remaining_bars"] == 2_500
    assert final["actual_appended_bars"] == final["total_appended_bars"] == cap
    assert final["remaining_bars"] == 2_500 - cap
    assert all(item["end_exclusive_ms"] == fixed_end for item in messages)
    assert not any(item["state"] == "COMPLETED" for item in messages)
    assert harness.publications == [("BTCUSDT", minute * MINUTE) for minute in range(1, cap + 1)]
    assert len(harness.requests) == 2 * ((cap + 198) // 199)
    pages = harness.requests[::2]
    assert pages == harness.requests[1::2]
    for index, page in enumerate(pages):
        assert page["symbol"] == "BTCUSDT"
        assert 2 <= page["limit"] <= 200
        assert page["endTime"] + 1 - page["startTime"] == page["limit"] * MINUTE
        assert page["endTime"] + 1 <= (cap + 1) * MINUTE
        if index:
            assert page["startTime"] == pages[index - 1]["endTime"] + 1 - MINUTE
    assert pages[-1]["endTime"] + 1 == (cap + 1) * MINUTE
    assert harness.lifecycle[-2:] == ["session_close", "writer_close"]


def test_append_cap_is_global_and_repeated_runs_finish_in_established_symbol_order(monkeypatch):
    harness = RecoveryHarness(monkeypatch)
    receipts = []
    for expected_remaining in (11, 7, 3, 0):
        messages = []
        harness.run(messages, maximum_append_bars=4)
        final = messages[-1]
        receipts.append(final)
        assert final["remaining_bars"] == expected_remaining
        assert final["actual_appended_bars"] <= 4
        assert final["end_exclusive_ms"] == 4 * MINUTE
    assert [receipt["state"] for receipt in receipts] == ["PAUSED_LIMIT"] * 3 + ["COMPLETED"]
    assert [receipt["planned_remaining_bars"] for receipt in receipts] == [15, 11, 7, 3]
    assert [receipt["actual_appended_bars"] for receipt in receipts] == [4, 4, 4, 3]
    assert harness.publications == [
        (symbol, minute * MINUTE) for symbol in recovery.SYMBOLS for minute in range(1, 4)
    ]
    assert len(harness.publications) == len(set(harness.publications)) == 15
    for symbol, history in harness.committed.items():
        for minute, event in history.items():
            expected = parse_page(
                rows(minute, minute + MINUTE), symbol=symbol, start=minute, end=minute + MINUTE
            )[0]
            assert event.canonical_bar_bytes() == expected.canonical_bar_bytes()
            assert event.message_id == expected.message_id


def test_paused_limit_is_emitted_only_after_all_cleanup_finishes(monkeypatch):
    harness = RecoveryHarness(monkeypatch)
    observations = []

    def emit(item):
        observations.append((item, tuple(harness.lifecycle)))

    asyncio.run(
        recovery.run_recovery(
            4 * MINUTE,
            100,
            expected_database_name="kairos",
            maximum_append_bars=1,
            status=recovery.RecoveryStatus(emit),
        )
    )
    final, lifecycle = observations[-1]
    assert final["state"] == "PAUSED_LIMIT"
    assert lifecycle[-2:] == ("session_close", "writer_close")
    assert sum(item["state"] == "PAUSED_LIMIT" for item, _ in observations) == 1


@pytest.mark.parametrize("fault", ["session_close", "writer_close"])
def test_cleanup_failure_suppresses_paused_limit(monkeypatch, fault):
    harness = RecoveryHarness(monkeypatch, fault)
    messages = []
    with pytest.raises(TimeoutError):
        harness.run(messages, maximum_append_bars=1)
    assert not any(item["state"] in {"PAUSED_LIMIT", "COMPLETED"} for item in messages)
    assert messages[-1]["state"] == "FAILED"
    assert messages[-1]["actual_appended_bars"] == 1
    assert messages[-1]["remaining_bars"] == 14
    assert (
        messages[-1]["phase"]
        == {
            "session_close": "REST_SESSION_CLOSE",
            "writer_close": "OFFLINE_WRITER_CLOSE",
        }[fault]
    )
    assert harness.closes == 1
    assert "secret://" not in json.dumps(messages)


@pytest.mark.parametrize("cap", [0, -1, 150_001, True, False, 1.0, "1", 101])
def test_invalid_append_cap_fails_before_offline_writer_or_rest(monkeypatch, cap):
    harness = RecoveryHarness(monkeypatch)
    monkeypatch.setattr(
        recovery,
        "OfflineDurableWriter",
        lambda *_args, **_kwargs: pytest.fail("invalid cap must not create an offline writer"),
        raising=False,
    )
    messages = []
    with pytest.raises(recovery.RecoveryError, match="append limit"):
        harness.run(messages, maximum_append_bars=cap)
    assert harness.requests == harness.publications == []
    assert harness.closes == 0
    assert messages[-1]["state"] == "FAILED"
    assert messages[-1]["phase"] == "VALIDATE_SETTINGS"


def test_small_append_cap_cannot_bypass_total_required_budget(monkeypatch):
    harness = RecoveryHarness(monkeypatch)
    messages = []
    with pytest.raises(recovery.RecoveryError, match="explicit bar budget"):
        harness.run(messages, maximum_bars=10, maximum_append_bars=1)
    assert messages[-1]["planned_remaining_bars"] == 15
    assert messages[-1]["remaining_bars"] == 15
    assert messages[-1]["phase"] == "VALIDATE_BUDGET"
    assert harness.requests == harness.publications == []


@pytest.mark.parametrize("cap", [None, 15, 16, 150_000])
def test_omitted_or_sufficient_append_cap_completes_fixed_end(monkeypatch, cap):
    harness = RecoveryHarness(monkeypatch)
    messages = []
    harness.run(messages, maximum_bars=150_000, maximum_append_bars=cap)
    assert messages[-1]["state"] == "COMPLETED"
    assert messages[-1]["planned_remaining_bars"] == 15
    assert messages[-1]["actual_appended_bars"] == 15
    assert messages[-1]["remaining_bars"] == 0
    assert messages[-1]["confirmed_through_exclusive_ms"] == {
        symbol: 4 * MINUTE for symbol in recovery.SYMBOLS
    }
    resumed = []
    before_requests = list(harness.requests)
    harness.run(resumed, maximum_bars=150_000, maximum_append_bars=cap)
    assert resumed[-1]["state"] == "COMPLETED"
    assert resumed[-1]["planned_remaining_bars"] == resumed[-1]["actual_appended_bars"] == 0
    assert resumed[-1]["remaining_bars"] == 0
    assert harness.requests == before_requests
    assert len(harness.publications) == 15


def test_append_cap_unknown_publish_stops_without_retry_and_resume_uses_committed_anchor(monkeypatch):
    harness = RecoveryHarness(monkeypatch, "publish")
    failed = []
    with pytest.raises(TimeoutError):
        harness.run(failed, maximum_append_bars=2)
    assert harness.publications == [("BTCUSDT", MINUTE), ("BTCUSDT", 2 * MINUTE)]
    assert len(harness.requests) == 2
    assert failed[-1]["state"] == "FAILED"
    assert failed[-1]["publish_outcome"] == "UNKNOWN"
    assert failed[-1]["total_appended_bars"] == 1
    assert failed[-1]["actual_appended_bars"] is None
    assert failed[-1]["remaining_bars"] is None
    assert failed[-1]["planned_remaining_bars"] == 15
    assert not any(item["state"] in {"PAUSED_LIMIT", "COMPLETED"} for item in failed)

    harness.fault = None
    resumed = []
    harness.run(resumed, maximum_append_bars=2)
    assert harness.publications == [
        ("BTCUSDT", MINUTE),
        ("BTCUSDT", 2 * MINUTE),
        ("BTCUSDT", 3 * MINUTE),
        ("ETHUSDT", MINUTE),
    ]
    assert resumed[-1]["state"] == "PAUSED_LIMIT"
    assert resumed[-1]["planned_remaining_bars"] == 13
    assert resumed[-1]["actual_appended_bars"] == 2
    assert resumed[-1]["remaining_bars"] == 11
    assert len(harness.publications) == len(set(harness.publications))


@pytest.mark.parametrize("fault", ["incomplete", "conflict"])
def test_truncated_interval_still_requires_two_whole_valid_pages(monkeypatch, fault):
    harness = RecoveryHarness(monkeypatch, fault)
    messages = []
    with pytest.raises(recovery.RecoveryError):
        harness.run(messages, maximum_append_bars=1)
    assert harness.publications == []
    assert messages[-1]["state"] == "FAILED"
    assert all(page["limit"] == 2 and page["endTime"] + 1 == 2 * MINUTE for page in harness.requests)
    assert len(harness.requests) == (1 if fault == "incomplete" else 2)


@pytest.mark.parametrize("cap", [None, 7])
def test_cli_forwards_exact_database_identity_and_optional_append_limit(monkeypatch, cap):
    calls = []

    async def fake_run(end_exclusive, maximum_bars, *, expected_database_name, maximum_append_bars=None):
        calls.append((end_exclusive, maximum_bars, expected_database_name, maximum_append_bars))

    args = [
        "long_gap_recovery",
        "--end-exclusive-ms",
        "240000",
        "--maximum-bars",
        "100",
        "--expected-database-name",
        "kairos",
        "--offline-consumers-confirmed",
    ]
    if cap is not None:
        args.extend(["--maximum-append-bars", str(cap)])
    monkeypatch.setattr("sys.argv", args)
    monkeypatch.setattr(recovery, "run_recovery", fake_run)
    recovery.main()
    assert calls == [(240_000, 100, "kairos", cap)]
