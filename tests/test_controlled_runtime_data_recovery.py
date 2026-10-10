"""Focused transport-free tests for the current controlled-runtime runner."""

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest
from kairos_core.topics import Topics

from kairos_quant import controlled_runtime_data_recovery as recovery
from kairos_quant.long_gap_recovery import MINUTE, SYMBOLS, parse_page


def rows(start, end):
    return [
        [t, "100", "102", "99", "101", "20", t + 59_999, "2000", 1, "10", "1000"]
        for t in range(start, end, MINUTE)
    ]


def event(symbol, start=0):
    return parse_page(rows(start, start + MINUTE), symbol=symbol, start=start, end=start + MINUTE)[0]


class Harness:
    def __init__(
        self,
        monkeypatch,
        *,
        identity="kairos_runtime",
        access_error=None,
        append_error=None,
        anchor_ms=0,
    ):
        self.events = []
        self.writer_args = None
        self.identity = identity
        self.access_error = access_error
        self.append_error = append_error
        self.closed = False
        owner = self

        class Connection:
            async def fetchrow(self, sql):
                assert sql == "SELECT current_user AS current_actor, session_user AS session_actor"
                return {"current_actor": owner.identity, "session_actor": owner.identity}

        class Pool:
            @asynccontextmanager
            async def acquire(self):
                yield Connection()

            async def fetch(self, _sql, _topic, _source, symbol):
                return [{"payload": event(symbol, owner.anchor_ms).model_dump_json()}]

        class Writer:
            def __init__(self):
                self.database = SimpleNamespace(pool=Pool())
                self.repository = SimpleNamespace(pool=self.database.pool)

            async def start(self):
                owner.events.append("writer_start")

            async def append(self, topic, item):
                assert topic == Topics.CLOSED_BAR
                owner.events.append((item.symbol, item.open_time_ms))
                if owner.append_error:
                    raise owner.append_error
                return True

            async def close(self):
                owner.closed = True
                owner.events.append("writer_close")

        self.writer = Writer()
        self.anchor_ms = anchor_ms

        async def verify(_self):
            owner.events.append("verify_runtime_access")
            if owner.access_error:
                raise owner.access_error

        monkeypatch.setattr(recovery, "OfflineDurableWriter", self._writer)
        monkeypatch.setattr(recovery.OperatorControlRepository, "verify_runtime_access", verify)
        monkeypatch.setattr(
            recovery,
            "QuantSettings",
            lambda: SimpleNamespace(
                environment="paper",
                bus_backend="redis",
                service_name=recovery.SOURCE,
                symbols=SYMBOLS,
                binance_rest_base="https://fapi.binance.com",
                kline_finality_delay_s=5,
            ),
        )

    def _writer(self, **kwargs):
        self.writer_args = kwargs
        return self.writer


def test_manifest_is_exact_controlled_runtime_profile_and_excludes_simulator():
    names = recovery.CONTROLLED_RUNTIME_SCHEMA_VERSIONS
    assert names == recovery.Database.migration_names(recovery.MigrationProfile.CONTROLLED_RUNTIME)
    assert names[-2:] == ("018_offline_outbox_reconciliation.sql", "026_operator_control.sql")
    assert not any(
        name.startswith(("017_", "019_", "020_", "021_", "022_", "023_", "024_", "025_", "027_"))
        for name in names
    )


def test_identity_and_operator_access_are_verified_before_any_rest_or_append(monkeypatch):
    harness = Harness(monkeypatch, identity="administrator")
    monkeypatch.setattr(
        recovery.aiohttp,
        "ClientSession",
        lambda **_kwargs: pytest.fail("identity gate must run before REST session creation"),
    )
    status = recovery.RecoveryStatus(lambda _item: None)
    with pytest.raises(recovery.RecoveryError, match="kairos_runtime"):
        asyncio.run(
            recovery.run_recovery(
                int(recovery.time.time() * 1_000) // MINUTE * MINUTE - 10 * MINUTE,
                100,
                expected_database_name="kairos_runtime",
                status=status,
            )
        )
    assert harness.events == ["writer_start", "writer_close"]
    assert harness.events[-1] == "writer_close"
    assert harness.closed


def test_access_refusal_prevents_append_and_closes_writer(monkeypatch):
    harness = Harness(monkeypatch, access_error=PermissionError("refused"))
    status = recovery.RecoveryStatus(lambda _item: None)
    with pytest.raises(PermissionError, match="refused"):
        asyncio.run(
            recovery.run_recovery(
                int(recovery.time.time() * 1_000) // MINUTE * MINUTE - 10 * MINUTE,
                100,
                expected_database_name="kairos_runtime",
                status=status,
            )
        )
    assert not any(isinstance(item, tuple) for item in harness.events)
    assert harness.closed


def test_controlled_profile_is_passed_to_writer_without_transport_construction(monkeypatch):
    harness = Harness(monkeypatch, anchor_ms=7_680_000)
    sessions = []

    class Response:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        def raise_for_status(self):
            pass

        async def json(self):
            return rows(self.start, self.end)

    class Session:
        async def __aenter__(self):
            sessions.append("open")
            return self

        async def __aexit__(self, *_args):
            sessions.append("close")
            return False

        def get(self, _url, *, params):
            response = Response()
            response.start = params["startTime"]
            response.end = params["endTime"] + 1
            return response

    monkeypatch.setattr(recovery.aiohttp, "ClientSession", lambda **_kwargs: Session())
    original_sleep = asyncio.sleep
    monkeypatch.setattr(recovery.asyncio, "sleep", lambda _delay: original_sleep(0))
    # Keep the test deterministic while retaining the runner's finality check.
    monkeypatch.setattr(recovery, "time", SimpleNamespace(time=lambda: 10_000_000))
    status = recovery.RecoveryStatus(lambda _item: None)
    asyncio.run(
        recovery.run_recovery(
            7_800_000,
            100,
            expected_database_name="kairos_runtime",
            maximum_append_bars=1,
            status=status,
        )
    )
    assert harness.writer_args["expected_schema_versions"] == recovery.CONTROLLED_RUNTIME_SCHEMA_VERSIONS
    assert harness.writer_args["settings"].migration_profile == "controlled-runtime"
    assert harness.events[:2] == ["writer_start", "verify_runtime_access"]
    assert harness.events[-1] == "writer_close"
    assert status.total_appended_bars == 1
    assert status.planned_remaining_bars == 5
    assert sessions == ["open", "close"]


def test_append_failure_is_unknown_and_does_not_retry(monkeypatch):
    harness = Harness(
        monkeypatch,
        append_error=TimeoutError("transport details must not be emitted"),
        anchor_ms=7_680_000,
    )
    requests = []

    class Response:
        def __init__(self, params):
            self.start = params["startTime"]
            self.end = params["endTime"] + 1

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        def raise_for_status(self):
            pass

        async def json(self):
            return rows(self.start, self.end)

    class Session:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        def get(self, _url, *, params):
            requests.append(params)
            return Response(params)

    monkeypatch.setattr(recovery.aiohttp, "ClientSession", lambda **_kwargs: Session())
    monkeypatch.setattr(recovery, "time", SimpleNamespace(time=lambda: 10_000_000))
    original_sleep = asyncio.sleep
    monkeypatch.setattr(recovery.asyncio, "sleep", lambda _delay: original_sleep(0))
    status = recovery.RecoveryStatus(lambda _item: None)
    with pytest.raises(TimeoutError):
        asyncio.run(
            recovery.run_recovery(
                7_800_000,
                100,
                expected_database_name="kairos_runtime",
                status=status,
            )
        )
    assert status.publish_outcome_unknown
    assert status.total_appended_bars == 0
    assert len([item for item in harness.events if isinstance(item, tuple)]) == 1
    assert len(requests) == 2
    assert harness.closed
    assert harness.events.count(("BTCUSDT", 7_740_000)) == 1
