"""Long-gap repair is lossless, bounded, fail-closed and transport independent."""

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest

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
