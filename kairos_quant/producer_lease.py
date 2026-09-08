"""Exclusive PostgreSQL lease shared by live production and offline recovery."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from kairos_core.bus import MessageBus
from kairos_persistence import DurableMessageBus


@asynccontextmanager
async def producer_lease(bus: MessageBus) -> AsyncIterator[None]:
    if not isinstance(bus, DurableMessageBus):
        yield
        return
    await bus.start()
    if bus.repository is None:
        raise RuntimeError("durable producer repository unavailable")
    key = f"closed-bar-producer:{bus.service_name}"
    async with bus.repository.pool.acquire() as connection:
        acquired = await connection.fetchval("SELECT pg_try_advisory_lock(hashtextextended($1, 0))", key)
        if not acquired:
            raise RuntimeError("closed-bar producer/recovery already running")
        try:
            yield
        finally:
            await connection.execute("SELECT pg_advisory_unlock(hashtextextended($1, 0))", key)
