"""Regression tests for Quant Scouts service lifecycle."""
import asyncio

from kairos_quant.config import QuantSettings
from kairos_quant.service import QuantScoutsService


class _RecordingBus:
    def __init__(self) -> None:
        self.messages = []

    async def publish(self, topic, message):
        self.messages.append((topic, message))


async def _run_one_emit_iteration(service: QuantScoutsService) -> None:
    task = asyncio.create_task(service._emit_loop())
    await asyncio.sleep(0.01)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


def test_emit_loop_skips_order_book_missing_ask_side():
    settings = QuantSettings(
        bus_backend="memory", symbols=["BTCUSDT"], snapshot_interval_s=0.001
    )
    service = QuantScoutsService(settings)
    service.bus = _RecordingBus()
    service.collector.books["btcusdt"] = {"bids": [(65_000.0, 1.0)], "asks": []}

    asyncio.run(_run_one_emit_iteration(service))

    assert service.bus.messages == []
