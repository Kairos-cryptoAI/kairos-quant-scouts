from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest
from kairos_core.topics import Topics
from pydantic import ValidationError

from kairos_quant.config import QuantSettings
from kairos_quant.service import QuantScoutsService, _advance_fixed_rate_deadline
from kairos_quant.venue_poll import (
    VENUE_POLL_TOPIC,
    VenuePollFactV1,
    VenuePollStatus,
    build_venue_poll_fact,
)

from .test_service import _RecordingBus, _service


def _fact(**overrides: object) -> VenuePollFactV1:
    values: dict[str, object] = {
        "source": "kairos-quant-scouts",
        "config_fingerprint": "a" * 64,
        "status": VenuePollStatus.ATTEMPTED,
        "binance_symbol": "BTCUSDT",
        "venue_symbol": "BTCUSD:DEV",
        "expected_symbols": ("BTCUSD:DEV", "ETHUSD:DEV"),
        "interval_ms": 30_000,
        "scheduled_at_ms": 1_800_000_000_000,
        "attempted_at_ms": 1_800_000_000_001,
    }
    values.update(overrides)
    return build_venue_poll_fact(**values)  # type: ignore[arg-type]


def test_poll_fact_has_deterministic_identity_and_strict_outcome_geometry() -> None:
    assert _fact().to_json() == _fact().to_json()
    assert (
        _fact().message_id
        != _fact(
            status=VenuePollStatus.FAILED,
            completed_at_ms=1_800_000_000_002,
            failure_code="TimeoutError",
        ).message_id
    )

    with pytest.raises(ValidationError, match="completion"):
        _fact(status=VenuePollStatus.SUCCEEDED)
    with pytest.raises(ValidationError, match="sorted and unique"):
        _fact(expected_symbols=("ETHUSD:DEV", "BTCUSD:DEV"))
    with pytest.raises(ValidationError, match="message_id"):
        VenuePollFactV1(**{**_fact().model_dump(), "message_id": "b" * 64})


def test_same_scheduled_slot_keeps_poll_identity_without_payload_conflict() -> None:
    first = _fact(attempted_at_ms=1_800_000_000_001)
    replay = _fact(attempted_at_ms=1_800_000_000_999)
    assert replay.poll_id == first.poll_id
    assert replay.message_id != first.message_id


@pytest.mark.asyncio
async def test_failed_read_emits_durable_attempt_and_terminal_failure(monkeypatch) -> None:
    service = _service()
    service._venue_wall_clock_ms = lambda: 1_800_000_000_010

    async def unavailable(*_args, **_kwargs):
        raise TimeoutError("public venue read timed out")

    monkeypatch.setattr("kairos_quant.service.fetch_venue_quality", unavailable)
    await service._emit_venue_quality_once(
        object(),  # type: ignore[arg-type]
        scheduled_at_ms=1_800_000_000_000,
    )

    assert [topic for topic, _ in service.bus.messages] == [VENUE_POLL_TOPIC, VENUE_POLL_TOPIC]
    attempted, failed = (message for _, message in service.bus.messages)
    assert attempted.status is VenuePollStatus.ATTEMPTED
    assert failed.status is VenuePollStatus.FAILED
    assert failed.poll_id == attempted.poll_id
    assert failed.failure_code == "TimeoutError"


@pytest.mark.asyncio
async def test_success_is_recorded_only_after_the_quality_measurement(monkeypatch) -> None:
    service = _service()
    service._venue_wall_clock_ms = lambda: 1_800_000_000_010
    quality = SimpleNamespace(
        entry_allowed=True,
        reason_codes=(),
        basis_bps=1.0,
        spread_bps=2.0,
    )

    async def available(*_args, **_kwargs):
        return quality

    monkeypatch.setattr("kairos_quant.service.fetch_venue_quality", available)
    await service._emit_venue_quality_once(
        object(),  # type: ignore[arg-type]
        scheduled_at_ms=1_800_000_000_000,
    )

    assert [topic for topic, _ in service.bus.messages] == [
        VENUE_POLL_TOPIC,
        Topics.VENUE_QUALITY,
        VENUE_POLL_TOPIC,
    ]
    assert service.bus.messages[-1][1].status is VenuePollStatus.SUCCEEDED


def test_fixed_rate_schedule_does_not_add_poll_latency_or_catch_up_burst() -> None:
    deadline, skipped = _advance_fixed_rate_deadline(
        previous_deadline=100.0,
        interval_s=30.0,
        now=108.0,
    )
    assert deadline == 130.0
    assert skipped == 0

    deadline, skipped = _advance_fixed_rate_deadline(
        previous_deadline=100.0,
        interval_s=30.0,
        now=165.0,
    )
    assert deadline == 190.0
    assert skipped == 2


def test_attempts_for_all_symbols_are_persisted_before_any_network_fetch(monkeypatch) -> None:
    settings_service = QuantScoutsService(
        QuantSettings(
            bus_backend="memory",
            trading_symbols=["BTCUSDT", "ETHUSDT"],
        )
    )
    settings_service.bus = _RecordingBus()
    settings_service._venue_wall_clock_ms = lambda: 1_800_000_000_010
    network_started_after_messages: list[int] = []

    async def unavailable(*_args, **_kwargs):
        network_started_after_messages.append(len(settings_service.bus.messages))
        raise TimeoutError

    monkeypatch.setattr("kairos_quant.service.fetch_venue_quality", unavailable)
    asyncio.run(
        settings_service._emit_venue_quality_once(
            object(),  # type: ignore[arg-type]
            scheduled_at_ms=1_800_000_000_000,
        )
    )

    assert network_started_after_messages == [2, 2]
