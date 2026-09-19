"""Strict top-N recording tests for the sealed market-data simulator."""

from __future__ import annotations

import hashlib
import json

import pytest

from kairos_quant.collectors.binance_ws import BinanceFuturesCollector
from kairos_quant.simulation_book import (
    SimulationBookQueueOverflow,
    SimulationBookTapeBlocked,
    SimulationBookTapeRecorder,
    normalize_simulation_book,
)


def _recorder(**overrides: object) -> SimulationBookTapeRecorder:
    values: dict[str, object] = {
        "tape_id": "sim-tape",
        "max_pending_frames": 32,
        "wall_clock_ms": lambda: 60_100,
    }
    values.update(overrides)
    return SimulationBookTapeRecorder(**values)


def _capture(recorder: SimulationBookTapeRecorder, *, update_id: int = 1):
    return recorder.capture_depth(
        raw_payload='{ "stream" : "btcusdt@depth10@100ms", "data" : { "u" : 1 } }',
        symbol="BTCUSDT",
        exchange_update_id=update_id,
        exchange_at_ms=60_000,
        received_at_ms=60_050,
        bids=[(100.0, 2.0), (99.0, 1.0)],
        asks=[(101.0, 3.0), (102.0, 1.0)],
    )


def test_strict_simulator_book_rejects_locked_and_duplicate_levels() -> None:
    with pytest.raises(ValueError, match="strictly unlocked"):
        normalize_simulation_book([(100.0, 1.0)], [(100.0, 1.0)])
    with pytest.raises(ValueError, match="duplicate"):
        normalize_simulation_book([(100.0, 1.0), (100.0, 2.0)], [(101.0, 1.0)])


def test_capture_preserves_original_websocket_text_hash_and_global_order() -> None:
    recorder = _recorder()
    assert recorder.begin_public_stream(connected_at_ms=60_000) == "public-000001"
    frame = _capture(recorder)

    assert (
        frame.raw_payload_sha256
        == hashlib.sha256(b'{ "stream" : "btcusdt@depth10@100ms", "data" : { "u" : 1 } }').hexdigest()
    )
    assert frame.tape_sequence == 1
    assert frame.previous_frame_sha256 is None
    assert frame.received_at_ms == 60_050
    assert frame.persisted_at_ms == 60_100
    assert frame.continuity == "ADMITTED"
    assert recorder.pending_frames() == (frame,)

    recorder.acknowledge(frame.frame_sha256 or "")
    assert recorder.pending_frames() == ()


def test_queue_overflow_blocks_without_silently_dropping_a_frame() -> None:
    recorder = _recorder(max_pending_frames=2)
    recorder.begin_public_stream(connected_at_ms=60_000)
    first = _capture(recorder)

    with pytest.raises(SimulationBookQueueOverflow):
        _capture(recorder, update_id=2)

    assert recorder.pending_frames() == (first,)
    assert recorder.status.state == "BLOCKED"
    assert recorder.status.blocked_reason == "PENDING_QUEUE_OVERFLOW"


def test_reconnect_records_one_global_barrier_then_permanently_blocks_tape() -> None:
    recorder = _recorder()
    recorder.begin_public_stream(connected_at_ms=60_000)
    first = _capture(recorder)
    barriers = recorder.record_source_barrier(
        continuity="RECONNECT",
        at_ms=60_200,
        reason="PUBLIC_STREAM_RECONNECT",
    )

    assert len(barriers) == 1
    assert barriers[0].symbol == "BNBUSDT"
    assert barriers[0].continuity == "RECONNECT"
    assert not barriers[0].bids and not barriers[0].asks
    assert barriers[0].previous_frame_sha256 == first.frame_sha256
    assert recorder.status.state == "BLOCKED"
    assert recorder.status.blocked_reason == "PUBLIC_STREAM_RECONNECT"

    with pytest.raises(SimulationBookTapeBlocked):
        _capture(recorder, update_id=2)


def test_invalid_book_records_unknown_barrier_and_stops_capture() -> None:
    recorder = _recorder()
    recorder.begin_public_stream(connected_at_ms=60_000)

    with pytest.raises(SimulationBookTapeBlocked, match="INVALID_BOOK"):
        recorder.capture_depth(
            raw_payload="{}",
            symbol="BTCUSDT",
            exchange_update_id=1,
            exchange_at_ms=60_000,
            received_at_ms=60_050,
            bids=[(100.0, 1.0)],
            asks=[(100.0, 1.0)],
        )

    barriers = recorder.pending_frames()
    assert len(barriers) == 1
    assert all(frame.continuity == "UNKNOWN" for frame in barriers)
    assert recorder.status.blocked_reason == "INVALID_BOOK"


def test_collector_passes_original_text_and_receive_time_to_enabled_recorder() -> None:
    recorder = _recorder(wall_clock_ms=lambda: 60_100)
    recorder.begin_public_stream(connected_at_ms=60_000)
    collector = BinanceFuturesCollector(
        ["BTCUSDT"],
        "wss://example.invalid/stream",
        "https://example.invalid",
        wall_clock=lambda: 60.1,
        simulation_book_recorder=recorder,
    )
    raw_payload = json.dumps(
        {
            "stream": "btcusdt@depth10@100ms",
            "data": {"s": "BTCUSDT", "E": 60_000, "u": 1, "b": [["100", "2"]], "a": [["101", "3"]]},
        },
        separators=(",", ":"),
    )

    assert collector._on_message(
        json.loads(raw_payload),
        route="public",
        raw_payload=raw_payload,
        received_at_ms=60_050,
    )
    frame = recorder.pending_frames()[0]
    assert frame.raw_payload_sha256 == hashlib.sha256(raw_payload.encode("utf-8")).hexdigest()
    assert frame.received_at_ms == 60_050
    assert frame.persisted_at_ms == 60_100
    assert collector.books["btcusdt"]["bids"] == [(100.0, 2.0)]


def test_enabled_collector_refuses_depth_without_original_text_lineage() -> None:
    recorder = _recorder()
    recorder.begin_public_stream(connected_at_ms=60_000)
    collector = BinanceFuturesCollector(
        ["BTCUSDT"],
        "wss://example.invalid/stream",
        "https://example.invalid",
        simulation_book_recorder=recorder,
    )
    payload = {
        "stream": "btcusdt@depth10@100ms",
        "data": {"s": "BTCUSDT", "E": 60_000, "u": 1, "b": [["100", "2"]], "a": [["101", "3"]]},
    }

    with pytest.raises(SimulationBookTapeBlocked, match="original websocket text"):
        collector._on_message(payload, route="public")
