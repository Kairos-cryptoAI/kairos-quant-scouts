"""Fail-closed capture of sealed top-N book inputs for the offline simulator.

This module is deliberately a small ingress boundary.  It accepts the original
WebSocket text and its receive timestamp, validates a strict two-sided book,
and queues immutable frames in one global tape order.  A public-stream
reconnect or queue overflow blocks the tape; a blocked tape cannot resume.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
import time
from collections import deque
from collections.abc import Callable, Sequence
from dataclasses import dataclass

from kairos_core.contracts import RecordedBookLevelV1, RecordedTopNBookFrameV1

from .orderbook import Level, normalize_order_book

SIMULATION_SYMBOLS = ("BNBUSDT", "BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT")
_CONTINUITIES = frozenset({"GAP", "RECONNECT", "UNKNOWN", "UNAVAILABLE"})


class SimulationBookCaptureError(RuntimeError):
    """A source input cannot safely enter the immutable simulator tape."""


class SimulationBookQueueOverflow(SimulationBookCaptureError):
    """The bounded pending queue reached its reserved source-barrier capacity."""


class SimulationBookTapeBlocked(SimulationBookCaptureError):
    """A barrier or source fault permanently terminated this tape."""


@dataclass(frozen=True, slots=True)
class SimulationBookRecorderStatus:
    """Small non-secret status receipt suitable for operational monitoring."""

    tape_id: str
    state: str
    stream_epoch: str | None
    next_tape_sequence: int
    pending_frames: int
    blocked_reason: str | None


def normalize_simulation_book(
    bids: list[Level],
    asks: list[Level],
    *,
    maximum_levels: int = 10,
) -> tuple[tuple[RecordedBookLevelV1, ...], tuple[RecordedBookLevelV1, ...]]:
    """Return a strict, top-N snapshot suitable for a causal replay tape.

    The ordinary feature pipeline tolerates a locked book because it only
    derives indicators.  A simulator input cannot do that: a locked/crossed,
    empty, duplicated, non-finite, or oversized snapshot is rejected instead
    of being normalized into an apparently executable frame.
    """

    if (
        isinstance(maximum_levels, bool)
        or not isinstance(maximum_levels, int)
        or not 1 <= maximum_levels <= 100
    ):
        raise ValueError("maximum_levels must be an integer in [1, 100]")
    normalized_bids, normalized_asks = normalize_order_book(bids, asks)
    if not normalized_bids or not normalized_asks:
        raise ValueError("simulator book requires both non-empty sides")
    if normalized_bids[0][0] >= normalized_asks[0][0]:
        raise ValueError("simulator book must be strictly unlocked")
    for levels, side in ((normalized_bids, "bids"), (normalized_asks, "asks")):
        prices = [price for price, _quantity in levels]
        if len(prices) != len(set(prices)):
            raise ValueError(f"simulator {side} must not contain duplicate prices")
        if any(not math.isfinite(price) or not math.isfinite(quantity) for price, quantity in levels):
            raise ValueError(f"simulator {side} must contain finite levels")
    return (
        tuple(
            RecordedBookLevelV1(price=price, quantity=quantity)
            for price, quantity in normalized_bids[:maximum_levels]
        ),
        tuple(
            RecordedBookLevelV1(price=price, quantity=quantity)
            for price, quantity in normalized_asks[:maximum_levels]
        ),
    )


class SimulationBookTapeRecorder:
    """Capture one public top-N stream into an append-only, bounded tape queue.

    Consumers must persist the first pending frame and call ``acknowledge``
    only after the isolated simulator journal accepted that exact canonical
    payload.  The queue deliberately reserves one global source-barrier slot,
    so a reconnect cannot be hidden by a saturated queue.  One barrier blocks
    the entire tape atomically in the durable repository; its hashed marker
    declares that it covers all five subscribed symbols.
    """

    def __init__(
        self,
        *,
        tape_id: str,
        symbols: Sequence[str] = SIMULATION_SYMBOLS,
        source: str = "kairos-quant-scouts",
        max_pending_frames: int = 1_024,
        wall_clock_ms: Callable[[], int] | None = None,
    ) -> None:
        if not tape_id or tape_id != tape_id.strip() or len(tape_id) > 128:
            raise ValueError("tape_id must be a normalized string no longer than 128 characters")
        if not source or source != source.strip():
            raise ValueError("source must be a normalized non-empty string")
        normalized_symbols = tuple(sorted(symbol.strip().upper() for symbol in symbols))
        if normalized_symbols != SIMULATION_SYMBOLS:
            raise ValueError("simulation recorder requires exactly the fixed five-symbol universe")
        if (
            isinstance(max_pending_frames, bool)
            or not isinstance(max_pending_frames, int)
            or max_pending_frames <= 1
        ):
            raise ValueError("max_pending_frames must reserve one source barrier frame")

        self.tape_id = tape_id
        self.symbols = normalized_symbols
        self.source = source
        self._capacity = max_pending_frames
        self._wall_clock_ms = wall_clock_ms or (lambda: int(time.time() * 1_000))
        self._pending: deque[RecordedTopNBookFrameV1] = deque()
        self._next_sequence = 1
        self._previous_frame_sha256: str | None = None
        self._stream_epoch: str | None = None
        self._epoch_number = 0
        self._last_update_id: dict[str, int] = {}
        self._blocked_reason: str | None = None

    @property
    def status(self) -> SimulationBookRecorderStatus:
        return SimulationBookRecorderStatus(
            tape_id=self.tape_id,
            state="BLOCKED"
            if self._blocked_reason is not None
            else "RECORDING"
            if self._stream_epoch
            else "IDLE",
            stream_epoch=self._stream_epoch,
            next_tape_sequence=self._next_sequence,
            pending_frames=len(self._pending),
            blocked_reason=self._blocked_reason,
        )

    def begin_public_stream(self, *, connected_at_ms: int) -> str:
        """Begin the one allowed public stream epoch for this tape."""

        self._validate_timestamp(connected_at_ms, name="connected_at_ms")
        if self._blocked_reason is not None:
            raise SimulationBookTapeBlocked(f"simulation tape is blocked: {self._blocked_reason}")
        if self._stream_epoch is not None:
            raise SimulationBookCaptureError("a simulation public stream epoch is already active")
        self._epoch_number += 1
        self._stream_epoch = f"public-{self._epoch_number:06d}"
        return self._stream_epoch

    def capture_depth(
        self,
        *,
        raw_payload: str,
        symbol: str,
        exchange_update_id: int,
        exchange_at_ms: int,
        received_at_ms: int,
        bids: list[Level],
        asks: list[Level],
    ) -> RecordedTopNBookFrameV1:
        """Queue one validated frame without altering its original source bytes."""

        if self._blocked_reason is not None:
            raise SimulationBookTapeBlocked(f"simulation tape is blocked: {self._blocked_reason}")
        if self._stream_epoch is None:
            raise SimulationBookCaptureError("simulation public stream epoch has not started")
        self._reserve_capture_capacity()
        if not isinstance(raw_payload, str):
            self._block_with_barrier(
                continuity="UNKNOWN", at_ms=received_at_ms, reason="RAW_PAYLOAD_NOT_TEXT"
            )
            raise SimulationBookTapeBlocked("simulation tape blocked: RAW_PAYLOAD_NOT_TEXT")
        normalized_symbol = self._normalize_symbol(symbol)
        self._validate_timestamp(exchange_at_ms, name="exchange_at_ms")
        self._validate_timestamp(received_at_ms, name="received_at_ms")
        if exchange_at_ms > received_at_ms:
            self._block_with_barrier(
                continuity="UNKNOWN", at_ms=received_at_ms, reason="SOURCE_CLOCK_REGRESSION"
            )
            raise SimulationBookTapeBlocked("simulation tape blocked: SOURCE_CLOCK_REGRESSION")
        if (
            isinstance(exchange_update_id, bool)
            or not isinstance(exchange_update_id, int)
            or exchange_update_id <= 0
        ):
            self._block_with_barrier(continuity="UNKNOWN", at_ms=received_at_ms, reason="INVALID_UPDATE_ID")
            raise SimulationBookTapeBlocked("simulation tape blocked: INVALID_UPDATE_ID")
        prior_update_id = self._last_update_id.get(normalized_symbol)
        if prior_update_id is not None and exchange_update_id <= prior_update_id:
            self._block_with_barrier(
                continuity="UNKNOWN", at_ms=received_at_ms, reason="UPDATE_ID_REGRESSION"
            )
            raise SimulationBookTapeBlocked("simulation tape blocked: UPDATE_ID_REGRESSION")
        try:
            recorded_bids, recorded_asks = normalize_simulation_book(bids, asks)
        except ValueError:
            self._block_with_barrier(continuity="UNKNOWN", at_ms=received_at_ms, reason="INVALID_BOOK")
            raise SimulationBookTapeBlocked("simulation tape blocked: INVALID_BOOK") from None

        try:
            raw_payload_sha256 = hashlib.sha256(raw_payload.encode("utf-8")).hexdigest()
        except UnicodeEncodeError:
            self._block_with_barrier(
                continuity="UNKNOWN", at_ms=received_at_ms, reason="RAW_PAYLOAD_NOT_UTF8"
            )
            raise SimulationBookTapeBlocked("simulation tape blocked: RAW_PAYLOAD_NOT_UTF8") from None
        persisted_at_ms = max(received_at_ms, self._current_time_ms())
        frame = self._build_frame(
            stream_epoch=self._stream_epoch,
            symbol=normalized_symbol,
            exchange_update_id=exchange_update_id,
            exchange_at_ms=exchange_at_ms,
            received_at_ms=received_at_ms,
            persisted_at_ms=persisted_at_ms,
            raw_payload_sha256=raw_payload_sha256,
            continuity="ADMITTED",
            bids=recorded_bids,
            asks=recorded_asks,
        )
        self._append(frame)
        self._last_update_id[normalized_symbol] = exchange_update_id
        return frame

    def record_source_barrier(
        self, *, continuity: str, at_ms: int, reason: str
    ) -> tuple[RecordedTopNBookFrameV1, ...]:
        """Terminate this tape with an explicit all-symbol public-feed barrier."""

        if continuity not in _CONTINUITIES:
            raise ValueError("simulation source barrier continuity is unsupported")
        self._validate_timestamp(at_ms, name="at_ms")
        if not reason or reason != reason.strip() or not re.fullmatch(r"[A-Z0-9_]{1,100}", reason):
            raise ValueError("simulation source barrier reason must be uppercase snake case")
        if self._blocked_reason is not None:
            raise SimulationBookTapeBlocked(f"simulation tape is blocked: {self._blocked_reason}")
        if self._stream_epoch is None:
            raise SimulationBookCaptureError("simulation public stream epoch has not started")
        return self._block_with_barrier(continuity=continuity, at_ms=at_ms, reason=reason)

    def pending_frames(self) -> tuple[RecordedTopNBookFrameV1, ...]:
        """Return queued frames in immutable global sequence order."""

        return tuple(self._pending)

    def acknowledge(self, frame_sha256: str) -> None:
        """Remove exactly the durable queue head after journal persistence."""

        if not self._pending:
            raise SimulationBookCaptureError("simulation book queue is empty")
        expected = self._pending[0].frame_sha256
        if frame_sha256 != expected:
            raise SimulationBookCaptureError("simulation book acknowledgement must match the queue head")
        self._pending.popleft()

    def _reserve_capture_capacity(self) -> None:
        if len(self._pending) < self._capacity - 1:
            return
        self._blocked_reason = "PENDING_QUEUE_OVERFLOW"
        raise SimulationBookQueueOverflow("simulation book queue reached reserved source-barrier capacity")

    def _block_with_barrier(
        self,
        *,
        continuity: str,
        at_ms: int,
        reason: str,
    ) -> tuple[RecordedTopNBookFrameV1, ...]:
        self._validate_timestamp(at_ms, name="at_ms")
        if len(self._pending) + 1 > self._capacity:
            self._blocked_reason = "SOURCE_BARRIER_QUEUE_OVERFLOW"
            raise SimulationBookQueueOverflow("simulation source barrier cannot fit in the bounded queue")
        self._epoch_number += 1
        barrier_epoch = f"public-{self._epoch_number:06d}"
        symbol = self.symbols[0]
        marker = json.dumps(
            {
                "at_ms": at_ms,
                "continuity": continuity,
                "reason": reason,
                "scope": "PUBLIC_TOP_N_ALL_SYMBOLS",
                "stream_epoch": barrier_epoch,
                "symbols": self.symbols,
                "tape_id": self.tape_id,
            },
            separators=(",", ":"),
            sort_keys=True,
        )
        frame = self._build_frame(
            stream_epoch=barrier_epoch,
            symbol=symbol,
            exchange_update_id=1,
            exchange_at_ms=at_ms,
            received_at_ms=at_ms,
            persisted_at_ms=max(at_ms, self._current_time_ms()),
            raw_payload_sha256=hashlib.sha256(marker.encode("utf-8")).hexdigest(),
            continuity=continuity,
            bids=(),
            asks=(),
        )
        self._append(frame)
        self._stream_epoch = None
        self._blocked_reason = reason
        return (frame,)

    def _build_frame(
        self,
        *,
        stream_epoch: str,
        symbol: str,
        exchange_update_id: int,
        exchange_at_ms: int,
        received_at_ms: int,
        persisted_at_ms: int,
        raw_payload_sha256: str,
        continuity: str,
        bids: tuple[RecordedBookLevelV1, ...],
        asks: tuple[RecordedBookLevelV1, ...],
    ) -> RecordedTopNBookFrameV1:
        return RecordedTopNBookFrameV1(
            source=self.source,
            tape_id=self.tape_id,
            stream_epoch=stream_epoch,
            symbol=symbol,
            tape_sequence=self._next_sequence,
            exchange_update_id=exchange_update_id,
            exchange_at_ms=exchange_at_ms,
            received_at_ms=received_at_ms,
            persisted_at_ms=persisted_at_ms,
            raw_payload_sha256=raw_payload_sha256,
            previous_frame_sha256=self._previous_frame_sha256,
            continuity=continuity,
            bids=bids,
            asks=asks,
        )

    def _append(self, frame: RecordedTopNBookFrameV1) -> None:
        if frame.frame_sha256 is None:
            raise AssertionError("recorded simulator book frame has no canonical identity")
        self._pending.append(frame)
        self._next_sequence += 1
        self._previous_frame_sha256 = frame.frame_sha256

    def _current_time_ms(self) -> int:
        value = self._wall_clock_ms()
        self._validate_timestamp(value, name="wall_clock_ms")
        return value

    def _normalize_symbol(self, symbol: str) -> str:
        normalized = symbol.strip().upper() if isinstance(symbol, str) else ""
        if normalized not in self.symbols or symbol != normalized:
            raise SimulationBookCaptureError(
                "simulation book symbol is outside the fixed normalized universe"
            )
        return normalized

    @staticmethod
    def _validate_timestamp(value: int, *, name: str) -> None:
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValueError(f"{name} must be a non-negative integer")
