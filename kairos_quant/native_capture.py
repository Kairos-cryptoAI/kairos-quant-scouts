"""Bounded native capture records for the existing Binance market transforms.

The capture is local source evidence only. It authenticates neither Binance nor
the Quant process and creates no trading/producer authority. Deployment injects
an existing synchronous create-only native session sink; no sink is constructed
by this module or by default Quant settings.
"""

from __future__ import annotations

import base64
import hashlib
import json
import re
import threading
import uuid
from collections.abc import Mapping, Sequence
from typing import Any, Literal, NoReturn, Protocol
from urllib.parse import parse_qs, urlsplit

from kairos_core.contracts import canonical_json_bytes

_RAW_DOMAIN = b"kairos.quant-source-capture.v1\x00"
_OUTPUT_DOMAIN = b"kairos.quant-derived-market-event.v1\x00"
_MAX_BODY_BYTES = 1_000_000
_MAX_RECORD_BYTES = 2_000_000
_MAX_RECORDS_PER_CAPTURE = 8_000
_MAX_CAPTURE_BYTES = 64 * 1024 * 1024
_HEX32 = frozenset("0123456789abcdef")

CaptureKind = Literal[
    "BINANCE_KLINES_REST",
    "BINANCE_WS_FRAME",
    "BINANCE_OPEN_INTEREST_REST",
    "BINANCE_PREMIUM_INDEX_REST",
]
DerivedEventKind = Literal["CLOSED_BAR", "MARKET_SNAPSHOT"]


class NativeQuantSourceCaptureFailure(PermissionError):
    """Capture is missing, malformed, over budget, or failed create-only persistence."""


class NativeCreateOnlySink(Protocol):
    """Structural subset implemented by NativeCaptureSessionSink."""

    def create_only(self, observation_id: str, record: bytes) -> object: ...


class NativeQuantCapture:
    """Capture exact inputs before collector mutation, with fixed memory bounds."""

    def __init__(
        self,
        sink: NativeCreateOnlySink,
        *,
        capture_epoch: str | None = None,
        maximum_records: int = _MAX_RECORDS_PER_CAPTURE,
        maximum_bytes: int = _MAX_CAPTURE_BYTES,
    ) -> None:
        if not callable(getattr(sink, "create_only", None)):
            raise TypeError("a synchronous create-only native session sink is required")
        if type(maximum_records) is not int or not 1 <= maximum_records <= _MAX_RECORDS_PER_CAPTURE:
            raise ValueError("native capture record limit is outside its fixed bound")
        if type(maximum_bytes) is not int or not 1 <= maximum_bytes <= _MAX_CAPTURE_BYTES:
            raise ValueError("native capture byte limit is outside its fixed bound")
        epoch = capture_epoch or uuid.uuid4().hex
        if not isinstance(epoch, str) or len(epoch) != 32 or any(char not in _HEX32 for char in epoch):
            raise ValueError("native capture epoch must be a 32-character lowercase hex identity")
        self._sink = sink
        self.capture_epoch = epoch
        self._maximum_records = maximum_records
        self._maximum_bytes = maximum_bytes
        self._sequence = 0
        self._records = 0
        self._bytes_written = 0
        self._failed = False
        self._lock = threading.Lock()

    @property
    def records_written(self) -> int:
        return self._records

    @property
    def bytes_written(self) -> int:
        return self._bytes_written

    def capture_source(
        self,
        *,
        kind: CaptureKind,
        url: str,
        raw_body: bytes,
        received_at_ms: int,
        route: str | None = None,
        stream_epoch: str | None = None,
        requested_at_ms: int | None = None,
        query_json: str | None = None,
        status: int | None = None,
    ) -> str:
        """Persist exact REST entity or UTF-8 WebSocket text before parsing it."""
        self._ensure_healthy()
        if (
            kind
            not in {
                "BINANCE_KLINES_REST",
                "BINANCE_WS_FRAME",
                "BINANCE_OPEN_INTEREST_REST",
                "BINANCE_PREMIUM_INDEX_REST",
            }
            or not isinstance(url, str)
            or not url
            or len(url) > 2_048
            or not isinstance(raw_body, bytes)
            or len(raw_body) > _MAX_BODY_BYTES
            or type(received_at_ms) is not int
            or received_at_ms < 0
        ):
            self._fail("native source capture arguments are invalid or over bound")
        is_websocket = kind == "BINANCE_WS_FRAME"
        if is_websocket:
            if (
                route not in {"public", "market"}
                or not isinstance(stream_epoch, str)
                or len(stream_epoch) != 32
                or any(char not in _HEX32 for char in stream_epoch)
                or requested_at_ms is not None
                or query_json is not None
                or status is not None
            ):
                self._fail("WebSocket source capture fields are inconsistent")
        elif (
            route is not None
            or stream_epoch is not None
            or type(requested_at_ms) is not int
            or requested_at_ms < 0
            or requested_at_ms > received_at_ms
            or type(status) is not int
            or not 100 <= status <= 599
            or not isinstance(query_json, str)
            or len(query_json) > 512
        ):
            self._fail("REST source capture fields are inconsistent")
        try:
            self._validate_fixed_source(kind=kind, url=url, query_json=query_json, route=route)
        except Exception as exc:
            self._fail("native source does not match a supported fixed Binance request", exc)

        with self._lock:
            sequence = self._next_sequence()
            record = {
                "schema_version": "kairos.quant-source-capture.v1",
                "capture_scope": "public-source-bytes-not-authority",
                "kind": kind,
                "source": "BINANCE_UM",
                "method": "WS" if is_websocket else "GET",
                "url": url,
                "query_json": query_json,
                "route": route,
                "stream_epoch": stream_epoch,
                "requested_at_ms": requested_at_ms,
                "received_at_ms": received_at_ms,
                "status": status,
                "body_bytes": len(raw_body),
                "body_sha256": hashlib.sha256(raw_body).hexdigest(),
                "body_base64": base64.b64encode(raw_body).decode("ascii"),
                "capture_epoch": self.capture_epoch,
                "capture_sequence": sequence,
            }
            encoded = canonical_json_bytes(record)
            if len(encoded) > _MAX_RECORD_BYTES:
                self._fail("native raw source record exceeds its fixed bound")
            observation_id = hashlib.sha256(_RAW_DOMAIN + encoded).hexdigest()[:32]
            self._persist(observation_id, encoded)
            return observation_id

    def capture_derived_event(
        self,
        *,
        event_kind: DerivedEventKind,
        event: Any,
        parents: Mapping[str, str | Sequence[str]],
        output_observed_at_ms: int | None = None,
        replay_profile: Mapping[str, int] | None = None,
    ) -> str:
        """Persist an exact output only when its complete source lineage is supplied."""
        self._ensure_healthy()
        model_dump = getattr(event, "model_dump", None)
        if not callable(model_dump):
            self._fail("exact native market DTO is required")
        try:
            payload = model_dump(mode="json")
            event_id = event.message_id
            produced_at = event.produced_at
            captured_output_ms = (
                int(produced_at.timestamp() * 1_000)
                if output_observed_at_ms is None
                else output_observed_at_ms
            )
            if (
                not isinstance(event_id, str)
                or not event_id
                or type(captured_output_ms) is not int
                or captured_output_ms < 0
            ):
                raise ValueError("output identity/clock is invalid")
        except Exception as exc:
            self._fail("native Quant output/lineage is malformed", exc)
        return self.capture_derived_payload(
            event_kind=event_kind,
            event_id=event_id,
            event_payload=payload,
            output_observed_at_ms=captured_output_ms,
            parents=parents,
            replay_profile=replay_profile,
        )

    def capture_derived_payload(
        self,
        *,
        event_kind: DerivedEventKind,
        event_id: str,
        event_payload: Mapping[str, Any],
        output_observed_at_ms: int,
        parents: Mapping[str, str | Sequence[str]],
        replay_profile: Mapping[str, int] | None = None,
    ) -> str:
        """Persist canonical derived fields with the real output clock and exact parents."""
        self._ensure_healthy()
        if (
            event_kind not in {"CLOSED_BAR", "MARKET_SNAPSHOT"}
            or not isinstance(event_id, str)
            or not event_id
            or type(output_observed_at_ms) is not int
            or output_observed_at_ms < 0
        ):
            self._fail("native Quant output identity or clock is invalid")
        try:
            parent_payload: dict[str, str | list[str]] = {}
            for key, value in parents.items():
                if not isinstance(key, str) or not key or len(key) > 64:
                    raise ValueError("parent key is invalid")
                if isinstance(value, str):
                    values = [value]
                elif isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
                    values = list(value)
                else:
                    raise ValueError("parent capture IDs must be strings or bounded string sequences")
                if (
                    len(values) > 2_000
                    or any(
                        not isinstance(item, str)
                        or len(item) != 32
                        or any(char not in _HEX32 for char in item)
                        for item in values
                    )
                    or len(values) != len(set(values))
                ):
                    raise ValueError("parent capture IDs are malformed or duplicated")
                parent_payload[key] = values[0] if isinstance(value, str) else values
            if replay_profile is not None:
                profile = dict(replay_profile)
                if (
                    set(profile)
                    != {
                        "price_window",
                        "depth_levels",
                        "snapshot_interval_ms",
                        "kline_finality_delay_ms",
                        "maximum_binance_future_skew_ms",
                        "book_stale_after_ms",
                        "kline_stale_after_ms",
                        "derivatives_stale_after_ms",
                    }
                    or any(type(value) is not int or value < 0 for value in profile.values())
                    or any(
                        profile[key] == 0
                        for key in (
                            "price_window",
                            "depth_levels",
                            "snapshot_interval_ms",
                            "book_stale_after_ms",
                            "kline_stale_after_ms",
                            "derivatives_stale_after_ms",
                        )
                    )
                ):
                    raise ValueError("replay profile must contain bounded positive transform settings")
            else:
                profile = None
        except Exception as exc:
            self._fail("native Quant output/lineage is malformed", exc)

        try:
            event_bytes = canonical_json_bytes(event_payload)
        except Exception as exc:
            self._fail("native Quant output payload is not canonical JSON", exc)
        record = {
            "schema_version": "kairos.quant-derived-market-event.v1",
            "capture_scope": "native-quant-derived-output-not-authority",
            "event_kind": event_kind,
            "event_id": event_id,
            "event_sha256": hashlib.sha256(event_bytes).hexdigest(),
            "event_payload": dict(event_payload),
            "output_observed_at_ms": output_observed_at_ms,
            "parents": parent_payload,
            "replay_profile": profile,
        }
        try:
            encoded = canonical_json_bytes(record)
        except Exception as exc:
            self._fail("native Quant output record is not canonical JSON", exc)
        if len(encoded) > _MAX_RECORD_BYTES:
            self._fail("native Quant output record exceeds its fixed bound")
        observation_id = hashlib.sha256(_OUTPUT_DOMAIN + f"{event_kind}:{event_id}".encode()).hexdigest()[:32]
        with self._lock:
            self._persist(observation_id, encoded)
        return observation_id

    def _next_sequence(self) -> int:
        if self._records >= self._maximum_records:
            self._fail("native source capture record budget is exhausted")
        self._sequence += 1
        return self._sequence

    def _ensure_healthy(self) -> None:
        if self._failed:
            raise NativeQuantSourceCaptureFailure("native capture is latched after an earlier failure")

    def _fail(self, message: str, cause: Exception | None = None) -> NoReturn:
        self._failed = True
        error = NativeQuantSourceCaptureFailure(message)
        if cause is not None:
            raise error from cause
        raise error

    @staticmethod
    def _validate_fixed_source(
        *,
        kind: CaptureKind,
        url: str,
        query_json: str | None,
        route: str | None,
    ) -> None:
        parsed = urlsplit(url)
        if kind == "BINANCE_WS_FRAME":
            if (
                parsed.scheme != "wss"
                or parsed.netloc != "fstream.binance.com"
                or parsed.path != f"/{route}/stream"
                or parsed.fragment
                or parsed.username is not None
                or parsed.password is not None
            ):
                raise NativeQuantSourceCaptureFailure("WebSocket source is outside the fixed Binance route")
            stream_values = parse_qs(parsed.query, keep_blank_values=True, strict_parsing=True)
            streams = stream_values.get("streams", [])
            if set(stream_values) != {"streams"} or len(streams) != 1 or not streams[0]:
                raise NativeQuantSourceCaptureFailure("WebSocket stream subscription is malformed")
            if len(streams[0]) > 4_096 or any(
                not item or re.fullmatch(r"[a-z0-9]{2,32}@[A-Za-z0-9_]+(?:@[A-Za-z0-9]+)*", item) is None
                for item in streams[0].split("/")
            ):
                raise NativeQuantSourceCaptureFailure("WebSocket subscription exceeds its fixed bound")
            return

        expected_paths = {
            "BINANCE_KLINES_REST": "/fapi/v1/klines",
            "BINANCE_OPEN_INTEREST_REST": "/futures/data/openInterestHist",
            "BINANCE_PREMIUM_INDEX_REST": "/fapi/v1/premiumIndex",
        }
        if (
            parsed.scheme != "https"
            or parsed.netloc != "fapi.binance.com"
            or parsed.path != expected_paths[kind]
            or parsed.query
            or parsed.fragment
            or parsed.username is not None
            or parsed.password is not None
            or not isinstance(query_json, str)
        ):
            raise NativeQuantSourceCaptureFailure("REST source is outside the fixed Binance endpoint")
        try:
            query = json.loads(query_json)
        except (TypeError, ValueError) as exc:
            raise NativeQuantSourceCaptureFailure("REST query JSON is malformed") from exc
        if not isinstance(query, dict) or canonical_json_bytes(query).decode("utf-8") != query_json:
            raise NativeQuantSourceCaptureFailure("REST query JSON is not canonical")
        if any(not isinstance(key, str) or not isinstance(value, str) for key, value in query.items()):
            raise NativeQuantSourceCaptureFailure("REST query values must be exact wire strings")
        if kind == "BINANCE_KLINES_REST":
            valid = (
                set(query) == {"symbol", "interval", "limit"}
                and query["interval"] == "1m"
                and query["symbol"].isalnum()
                and query["symbol"] == query["symbol"].upper()
                and query["limit"].isdigit()
                and 1 <= int(query["limit"]) <= 1_500
            )
        elif kind == "BINANCE_OPEN_INTEREST_REST":
            valid = (
                set(query) == {"symbol", "period", "limit"}
                and query["period"] == "5m"
                and query["limit"] == "13"
                and query["symbol"].isalnum()
                and query["symbol"] == query["symbol"].upper()
            )
        else:
            valid = query == {}
        if not valid:
            raise NativeQuantSourceCaptureFailure(
                "REST request parameters do not match the fixed native source"
            )

    def _persist(self, observation_id: str, encoded: bytes) -> None:
        if self._records >= self._maximum_records:
            self._fail("native source capture record budget is exhausted")
        if self._bytes_written + len(encoded) > self._maximum_bytes:
            self._fail("native capture cumulative byte budget is exhausted")
        try:
            result = self._sink.create_only(observation_id, encoded)
        except Exception as exc:
            self._fail("native create-only source persistence failed", exc)
        if result is not None:
            self._fail("native source sink must synchronously return None")
        self._records += 1
        self._bytes_written += len(encoded)


__all__ = [
    "DerivedEventKind",
    "NativeCreateOnlySink",
    "NativeQuantCapture",
    "NativeQuantSourceCaptureFailure",
]
