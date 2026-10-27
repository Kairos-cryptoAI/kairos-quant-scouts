"""Focused tests for opt-in, create-only Binance source capture."""

from __future__ import annotations

import hashlib
import json
from types import SimpleNamespace

import pytest

pytest.importorskip("kairos_core")

from kairos_core.contracts import ClosedBarEventV1, canonical_json_bytes
from kairos_core.contracts.base import datetime_from_unix_ms

from kairos_quant.collectors.binance_ws import BinanceFuturesCollector
from kairos_quant.config import QuantSettings
from kairos_quant.native_capture import NativeQuantCapture, NativeQuantSourceCaptureFailure
from kairos_quant.native_replay import replay_native_quant_event
from kairos_quant.service import QuantScoutsService


class MemoryCreateOnlySink:
    def __init__(self) -> None:
        self.records: dict[str, bytes] = {}

    def create_only(self, observation_id: str, record: bytes) -> None:
        if observation_id in self.records:
            raise FileExistsError(observation_id)
        self.records[observation_id] = record


def _profile() -> dict[str, int]:
    return {
        "price_window": 200,
        "depth_levels": 10,
        "snapshot_interval_ms": 5_000,
        "kline_finality_delay_ms": 5_000,
        "maximum_binance_future_skew_ms": 2_000,
        "book_stale_after_ms": 10_000,
        "kline_stale_after_ms": 90_000,
        "derivatives_stale_after_ms": 180_000,
    }


def _raw_kline_body() -> bytes:
    return json.dumps(
        [[0, "100", "105", "98", "102", "7", 59_999, "714", 10, "4", "408", "0"]],
        separators=(",", ":"),
    ).encode()


def test_native_capture_persists_exact_body_and_enforces_fixed_endpoint() -> None:
    sink = MemoryCreateOnlySink()
    capture = NativeQuantCapture(sink, capture_epoch="a" * 32)
    raw = b'{ "exact" : true }'
    source_id = capture.capture_source(
        kind="BINANCE_KLINES_REST",
        url="https://fapi.binance.com/fapi/v1/klines",
        query_json='{"interval":"1m","limit":"200","symbol":"BTCUSDT"}',
        raw_body=raw,
        requested_at_ms=10,
        received_at_ms=11,
        status=200,
    )
    record = json.loads(sink.records[source_id])
    assert record["body_base64"]
    assert record["body_sha256"] == hashlib.sha256(raw).hexdigest()
    assert record["body_bytes"] == len(raw)
    assert (
        hashlib.sha256(b"kairos.quant-source-capture.v1\x00" + sink.records[source_id]).hexdigest()[:32]
        == source_id
    )

    with pytest.raises(NativeQuantSourceCaptureFailure):
        capture.capture_source(
            kind="BINANCE_KLINES_REST",
            url="https://example.invalid/fapi/v1/klines",
            query_json='{"interval":"1m","limit":"200","symbol":"BTCUSDT"}',
            raw_body=raw,
            requested_at_ms=10,
            received_at_ms=11,
            status=200,
        )
    assert len(sink.records) == 1


def test_create_only_failure_propagates_without_advancing_record_count() -> None:
    class BrokenSink:
        def create_only(self, observation_id: str, record: bytes) -> None:
            raise OSError("disk full")

    capture = NativeQuantCapture(BrokenSink())
    with pytest.raises(NativeQuantSourceCaptureFailure):
        capture.capture_source(
            kind="BINANCE_PREMIUM_INDEX_REST",
            url="https://fapi.binance.com/fapi/v1/premiumIndex",
            query_json="{}",
            raw_body=b"{}",
            requested_at_ms=1,
            received_at_ms=2,
            status=200,
        )
    with pytest.raises(NativeQuantSourceCaptureFailure, match="latched"):
        capture.capture_source(
            kind="BINANCE_PREMIUM_INDEX_REST",
            url="https://fapi.binance.com/fapi/v1/premiumIndex",
            query_json="{}",
            raw_body=b"{}",
            requested_at_ms=1,
            received_at_ms=2,
            status=200,
        )
    assert capture.records_written == 0


def test_cumulative_byte_budget_is_checked_before_sink_and_latches() -> None:
    first_sink = MemoryCreateOnlySink()
    first_capture = NativeQuantCapture(first_sink, capture_epoch="e" * 32)
    args = {
        "kind": "BINANCE_PREMIUM_INDEX_REST",
        "url": "https://fapi.binance.com/fapi/v1/premiumIndex",
        "query_json": "{}",
        "raw_body": b"{}",
        "requested_at_ms": 10,
        "received_at_ms": 11,
        "status": 200,
    }
    first_capture.capture_source(**args)  # type: ignore[arg-type]
    record_size = len(next(iter(first_sink.records.values())))

    sink = MemoryCreateOnlySink()
    capture = NativeQuantCapture(sink, capture_epoch="e" * 32, maximum_bytes=record_size)
    capture.capture_source(**args)  # type: ignore[arg-type]
    with pytest.raises(NativeQuantSourceCaptureFailure, match="byte budget"):
        capture.capture_source(**args)  # type: ignore[arg-type]
    with pytest.raises(NativeQuantSourceCaptureFailure, match="latched"):
        capture.capture_source(**args)  # type: ignore[arg-type]
    assert capture.records_written == 1
    assert capture.bytes_written == record_size
    assert len(sink.records) == 1


def test_replay_requires_two_distinct_original_rest_confirmations() -> None:
    sink = MemoryCreateOnlySink()
    capture = NativeQuantCapture(sink, capture_epoch="b" * 32)
    raw_body = _raw_kline_body()
    query = '{"interval":"1m","limit":"200","symbol":"BTCUSDT"}'
    first = capture.capture_source(
        kind="BINANCE_KLINES_REST",
        url="https://fapi.binance.com/fapi/v1/klines",
        query_json=query,
        raw_body=raw_body,
        requested_at_ms=59_000,
        received_at_ms=59_100,
        status=200,
    )
    second = capture.capture_source(
        kind="BINANCE_KLINES_REST",
        url="https://fapi.binance.com/fapi/v1/klines",
        query_json=query,
        raw_body=raw_body,
        requested_at_ms=65_000,
        received_at_ms=65_100,
        status=200,
    )
    event = ClosedBarEventV1(
        source="kairos-quant-scouts",
        symbol="BTCUSDT",
        timeframe="1m",
        open_time_ms=0,
        close_time_ms=59_999,
        open=100,
        high=105,
        low=98,
        close=102,
        base_volume=7,
        quote_volume=714,
        taker_buy_base_volume=4,
        taker_buy_quote_volume=408,
    )
    output_id = capture.capture_derived_payload(
        event_kind="CLOSED_BAR",
        event_id=str(event.bar_sha256),
        event_payload=event.model_dump(mode="json"),
        output_observed_at_ms=65_150,
        parents={"rest_confirmations": (first, second)},
        replay_profile=_profile(),
    )
    records = {**sink.records}
    replayed = replay_native_quant_event(
        event_kind="CLOSED_BAR",
        expected_payload=event.model_dump(mode="json"),
        source_records=records,
        output_record=records[output_id],
    )
    assert canonical_json_bytes(replayed) == canonical_json_bytes(event.model_dump(mode="json"))

    output_before_second_parent_ack = json.loads(records[output_id])
    output_before_second_parent_ack["output_observed_at_ms"] = 65_099
    with pytest.raises(NativeQuantSourceCaptureFailure):
        replay_native_quant_event(
            event_kind="CLOSED_BAR",
            expected_payload=event.model_dump(mode="json"),
            source_records=records,
            output_record=output_before_second_parent_ack,
        )

    altered = json.loads(records[output_id])
    altered["parents"]["rest_confirmations"] = [first, first]
    with pytest.raises(NativeQuantSourceCaptureFailure):
        replay_native_quant_event(
            event_kind="CLOSED_BAR",
            expected_payload=event.model_dump(mode="json"),
            source_records=records,
            output_record=altered,
        )


def test_capture_mode_requires_official_source_roots_and_rejects_credentials() -> None:
    with pytest.raises(ValueError):
        BinanceFuturesCollector(
            ["btcusdt"],
            "wss://fstream.binance.com",
            "https://example.invalid",
            native_capture=NativeQuantCapture(MemoryCreateOnlySink()),
        )
    collector = BinanceFuturesCollector(
        ["btcusdt"],
        "wss://fstream.binance.com",
        "https://fapi.binance.com",
        native_capture=NativeQuantCapture(MemoryCreateOnlySink()),
    )

    class Session:
        trust_env = False
        auth = None
        _default_proxy = None
        _default_proxy_auth = None
        _default_auth = None
        _middlewares = ()
        headers = {"X-MBX-APIKEY": "not-a-real-key"}
        cookie_jar = None

    with pytest.raises(PermissionError):
        collector._require_native_session_policy(Session())  # type: ignore[arg-type]

    class CookieJar:
        def filter_cookies(self, url: str) -> dict[str, str]:
            return {"session": "value"}

    Session.headers = {}
    Session.cookie_jar = CookieJar()
    with pytest.raises(PermissionError):
        collector._require_native_session_policy(Session())  # type: ignore[arg-type]


def test_market_snapshot_replays_through_existing_collector_and_builder() -> None:
    from kairos_quant.snapshot import SnapshotBuilder

    sink = MemoryCreateOnlySink()
    capture = NativeQuantCapture(sink, capture_epoch="c" * 32)
    now_ms = 1_800_000_000_000
    open_ms = now_ms - 120_000
    close_ms = open_ms + 59_999
    raw_kline = json.dumps(
        [[open_ms, "100", "105", "98", "102", "7", close_ms, "714", 10, "4", "408", "0"]],
        separators=(",", ":"),
    ).encode()
    query = '{"interval":"1m","limit":"200","symbol":"BTCUSDT"}'
    first_bar_source = capture.capture_source(
        kind="BINANCE_KLINES_REST",
        url="https://fapi.binance.com/fapi/v1/klines",
        query_json=query,
        raw_body=raw_kline,
        requested_at_ms=now_ms - 7_000,
        received_at_ms=now_ms - 6_500,
        status=200,
    )
    second_bar_source = capture.capture_source(
        kind="BINANCE_KLINES_REST",
        url="https://fapi.binance.com/fapi/v1/klines",
        query_json=query,
        raw_body=raw_kline,
        requested_at_ms=now_ms - 6_000,
        received_at_ms=now_ms - 5_500,
        status=200,
    )
    bar = ClosedBarEventV1(
        source="kairos-quant-scouts",
        symbol="BTCUSDT",
        timeframe="1m",
        open_time_ms=open_ms,
        close_time_ms=close_ms,
        open=100,
        high=105,
        low=98,
        close=102,
        base_volume=7,
        quote_volume=714,
        taker_buy_base_volume=4,
        taker_buy_quote_volume=408,
    )
    bar_output = capture.capture_derived_payload(
        event_kind="CLOSED_BAR",
        event_id=str(bar.bar_sha256),
        event_payload=bar.model_dump(mode="json"),
        output_observed_at_ms=now_ms - 5_500,
        parents={"rest_confirmations": (first_bar_source, second_bar_source)},
        replay_profile=_profile(),
    )

    depth_message = {
        "stream": "btcusdt@depth10@100ms",
        "data": {
            "e": "depthUpdate",
            "E": now_ms - 1_200,
            "T": now_ms - 1_200,
            "s": "BTCUSDT",
            "u": 100,
            "b": [["101", "3"]],
            "a": [["103", "2"]],
        },
    }
    mark_message = {
        "stream": "btcusdt@markPrice@1s",
        "data": {"e": "markPriceUpdate", "E": now_ms - 1_100, "s": "BTCUSDT", "r": "0.0001"},
    }
    depth_body = json.dumps(depth_message, separators=(",", ":")).encode()
    mark_body = json.dumps(mark_message, separators=(",", ":")).encode()
    depth_id = capture.capture_source(
        kind="BINANCE_WS_FRAME",
        url="wss://fstream.binance.com/public/stream?streams=btcusdt@depth10@100ms",
        raw_body=depth_body,
        received_at_ms=now_ms - 1_000,
        route="public",
        stream_epoch="1" * 32,
    )
    mark_id = capture.capture_source(
        kind="BINANCE_WS_FRAME",
        url="wss://fstream.binance.com/market/stream?streams=btcusdt@markPrice@1s",
        raw_body=mark_body,
        received_at_ms=now_ms - 900,
        route="market",
        stream_epoch="2" * 32,
    )
    oi_body = json.dumps(
        [
            {
                "symbol": "BTCUSDT",
                "timestamp": now_ms - 3_850_000 + index * 300_000,
                "sumOpenInterestValue": str(1000 + index),
            }
            for index in range(13)
        ],
        separators=(",", ":"),
    ).encode()
    oi_id = capture.capture_source(
        kind="BINANCE_OPEN_INTEREST_REST",
        url="https://fapi.binance.com/futures/data/openInterestHist",
        query_json='{"limit":"13","period":"5m","symbol":"BTCUSDT"}',
        raw_body=oi_body,
        requested_at_ms=now_ms - 10_000,
        received_at_ms=now_ms - 1_000,
        status=200,
    )

    from kairos_quant.collectors.binance_ws import BinanceFuturesCollector

    collector = BinanceFuturesCollector(["btcusdt"], "wss://fstream.binance.com", "https://fapi.binance.com")
    assert collector._on_message(depth_message, route="public")
    assert collector._on_message(mark_message, route="market")
    # Match the production OI input projection for this captured 13-point series.
    # The newest exchange sample is 250 seconds old but was fetched just now;
    # Quant's source-age allowance is 600 seconds and local freshness is recent.
    observations = [(now_ms - 3_850_000 + index * 300_000, 1000 + index) for index in range(13)]
    collector.open_interest["btcusdt"] = float(observations[-1][1])
    collector.oi_change_pct_1h["btcusdt"] = (
        100.0 * (observations[-1][1] - observations[0][1]) / observations[0][1]
    )
    builder = SnapshotBuilder("kairos-quant-scouts", window=200, depth_levels=10)
    builder.push_candle("BTCUSDT", high=105, low=98, close=102)
    expected = builder.build(
        "BTCUSDT",
        bids=collector.books["btcusdt"]["bids"],
        asks=collector.books["btcusdt"]["asks"],
        funding_rate=collector.funding["btcusdt"],
        open_interest=collector.open_interest["btcusdt"],
        oi_change_pct_1h=collector.oi_change_pct_1h["btcusdt"],
        volume_usd=714,
    ).model_copy(update={"produced_at": datetime_from_unix_ms(now_ms)})
    parents = {
        "depth_frame": depth_id,
        "funding_source": mark_id,
        "open_interest_source": oi_id,
        "force_order_frames": (),
        "confirmed_bars": (bar_output,),
    }
    output_id = capture.capture_derived_event(
        event_kind="MARKET_SNAPSHOT",
        event=expected,
        output_observed_at_ms=now_ms,
        parents=parents,
        replay_profile=_profile(),
    )
    records = {**sink.records}
    replayed = replay_native_quant_event(
        event_kind="MARKET_SNAPSHOT",
        expected_payload=expected.model_dump(mode="json"),
        source_records=records,
        output_record=records[output_id],
    )
    assert replayed["symbol"] == expected.symbol
    assert replayed["order_book"] == expected.order_book.model_dump(mode="json")

    stale_source_body = json.dumps(
        [
            {
                "symbol": "BTCUSDT",
                "timestamp": now_ms - 4_400_000 + index * 300_000,
                "sumOpenInterestValue": str(1000 + index),
            }
            for index in range(13)
        ],
        separators=(",", ":"),
    ).encode()
    stale_source_id = capture.capture_source(
        kind="BINANCE_OPEN_INTEREST_REST",
        url="https://fapi.binance.com/futures/data/openInterestHist",
        query_json='{"limit":"13","period":"5m","symbol":"BTCUSDT"}',
        raw_body=stale_source_body,
        requested_at_ms=now_ms - 10_000,
        received_at_ms=now_ms - 1_000,
        status=200,
    )
    stale_source_output = json.loads(records[output_id])
    stale_source_output["parents"]["open_interest_source"] = stale_source_id
    records[stale_source_id] = sink.records[stale_source_id]
    with pytest.raises(NativeQuantSourceCaptureFailure, match="stale"):
        replay_native_quant_event(
            event_kind="MARKET_SNAPSHOT",
            expected_payload=expected.model_dump(mode="json"),
            source_records=records,
            output_record=stale_source_output,
        )

    incomplete = dict(records)
    incomplete.pop(first_bar_source)
    with pytest.raises(NativeQuantSourceCaptureFailure):
        replay_native_quant_event(
            event_kind="MARKET_SNAPSHOT",
            expected_payload=expected.model_dump(mode="json"),
            source_records=incomplete,
            output_record=records[output_id],
        )

    other_epoch_sink = MemoryCreateOnlySink()
    other_epoch_capture = NativeQuantCapture(other_epoch_sink, capture_epoch="d" * 32)
    alternate_depth_id = other_epoch_capture.capture_source(
        kind="BINANCE_WS_FRAME",
        url="wss://fstream.binance.com/public/stream?streams=btcusdt@depth10@100ms",
        raw_body=depth_body,
        received_at_ms=now_ms - 1_000,
        route="public",
        stream_epoch="1" * 32,
    )
    alternate_depth = json.loads(next(iter(other_epoch_sink.records.values())))
    mixed_epoch_records = {**records, alternate_depth_id: alternate_depth}
    tampered_output = json.loads(records[output_id])
    tampered_output["parents"]["depth_frame"] = alternate_depth_id
    with pytest.raises(NativeQuantSourceCaptureFailure):
        replay_native_quant_event(
            event_kind="MARKET_SNAPSHOT",
            expected_payload=expected.model_dump(mode="json"),
            source_records=mixed_epoch_records,
            output_record=tampered_output,
        )

    altered_source_records = dict(records)
    altered_raw = json.loads(records[depth_id])
    altered_raw["body_base64"] = "e30="
    altered_source_records[depth_id] = altered_raw
    with pytest.raises(NativeQuantSourceCaptureFailure):
        replay_native_quant_event(
            event_kind="MARKET_SNAPSHOT",
            expected_payload=expected.model_dump(mode="json"),
            source_records=altered_source_records,
            output_record=records[output_id],
        )

    tampered_metadata = json.loads(records[output_id])
    tampered_metadata["event_sha256"] = "0" * 64
    with pytest.raises(NativeQuantSourceCaptureFailure):
        replay_native_quant_event(
            event_kind="MARKET_SNAPSHOT",
            expected_payload=expected.model_dump(mode="json"),
            source_records=records,
            output_record=tampered_metadata,
        )


@pytest.mark.asyncio
async def test_native_capture_sink_failure_prevents_bar_publication() -> None:
    class BrokenSink:
        def create_only(self, observation_id: str, record: bytes) -> None:
            raise OSError("disk full")

    service = QuantScoutsService(
        QuantSettings(bus_backend="memory"),
        native_capture=NativeQuantCapture(BrokenSink()),
    )

    class CountingBus:
        published = 0

        async def publish(self, topic: str, event: object) -> None:
            self.published += 1

    bus = CountingBus()
    service.bus = bus  # type: ignore[assignment]
    candle = service.collector._make_closed_kline("BTCUSDT", 0, 59_999, 100, 105, 98, 102, 7, 714, 4, 408)
    assert candle is not None
    service.collector._closed_klines["btcusdt"].append(candle)
    service.collector._native_bar_sources["btcusdt"][59_999] = (("1" * 32, "2" * 32), 65_100)
    with pytest.raises(NativeQuantSourceCaptureFailure):
        await service._emit_once()
    assert bus.published == 0
    assert service.collector.pending_closed_klines("btcusdt") == (candle,)


@pytest.mark.asyncio
async def test_native_http_reader_stops_at_limit_plus_one_byte() -> None:
    class Stream:
        total = 0

        async def read(self, amount: int) -> bytes:
            chunk = b"x" * amount
            self.total += len(chunk)
            return chunk

    stream = Stream()
    response = SimpleNamespace(headers={}, content=stream)
    collector = BinanceFuturesCollector(
        ["btcusdt"],
        "wss://fstream.binance.com",
        "https://fapi.binance.com",
        native_capture=NativeQuantCapture(MemoryCreateOnlySink()),
    )
    with pytest.raises(PermissionError):
        await collector._read_native_response(response)  # type: ignore[arg-type]
    assert stream.total == 1_000_001
