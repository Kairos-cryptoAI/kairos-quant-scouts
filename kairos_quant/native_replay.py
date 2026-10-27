"""Pure offline replay for Quant's native source-capture records.

This module opens no sockets, databases or sinks. It validates the persisted
parent records and delegates accepted Binance message parsing and snapshot
math to the existing collector and SnapshotBuilder.
"""

from __future__ import annotations

import base64
import hashlib
import json
from collections.abc import Mapping
from typing import Any, Literal, cast
from urllib.parse import parse_qs, urlsplit

from kairos_core.contracts import ClosedBarEventV1, MarketSnapshot, canonical_json_bytes

from .collectors.binance_ws import BinanceFuturesCollector
from .native_capture import NativeQuantCapture, NativeQuantSourceCaptureFailure
from .snapshot import SnapshotBuilder

_RAW_DOMAIN = b"kairos.quant-source-capture.v1\x00"
_OUTPUT_DOMAIN = b"kairos.quant-derived-market-event.v1\x00"


def _no_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def _decode_object(value: bytes | Mapping[str, Any]) -> dict[str, Any]:
    if isinstance(value, bytes):
        decoded = json.loads(value, object_pairs_hook=_no_duplicate_keys)
    elif isinstance(value, Mapping):
        decoded = dict(value)
    else:
        raise NativeQuantSourceCaptureFailure("replay input must be persisted record bytes or a mapping")
    if not isinstance(decoded, dict):
        raise NativeQuantSourceCaptureFailure("replay record must be a JSON object")
    return decoded


def _load_record(
    source_records: Mapping[str, bytes | Mapping[str, Any]], observation_id: str
) -> dict[str, Any]:
    try:
        record = _decode_object(source_records[observation_id])
    except KeyError as exc:
        raise NativeQuantSourceCaptureFailure("replay is missing a referenced persisted parent") from exc
    if record.get("schema_version") == "kairos.quant-source-capture.v1":
        encoded = canonical_json_bytes(record)
        expected_id = hashlib.sha256(_RAW_DOMAIN + encoded).hexdigest()[:32]
        if expected_id != observation_id:
            raise NativeQuantSourceCaptureFailure("raw source record identity does not match its content")
        try:
            NativeQuantCapture._validate_fixed_source(
                kind=record["kind"],
                url=record["url"],
                query_json=record.get("query_json"),
                route=record.get("route"),
            )
        except (KeyError, TypeError) as exc:
            raise NativeQuantSourceCaptureFailure("raw source fixed-route identity is malformed") from exc
        try:
            body = base64.b64decode(record["body_base64"], validate=True)
        except (KeyError, ValueError, TypeError) as exc:
            raise NativeQuantSourceCaptureFailure("raw source body encoding is invalid") from exc
        if (
            record.get("body_bytes") != len(body)
            or record.get("body_sha256") != hashlib.sha256(body).hexdigest()
            or type(record.get("received_at_ms")) is not int
            or record["received_at_ms"] < 0
        ):
            raise NativeQuantSourceCaptureFailure("raw source body digest or receipt clock is invalid")
        return record
    if record.get("schema_version") == "kairos.quant-derived-market-event.v1":
        event_bytes = canonical_json_bytes(record.get("event_payload", {}))
        if record.get("event_sha256") != hashlib.sha256(event_bytes).hexdigest():
            raise NativeQuantSourceCaptureFailure("derived event payload digest is invalid")
        expected_id = hashlib.sha256(
            _OUTPUT_DOMAIN + f"{record.get('event_kind')}:{record.get('event_id')}".encode()
        ).hexdigest()[:32]
        if expected_id != observation_id:
            raise NativeQuantSourceCaptureFailure("derived event identity does not match its record ID")
        return record
    raise NativeQuantSourceCaptureFailure("replay record schema is not supported")


def _raw_body(record: Mapping[str, Any]) -> bytes:
    try:
        return base64.b64decode(record["body_base64"], validate=True)
    except (KeyError, ValueError, TypeError) as exc:
        raise NativeQuantSourceCaptureFailure("raw source body encoding is invalid") from exc


def _parse_kline_row(row: Any, symbol: str) -> tuple[Any, ...] | None:
    if not isinstance(row, list) or len(row) < 11:
        return None
    collector = BinanceFuturesCollector
    open_time = collector._integer(row[0])
    close_time = collector._integer(row[6])
    floats = tuple(collector._finite_float(row[index]) for index in (1, 2, 3, 4, 5, 7, 9, 10))
    if open_time is None or close_time is None or any(value is None for value in floats):
        return None
    return (symbol, "1m", open_time, close_time, *floats)


def _replay_closed_bar(
    *,
    event_payload: Mapping[str, Any],
    output_record: Mapping[str, Any],
    source_records: Mapping[str, bytes | Mapping[str, Any]],
) -> dict[str, Any]:
    parents = output_record.get("parents")
    if not isinstance(parents, Mapping) or set(parents) != {"rest_confirmations"}:
        raise NativeQuantSourceCaptureFailure("closed-bar output has an unexpected parent set")
    profile = output_record.get("replay_profile")
    if (
        not isinstance(profile, Mapping)
        or set(profile)
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
    ):
        raise NativeQuantSourceCaptureFailure("closed-bar replay profile is incomplete")
    confirmation_ids = parents.get("rest_confirmations")
    if (
        not isinstance(confirmation_ids, list)
        or len(confirmation_ids) != 2
        or len(set(confirmation_ids)) != 2
    ):
        raise NativeQuantSourceCaptureFailure("closed bar requires two distinct REST confirmations")
    symbol = event_payload.get("symbol")
    if not isinstance(symbol, str) or not symbol or symbol != symbol.upper():
        raise NativeQuantSourceCaptureFailure("closed-bar symbol is not normalized")
    matching_rows: list[tuple[Any, ...]] = []
    confirmation_records: list[dict[str, Any]] = []
    for source_id in confirmation_ids:
        record = _load_record(source_records, source_id)
        if (
            record.get("kind") != "BINANCE_KLINES_REST"
            or record.get("source") != "BINANCE_UM"
            or record.get("method") != "GET"
            or record.get("url") != "https://fapi.binance.com/fapi/v1/klines"
            or record.get("status") != 200
        ):
            raise NativeQuantSourceCaptureFailure("closed-bar parent is not an official REST kline response")
        query_text = record.get("query_json")
        if not isinstance(query_text, str):
            raise NativeQuantSourceCaptureFailure("closed-bar query JSON is missing")
        try:
            query = json.loads(query_text, object_pairs_hook=_no_duplicate_keys)
        except (TypeError, ValueError) as exc:
            raise NativeQuantSourceCaptureFailure("closed-bar query JSON is invalid") from exc
        if (
            not isinstance(query, dict)
            or set(query) != {"symbol", "interval", "limit"}
            or query.get("symbol") != symbol
            or query.get("interval") != "1m"
            or not isinstance(query.get("limit"), str)
            or not query["limit"].isdigit()
            or not 1 <= int(query["limit"]) <= 1_500
            or canonical_json_bytes(query).decode("utf-8") != query_text
        ):
            raise NativeQuantSourceCaptureFailure("closed-bar REST request identity is inconsistent")
        try:
            body = json.loads(_raw_body(record), object_pairs_hook=_no_duplicate_keys)
        except (TypeError, ValueError) as exc:
            raise NativeQuantSourceCaptureFailure("closed-bar REST body is not valid JSON") from exc
        if not isinstance(body, list):
            raise NativeQuantSourceCaptureFailure("closed-bar REST response is not a kline array")
        rows = [
            parsed
            for raw_row in body
            if (parsed := _parse_kline_row(raw_row, symbol)) is not None
            and parsed[2] == event_payload.get("open_time_ms")
        ]
        if len(rows) != 1:
            raise NativeQuantSourceCaptureFailure("closed bar is missing or ambiguous in REST response")
        matching_rows.append(rows[0])
        confirmation_records.append(record)
    if matching_rows[0] != matching_rows[1]:
        raise NativeQuantSourceCaptureFailure("the two REST kline confirmations disagree")
    first, second = confirmation_records
    clock_values = (
        first.get("requested_at_ms"),
        first.get("received_at_ms"),
        second.get("requested_at_ms"),
        second.get("received_at_ms"),
        output_record.get("output_observed_at_ms"),
    )
    if any(type(value) is not int for value in clock_values):
        raise NativeQuantSourceCaptureFailure("REST confirmation clocks are not integers")
    first_requested_ms = cast(int, first["requested_at_ms"])
    first_received_ms = cast(int, first["received_at_ms"])
    second_requested_ms = cast(int, second["requested_at_ms"])
    second_received_ms = cast(int, second["received_at_ms"])
    output_observed_ms = cast(int, output_record["output_observed_at_ms"])
    if (
        first_requested_ms > second_requested_ms
        or first_received_ms > second_requested_ms
        or output_observed_ms < second_received_ms
    ):
        raise NativeQuantSourceCaptureFailure("REST confirmation or output clocks are inconsistent")
    row = matching_rows[0]
    if (
        row[3] > second_received_ms - profile["kline_finality_delay_ms"]
        or row[3] > second_received_ms + profile["maximum_binance_future_skew_ms"]
    ):
        raise NativeQuantSourceCaptureFailure(
            "closed-bar finality cannot be proven from captured receive clocks"
        )
    replayed = ClosedBarEventV1(
        source=str(event_payload.get("source", "kairos-quant-scouts")),
        symbol=symbol,
        timeframe="1m",
        open_time_ms=row[2],
        close_time_ms=row[3],
        open=row[4],
        high=row[5],
        low=row[6],
        close=row[7],
        base_volume=row[8],
        quote_volume=row[9],
        taker_buy_base_volume=row[10],
        taker_buy_quote_volume=row[11],
    )
    try:
        original_event = ClosedBarEventV1.model_validate(dict(event_payload))
    except Exception as exc:
        raise NativeQuantSourceCaptureFailure("captured full closed-bar DTO is invalid") from exc
    if canonical_json_bytes(replayed.identity_payload()) != canonical_json_bytes(
        original_event.identity_payload()
    ) or canonical_json_bytes(original_event.model_dump(mode="json")) != canonical_json_bytes(event_payload):
        raise NativeQuantSourceCaptureFailure("confirmed REST candle does not reproduce the published bar")
    if output_record.get("event_id") != original_event.bar_sha256:
        raise NativeQuantSourceCaptureFailure("closed-bar output identity does not match its reproduced hash")
    return original_event.model_dump(mode="json")


def replay_native_quant_event(
    *,
    event_kind: Literal["CLOSED_BAR", "MARKET_SNAPSHOT"],
    expected_payload: Mapping[str, Any],
    source_records: Mapping[str, bytes | Mapping[str, Any]],
    output_record: bytes | Mapping[str, Any],
) -> dict[str, Any]:
    """Replay one persisted Quant output and refuse any missing or altered lineage.

    ``source_records`` is keyed by create-only observation ID and contains the
    original persisted bytes (or already-decoded records from the same reader).
    The return value is only the reconstructed domain payload; no authority is
    created by successful replay.
    """
    output = _decode_object(output_record)
    if (
        output.get("schema_version") != "kairos.quant-derived-market-event.v1"
        or output.get("event_kind") != event_kind
        or canonical_json_bytes(output.get("event_payload", {})) != canonical_json_bytes(expected_payload)
        or output.get("event_sha256") != hashlib.sha256(canonical_json_bytes(expected_payload)).hexdigest()
    ):
        raise NativeQuantSourceCaptureFailure("selected derived output does not match the requested event")
    event_id = output.get("event_id")
    if not isinstance(event_id, str):
        raise NativeQuantSourceCaptureFailure("selected derived output has no stable event identity")
    if event_kind == "CLOSED_BAR":
        return _replay_closed_bar(
            event_payload=expected_payload,
            output_record=output,
            source_records=source_records,
        )
    return _replay_market_snapshot(
        event_payload=expected_payload,
        output_record=output,
        source_records=source_records,
    )


def _replay_market_snapshot(
    *,
    event_payload: Mapping[str, Any],
    output_record: Mapping[str, Any],
    source_records: Mapping[str, bytes | Mapping[str, Any]],
) -> dict[str, Any]:
    parents = output_record.get("parents")
    required = {
        "depth_frame",
        "funding_source",
        "open_interest_source",
        "force_order_frames",
        "confirmed_bars",
    }
    if not isinstance(parents, Mapping) or set(parents) != required:
        raise NativeQuantSourceCaptureFailure("market snapshot output has an unexpected parent set")
    profile = output_record.get("replay_profile")
    if (
        not isinstance(profile, Mapping)
        or set(profile)
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
        or profile["price_window"] < 34
    ):
        raise NativeQuantSourceCaptureFailure("market snapshot replay profile is incomplete")
    symbol = event_payload.get("symbol")
    if not isinstance(symbol, str) or symbol != symbol.upper() or not symbol:
        raise NativeQuantSourceCaptureFailure("market snapshot symbol is not normalized")

    bar_ids = parents.get("confirmed_bars")
    if (
        not isinstance(bar_ids, list)
        or not bar_ids
        or len(bar_ids) > 2_000
        or len(bar_ids) != len(set(bar_ids))
    ):
        raise NativeQuantSourceCaptureFailure("market snapshot lacks a bounded complete bar prefix")
    bars: list[dict[str, Any]] = []
    bar_records: list[dict[str, Any]] = []
    for bar_id in bar_ids:
        bar_record = _load_record(source_records, bar_id)
        if bar_record.get("event_kind") != "CLOSED_BAR":
            raise NativeQuantSourceCaptureFailure("market snapshot prefix contains a non-bar output")
        bar_payload = bar_record.get("event_payload")
        if not isinstance(bar_payload, Mapping) or bar_payload.get("symbol") != symbol:
            raise NativeQuantSourceCaptureFailure("market snapshot prefix symbol does not match")
        replayed_bar = _replay_closed_bar(
            event_payload=bar_payload,
            output_record=bar_record,
            source_records=source_records,
        )
        bars.append(replayed_bar)
        bar_records.append(bar_record)
    if any(
        current["close_time_ms"] - previous["close_time_ms"] != 60_000
        for previous, current in zip(bars, bars[1:], strict=False)
    ):
        raise NativeQuantSourceCaptureFailure("market snapshot bar prefix contains a gap")

    output_ms = output_record.get("output_observed_at_ms")
    if type(output_ms) is not int or output_ms < 0:
        raise NativeQuantSourceCaptureFailure("market snapshot output clock is invalid")
    capture_epochs: set[str] = set()

    def raw_parent(parent_id: str, *, expected_kind: str) -> dict[str, Any]:
        record = _load_record(source_records, parent_id)
        if (
            record.get("schema_version") != "kairos.quant-source-capture.v1"
            or record.get("capture_scope") != "public-source-bytes-not-authority"
            or record.get("source") != "BINANCE_UM"
            or record.get("kind") != expected_kind
            or record.get("received_at_ms", output_ms + 1) > output_ms
            or record.get("method") not in {"GET", "WS"}
        ):
            raise NativeQuantSourceCaptureFailure(
                "market snapshot raw source parent is invalid or later than output"
            )
        epoch = record.get("capture_epoch")
        if not isinstance(epoch, str) or len(epoch) != 32:
            raise NativeQuantSourceCaptureFailure("market snapshot source lacks its capture epoch")
        capture_epochs.add(epoch)
        if record.get("method") == "GET" and (
            type(record.get("requested_at_ms")) is not int
            or record["requested_at_ms"] > record["received_at_ms"]
            or record.get("status") != 200
        ):
            raise NativeQuantSourceCaptureFailure("market snapshot REST source clocks/status are invalid")
        if record.get("method") == "WS" and (
            record.get("requested_at_ms") is not None or record.get("status") is not None
        ):
            raise NativeQuantSourceCaptureFailure("market snapshot WebSocket source fields are inconsistent")
        return record

    for bar_record in bar_records:
        for parent_id in bar_record["parents"]["rest_confirmations"]:
            raw_parent(parent_id, expected_kind="BINANCE_KLINES_REST")

    depth_id = parents.get("depth_frame")
    funding_id = parents.get("funding_source")
    oi_id = parents.get("open_interest_source")
    if not all(isinstance(item, str) for item in (depth_id, funding_id, oi_id)):
        raise NativeQuantSourceCaptureFailure("market snapshot has a malformed required source ID")
    depth_id = cast(str, depth_id)
    funding_id = cast(str, funding_id)
    oi_id = cast(str, oi_id)
    depth_record = raw_parent(depth_id, expected_kind="BINANCE_WS_FRAME")
    if depth_record.get("route") != "public" or not _stream_route_matches(
        depth_record, route="public", symbol=symbol, suffix="depth10@100ms"
    ):
        raise NativeQuantSourceCaptureFailure("market snapshot depth parent has the wrong symbol or route")
    funding_record = _load_record(source_records, funding_id)
    if funding_record.get("kind") == "BINANCE_WS_FRAME":
        funding_record = raw_parent(funding_id, expected_kind="BINANCE_WS_FRAME")
        if funding_record.get("route") != "market" or not _stream_route_matches(
            funding_record, route="market", symbol=symbol, suffix="markPrice@1s"
        ):
            raise NativeQuantSourceCaptureFailure("market snapshot funding WS parent is mismatched")
    elif funding_record.get("kind") == "BINANCE_PREMIUM_INDEX_REST":
        funding_record = raw_parent(funding_id, expected_kind="BINANCE_PREMIUM_INDEX_REST")
    else:
        raise NativeQuantSourceCaptureFailure("market snapshot funding source kind is unsupported")
    oi_record = raw_parent(oi_id, expected_kind="BINANCE_OPEN_INTEREST_REST")
    force_ids = parents.get("force_order_frames")
    if (
        not isinstance(force_ids, list)
        or len(force_ids) > 2_000
        or len(force_ids) != len(set(force_ids))
        or not all(isinstance(item, str) for item in force_ids)
    ):
        raise NativeQuantSourceCaptureFailure("market snapshot force-order lineage is malformed")
    force_records = [raw_parent(item, expected_kind="BINANCE_WS_FRAME") for item in force_ids]
    if any(
        right.get("capture_sequence", 0) <= left.get("capture_sequence", 0)
        for left, right in zip(force_records, force_records[1:], strict=False)
    ):
        raise NativeQuantSourceCaptureFailure("force-order source frames are not in original arrival order")
    for record in force_records:
        if record.get("route") != "market" or not _stream_route_matches(
            record, route="market", symbol=symbol, suffix="forceOrder"
        ):
            raise NativeQuantSourceCaptureFailure("market snapshot force-order parent is mismatched")
    if len(capture_epochs) != 1:
        raise NativeQuantSourceCaptureFailure(
            "market snapshot joins source records from different capture sessions"
        )

    collector = BinanceFuturesCollector(
        [symbol.lower()],
        "wss://fstream.binance.com",
        "https://fapi.binance.com",
    )
    depth_message = _decode_ws_message(depth_record)
    if not collector._on_message(depth_message, route="public"):
        raise NativeQuantSourceCaptureFailure("captured depth frame is rejected by the existing collector")
    funding_message: dict[str, Any] | None = None
    if funding_record.get("kind") == "BINANCE_WS_FRAME":
        funding_message = _decode_ws_message(funding_record)
        if not collector._on_message(funding_message, route="market"):
            raise NativeQuantSourceCaptureFailure(
                "captured mark-price frame is rejected by the existing collector"
            )
    else:
        _apply_premium_index(collector, funding_record, symbol)
    _apply_open_interest(collector, oi_record, symbol)
    for force_record in force_records:
        if not collector._on_message(_decode_ws_message(force_record), route="market"):
            raise NativeQuantSourceCaptureFailure(
                "captured force-order frame is rejected by the existing collector"
            )

    latest_bar = bars[-1]
    if (
        latest_bar["close_time_ms"] > output_ms
        or output_ms - latest_bar["close_time_ms"] > profile["kline_stale_after_ms"]
    ):
        raise NativeQuantSourceCaptureFailure("market snapshot closed-bar input is stale at output time")
    depth_data = depth_message.get("data", {})
    depth_time = collector._integer(depth_data.get("E")) if isinstance(depth_data, dict) else None
    if (
        depth_time is None
        or depth_time > output_ms + profile["maximum_binance_future_skew_ms"]
        or output_ms - depth_time > profile["book_stale_after_ms"]
    ):
        raise NativeQuantSourceCaptureFailure("market snapshot depth source is stale at output time")
    if funding_message is not None:
        funding_data = funding_message.get("data", {})
        funding_time = collector._integer(funding_data.get("E")) if isinstance(funding_data, dict) else None
    else:
        premium_payload = json.loads(_raw_body(funding_record), object_pairs_hook=_no_duplicate_keys)
        premium_items = premium_payload if isinstance(premium_payload, list) else [premium_payload]
        premium_matches = [
            item for item in premium_items if isinstance(item, dict) and item.get("symbol") == symbol
        ]
        funding_time = (
            collector._integer(premium_matches[0].get("time")) if len(premium_matches) == 1 else None
        )
    if (
        funding_time is None
        or funding_time > output_ms + profile["maximum_binance_future_skew_ms"]
        or output_ms - funding_time > profile["derivatives_stale_after_ms"]
    ):
        raise NativeQuantSourceCaptureFailure("market snapshot funding source is stale at output time")
    oi_payload = json.loads(_raw_body(oi_record), object_pairs_hook=_no_duplicate_keys)
    oi_stamps = [
        collector._integer(item.get("timestamp")) for item in oi_payload[-13:] if isinstance(item, dict)
    ]
    valid_oi_stamps = [stamp for stamp in oi_stamps if stamp is not None]
    if not valid_oi_stamps or output_ms - valid_oi_stamps[-1] > profile["derivatives_stale_after_ms"]:
        raise NativeQuantSourceCaptureFailure("market snapshot OI source is stale at output time")
    builder = SnapshotBuilder(
        str(event_payload.get("source", "kairos-quant-scouts")),
        window=profile["price_window"],
        depth_levels=profile["depth_levels"],
    )
    for bar in bars:
        builder.push_candle(symbol, high=bar["high"], low=bar["low"], close=bar["close"])
    liquidation = collector.liquidation_totals(symbol.lower())
    replayed = builder.build(
        symbol,
        bids=collector.books[symbol.lower()]["bids"],
        asks=collector.books[symbol.lower()]["asks"],
        funding_rate=collector.funding[symbol.lower()],
        open_interest=collector.open_interest[symbol.lower()],
        oi_change_pct_1h=collector.oi_change_pct_1h[symbol.lower()],
        long_liq_usd=liquidation.long_usd,
        short_liq_usd=liquidation.short_usd,
        volume_usd=latest_bar["quote_volume"],
    )
    expected = MarketSnapshot.model_validate(dict(event_payload)).model_dump(mode="json")
    replay_payload = replayed.model_dump(mode="json")
    domain_fields = (
        "source",
        "symbol",
        "timeframe",
        "mid_price",
        "volume_usd",
        "order_book",
        "derivatives",
        "indicators",
        "quant_bias",
    )
    expected_domain = {key: expected[key] for key in domain_fields}
    replay_domain = {key: replay_payload[key] for key in domain_fields}
    if canonical_json_bytes(expected_domain) != canonical_json_bytes(replay_domain):
        raise NativeQuantSourceCaptureFailure(
            "existing SnapshotBuilder did not reproduce the captured output"
        )
    return replay_payload


def _stream_route_matches(record: Mapping[str, Any], *, route: str, symbol: str, suffix: str) -> bool:
    url = record.get("url")
    if not isinstance(url, str):
        return False
    parsed = urlsplit(url)
    if (
        parsed.scheme != "wss"
        or parsed.netloc != "fstream.binance.com"
        or parsed.path != f"/{route}/stream"
        or parsed.fragment
    ):
        return False
    try:
        query = parse_qs(parsed.query, strict_parsing=True)
        if set(query) != {"streams"} or len(query["streams"]) != 1:
            return False
        stream_list = query["streams"][0].split("/")
    except (TypeError, ValueError, KeyError):
        return False
    return f"{symbol.lower()}@{suffix}" in stream_list


def _decode_ws_message(record: Mapping[str, Any]) -> dict[str, Any]:
    try:
        body = _raw_body(record).decode("utf-8", errors="strict")
        message = json.loads(body, object_pairs_hook=_no_duplicate_keys)
    except (UnicodeDecodeError, ValueError) as exc:
        raise NativeQuantSourceCaptureFailure("captured WebSocket frame is not strict UTF-8 JSON") from exc
    if not isinstance(message, dict):
        raise NativeQuantSourceCaptureFailure("captured WebSocket frame is not an object")
    return message


def _apply_premium_index(collector: BinanceFuturesCollector, record: Mapping[str, Any], symbol: str) -> None:
    try:
        payload = json.loads(_raw_body(record), object_pairs_hook=_no_duplicate_keys)
    except (ValueError, TypeError) as exc:
        raise NativeQuantSourceCaptureFailure("premium-index response is not valid JSON") from exc
    items = payload if isinstance(payload, list) else [payload]
    matches = [item for item in items if isinstance(item, dict) and item.get("symbol") == symbol]
    if len(matches) != 1:
        raise NativeQuantSourceCaptureFailure("premium-index response lacks a unique exact symbol")
    item = matches[0]
    funding = collector._finite_float(item.get("lastFundingRate"))
    event_ms = collector._integer(item.get("time"))
    if funding is None or event_ms is None or event_ms <= 0:
        raise NativeQuantSourceCaptureFailure("premium-index row is invalid under collector parser")
    collector.funding[symbol.lower()] = funding
    collector._funding_event_time_ms[symbol.lower()] = event_ms


def _apply_open_interest(collector: BinanceFuturesCollector, record: Mapping[str, Any], symbol: str) -> None:
    try:
        payload = json.loads(_raw_body(record), object_pairs_hook=_no_duplicate_keys)
    except (ValueError, TypeError) as exc:
        raise NativeQuantSourceCaptureFailure("open-interest response is not valid JSON") from exc
    if not isinstance(payload, list):
        raise NativeQuantSourceCaptureFailure("open-interest response must be a list")
    observations: list[tuple[int, float]] = []
    for item in payload[-13:]:
        if not isinstance(item, dict):
            continue
        stamp = collector._integer(item.get("timestamp"))
        value = collector._finite_float(item.get("sumOpenInterestValue"))
        if stamp is not None and stamp > 0 and value is not None and value > 0:
            observations.append((stamp, value))
    observations.sort(key=lambda pair: pair[0])
    if (
        len(observations) != 13
        or len({stamp for stamp, _ in observations}) != 13
        or any(
            right[0] - left[0] != 300_000 for left, right in zip(observations, observations[1:], strict=False)
        )
    ):
        raise NativeQuantSourceCaptureFailure("open-interest source lacks the existing 13-sample hour window")
    received = record.get("received_at_ms")
    source_age = received - observations[-1][0] if type(received) is int else -1
    if not 0 <= source_age <= 600_000:
        raise NativeQuantSourceCaptureFailure("open-interest point is stale at original source receipt")
    _, values = zip(*observations, strict=True)
    if not values[0] or not values[-1]:
        raise NativeQuantSourceCaptureFailure("open-interest window contains zero baseline")
    collector.open_interest[symbol.lower()] = values[-1]
    collector.oi_change_pct_1h[symbol.lower()] = (values[-1] - values[0]) / values[0] * 100.0


__all__ = ["replay_native_quant_event"]
