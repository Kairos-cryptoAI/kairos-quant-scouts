"""Public Binance/EVEDEX basis and executable-liquidity qualification."""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
import tempfile
import time
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any

try:
    import aiohttp
except Exception:  # pragma: no cover
    aiohttp = None  # type: ignore


class ComparisonStatus(StrEnum):
    PASS = "PASS"  # nosec B105
    BLOCKED = "BLOCKED"
    FAIL = "FAIL"


@dataclass(frozen=True)
class BookLevel:
    price: float
    quantity: float


@dataclass(frozen=True)
class BookSnapshot:
    source: str
    symbol: str
    timestamp_ms: int
    bids: tuple[BookLevel, ...]
    asks: tuple[BookLevel, ...]
    latency_ms: float

    def __post_init__(self) -> None:
        if not self.bids or not self.asks:
            raise ValueError("order book must contain both sides")
        if self.bids[0].price >= self.asks[0].price:
            raise ValueError("best bid must be below best ask")

    @property
    def mid(self) -> float:
        return (self.bids[0].price + self.asks[0].price) / 2

    @property
    def spread_bps(self) -> float:
        return (self.asks[0].price - self.bids[0].price) / self.mid * 10_000


@dataclass(frozen=True)
class VenueSample:
    logical_symbol: str
    captured_at: str
    evedex_timestamp_ms: int
    binance_timestamp_ms: int
    timestamp_skew_ms: int
    evedex_age_ms: int
    binance_age_ms: int
    basis_bps: float
    evedex_spread_bps: float
    binance_spread_bps: float
    evedex_buy_slippage_bps: float | None
    evedex_sell_slippage_bps: float | None
    binance_buy_slippage_bps: float | None
    binance_sell_slippage_bps: float | None
    evedex_latency_ms: float
    binance_latency_ms: float


@dataclass(frozen=True)
class SymbolComparison:
    logical_symbol: str
    samples: int
    availability: float
    p50_abs_basis_bps: float | None
    p95_abs_basis_bps: float | None
    p95_evedex_spread_bps: float | None
    p95_evedex_buy_slippage_bps: float | None
    p95_evedex_sell_slippage_bps: float | None
    max_timestamp_skew_ms: int | None
    max_book_age_ms: int | None
    status: ComparisonStatus
    reasons: tuple[str, ...]


@dataclass(frozen=True)
class VenueComparisonReport:
    schema_version: int
    generated_at: str
    samples_requested: int
    interval_s: float
    notional_usd: float
    symbol_map: dict[str, str]
    thresholds: dict[str, float]
    samples: tuple[VenueSample, ...]
    symbols: tuple[SymbolComparison, ...]
    live_orders_allowed: bool = False

    @property
    def status(self) -> ComparisonStatus:
        statuses = {item.status for item in self.symbols}
        if ComparisonStatus.FAIL in statuses:
            return ComparisonStatus.FAIL
        if ComparisonStatus.BLOCKED in statuses:
            return ComparisonStatus.BLOCKED
        return ComparisonStatus.PASS

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "generated_at": self.generated_at,
            "samples_requested": self.samples_requested,
            "interval_s": self.interval_s,
            "notional_usd": self.notional_usd,
            "symbol_map": dict(sorted(self.symbol_map.items())),
            "thresholds": dict(sorted(self.thresholds.items())),
            "status": self.status.value,
            "live_orders_allowed": False,
            "samples": [asdict(item) for item in self.samples],
            "symbols": [asdict(item) for item in self.symbols],
        }


PairFetcher = Callable[[str, str], Awaitable[tuple[BookSnapshot, BookSnapshot]]]

DEFAULT_SYMBOL_MAP = {
    "BTCUSDT": "BTCUSD",
    "ETHUSDT": "ETHUSD",
    "SOLUSDT": "SOLUSD",
    "BNBUSDT": "BNBUSD",
    "XRPUSDT": "XRPUSD",
}

DEFAULT_THRESHOLDS = {
    "minimum_samples": 30.0,
    "minimum_availability": 0.99,
    "maximum_p95_abs_basis_bps": 25.0,
    "maximum_p95_evedex_spread_bps": 25.0,
    "maximum_p95_evedex_slippage_bps": 25.0,
    "maximum_timestamp_skew_ms": 2_000.0,
    "maximum_book_age_ms": 5_000.0,
}


def _positive(value: Any, field: str) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{field} is not numeric")
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} is not numeric") from exc
    if not math.isfinite(parsed) or parsed <= 0:
        raise ValueError(f"{field} must be finite and positive")
    return parsed


def _timestamp(value: Any, field: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{field} is not an integer timestamp")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} is not an integer timestamp") from exc
    if parsed <= 0:
        raise ValueError(f"{field} must be positive")
    return parsed


def parse_evedex_book(symbol: str, payload: Any, *, latency_ms: float) -> BookSnapshot:
    if not isinstance(payload, dict):
        raise ValueError("EVEDEX order book is not an object")

    def levels(name: str) -> tuple[BookLevel, ...]:
        raw = payload.get(name)
        if not isinstance(raw, list):
            raise ValueError(f"EVEDEX {name} is not a list")
        if not all(isinstance(item, dict) for item in raw):
            raise ValueError(f"EVEDEX {name} contains a malformed level")
        return tuple(
            BookLevel(
                _positive(item.get("price"), f"EVEDEX {name}.price"),
                _positive(item.get("quantity"), f"EVEDEX {name}.quantity"),
            )
            for item in raw
        )

    bids = levels("bids")
    asks = levels("asks")
    if tuple(sorted(bids, key=lambda item: item.price, reverse=True)) != bids:
        raise ValueError("EVEDEX bids are not sorted best-first")
    if tuple(sorted(asks, key=lambda item: item.price)) != asks:
        raise ValueError("EVEDEX asks are not sorted best-first")
    return BookSnapshot("evedex", symbol, _timestamp(payload.get("t"), "EVEDEX t"), bids, asks, latency_ms)


def parse_binance_book(symbol: str, payload: Any, *, latency_ms: float) -> BookSnapshot:
    if not isinstance(payload, dict):
        raise ValueError("Binance order book is not an object")

    def levels(name: str) -> tuple[BookLevel, ...]:
        raw = payload.get(name)
        if not isinstance(raw, list):
            raise ValueError(f"Binance {name} is not a list")
        values: list[BookLevel] = []
        for item in raw:
            if not isinstance(item, list) or len(item) < 2:
                raise ValueError(f"Binance {name} contains a malformed level")
            values.append(
                BookLevel(
                    _positive(item[0], f"Binance {name}.price"),
                    _positive(item[1], f"Binance {name}.quantity"),
                )
            )
        return tuple(values)

    timestamp_ms = payload.get("T") or payload.get("E")
    bids = levels("bids")
    asks = levels("asks")
    if tuple(sorted(bids, key=lambda item: item.price, reverse=True)) != bids:
        raise ValueError("Binance bids are not sorted best-first")
    if tuple(sorted(asks, key=lambda item: item.price)) != asks:
        raise ValueError("Binance asks are not sorted best-first")
    return BookSnapshot(
        "binance",
        symbol,
        _timestamp(timestamp_ms, "Binance transaction time"),
        bids,
        asks,
        latency_ms,
    )


def market_slippage_bps(book: BookSnapshot, *, buy: bool, notional_usd: float) -> float | None:
    remaining = _positive(notional_usd, "notional_usd")
    levels = book.asks if buy else book.bids
    spent = 0.0
    quantity = 0.0
    for level in levels:
        level_notional = level.price * level.quantity
        taken_notional = min(remaining, level_notional)
        taken_quantity = taken_notional / level.price
        spent += taken_notional
        quantity += taken_quantity
        remaining -= taken_notional
        if remaining <= max(1e-9, notional_usd * 1e-12):
            average = spent / quantity
            reference = book.mid
            return (
                (average - reference) / reference * 10_000
                if buy
                else (reference - average) / reference * 10_000
            )
    return None


def _percentile(values: list[float], percentile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = (len(ordered) - 1) * percentile
    lower = math.floor(index)
    upper = math.ceil(index)
    if lower == upper:
        return ordered[lower]
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (index - lower)


def _summary(
    logical_symbol: str,
    samples: list[VenueSample],
    *,
    requested: int,
    thresholds: dict[str, float],
) -> SymbolComparison:
    availability = len(samples) / requested
    basis = [abs(item.basis_bps) for item in samples]
    spreads = [item.evedex_spread_bps for item in samples]
    buy_slippage = [item.evedex_buy_slippage_bps for item in samples]
    sell_slippage = [item.evedex_sell_slippage_bps for item in samples]
    complete_buy = [item for item in buy_slippage if item is not None]
    complete_sell = [item for item in sell_slippage if item is not None]
    p95_basis = _percentile(basis, 0.95)
    p95_spread = _percentile(spreads, 0.95)
    p95_buy = _percentile(complete_buy, 0.95)
    p95_sell = _percentile(complete_sell, 0.95)
    max_skew = max((item.timestamp_skew_ms for item in samples), default=None)
    max_age = max(
        (max(item.evedex_age_ms, item.binance_age_ms) for item in samples),
        default=None,
    )
    reasons: list[str] = []
    if len(samples) < int(thresholds["minimum_samples"]):
        reasons.append("insufficient_samples")
    if availability < thresholds["minimum_availability"]:
        reasons.append("insufficient_availability")
    if p95_basis is None or p95_basis > thresholds["maximum_p95_abs_basis_bps"]:
        reasons.append("basis_exceeds_limit")
    if p95_spread is None or p95_spread > thresholds["maximum_p95_evedex_spread_bps"]:
        reasons.append("evedex_spread_exceeds_limit")
    if len(complete_buy) != len(samples) or len(complete_sell) != len(samples):
        reasons.append("insufficient_evedex_depth")
    elif max(p95_buy or math.inf, p95_sell or math.inf) > thresholds["maximum_p95_evedex_slippage_bps"]:
        reasons.append("evedex_slippage_exceeds_limit")
    if max_skew is None or max_skew > thresholds["maximum_timestamp_skew_ms"]:
        reasons.append("timestamp_skew_exceeds_limit")
    if max_age is None or max_age > thresholds["maximum_book_age_ms"]:
        reasons.append("stale_order_book")
    status = ComparisonStatus.PASS if not reasons else ComparisonStatus.BLOCKED
    return SymbolComparison(
        logical_symbol=logical_symbol,
        samples=len(samples),
        availability=availability,
        p50_abs_basis_bps=_percentile(basis, 0.5),
        p95_abs_basis_bps=p95_basis,
        p95_evedex_spread_bps=p95_spread,
        p95_evedex_buy_slippage_bps=p95_buy,
        p95_evedex_sell_slippage_bps=p95_sell,
        max_timestamp_skew_ms=max_skew,
        max_book_age_ms=max_age,
        status=status,
        reasons=tuple(reasons),
    )


async def compare_venues(
    *,
    fetch_pair: PairFetcher,
    symbol_map: dict[str, str],
    samples: int = 30,
    interval_s: float = 2.0,
    notional_usd: float = 1_000.0,
    thresholds: dict[str, float] | None = None,
    clock: Callable[[], datetime] | None = None,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> VenueComparisonReport:
    if samples <= 0:
        raise ValueError("samples must be positive")
    if interval_s < 0 or not math.isfinite(interval_s):
        raise ValueError("interval_s must be finite and non-negative")
    _positive(notional_usd, "notional_usd")
    policy = dict(DEFAULT_THRESHOLDS if thresholds is None else thresholds)
    if policy.keys() != DEFAULT_THRESHOLDS.keys():
        raise ValueError("thresholds must contain the exact registered comparison policy")
    for name, value in policy.items():
        _positive(value, name)
    if policy["minimum_availability"] > 1:
        raise ValueError("minimum_availability cannot exceed 1")
    captured_samples: list[VenueSample] = []
    now_fn = clock or (lambda: datetime.now(UTC))
    for sample_index in range(samples):
        results = await asyncio.gather(
            *(fetch_pair(logical, venue) for logical, venue in sorted(symbol_map.items())),
            return_exceptions=True,
        )
        captured_at = now_fn().astimezone(UTC)
        captured_ms = int(captured_at.timestamp() * 1000)
        for (logical, _venue), result in zip(sorted(symbol_map.items()), results, strict=True):
            if isinstance(result, BaseException):
                continue
            evedex, binance = result
            if evedex.source != "evedex" or binance.source != "binance":
                raise ValueError("fetch_pair returned venue snapshots in the wrong order")
            captured_samples.append(
                VenueSample(
                    logical_symbol=logical,
                    captured_at=captured_at.isoformat(),
                    evedex_timestamp_ms=evedex.timestamp_ms,
                    binance_timestamp_ms=binance.timestamp_ms,
                    timestamp_skew_ms=abs(evedex.timestamp_ms - binance.timestamp_ms),
                    evedex_age_ms=max(0, captured_ms - evedex.timestamp_ms),
                    binance_age_ms=max(0, captured_ms - binance.timestamp_ms),
                    basis_bps=(evedex.mid - binance.mid) / binance.mid * 10_000,
                    evedex_spread_bps=evedex.spread_bps,
                    binance_spread_bps=binance.spread_bps,
                    evedex_buy_slippage_bps=market_slippage_bps(evedex, buy=True, notional_usd=notional_usd),
                    evedex_sell_slippage_bps=market_slippage_bps(
                        evedex, buy=False, notional_usd=notional_usd
                    ),
                    binance_buy_slippage_bps=market_slippage_bps(
                        binance, buy=True, notional_usd=notional_usd
                    ),
                    binance_sell_slippage_bps=market_slippage_bps(
                        binance, buy=False, notional_usd=notional_usd
                    ),
                    evedex_latency_ms=evedex.latency_ms,
                    binance_latency_ms=binance.latency_ms,
                )
            )
        if sample_index + 1 < samples:
            await sleep(interval_s)
    summaries = tuple(
        _summary(
            logical,
            [item for item in captured_samples if item.logical_symbol == logical],
            requested=samples,
            thresholds=policy,
        )
        for logical in sorted(symbol_map)
    )
    return VenueComparisonReport(
        schema_version=1,
        generated_at=now_fn().astimezone(UTC).isoformat(),
        samples_requested=samples,
        interval_s=interval_s,
        notional_usd=notional_usd,
        symbol_map=symbol_map,
        thresholds=policy,
        samples=tuple(captured_samples),
        symbols=summaries,
    )


async def compare_public_venues(
    *,
    symbol_map: dict[str, str],
    samples: int,
    interval_s: float,
    notional_usd: float,
    evedex_base_url: str = "https://exchange-api.evedex.com",
    binance_base_url: str = "https://fapi.binance.com",
    timeout_s: float = 10.0,
) -> VenueComparisonReport:
    if aiohttp is None:  # pragma: no cover
        raise RuntimeError("aiohttp is required for venue comparison")
    timeout = aiohttp.ClientTimeout(total=timeout_s)
    async with aiohttp.ClientSession(timeout=timeout) as session:

        async def request_json(url: str) -> tuple[Any, float]:
            started = time.perf_counter()
            async with session.get(url) as response:
                response.raise_for_status()
                payload = await response.json(content_type=None)
            return payload, (time.perf_counter() - started) * 1000

        async def fetch_pair(logical: str, venue: str) -> tuple[BookSnapshot, BookSnapshot]:
            evedex_result, binance_result = await asyncio.gather(
                request_json(f"{evedex_base_url.rstrip('/')}/api/market/{venue}/deep"),
                request_json(f"{binance_base_url.rstrip('/')}/fapi/v1/depth?symbol={logical}&limit=100"),
            )
            evedex_payload, evedex_latency = evedex_result
            binance_payload, binance_latency = binance_result
            return (
                parse_evedex_book(venue, evedex_payload, latency_ms=evedex_latency),
                parse_binance_book(logical, binance_payload, latency_ms=binance_latency),
            )

        return await compare_venues(
            fetch_pair=fetch_pair,
            symbol_map=symbol_map,
            samples=samples,
            interval_s=interval_s,
            notional_usd=notional_usd,
        )


def _write_report(path: Path, report: VenueComparisonReport, *, overwrite: bool) -> None:
    resolved = path.resolve()
    if resolved.exists() and not overwrite:
        raise FileExistsError(f"refusing to overwrite venue comparison: {resolved}")
    resolved.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(report.to_dict(), sort_keys=True, indent=2, allow_nan=False) + "\n"
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{resolved.name}.", suffix=".tmp", dir=resolved.parent
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, resolved)
    finally:
        if temporary.exists():
            temporary.unlink()


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Compare public EVEDEX and Binance execution data")
    parser.add_argument("--samples", type=int, default=30)
    parser.add_argument("--interval-s", type=float, default=2.0)
    parser.add_argument("--notional-usd", type=float, default=1_000.0)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--overwrite", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    report = asyncio.run(
        compare_public_venues(
            symbol_map=DEFAULT_SYMBOL_MAP,
            samples=args.samples,
            interval_s=args.interval_s,
            notional_usd=args.notional_usd,
        )
    )
    _write_report(args.output, report, overwrite=args.overwrite)
    print(f"Venue comparison: {report.status.value}; live_orders_allowed=false")
    return 0 if report.status is ComparisonStatus.PASS else 2


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
