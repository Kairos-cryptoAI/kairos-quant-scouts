"""Runtime Binance/EVEDEX executable-quality gate for PAPER candidates."""

from __future__ import annotations

import asyncio
import math
import time
from dataclasses import dataclass
from typing import Any

import aiohttp
from kairos_core.contracts import VenueQualityV1
from kairos_core.enums import EvedexProfile

from .venue_comparison import (
    BookSnapshot,
    market_slippage_bps,
    parse_binance_book,
    parse_evedex_book,
)


@dataclass(frozen=True, slots=True)
class VenueGatePolicy:
    """Fail-closed thresholds shared by runtime admission and the 24h gate."""

    assessed_notional_usd: float = 1_000.0
    maximum_abs_basis_bps: float = 25.0
    maximum_spread_bps: float = 25.0
    maximum_slippage_bps: float = 25.0
    maximum_book_age_ms: int = 5_000
    maximum_timestamp_skew_ms: int = 2_000
    maximum_latency_ms: int = 5_000
    measurement_ttl_ms: int = 5_000
    taker_fee_bps: float = 5.0

    def __post_init__(self) -> None:
        numeric = (
            self.assessed_notional_usd,
            self.maximum_abs_basis_bps,
            self.maximum_spread_bps,
            self.maximum_slippage_bps,
            self.taker_fee_bps,
        )
        if not all(math.isfinite(value) and value >= 0 for value in numeric):
            raise ValueError("venue gate numeric thresholds must be finite and non-negative")
        if self.assessed_notional_usd <= 0:
            raise ValueError("assessed_notional_usd must be positive")
        integers = (
            self.maximum_book_age_ms,
            self.maximum_timestamp_skew_ms,
            self.maximum_latency_ms,
            self.measurement_ttl_ms,
        )
        if any(isinstance(value, bool) or not isinstance(value, int) or value <= 0 for value in integers):
            raise ValueError("venue gate time thresholds must be positive integers")


def evedex_dev_symbol(binance_symbol: str) -> str:
    normalized = binance_symbol.strip().upper()
    if not normalized.endswith("USDT") or len(normalized) <= 4:
        raise ValueError("Binance signal symbols must be normalized USDT pairs")
    return f"{normalized[:-4]}USD:DEV"


def _side_depth_usd(book: BookSnapshot, *, buy: bool) -> float:
    levels = book.asks if buy else book.bids
    return sum(level.price * level.quantity for level in levels)


def build_venue_quality(
    *,
    binance: BookSnapshot,
    evedex: BookSnapshot,
    venue_symbol: str,
    observed_at_ms: int,
    latency_ms: int,
    policy: VenueGatePolicy,
    source: str = "quant-scouts",
) -> VenueQualityV1:
    """Convert one contemporaneous book pair into a strict admission fact."""

    if binance.source != "binance" or evedex.source != "evedex":
        raise ValueError("venue books were supplied in the wrong order")
    if observed_at_ms <= 0:
        raise ValueError("observed_at_ms must be positive")
    observed = max(observed_at_ms, binance.timestamp_ms, evedex.timestamp_ms)
    reasons: list[str] = []
    local_future_skew = max(binance.timestamp_ms, evedex.timestamp_ms) - observed_at_ms
    if local_future_skew > policy.maximum_timestamp_skew_ms:
        reasons.append("exchange_clock_ahead")

    basis_bps = (evedex.mid - binance.mid) / binance.mid * 10_000
    buy_slippage = market_slippage_bps(
        evedex,
        buy=True,
        notional_usd=policy.assessed_notional_usd,
    )
    sell_slippage = market_slippage_bps(
        evedex,
        buy=False,
        notional_usd=policy.assessed_notional_usd,
    )
    bid_depth = _side_depth_usd(evedex, buy=False)
    ask_depth = _side_depth_usd(evedex, buy=True)
    depth_usd = min(bid_depth, ask_depth)
    if abs(basis_bps) > policy.maximum_abs_basis_bps:
        reasons.append("basis_exceeds_limit")
    if evedex.spread_bps > policy.maximum_spread_bps:
        reasons.append("spread_exceeds_limit")
    if depth_usd < policy.assessed_notional_usd or buy_slippage is None or sell_slippage is None:
        reasons.append("insufficient_depth")
    if buy_slippage is not None and buy_slippage > policy.maximum_slippage_bps:
        reasons.append("buy_slippage_exceeds_limit")
    if sell_slippage is not None and sell_slippage > policy.maximum_slippage_bps:
        reasons.append("sell_slippage_exceeds_limit")

    reference_age_ms = observed - binance.timestamp_ms
    book_age_ms = observed - evedex.timestamp_ms
    timestamp_skew_ms = abs(binance.timestamp_ms - evedex.timestamp_ms)
    if max(reference_age_ms, book_age_ms) > policy.maximum_book_age_ms:
        reasons.append("stale_order_book")
    if timestamp_skew_ms > policy.maximum_timestamp_skew_ms:
        reasons.append("timestamp_skew_exceeds_limit")
    if latency_ms > policy.maximum_latency_ms:
        reasons.append("latency_exceeds_limit")

    # Blocked measurements still carry a finite conservative value so they can
    # be stored and graphed.  Risk never consumes it while entry_allowed=false.
    blocked_slippage = policy.maximum_slippage_bps + 1.0
    return VenueQualityV1(
        source=source,
        profile=EvedexProfile.DEV,
        symbol=venue_symbol,
        observed_at_ms=observed,
        expires_at_ms=observed + policy.measurement_ttl_ms,
        reference_timestamp_ms=binance.timestamp_ms,
        book_timestamp_ms=evedex.timestamp_ms,
        reference_mid_price=binance.mid,
        best_bid=evedex.bids[0].price,
        best_ask=evedex.asks[0].price,
        venue_mid_price=evedex.mid,
        basis_bps=basis_bps,
        spread_bps=evedex.spread_bps,
        assessed_notional_usd=policy.assessed_notional_usd,
        depth_usd=depth_usd,
        buy_slippage_bps=buy_slippage if buy_slippage is not None else blocked_slippage,
        sell_slippage_bps=sell_slippage if sell_slippage is not None else blocked_slippage,
        taker_fee_bps=policy.taker_fee_bps,
        reference_age_ms=reference_age_ms,
        book_age_ms=book_age_ms,
        latency_ms=latency_ms,
        timestamp_skew_ms=timestamp_skew_ms,
        entry_allowed=not reasons,
        reason_codes=tuple(reasons),
    )


async def fetch_venue_quality(
    session: aiohttp.ClientSession,
    *,
    binance_symbol: str,
    evedex_symbol: str,
    binance_base_url: str,
    evedex_base_url: str,
    policy: VenueGatePolicy,
    source: str,
) -> VenueQualityV1:
    """Fetch both public books once; this function never places an order."""

    async def request_json(url: str) -> tuple[Any, float]:
        started = time.perf_counter()
        async with session.get(url) as response:
            response.raise_for_status()
            payload = await response.json(content_type=None)
        return payload, (time.perf_counter() - started) * 1_000

    evedex_result, binance_result = await asyncio.gather(
        request_json(f"{evedex_base_url.rstrip('/')}/api/market/{evedex_symbol}/deep"),
        request_json(f"{binance_base_url.rstrip('/')}/fapi/v1/depth?symbol={binance_symbol}&limit=100"),
    )
    evedex_payload, evedex_latency = evedex_result
    binance_payload, binance_latency = binance_result
    evedex = parse_evedex_book(evedex_symbol, evedex_payload, latency_ms=evedex_latency)
    binance = parse_binance_book(binance_symbol, binance_payload, latency_ms=binance_latency)
    observed_at_ms = int(time.time() * 1_000)
    return build_venue_quality(
        binance=binance,
        evedex=evedex,
        venue_symbol=evedex_symbol,
        observed_at_ms=observed_at_ms,
        latency_ms=math.ceil(max(evedex_latency, binance_latency)),
        policy=policy,
        source=source,
    )
