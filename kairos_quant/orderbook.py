"""Order-book derived features."""

from __future__ import annotations

import math

Level = tuple[float, float]  # (price, size)


def _positive_levels(levels: int) -> None:
    if isinstance(levels, bool) or not isinstance(levels, int) or levels <= 0:
        raise ValueError("levels must be a positive integer")


def _validated_levels(values: list[Level], *, bids: bool) -> list[Level]:
    clean: list[Level] = []
    for price, size in values:
        if not math.isfinite(price) or not math.isfinite(size) or price <= 0 or size < 0:
            raise ValueError("order-book levels must contain finite positive prices and non-negative sizes")
        if size > 0:
            clean.append((price, size))
    return sorted(clean, key=lambda level: level[0], reverse=bids)


def normalize_order_book(bids: list[Level], asks: list[Level]) -> tuple[list[Level], list[Level]]:
    """Validate and price-sort both sides; reject a crossed book."""
    normalized_bids = _validated_levels(bids, bids=True)
    normalized_asks = _validated_levels(asks, bids=False)
    if normalized_bids and normalized_asks and normalized_bids[0][0] > normalized_asks[0][0]:
        raise ValueError("crossed order book")
    return normalized_bids, normalized_asks


def spread_bps(best_bid: float, best_ask: float) -> float:
    if not all(math.isfinite(value) and value > 0 for value in (best_bid, best_ask)):
        raise ValueError("best bid and ask must be finite and positive")
    if best_ask < best_bid:
        raise ValueError("best ask cannot be below best bid")
    mid = best_bid + (best_ask - best_bid) / 2.0
    return (best_ask - best_bid) / mid * 10_000.0


def order_book_imbalance(bids: list[Level], asks: list[Level], levels: int = 10) -> float:
    """(bidVol - askVol) / (bidVol + askVol) over the top ``levels``, in [-1, 1]."""
    _positive_levels(levels)
    normalized_bids, normalized_asks = normalize_order_book(bids, asks)
    bid_sizes = [size for _, size in normalized_bids[:levels]]
    ask_sizes = [size for _, size in normalized_asks[:levels]]
    scale = max((*bid_sizes, *ask_sizes), default=0.0)
    if scale == 0:
        return 0.0
    bid_vol = sum(size / scale for size in bid_sizes)
    ask_vol = sum(size / scale for size in ask_sizes)
    total = bid_vol + ask_vol
    return (bid_vol - ask_vol) / total


def depth_usd(bids: list[Level], asks: list[Level], levels: int = 10) -> float:
    """Notional resting within the top ``levels`` on both sides."""
    _positive_levels(levels)
    normalized_bids, normalized_asks = normalize_order_book(bids, asks)
    total = 0.0
    for price, size in (*normalized_bids[:levels], *normalized_asks[:levels]):
        notional = price * size
        if not math.isfinite(notional) or not math.isfinite(total + notional):
            raise ValueError("order-book notional exceeds the finite numeric range")
        total += notional
    return total
