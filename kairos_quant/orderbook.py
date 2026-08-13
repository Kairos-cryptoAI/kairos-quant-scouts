"""Order-book derived features."""

from __future__ import annotations

Level = tuple[float, float]  # (price, size)


def spread_bps(best_bid: float, best_ask: float) -> float:
    mid = (best_bid + best_ask) / 2.0
    if mid <= 0:
        return 0.0
    return (best_ask - best_bid) / mid * 10_000.0


def order_book_imbalance(bids: list[Level], asks: list[Level], levels: int = 10) -> float:
    """(bidVol - askVol) / (bidVol + askVol) over the top ``levels``, in [-1, 1]."""
    bid_vol = sum(size for _, size in bids[:levels])
    ask_vol = sum(size for _, size in asks[:levels])
    total = bid_vol + ask_vol
    if total <= 0:
        return 0.0
    return (bid_vol - ask_vol) / total


def depth_usd(bids: list[Level], asks: list[Level], levels: int = 10) -> float:
    """Notional resting within the top ``levels`` on both sides."""
    return sum(p * s for p, s in bids[:levels]) + sum(p * s for p, s in asks[:levels])
