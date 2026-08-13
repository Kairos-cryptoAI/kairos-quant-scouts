"""Derive a pure-math directional bias from indicators + book pressure."""

from __future__ import annotations

from kairos_core.enums import Side


def derive_bias(
    *,
    rsi_14: float,
    macd_hist: float,
    ob_imbalance: float,
    rsi_high: float = 55.0,
    rsi_low: float = 45.0,
    imb_thr: float = 0.15,
) -> Side:
    """Simple, transparent rule combining momentum, MACD and book pressure.

    LONG  when momentum and MACD both lean up (and the book is not against us);
    SHORT in the mirror case; otherwise FLAT.
    """
    long_votes = (rsi_14 >= rsi_high) + (macd_hist > 0) + (ob_imbalance > imb_thr)
    short_votes = (rsi_14 <= rsi_low) + (macd_hist < 0) + (ob_imbalance < -imb_thr)
    if long_votes >= 2 and long_votes > short_votes:
        return Side.LONG
    if short_votes >= 2 and short_votes > long_votes:
        return Side.SHORT
    return Side.FLAT
