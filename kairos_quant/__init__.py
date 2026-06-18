"""Kairos Layer 1A — Quant Scouts.

Pure-math collectors and indicators (no LLM). They connect to the exchange,
digest raw order-book / derivatives streams and emit a compact ``MarketSnapshot``
— the only numeric payload the upper layers ever see.
"""
from __future__ import annotations

__version__ = "0.1.0"

from .indicators import ema, rsi, macd, atr
from .orderbook import order_book_imbalance, spread_bps, depth_usd
from .bias import derive_bias
from .snapshot import SnapshotBuilder

__all__ = ["ema", "rsi", "macd", "atr", "order_book_imbalance", "spread_bps",
           "depth_usd", "derive_bias", "SnapshotBuilder", "__version__"]
