"""Assemble raw inputs into a typed MarketSnapshot."""
from __future__ import annotations

from collections import deque
from typing import Deque, List, Tuple

from kairos_core.contracts import (
    DerivativesMetrics,
    MarketSnapshot,
    OrderBookSummary,
    TechnicalIndicators,
)

from .bias import derive_bias
from .indicators import macd, rsi
from .orderbook import depth_usd, order_book_imbalance, spread_bps

Level = Tuple[float, float]


class SnapshotBuilder:
    """Keeps a rolling price window per symbol and emits snapshots on demand."""

    def __init__(self, source: str = "quant-scouts", window: int = 200, depth_levels: int = 10) -> None:
        self.source = source
        self.depth_levels = depth_levels
        self._closes: dict[str, Deque[float]] = {}
        self._window = window

    def push_close(self, symbol: str, close: float) -> None:
        self._closes.setdefault(symbol, deque(maxlen=self._window)).append(close)

    def build(
        self,
        symbol: str,
        *,
        bids: List[Level],
        asks: List[Level],
        funding_rate: float,
        open_interest: float,
        timeframe: str = "1m",
        oi_change_pct_1h: float = 0.0,
        long_liq_usd: float = 0.0,
        short_liq_usd: float = 0.0,
        volume_usd: float = 0.0,
    ) -> MarketSnapshot:
        closes = list(self._closes.get(symbol, []))
        best_bid = bids[0][0] if bids else 0.0
        best_ask = asks[0][0] if asks else 0.0
        mid = (best_bid + best_ask) / 2.0 or (closes[-1] if closes else 0.0)

        rsi14 = rsi(closes, 14) if closes else 50.0
        m, s, hist = macd(closes) if closes else (0.0, 0.0, 0.0)
        imb = order_book_imbalance(bids, asks, self.depth_levels)

        return MarketSnapshot(
            source=self.source, symbol=symbol, timeframe=timeframe, mid_price=mid,
            volume_usd=volume_usd,
            order_book=OrderBookSummary(
                best_bid=best_bid or mid, best_ask=best_ask or mid,
                spread_bps=spread_bps(best_bid or mid, best_ask or mid),
                imbalance=imb, depth_usd=depth_usd(bids, asks, self.depth_levels),
            ),
            derivatives=DerivativesMetrics(
                funding_rate=funding_rate, open_interest=open_interest,
                oi_change_pct_1h=oi_change_pct_1h,
                long_liquidations_usd=long_liq_usd, short_liquidations_usd=short_liq_usd,
            ),
            indicators=TechnicalIndicators(rsi_14=rsi14, macd=m, macd_signal=s, macd_hist=hist),
            quant_bias=derive_bias(rsi_14=rsi14, macd_hist=hist, ob_imbalance=imb),
        )
