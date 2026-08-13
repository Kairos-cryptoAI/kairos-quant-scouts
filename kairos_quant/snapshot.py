"""Assemble raw inputs into a typed MarketSnapshot."""

from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass, field

from kairos_core.contracts import (
    DerivativesMetrics,
    MarketSnapshot,
    OrderBookSummary,
    TechnicalIndicators,
)
from kairos_core.enums import Side

from .bias import derive_bias
from .indicators import (
    ATR_PERIOD,
    MACD_FAST_PERIOD,
    MACD_MIN_SAMPLES,
    MACD_SIGNAL_PERIOD,
    MACD_SLOW_PERIOD,
    RSI_PERIOD,
)
from .orderbook import depth_usd, normalize_order_book, order_book_imbalance, spread_bps

Level = tuple[float, float]


@dataclass(slots=True)
class _StreamingEma:
    period: int
    seed_mean: float = 0.0
    seed_count: int = 0
    value: float | None = None

    def push(self, observation: float) -> float | None:
        if self.value is None:
            self.seed_count += 1
            self.seed_mean += (observation - self.seed_mean) / self.seed_count
            if self.seed_count == self.period:
                self.value = self.seed_mean
        else:
            alpha = 2.0 / (self.period + 1.0)
            self.value = alpha * observation + (1.0 - alpha) * self.value
        return self.value


@dataclass(slots=True)
class _StreamingIndicators:
    samples: int = 0
    previous_close: float | None = None
    rsi_gain: float = 0.0
    rsi_loss: float = 0.0
    rsi_deltas: int = 0
    rsi_value: float = 50.0
    atr_total: float = 0.0
    atr_ranges: int = 0
    atr_value: float | None = None
    macd_value: float = 0.0
    macd_signal: float = 0.0
    macd_hist: float = 0.0
    fast_ema: _StreamingEma = field(default_factory=lambda: _StreamingEma(MACD_FAST_PERIOD))
    slow_ema: _StreamingEma = field(default_factory=lambda: _StreamingEma(MACD_SLOW_PERIOD))
    signal_ema: _StreamingEma = field(default_factory=lambda: _StreamingEma(MACD_SIGNAL_PERIOD))

    def push(self, *, high: float, low: float, close: float) -> None:
        self.samples += 1
        fast = self.fast_ema.push(close)
        slow = self.slow_ema.push(close)
        if fast is not None and slow is not None:
            macd_value = fast - slow
            signal = self.signal_ema.push(macd_value)
            if signal is not None:
                self.macd_value = macd_value
                self.macd_signal = signal
                self.macd_hist = macd_value - signal

        if self.previous_close is not None:
            delta = close - self.previous_close
            gain = max(delta, 0.0)
            loss = max(-delta, 0.0)
            if self.rsi_deltas < RSI_PERIOD:
                self.rsi_gain += gain
                self.rsi_loss += loss
                self.rsi_deltas += 1
                if self.rsi_deltas == RSI_PERIOD:
                    self.rsi_gain /= RSI_PERIOD
                    self.rsi_loss /= RSI_PERIOD
            else:
                self.rsi_gain += (gain - self.rsi_gain) / RSI_PERIOD
                self.rsi_loss += (loss - self.rsi_loss) / RSI_PERIOD
            if self.rsi_deltas == RSI_PERIOD:
                if self.rsi_gain == 0 and self.rsi_loss == 0:
                    self.rsi_value = 50.0
                elif self.rsi_loss == 0:
                    self.rsi_value = 100.0
                else:
                    ratio = self.rsi_gain / self.rsi_loss
                    self.rsi_value = 100.0 - 100.0 / (1.0 + ratio)

            true_range = max(
                high - low,
                abs(high - self.previous_close),
                abs(low - self.previous_close),
            )
            if self.atr_value is None:
                self.atr_total += true_range
                self.atr_ranges += 1
                if self.atr_ranges == ATR_PERIOD:
                    self.atr_value = self.atr_total / ATR_PERIOD
            else:
                self.atr_value += (true_range - self.atr_value) / ATR_PERIOD

        self.previous_close = close


class SnapshotBuilder:
    """Keeps a rolling price window per symbol and emits snapshots on demand."""

    def __init__(self, source: str = "quant-scouts", window: int = 200, depth_levels: int = 10) -> None:
        if isinstance(window, bool) or not isinstance(window, int) or window < MACD_MIN_SAMPLES:
            raise ValueError(f"price window must be at least {MACD_MIN_SAMPLES}")
        if isinstance(depth_levels, bool) or not isinstance(depth_levels, int) or depth_levels <= 0:
            raise ValueError("depth levels must be a positive integer")
        self.source = source
        self.depth_levels = depth_levels
        self._closes: dict[str, deque[float]] = {}
        self._highs: dict[str, deque[float]] = {}
        self._lows: dict[str, deque[float]] = {}
        self._indicators: dict[str, _StreamingIndicators] = {}
        self._window = window

    def push_close(self, symbol: str, close: float) -> None:
        self.push_candle(symbol, high=close, low=close, close=close)

    def push_candle(self, symbol: str, *, high: float, low: float, close: float) -> None:
        key = self._symbol_key(symbol)
        if (
            not all(math.isfinite(value) and value > 0 for value in (high, low, close))
            or high < low
            or not low <= close <= high
        ):
            raise ValueError("invalid candle")
        self._highs.setdefault(key, deque(maxlen=self._window)).append(high)
        self._lows.setdefault(key, deque(maxlen=self._window)).append(low)
        self._closes.setdefault(key, deque(maxlen=self._window)).append(close)
        self._indicators.setdefault(key, _StreamingIndicators()).push(high=high, low=low, close=close)

    def reset(self, symbol: str) -> None:
        """Discard an indicator history that is no longer time-contiguous."""
        key = self._symbol_key(symbol)
        self._highs.pop(key, None)
        self._lows.pop(key, None)
        self._closes.pop(key, None)
        self._indicators.pop(key, None)

    @staticmethod
    def _symbol_key(symbol: str) -> str:
        key = symbol.strip().upper()
        if not key:
            raise ValueError("symbol is required")
        return key

    def build(
        self,
        symbol: str,
        *,
        bids: list[Level],
        asks: list[Level],
        funding_rate: float,
        open_interest: float,
        timeframe: str = "1m",
        oi_change_pct_1h: float = 0.0,
        long_liq_usd: float = 0.0,
        short_liq_usd: float = 0.0,
        volume_usd: float = 0.0,
    ) -> MarketSnapshot:
        key = self._symbol_key(symbol)
        closes = list(self._closes.get(key, []))
        indicator_state = self._indicators.get(key)
        normalized_bids, normalized_asks = normalize_order_book(bids, asks)
        if not normalized_bids or not normalized_asks:
            raise ValueError("both sides of the order book are required")
        if not timeframe.strip():
            raise ValueError("timeframe is required")
        numeric_inputs = (
            funding_rate,
            open_interest,
            oi_change_pct_1h,
            long_liq_usd,
            short_liq_usd,
            volume_usd,
        )
        if not all(math.isfinite(value) for value in numeric_inputs):
            raise ValueError("snapshot inputs must be finite")
        if min(open_interest, long_liq_usd, short_liq_usd, volume_usd) < 0:
            raise ValueError("snapshot notionals must be non-negative")

        best_bid = normalized_bids[0][0]
        best_ask = normalized_asks[0][0]
        mid = best_bid + (best_ask - best_bid) / 2.0

        rsi14 = indicator_state.rsi_value if indicator_state is not None else 50.0
        m = indicator_state.macd_value if indicator_state is not None else 0.0
        s = indicator_state.macd_signal if indicator_state is not None else 0.0
        hist = indicator_state.macd_hist if indicator_state is not None else 0.0
        atr_pct = (
            indicator_state.atr_value / closes[-1]
            if indicator_state is not None and indicator_state.atr_value is not None and closes
            else None
        )
        imb = order_book_imbalance(normalized_bids, normalized_asks, self.depth_levels)
        quant_bias = Side.FLAT
        if indicator_state is not None and indicator_state.samples >= MACD_MIN_SAMPLES:
            quant_bias = derive_bias(rsi_14=rsi14, macd_hist=hist, ob_imbalance=imb)

        return MarketSnapshot(
            source=self.source,
            symbol=key,
            timeframe=timeframe,
            mid_price=mid,
            volume_usd=volume_usd,
            order_book=OrderBookSummary(
                best_bid=best_bid or mid,
                best_ask=best_ask or mid,
                spread_bps=spread_bps(best_bid or mid, best_ask or mid),
                imbalance=imb,
                depth_usd=depth_usd(normalized_bids, normalized_asks, self.depth_levels),
            ),
            derivatives=DerivativesMetrics(
                funding_rate=funding_rate,
                open_interest=open_interest,
                oi_change_pct_1h=oi_change_pct_1h,
                long_liquidations_usd=long_liq_usd,
                short_liquidations_usd=short_liq_usd,
            ),
            indicators=TechnicalIndicators(
                rsi_14=rsi14,
                macd=m,
                macd_signal=s,
                macd_hist=hist,
                atr_pct=atr_pct,
            ),
            quant_bias=quant_bias,
        )
