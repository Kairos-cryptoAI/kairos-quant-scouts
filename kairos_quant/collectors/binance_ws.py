"""Resilient Binance USD-M Futures market-data collector.

Only closed one-minute klines enter the indicator history. Order-book and mark-price
streams remain live inputs for snapshot metadata, while forced orders are aggregated
between published snapshots.
"""

from __future__ import annotations

import asyncio
import json
import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass

import aiohttp
from kairos_core.logging import get_logger

log = get_logger("quant-scouts.binance")

Level = tuple[float, float]


@dataclass(frozen=True, slots=True)
class ClosedKline:
    close_time_ms: int
    high: float
    low: float
    close: float
    quote_volume: float


@dataclass(frozen=True, slots=True)
class LiquidationTotals:
    long_usd: float = 0.0
    short_usd: float = 0.0


class BinanceFuturesCollector:
    def __init__(
        self,
        symbols: list[str],
        ws_base: str,
        rest_base: str,
        *,
        reconnect_initial_s: float = 1.0,
        reconnect_max_s: float = 30.0,
        kline_buffer_size: int = 1_000,
        clock: Callable[[], float] = time.monotonic,
        wall_clock: Callable[[], float] = time.time,
    ) -> None:
        if reconnect_initial_s <= 0 or reconnect_max_s < reconnect_initial_s:
            raise ValueError("invalid reconnect backoff")
        if kline_buffer_size <= 0:
            raise ValueError("kline_buffer_size must be positive")

        self.symbols = [symbol.lower() for symbol in symbols]
        self.ws_base = ws_base
        self.rest_base = rest_base
        self.reconnect_initial_s = reconnect_initial_s
        self.reconnect_max_s = reconnect_max_s
        self._clock = clock
        self._wall_clock = wall_clock
        self._kline_buffer_size = kline_buffer_size

        self.books: dict[str, dict[str, list[Level]]] = {
            symbol: {"bids": [], "asks": []} for symbol in self.symbols
        }
        self.funding: dict[str, float] = dict.fromkeys(self.symbols, 0.0)
        self.open_interest: dict[str, float] = dict.fromkeys(self.symbols, 0.0)
        self.oi_change_pct_1h: dict[str, float] = dict.fromkeys(self.symbols, 0.0)
        self.volume_usd: dict[str, float] = dict.fromkeys(self.symbols, 0.0)

        self._closed_klines: dict[str, deque[ClosedKline]] = {
            symbol: deque(maxlen=kline_buffer_size) for symbol in self.symbols
        }
        self._last_kline_close_time_ms: dict[str, int] = dict.fromkeys(self.symbols, 0)
        self._long_liquidations_usd: dict[str, float] = dict.fromkeys(self.symbols, 0.0)
        self._short_liquidations_usd: dict[str, float] = dict.fromkeys(self.symbols, 0.0)
        self._book_updated_at: dict[str, float | None] = dict.fromkeys(self.symbols)
        self._kline_updated_at: dict[str, float | None] = dict.fromkeys(self.symbols)

    def _streams(self) -> str:
        parts: list[str] = []
        for symbol in self.symbols:
            parts.extend(
                (
                    f"{symbol}@depth10@100ms",
                    f"{symbol}@markPrice@1s",
                    f"{symbol}@kline_1m",
                    f"{symbol}@forceOrder",
                )
            )
        return "/".join(parts)

    async def run(self) -> None:  # pragma: no cover - exercises the live network
        """Consume combined streams forever, reconnecting with bounded backoff."""
        url = f"{self.ws_base}?streams={self._streams()}"
        backoff_s = self.reconnect_initial_s
        timeout = aiohttp.ClientTimeout(total=None, connect=15, sock_connect=15, sock_read=90)

        async with aiohttp.ClientSession(timeout=timeout) as session:
            while True:
                received_data = False
                try:
                    await self.refresh_klines(session)
                    async with session.ws_connect(url, heartbeat=15, autoping=True) as ws:
                        log.info("binance.connected", symbols=self.symbols)
                        async for message in ws:
                            if message.type == aiohttp.WSMsgType.TEXT:
                                try:
                                    payload = json.loads(message.data)
                                    if not isinstance(payload, dict):
                                        raise ValueError("combined-stream payload must be an object")
                                    self._on_message(payload)
                                    received_data = True
                                except (TypeError, ValueError, json.JSONDecodeError):
                                    log.warning("binance.invalid_message")
                            elif message.type in {
                                aiohttp.WSMsgType.CLOSE,
                                aiohttp.WSMsgType.CLOSED,
                                aiohttp.WSMsgType.ERROR,
                            }:
                                break
                except asyncio.CancelledError:
                    raise
                except (TimeoutError, aiohttp.ClientError) as exc:
                    log.warning(
                        "binance.disconnected",
                        error=type(exc).__name__,
                        retry_in_s=backoff_s,
                    )

                if received_data:
                    backoff_s = self.reconnect_initial_s
                await asyncio.sleep(backoff_s)
                backoff_s = min(backoff_s * 2, self.reconnect_max_s)

    def _on_message(self, message: dict) -> None:
        stream = str(message.get("stream", ""))
        stream_lower = stream.lower()
        symbol = stream_lower.partition("@")[0]
        data = message.get("data", {})
        if symbol not in self.books or not isinstance(data, dict):
            return

        if "@depth" in stream_lower:
            self.books[symbol] = {
                "bids": self._levels(data.get("b", [])),
                "asks": self._levels(data.get("a", [])),
            }
            self._book_updated_at[symbol] = self._clock()
        elif "@markprice" in stream_lower:
            self.funding[symbol] = self._float(data.get("r"))
        elif "@kline_1m" in stream_lower:
            self._on_kline(symbol, data)
        elif "@forceorder" in stream_lower:
            self._on_liquidation(symbol, data)

    def _on_kline(self, symbol: str, data: dict) -> None:
        kline = data.get("k", {})
        if not isinstance(kline, dict) or not kline.get("x"):
            return

        close_time_ms = int(kline.get("T", 0))
        if close_time_ms <= self._last_kline_close_time_ms[symbol]:
            return

        close = self._float(kline.get("c"))
        high = self._float(kline.get("h")) or close
        low = self._float(kline.get("l")) or close
        quote_volume = self._float(kline.get("q"))
        self._append_closed_kline(symbol, close_time_ms, high, low, close, quote_volume)

    def _on_liquidation(self, symbol: str, data: dict) -> None:
        order = data.get("o", {})
        if not isinstance(order, dict):
            return

        price = self._float(order.get("ap")) or self._float(order.get("p"))
        quantity = self._float(order.get("z")) or self._float(order.get("q"))
        notional_usd = price * quantity
        if notional_usd <= 0:
            return

        side = str(order.get("S", "")).upper()
        if side == "SELL":
            self._long_liquidations_usd[symbol] += notional_usd
        elif side == "BUY":
            self._short_liquidations_usd[symbol] += notional_usd

    def drain_closed_klines(self, symbol: str) -> list[ClosedKline]:
        pending = self._closed_klines.get(symbol.lower())
        if pending is None:
            return []
        klines = list(pending)
        pending.clear()
        return klines

    def drain_liquidations(self, symbol: str) -> LiquidationTotals:
        totals = self.liquidation_totals(symbol)
        self.clear_liquidations(symbol)
        return totals

    def liquidation_totals(self, symbol: str) -> LiquidationTotals:
        key = symbol.lower()
        return LiquidationTotals(
            long_usd=self._long_liquidations_usd.get(key, 0.0),
            short_usd=self._short_liquidations_usd.get(key, 0.0),
        )

    def clear_liquidations(self, symbol: str) -> None:
        key = symbol.lower()
        if key in self._long_liquidations_usd:
            self._long_liquidations_usd[key] = 0.0
            self._short_liquidations_usd[key] = 0.0

    def acknowledge_liquidations(self, symbol: str, published: LiquidationTotals) -> None:
        """Remove only the totals included in a successfully published snapshot."""
        key = symbol.lower()
        if key in self._long_liquidations_usd:
            self._long_liquidations_usd[key] = max(0.0, self._long_liquidations_usd[key] - published.long_usd)
            self._short_liquidations_usd[key] = max(
                0.0, self._short_liquidations_usd[key] - published.short_usd
            )

    def is_book_fresh(self, symbol: str, max_age_s: float) -> bool:
        return self._is_fresh(self._book_updated_at.get(symbol.lower()), max_age_s)

    def is_kline_fresh(self, symbol: str, max_age_s: float) -> bool:
        return self._is_fresh(self._kline_updated_at.get(symbol.lower()), max_age_s)

    def _is_fresh(self, updated_at: float | None, max_age_s: float) -> bool:
        return updated_at is not None and 0 <= self._clock() - updated_at <= max_age_s

    async def refresh_open_interest(
        self, session: aiohttp.ClientSession | None = None
    ) -> None:  # pragma: no cover - network is covered through fakes
        """Refresh the latest open interest for every configured symbol."""
        if session is None:
            timeout = aiohttp.ClientTimeout(total=15)
            async with aiohttp.ClientSession(timeout=timeout) as owned_session:
                await self._refresh_open_interest(owned_session)
            return
        await self._refresh_open_interest(session)

    async def _refresh_open_interest(self, session: aiohttp.ClientSession) -> None:
        for symbol in self.symbols:
            url = f"{self.rest_base}/futures/data/openInterestHist"
            try:
                async with session.get(
                    url,
                    params={"symbol": symbol.upper(), "period": "5m", "limit": 13},
                ) as response:
                    if response.status == 200:
                        body = await response.json()
                        if isinstance(body, list) and body:
                            values = [
                                self._float(item.get("sumOpenInterestValue"))
                                for item in body
                                if isinstance(item, dict)
                            ]
                            values = [value for value in values if value > 0]
                            if values:
                                self.open_interest[symbol] = values[-1]
                                self.oi_change_pct_1h[symbol] = self._pct_change(values[0], values[-1])
                    else:
                        log.warning(
                            "binance.open_interest_failed",
                            symbol=symbol.upper(),
                            status=response.status,
                        )
            except (TimeoutError, aiohttp.ClientError) as exc:
                log.warning(
                    "binance.open_interest_error",
                    symbol=symbol.upper(),
                    error=type(exc).__name__,
                )

    async def run_open_interest_loop(self, interval_s: float) -> None:
        """Refresh open interest immediately and then at a fixed interval."""
        if interval_s <= 0:
            raise ValueError("open-interest interval must be positive")

        timeout = aiohttp.ClientTimeout(total=15)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            while True:
                try:
                    await self.refresh_open_interest(session)
                except asyncio.CancelledError:
                    raise
                except (TimeoutError, aiohttp.ClientError) as exc:
                    log.warning("binance.open_interest_error", error=type(exc).__name__)
                await asyncio.sleep(interval_s)

    async def refresh_klines(self, session: aiohttp.ClientSession) -> None:
        """Backfill closed one-minute candles on startup and every reconnect."""
        now_ms = int(self._wall_clock() * 1_000)
        for symbol in self.symbols:
            url = f"{self.rest_base}/fapi/v1/klines"
            try:
                async with session.get(
                    url,
                    params={
                        "symbol": symbol.upper(),
                        "interval": "1m",
                        "limit": min(self._kline_buffer_size, 1_500),
                    },
                ) as response:
                    if response.status != 200:
                        log.warning(
                            "binance.kline_backfill_failed",
                            symbol=symbol.upper(),
                            status=response.status,
                        )
                        continue
                    body = await response.json()
                    if not isinstance(body, list):
                        continue
                    for item in body:
                        if not isinstance(item, list) or len(item) < 8:
                            continue
                        close_time_ms = int(item[6])
                        if close_time_ms >= now_ms:
                            continue
                        self._append_closed_kline(
                            symbol,
                            close_time_ms,
                            self._float(item[2]),
                            self._float(item[3]),
                            self._float(item[4]),
                            self._float(item[7]),
                        )
            except (TimeoutError, aiohttp.ClientError, TypeError, ValueError) as exc:
                log.warning(
                    "binance.kline_backfill_error",
                    symbol=symbol.upper(),
                    error=type(exc).__name__,
                )

    def _append_closed_kline(
        self,
        symbol: str,
        close_time_ms: int,
        high: float,
        low: float,
        close: float,
        quote_volume: float,
    ) -> None:
        if (
            close_time_ms <= self._last_kline_close_time_ms[symbol]
            or close_time_ms <= 0
            or close <= 0
            or low <= 0
            or high < low
            or not low <= close <= high
        ):
            return
        self._closed_klines[symbol].append(
            ClosedKline(
                close_time_ms=close_time_ms,
                high=high,
                low=low,
                close=close,
                quote_volume=quote_volume,
            )
        )
        self._last_kline_close_time_ms[symbol] = close_time_ms
        self._kline_updated_at[symbol] = self._clock()
        self.volume_usd[symbol] = quote_volume

    @staticmethod
    def _pct_change(old: float, new: float) -> float:
        return (new - old) / old * 100.0 if old > 0 else 0.0

    @staticmethod
    def _levels(raw_levels: object) -> list[Level]:
        if not isinstance(raw_levels, list):
            return []
        levels: list[Level] = []
        for level in raw_levels:
            if isinstance(level, (list, tuple)) and len(level) >= 2:
                try:
                    price, quantity = float(level[0]), float(level[1])
                except (TypeError, ValueError):
                    continue
                if price > 0 and quantity >= 0:
                    levels.append((price, quantity))
        return levels

    @staticmethod
    def _float(value: str | int | float | None) -> float:
        try:
            return float(value or 0.0)
        except (TypeError, ValueError):
            return 0.0
