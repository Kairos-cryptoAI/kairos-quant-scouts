"""Resilient Binance USD-M Futures market-data collector.

Only closed one-minute klines enter the indicator history. Order-book and mark-price
streams remain live inputs for snapshot metadata, while forced orders are aggregated
between published snapshots.
"""

from __future__ import annotations

import asyncio
import json
import math
import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass

import aiohttp
from kairos_core.logging import get_logger

from ..orderbook import normalize_order_book

log = get_logger("quant-scouts.binance")

Level = tuple[float, float]
_ONE_MINUTE_MS = 60_000
_OPEN_INTEREST_PERIOD_MS = 5 * _ONE_MINUTE_MS
_OPEN_INTEREST_MAX_SOURCE_LAG_MS = 2 * _OPEN_INTEREST_PERIOD_MS
_LIQUIDATION_DEDUP_SIZE = 10_000


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

        self.symbols = list(dict.fromkeys(symbol.strip().lower() for symbol in symbols))
        if not self.symbols or any(not symbol for symbol in self.symbols):
            raise ValueError("at least one non-empty symbol is required")
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
        self._pending_klines: dict[str, dict[int, ClosedKline]] = {symbol: {} for symbol in self.symbols}
        self._needs_kline_backfill: set[str] = set()
        self._last_kline_close_time_ms: dict[str, int] = dict.fromkeys(self.symbols, 0)
        self._long_liquidations_usd: dict[str, float] = dict.fromkeys(self.symbols, 0.0)
        self._short_liquidations_usd: dict[str, float] = dict.fromkeys(self.symbols, 0.0)
        self._book_updated_at: dict[str, float | None] = dict.fromkeys(self.symbols)
        self._kline_updated_at: dict[str, float | None] = dict.fromkeys(self.symbols)
        self._funding_updated_at: dict[str, float | None] = dict.fromkeys(self.symbols)
        self._open_interest_updated_at: dict[str, float | None] = dict.fromkeys(self.symbols)
        self._book_event_time_ms: dict[str, int | None] = dict.fromkeys(self.symbols)
        self._funding_event_time_ms: dict[str, int | None] = dict.fromkeys(self.symbols)
        self._last_kline_backfill_at: dict[str, float | None] = dict.fromkeys(self.symbols)
        self._last_book_update_id: dict[str, int | None] = dict.fromkeys(self.symbols)
        self._seen_liquidations: set[tuple[str, int, str, float, float]] = set()
        self._liquidation_order: deque[tuple[str, int, str, float, float]] = deque()

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
                    async with session.ws_connect(url, heartbeat=15, autoping=True) as ws:
                        log.info("binance.connected", symbols=self.symbols)
                        # Connect first so final kline events are buffered while the REST
                        # backfill closes the startup/reconnect race window.
                        await self.refresh_klines(session)
                        async for message in ws:
                            if message.type == aiohttp.WSMsgType.TEXT:
                                try:
                                    payload = json.loads(message.data)
                                    if not isinstance(payload, dict):
                                        raise ValueError("combined-stream payload must be an object")
                                    received_data = self._on_message(payload) or received_data
                                    if self._gap_backfill_due():
                                        await self.refresh_klines(session)
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

    def _on_message(self, message: dict) -> bool:
        stream = str(message.get("stream", ""))
        stream_lower = stream.lower()
        symbol = stream_lower.partition("@")[0]
        data = message.get("data", {})
        if symbol not in self.books or not isinstance(data, dict):
            return False

        if "@depth" in stream_lower:
            update_id = self._integer(data.get("u"))
            event_time_ms = self._integer(data.get("E"))
            if update_id is None or update_id <= 0 or event_time_ms is None or event_time_ms <= 0:
                return False
            previous_update_id = self._last_book_update_id[symbol]
            if previous_update_id is not None and update_id <= previous_update_id:
                return False
            bids = self._levels(data.get("b", []))
            asks = self._levels(data.get("a", []))
            if not bids or not asks:
                return False
            try:
                bids, asks = normalize_order_book(bids, asks)
            except ValueError:
                return False
            self.books[symbol] = {"bids": bids, "asks": asks}
            self._last_book_update_id[symbol] = update_id
            self._book_event_time_ms[symbol] = event_time_ms
            self._book_updated_at[symbol] = self._clock()
            return True
        elif "@markprice" in stream_lower:
            funding = self._finite_float(data.get("r"))
            event_time_ms = self._integer(data.get("E"))
            previous_event_time_ms = self._funding_event_time_ms[symbol]
            if (
                funding is None
                or event_time_ms is None
                or event_time_ms <= 0
                or (previous_event_time_ms is not None and event_time_ms <= previous_event_time_ms)
            ):
                return False
            self.funding[symbol] = funding
            self._funding_event_time_ms[symbol] = event_time_ms
            self._funding_updated_at[symbol] = self._clock()
            return True
        elif "@kline_1m" in stream_lower:
            return self._on_kline(symbol, data)
        elif "@forceorder" in stream_lower:
            return self._on_liquidation(symbol, data)
        return False

    def _on_kline(self, symbol: str, data: dict) -> bool:
        kline = data.get("k", {})
        if not isinstance(kline, dict) or kline.get("x") is not True:
            return False

        close_time_ms = self._integer(kline.get("T"))
        if close_time_ms is None or close_time_ms >= int(self._wall_clock() * 1_000):
            return False

        close = self._finite_float(kline.get("c"))
        high = self._finite_float(kline.get("h"))
        low = self._finite_float(kline.get("l"))
        quote_volume = self._finite_float(kline.get("q"))
        if close is None or high is None or low is None or quote_volume is None:
            return False
        return self._append_closed_kline(symbol, close_time_ms, high, low, close, quote_volume)

    def _on_liquidation(self, symbol: str, data: dict) -> bool:
        order = data.get("o", {})
        if not isinstance(order, dict):
            return False

        price = self._finite_float(order.get("ap"))
        if price is None or price <= 0:
            price = self._finite_float(order.get("p"))
        quantity = self._finite_float(order.get("z"))
        if quantity is None or quantity <= 0:
            quantity = self._finite_float(order.get("q"))
        if price is None or quantity is None:
            return False
        notional_usd = price * quantity
        if not math.isfinite(notional_usd) or notional_usd <= 0:
            return False

        side = str(order.get("S", "")).upper()
        if side not in {"BUY", "SELL"}:
            return False
        event_time_ms = self._integer(data.get("E")) or self._integer(order.get("T"))
        if event_time_ms is not None and event_time_ms > 0:
            fingerprint = (symbol, event_time_ms, side, price, quantity)
            if fingerprint in self._seen_liquidations:
                return False
            if len(self._liquidation_order) >= _LIQUIDATION_DEDUP_SIZE:
                self._seen_liquidations.discard(self._liquidation_order.popleft())
            self._liquidation_order.append(fingerprint)
            self._seen_liquidations.add(fingerprint)
        if side == "SELL":
            self._long_liquidations_usd[symbol] += notional_usd
        else:
            self._short_liquidations_usd[symbol] += notional_usd
        return True

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
        key = symbol.lower()
        return self._is_fresh(self._book_updated_at.get(key), max_age_s) and self._exchange_time_is_fresh(
            self._book_event_time_ms.get(key), max_age_s
        )

    def is_kline_fresh(self, symbol: str, max_age_s: float) -> bool:
        key = symbol.lower()
        received_fresh = self._is_fresh(self._kline_updated_at.get(key), max_age_s)
        close_time_ms = self._last_kline_close_time_ms.get(key, 0)
        event_age_s = self._wall_clock() - close_time_ms / 1_000.0
        return received_fresh and 0 <= event_age_s <= max_age_s

    def is_funding_fresh(self, symbol: str, max_age_s: float) -> bool:
        key = symbol.lower()
        return self._is_fresh(self._funding_updated_at.get(key), max_age_s) and self._exchange_time_is_fresh(
            self._funding_event_time_ms.get(key), max_age_s
        )

    def is_open_interest_fresh(self, symbol: str, max_age_s: float) -> bool:
        return self._is_fresh(self._open_interest_updated_at.get(symbol.lower()), max_age_s)

    def _is_fresh(self, updated_at: float | None, max_age_s: float) -> bool:
        return max_age_s > 0 and updated_at is not None and 0 <= self._clock() - updated_at <= max_age_s

    def _exchange_time_is_fresh(self, event_time_ms: int | None, max_age_s: float) -> bool:
        if max_age_s <= 0 or event_time_ms is None:
            return False
        event_age_s = self._wall_clock() - event_time_ms / 1_000.0
        return 0 <= event_age_s <= max_age_s

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
                        if isinstance(body, list):
                            records = [item for item in body if isinstance(item, dict)]
                            records = records[-13:]
                            observations: list[tuple[int, float]] = []
                            for item in records:
                                timestamp = self._integer(item.get("timestamp"))
                                value = self._finite_float(item.get("sumOpenInterestValue"))
                                if (
                                    timestamp is not None
                                    and timestamp > 0
                                    and value is not None
                                    and value > 0
                                ):
                                    observations.append((timestamp, value))
                            observations.sort(key=lambda observation: observation[0])
                            timestamps = [timestamp for timestamp, _ in observations]
                            values = [value for _, value in observations]
                            has_hour_window = (
                                len(timestamps) == 13
                                and len(set(timestamps)) == 13
                                and all(
                                    current - previous == _OPEN_INTEREST_PERIOD_MS
                                    for previous, current in zip(timestamps, timestamps[1:], strict=False)
                                )
                            )
                            last_timestamp = timestamps[-1] if timestamps else None
                            source_age_ms = (
                                int(self._wall_clock() * 1_000) - last_timestamp
                                if last_timestamp is not None
                                else -1
                            )
                            if has_hour_window and 0 <= source_age_ms <= _OPEN_INTEREST_MAX_SOURCE_LAG_MS:
                                self.open_interest[symbol] = values[-1]
                                self.oi_change_pct_1h[symbol] = self._pct_change(values[0], values[-1])
                                self._open_interest_updated_at[symbol] = self._clock()
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
        for symbol in self.symbols:
            # Record attempts, including failures, so a persistent gap cannot turn
            # high-frequency depth traffic into an unbounded REST retry loop.
            self._last_kline_backfill_at[symbol] = self._clock()
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
                    # Evaluate closure after the awaited response so a candle that
                    # closes while the request is in flight is not lost forever.
                    now_ms = int(self._wall_clock() * 1_000)
                    closed_klines: list[tuple[int, float, float, float, float]] = []
                    for item in body:
                        if not isinstance(item, list) or len(item) < 8:
                            continue
                        close_time_ms = self._integer(item[6])
                        if close_time_ms is None:
                            continue
                        if close_time_ms >= now_ms:
                            continue
                        high = self._finite_float(item[2])
                        low = self._finite_float(item[3])
                        close = self._finite_float(item[4])
                        quote_volume = self._finite_float(item[7])
                        if (
                            high is not None
                            and low is not None
                            and close is not None
                            and quote_volume is not None
                        ):
                            closed_klines.append(
                                (
                                    close_time_ms,
                                    high,
                                    low,
                                    close,
                                    quote_volume,
                                )
                            )
                    for values in sorted(closed_klines, key=lambda item: item[0]):
                        self._append_closed_kline(symbol, *values)
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
    ) -> bool:
        if (
            close_time_ms <= self._last_kline_close_time_ms[symbol]
            or close_time_ms <= 0
            or close_time_ms % _ONE_MINUTE_MS != _ONE_MINUTE_MS - 1
            or not all(math.isfinite(value) for value in (high, low, close, quote_volume))
            or min(high, low, close) <= 0
            or high < low
            or not low <= close <= high
            or quote_volume < 0
        ):
            return False
        pending = self._pending_klines[symbol]
        if close_time_ms in pending:
            return False

        candle = ClosedKline(
            close_time_ms=close_time_ms,
            high=high,
            low=low,
            close=close,
            quote_volume=quote_volume,
        )
        last_close_time_ms = self._last_kline_close_time_ms[symbol]
        if last_close_time_ms and close_time_ms != last_close_time_ms + _ONE_MINUTE_MS:
            pending[close_time_ms] = candle
            if len(pending) > self._kline_buffer_size:
                pending.pop(max(pending))
            self._needs_kline_backfill.add(symbol)
            return True

        self._promote_closed_kline(symbol, candle)
        next_close_time_ms = close_time_ms + _ONE_MINUTE_MS
        while next_close_time_ms in pending:
            self._promote_closed_kline(symbol, pending.pop(next_close_time_ms))
            next_close_time_ms += _ONE_MINUTE_MS
        if not pending:
            self._needs_kline_backfill.discard(symbol)
        return True

    def _promote_closed_kline(self, symbol: str, candle: ClosedKline) -> None:
        self._closed_klines[symbol].append(candle)
        self._last_kline_close_time_ms[symbol] = candle.close_time_ms
        self._kline_updated_at[symbol] = self._clock()
        self.volume_usd[symbol] = candle.quote_volume

    def _gap_backfill_due(self) -> bool:
        now = self._clock()
        for symbol in self._needs_kline_backfill:
            attempted_at = self._last_kline_backfill_at[symbol]
            if attempted_at is None or now - attempted_at >= self.reconnect_initial_s:
                return True
        return False

    @staticmethod
    def _pct_change(old: float, new: float) -> float:
        if not all(math.isfinite(value) for value in (old, new)) or old <= 0 or new < 0:
            return 0.0
        return (new - old) / old * 100.0

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
                if math.isfinite(price) and math.isfinite(quantity) and price > 0 and quantity > 0:
                    levels.append((price, quantity))
        return levels

    @staticmethod
    def _finite_float(value: str | int | float | None) -> float | None:
        try:
            result = float(value) if value is not None else None
        except (TypeError, ValueError):
            return None
        return result if result is not None and math.isfinite(result) else None

    @staticmethod
    def _integer(value: str | int | float | None) -> int | None:
        if isinstance(value, bool) or (
            isinstance(value, float) and (not math.isfinite(value) or not value.is_integer())
        ):
            return None
        try:
            return int(value) if value is not None else None
        except (OverflowError, TypeError, ValueError):
            return None
