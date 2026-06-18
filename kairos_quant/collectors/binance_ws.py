"""Async Binance USD-M Futures collector (depth + mark/funding + liquidations).

This is the dev/test data source. It keeps the latest order book and a rolling
kline series so the :class:`SnapshotBuilder` can produce 1-minute snapshots.
Network I/O is isolated here; everything above consumes typed snapshots only.
"""
from __future__ import annotations

import asyncio
import json
from typing import Dict, List, Tuple

try:
    import aiohttp
except Exception:  # pragma: no cover
    aiohttp = None  # type: ignore

Level = Tuple[float, float]


class BinanceFuturesCollector:
    def __init__(self, symbols: List[str], ws_base: str, rest_base: str) -> None:
        self.symbols = [s.lower() for s in symbols]
        self.ws_base = ws_base
        self.rest_base = rest_base
        self.books: Dict[str, Dict[str, List[Level]]] = {s: {"bids": [], "asks": []} for s in self.symbols}
        self.funding: Dict[str, float] = {s: 0.0 for s in self.symbols}
        self.open_interest: Dict[str, float] = {s: 0.0 for s in self.symbols}

    def _streams(self) -> str:
        parts = []
        for s in self.symbols:
            parts.append(f"{s}@depth10@100ms")
            parts.append(f"{s}@markPrice@1s")
        return "/".join(parts)

    async def run(self) -> None:  # pragma: no cover - requires network
        if aiohttp is None:
            raise RuntimeError("aiohttp is required for the live collector")
        url = f"{self.ws_base}?streams={self._streams()}"
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(url, heartbeat=15) as ws:
                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        self._on_message(json.loads(msg.data))

    def _on_message(self, message: dict) -> None:
        stream = message.get("stream", "")
        data = message.get("data", {})
        if "@depth" in stream:
            sym = stream.split("@")[0]
            self.books[sym] = {
                "bids": [(float(p), float(q)) for p, q in data.get("b", [])],
                "asks": [(float(p), float(q)) for p, q in data.get("a", [])],
            }
        elif "@markPrice" in stream:
            sym = stream.split("@")[0]
            self.funding[sym] = float(data.get("r", 0.0))  # funding rate

    async def refresh_open_interest(self) -> None:  # pragma: no cover - requires network
        if aiohttp is None:
            return
        async with aiohttp.ClientSession() as session:
            for s in self.symbols:
                url = f"{self.rest_base}/fapi/v1/openInterest?symbol={s.upper()}"
                async with session.get(url) as resp:
                    if resp.status == 200:
                        body = await resp.json()
                        self.open_interest[s] = float(body.get("openInterest", 0.0))
                await asyncio.sleep(0.2)
