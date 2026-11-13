"""Measure one EVEDEX PROD venue quality now, from both public order books.

    python -m kairos_quant.venue_now --signal SOLUSDT --taker-fee-bps 5 --assessed-notional-usd 200

The EVEDEX instrument comes from the closed ``PROD_SYMBOL_MAP``. Both books are fetched
concurrently and turned into one ``VenueQualityV1`` by ``build_venue_quality`` with the
PROD profile; it expires ``measurement_ttl_ms`` (5 s) after observation, so a consumer
asks for it immediately before deciding. The result is one JSON line; failures print
``{"error": ...}`` and exit 1.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import time
from collections.abc import Awaitable, Callable, Sequence
from typing import Any

from kairos_core.contracts import VenueQualityV1
from kairos_core.enums import EvedexProfile

from .runtime_venue_gate import PROD_SYMBOL_MAP, VenueGatePolicy, build_venue_quality
from .venue_comparison import parse_binance_book, parse_evedex_book

BINANCE_DEPTH = "https://fapi.binance.com/fapi/v1/depth"
EVEDEX_DEPTH = "https://exchange-api.evedex.com/api/market/{symbol}/deep"

Fetch = Callable[[str, dict[str, str] | None], Awaitable[tuple[Any, float]]]


async def _aiohttp_fetch_factory() -> tuple[Fetch, Callable[[], Awaitable[None]]]:
    import aiohttp

    session = aiohttp.ClientSession(trust_env=True, timeout=aiohttp.ClientTimeout(total=10))

    async def fetch(url: str, params: dict[str, str] | None) -> tuple[Any, float]:
        started = time.perf_counter()
        async with session.get(url, params=params) as response:
            response.raise_for_status()
            payload = await response.json(content_type=None)
        return payload, (time.perf_counter() - started) * 1000

    return fetch, session.close


async def measure_prod_venue(
    signal_symbol: str,
    *,
    taker_fee_bps: float,
    assessed_notional_usd: float,
    fetch: Fetch | None = None,
    now_ms: Callable[[], int] = lambda: int(time.time() * 1000),
) -> VenueQualityV1:
    """One PROD measurement of the reviewed pair of ``signal_symbol``."""
    venue_symbol = PROD_SYMBOL_MAP.get(signal_symbol)
    if venue_symbol is None:
        raise ValueError(f"{signal_symbol} is outside the approved PROD symbol map")
    close: Callable[[], Awaitable[None]] | None = None
    if fetch is None:
        fetch, close = await _aiohttp_fetch_factory()
    try:
        (evedex_raw, evedex_ms), (binance_raw, binance_ms) = await asyncio.gather(
            fetch(EVEDEX_DEPTH.format(symbol=venue_symbol), None),
            fetch(BINANCE_DEPTH, {"symbol": signal_symbol, "limit": "100"}),
        )
    finally:
        if close is not None:
            await close()
    return build_venue_quality(
        binance=parse_binance_book(signal_symbol, binance_raw, latency_ms=binance_ms),
        evedex=parse_evedex_book(venue_symbol, evedex_raw, latency_ms=evedex_ms),
        venue_symbol=venue_symbol,
        observed_at_ms=now_ms(),
        latency_ms=math.ceil(max(evedex_ms, binance_ms)),
        policy=VenueGatePolicy(assessed_notional_usd=assessed_notional_usd, taker_fee_bps=taker_fee_bps),
        profile=EvedexProfile.PROD,
        source="kairos-quant-scouts",
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m kairos_quant.venue_now", description=__doc__)
    parser.add_argument("--signal", required=True)
    parser.add_argument("--taker-fee-bps", type=float, required=True)
    parser.add_argument("--assessed-notional-usd", type=float, required=True)
    args = parser.parse_args(argv)
    try:
        venue = asyncio.run(
            measure_prod_venue(
                args.signal,
                taker_fee_bps=args.taker_fee_bps,
                assessed_notional_usd=args.assessed_notional_usd,
            )
        )
    except Exception as exc:  # noqa: BLE001 - one sanitized line for the caller, which fails closed
        print(json.dumps({"error": f"{type(exc).__name__}: {str(exc)[:300]}"}))
        return 1
    print(venue.model_dump_json())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
