# kairos-quant-scouts

**Layer 1A — Quant Scouts.** Pure-math collectors and indicators (no LLM). They connect
to the exchange, digest raw order-book / derivatives streams and emit a compact
`MarketSnapshot` — the only numeric payload the upper layers ever see.

## What it computes
- **Order book:** top-N imbalance `(bidVol-askVol)/(bidVol+askVol)`, spread (bps), resting depth.
- **Derivatives:** funding rate, open interest (+1h change), long/short liquidations.
- **Indicators:** Wilder's **RSI(14)**, **MACD(12,26,9)**, ATR.
- **Quant bias:** a transparent LONG/SHORT/FLAT vote from momentum + MACD + book pressure.

Raw ticks never leave this layer — everything above consumes `MarketSnapshot` only.

## Data source
Ships with a Binance USD-M Futures WebSocket collector for dev/testing. The production
EVEDEX feed lives in [`kairos-execution-engine`](https://github.com/TheLitis/kairos-execution-engine)
and is injected through the same `SnapshotBuilder`.

## Run
```bash
pip install -e ../kairos-core && pip install -e ".[dev]"
make test
python -m kairos_quant
```
Emits `kairos.market.snapshot`.

---
Part of the [Kairos](https://github.com/TheLitis/kairos) system. MIT licensed.
