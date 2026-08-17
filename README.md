# kairos-quant-scouts

**Layer 1A — Quant Scouts.** Pure-math collectors and indicators (no LLM). They
connect to the exchange, digest raw order-book and derivatives streams, and emit a
compact `MarketSnapshot` — the only numeric payload the upper layers consume.

## What it computes

- **Order book:** top-N imbalance, spread in basis points, and resting depth.
- **Derivatives:** funding rate, USD open-interest value, one-hour OI delta, and
  interval long/short liquidations.
- **Indicators:** Wilder's RSI(14), fully warmed MACD(12,26,9), and Wilder-smoothed
  ATR as a fraction of the last closed-candle price.
- **Quant bias:** a transparent LONG/SHORT/FLAT vote from momentum, MACD, and book pressure.

Raw ticks never leave this layer. RSI and MACD history is populated exclusively from
closed Binance one-minute klines; the current order book is used only for live price,
spread, depth, and imbalance. Recursive EMA/Wilder state persists when the bounded raw
candle window evicts old rows, so live values remain consistent with an unbounded replay.

## Data source

The development collector consumes Binance USD-M Futures combined streams:

- `depth10@100ms` for the top of book;
- `markPrice@1s` for funding;
- `kline_1m` for closed indicator candles and quote volume;
- `forceOrder` for liquidation notional.

Open interest value and its one-hour change are refreshed periodically from Binance's
5-minute statistics. Closed candles are backfilled over REST on startup and reconnect,
then deduplicated against the WebSocket stream. Connections use bounded exponential
backoff, and snapshots are suppressed when their book or last closed kline is stale.
Funding and open interest must also have fresh successful observations; the OI series
must be a contiguous 13-point five-minute grid and its last source point cannot be stale. Kline freshness
checks both receipt time and the exchange close timestamp, so a newly received historical
backfill cannot masquerade as current data. A gap in the one-minute candle sequence resets
indicator history instead of blending unequal time intervals.
Liquidation totals are removed only after their snapshot is published successfully.
The production EVEDEX feed lives in
[`kairos-execution-engine`](https://github.com/Kairos-cryptoAI/kairos-execution-engine)
and is injected through the same `SnapshotBuilder` contract.

## Local development

Install [uv](https://docs.astral.sh/uv/) once. The repository pins uv 0.12.3,
Python 3.11, every transitive dependency, and the exact compatible `kairos-core`
revision:

```powershell
winget install --id astral-sh.uv --exact
uv sync --locked
uv run --locked python -m kairos_quant
```

The service emits `kairos.market.snapshot`. Configuration uses the `KAIROS_` prefix;
see `.env.example` for the local defaults.

## Checks

Run the same blocking checks as CI:

```powershell
uv run --locked ruff check kairos_quant tests
uv run --locked ruff format --check kairos_quant tests
uv run --locked mypy kairos_quant
uv run --locked bandit -q -r kairos_quant -x tests
uv run --locked pytest -q --tb=short
uv build --no-sources
```

CI covers Linux on Python 3.11 and 3.14 plus Windows on Python 3.11.

## Runtime delivery durability

The Redis backend uses `kairos-persistence`: publications are committed to a
PostgreSQL outbox before dispatch. Configure `KAIROS_PERSISTENCE_DATABASE_URL`
through the deployment secret provider. The in-memory backend intentionally
bypasses persistence and is limited to local tests.

---

Part of the [Kairos](https://github.com/Kairos-cryptoAI/kairos) system. MIT licensed.
