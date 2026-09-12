# kairos-quant-scouts

## Offline long-gap recovery

For an outage beyond the live collector's bounded REST window, use the deploy
long-gap recovery wrapper in the isolated `kairos-paper-gate` project. Stop the
old quant producer and strategy/risk/execution consumers first; preserve a fresh
PostgreSQL backup. The module `kairos_quant.long_gap_recovery` requires an
explicit closed end boundary, a total bar budget (at most 150,000) and offline
consumer confirmation. It accepts only the five-symbol PAPER Redis profile and
the official Binance UM URL. No trading credentials or LLM/feed APIs are used.

Each page includes the persisted anchor, requires two identical full REST
responses, and publishes only a contiguous verified suffix via the existing
atomic event-audit/outbox path. Resume reads the durable audit prefix, never an
uncommitted memory cursor. Conflicting history/anchors, missing/reordered bars,
non-finite values, request errors or exhausted bounds stop the operation.
Structured logs expose retrieval time, coverage and stop reason, not PnL.

Live quant and offline recovery share an exclusive PostgreSQL advisory lease.
Do not run older images without this lease alongside recovery. After repair,
validate per-symbol continuity and restore the stopped read-only consumers;
do not delete Redis entries, inbox cursors, audit history or old backups.
Historical restoration does not count as continuous online observation or a
fresh 24-hour DEV qualification window.

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

The development collector uses two independent Binance USD-M Futures combined
connections, as specified in Binance's
[routing notice](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Important-WebSocket-Change-Notice):

- `/public/stream`: `depth10@100ms` for the top of book;
- `/market/stream`: `markPrice@1s` (with `premiumIndex` REST fallback for funding),
  `kline_1m` as provisional close/liveness evidence, and `forceOrder`.

`KAIROS_BINANCE_WS_BASE` defaults to `wss://fstream.binance.com`. An existing
`.../stream` configuration is normalized to the root; no mixed/unrouted socket
is dialed and no legacy network fallback exists. Embedded routes, subscriptions
or credentials are startup errors. Each worker reconnects independently;
structured cancellation/fatal errors close both sessions. Only the market worker
waits for REST reconciliation, so it cannot block public depth processing.
Public disconnect immediately invalidates the old book; reconnect alone or a
replayed update ID cannot refresh it. Only exact subscribed streams belonging
to that worker are accepted; diff-depth is not treated as a top-N snapshot.

`forceOrder` is a sampled liquidation snapshot stream, not a complete event tape:
its totals are observed notional, not the market's exact total liquidation volume.
Reconnect does not recover missing liquidation events or prove lossless depth.

Open interest value and its one-hour change are refreshed periodically from Binance's
5-minute statistics. A Binance WebSocket candle marked `x=true` can still be revised,
so it is never authoritative strategy input. Closed candles are continuously read from
REST after a bounded finality delay and require two byte-identical observations before
promotion. The latest retained REST window is also reconciled after publication, so a
later price or volume mutation permanently blocks only that symbol. Each accepted close is emitted as a
strict `ClosedBarEventV1` containing the complete OHLCV, quote-volume and taker-buy
volumes before it is acknowledged locally. A gap pauses that symbol until REST backfill
repairs the exact sequence; a reordered or conflicting close fails the stream closed.
Connections use bounded exponential
backoff, and snapshots are suppressed when their book or last closed kline is stale.
Funding and open interest must also have fresh successful observations; funding falls
back to the official public REST snapshot when its WebSocket stream is unavailable. The OI series
must be a contiguous 13-point five-minute grid and its last source point cannot be stale. Kline freshness
checks both receipt time and the exchange close timestamp, so a newly received historical
backfill cannot masquerade as current data.
Liquidation totals are removed only after their snapshot is published successfully.
The production EVEDEX feed lives in
[`kairos-execution-engine`](https://github.com/Kairos-cryptoAI/kairos-execution-engine)
and is injected through the same `SnapshotBuilder` contract.

## Venue comparison

Binance remains a research/data proxy, not proof of EVEDEX execution quality. Run the
public, order-free comparison before any shadow or canary phase:

```powershell
uv run --locked kairos-venue-compare `
  --samples 30 `
  --interval-s 2 `
  --notional-usd 1000 `
  --output $env:TEMP\kairos-venue-comparison.json `
  --overwrite
```

Each synchronized observation records both venue timestamps, basis, spread, request
latency, and executable buy/sell slippage from up to 100 book levels. The blocking gate
requires 30/30 observations per symbol, at least 99% availability, fresh books with no
more than two seconds of timestamp skew, p95 absolute basis/spread/slippage below the
registered limits, and sufficient EVEDEX depth for the requested notional. Reports
always set `live_orders_allowed=false`: a short PASS qualifies only that observation
window and not future liquidity, order placement, fills, or custody.

For PAPER, the same measurement runs continuously as a read-only runtime gate and emits
`VenueQualityV1` for the exact `BTCUSD:DEV`, `ETHUSD:DEV`, `SOLUSD:DEV`, `BNBUSD:DEV`,
and `XRPUSD:DEV` symbols. Enable it explicitly with
`KAIROS_ENABLE_VENUE_QUALITY_GATE=true`. The service rejects any EVEDEX base URL other
than the official DEV endpoint, and a stale, shallow or out-of-bounds observation can
only block entry.

The poller uses a monotonic start-to-start schedule, with all configured symbols fetched
concurrently. Before public network I/O it persists one strict `ATTEMPTED` fact per
symbol on `kairos.venue.poll.v1`; after the quality measurement is durable it persists
`SUCCEEDED`, otherwise `FAILED`. The 24-hour operations gate derives its denominator
from the latest durable interval and symbol-set fingerprint, so latency, failures,
process downtime and configuration changes cannot inflate venue availability.

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
