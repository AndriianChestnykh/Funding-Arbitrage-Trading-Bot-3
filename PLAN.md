# Funding Arbitrage Trading Bot — System Design & Tech Stack

## Context

User wants to build a **CEX-vs-CEX perpetual funding-rate arbitrage bot**. The bot identifies funding-rate divergences across centralized exchanges (Binance, Bybit, OKX, Hyperliquid, etc.), proposes opportunities to the user via Telegram for approval, executes the delta-neutral pair (long on the negative-funding venue, short on the positive-funding venue), then monitors open positions and warns when funding economics deteriorate, proposing exits.

**Constraints decided:**
- Venues: CEX perp vs CEX perp (delta-neutral funding harvest)
- Autonomy: Semi-auto via Telegram — bot proposes, user approves; bot monitors and proposes exits
- Stack: **Go (Golang)** — chosen for low-latency execution, strong concurrency primitives (goroutines + channels) ideal for many concurrent websocket feeds, single static binary deploy, and predictable GC behavior on a long-lived trading process.
- Deploy: VPS / dedicated (low-latency, long-lived process). Colocated near exchange API endpoints (e.g. AWS ap-northeast-1 Tokyo for Binance/Bybit/OKX; us-east for Hyperliquid).

> NOTE: The user mentioned a link with funding metrics but it did not come through in the message. The plan below assumes standard funding-rate data sources (CoinGlass-style aggregators + direct exchange websockets). Update the **Data Sources** section once the link is shared.

---

## What "funding arbitrage" actually captures

Perpetual futures use a **funding rate** paid periodically (every 1h on Hyperliquid, every 4h or 8h on Binance/Bybit/OKX) between longs and shorts to anchor the perp price to spot.

- **Positive funding** → longs pay shorts → shorting that venue earns funding.
- **Negative funding** → shorts pay longs → going long that venue earns funding.

**The arbitrage**: when the same asset (e.g. BTC-PERP) has materially different funding on two venues, open offsetting positions so the **net delta is ~0** but you **net-collect funding**. Example:
- Binance BTC funding = +0.05% / 8h (longs pay)
- Hyperliquid BTC funding = -0.02% / 8h (shorts pay)
- → **Short Binance + Long Hyperliquid**: earn 0.05% + 0.02% = **0.07% / 8h ≈ 0.21% / day ≈ 76%/yr** (gross, before costs).

**Real PnL** = Funding earned − (taker fees in + taker fees out + slippage + borrow/maker rebates + transfer costs + execution latency cost). Most theoretical edges die after costs; the bot's job is to filter for **net-positive opportunities** with sufficient margin of safety.

---

## Bot Ideas (3 variants, increasing sophistication)

### Idea 1 — **"Funding Sniper"** (MVP, recommended starting point)
Monitor top-N perp pairs across 4-5 venues. When the **funding spread × time-to-next-funding** exceeds a configurable threshold (after costs), Telegram-alert the user with a one-tap "Execute" button. Bot opens delta-neutral pair, monitors, and proposes exit when:
- Funding spread collapses below exit threshold, OR
- Funding direction flips, OR
- Mark-price divergence exceeds risk band (delta drift), OR
- Position has captured ≥ N funding payments and edge is decaying.

### Idea 2 — **"Funding Sniper + Basis-Aware"**
Adds spot-basis context: only enter when the perp premium/discount aligns with the funding signal (avoids entering right before a funding flip). Also avoids new entries within ~30s of a funding settlement (where last-minute manipulation is common). Same UX, smarter filters.

### Idea 3 — **"Multi-Leg Funding Portfolio"**
Treats the book as a portfolio: caps total notional, balances exposure per asset and per venue, rotates capital toward the highest-Sharpe opportunities. Adds a **predictive funding model** (simple linear/ARIMA on open-interest + perp-spot basis) to enter *before* funding spikes. Telegram digest summarizes portfolio PnL daily. **Build this after Idea 1 proves out.**

**Recommendation: ship Idea 1, design data layer so Ideas 2 & 3 layer on without rewrites.**

---

## System Architecture

**One Go binary. One SQLite file. No Redis, no Postgres, no Docker, no message broker.** All inter-stage communication is in-process Go channels.

```
                    ┌────────────────────────────────────────────────┐
                    │             VPS · single Go process             │
                    │                                                 │
   Binance  ──WS──► │ ┌─────────────┐    ┌──────────────────────┐    │
   Bybit    ──WS──► │ │  Ingestors  │─ch→│   Normalizer         │    │
   OKX      ──WS──► │ │  (goroutine │    │   (per-venue decode  │    │
   HL       ──WS──► │ │   per venue)│    │    → unified Tick)   │    │
                    │ └─────────────┘    └──────────┬───────────┘    │
                    │                               │ chan Tick       │
                    │                    ┌──────────▼───────────┐    │
                    │                    │  Opportunity Engine  │    │
                    │                    │  (spread × ttf − cost│    │
                    │                    │   ≥ threshold)       │    │
                    │                    └──────────┬───────────┘    │
                    │                               │ chan Candidate  │
                    │   ┌───────────────────────────▼──────────────┐ │
                    │   │  Strategy / Risk Manager (single owner)  │ │
                    │   │  - in-RAM open positions + caps          │ │
                    │   │  - delta drift monitor · exit logic      │ │
                    │   └────┬─────────────────────────┬───────────┘ │
                    │        │ chan Proposal           │ direct calls │
                    │        ▼                         ▼              │
                    │   ┌──────────────┐         ┌─────────────────┐ │
                    │   │ Telegram Bot │ ◄─call─ │  SQLite (WAL)   │ │
                    │   │ (approval UX)│         │  bot.db         │ │
                    │   └──────┬───────┘         │  positions, fills│ │
                    │          │ chan Approve    │  funding_history │ │
                    │          ▼                 └─────────────────┘ │
                    │   ┌──────────────────┐                          │
                    │   │ Execution Router │── REST/WS ──►  Exchanges │
                    │   │ (atomic 2-leg)   │                          │
                    │   └──────────────────┘                          │
                    │                                                 │
                    │   ┌──────────────────┐    /metrics              │
                    │   │ embedded HTTP    │──► Prometheus (optional) │
                    │   │  /metrics /admin │    plain log files       │
                    │   └──────────────────┘                          │
                    └────────────────────────────────────────────────┘
```

Everything inside the dashed box is one binary: `./fab-bot`. Stages communicate over typed bounded `chan` values (`chan Tick`, `chan Candidate`, `chan Proposal`, `chan Approval`). SQLite holds all durable state in a single file (`bot.db` + WAL/SHM siblings).

### Component responsibilities

| Component | Role | Notes |
|---|---|---|
| **Ingestors** | One goroutine pool per venue. Maintains WS for funding, mark, orderbook top, account events. Heartbeats + auto-reconnect with exponential backoff. | Use venue-native Go SDKs (`go-binance/v2`, `hirokisan/bybit`, etc.) behind a shared `Venue` interface; fall back to `ccxt/go` only for venues without a good native SDK. |
| **Normalizer** | Maps each venue's symbol/precision/funding-interval to a unified `Tick` struct. Writes funding history to SQLite for backtest replay. | Pure-Go transform; ~1 KB allocs per tick; periodic batched insert into `funding_history`. |
| **Opportunity Engine** | Pure function over latest snapshot held in an in-memory map; emits candidates on each tick. | Runs on every funding/mark tick; debounced (e.g. 1s) via `time.Ticker`. |
| **Strategy / Risk Manager** | Stateful. Holds caps and open positions **in RAM**; mirrors to SQLite on every change. The sole goroutine that writes positions. | Single-owner goroutine — no locks needed for hot path, no race conditions on position state. |
| **Telegram Bot** | UX layer. Sends rich proposals with inline buttons (Approve / Reject / Snooze). Sends warnings on degrading positions. | `mymmrac/telego` (modern Go Telegram framework with generics + clean inline-keyboard API). |
| **Execution Router** | Places both legs as close to simultaneously as possible. Handles partial fills, rolls back on failure of one leg. | Critical path — see Execution Semantics below. |
| **Position Store** | Authoritative record of open arbs, entry funding, cumulative funding collected, fees paid, current PnL. | **SQLite** (`bot.db`) in WAL mode. The strategy goroutine is the only writer; readers (Telegram queries, admin endpoint) can read concurrently. |
| **Observability** | `/metrics` endpoint with `prometheus/client_golang` (optional scrape); structured logs (`log/slog`) to stdout / a log file. | No Grafana required — `log/slog` JSON logs + `journalctl` are enough for MVP. Add Prometheus only if you actually run it. |

### Execution Semantics (the dangerous bit)

Opening two legs on two venues is **not** atomic. Mitigations:
1. **Sequence the riskier leg first** (less-liquid venue or smaller venue) — reduces probability you're stuck with naked exposure.
2. **Hedge-first option**: place limit on leg A; once filled, market on leg B. Trades latency for fill certainty.
3. **Abort & unwind**: if leg B fails within N ms, immediately close leg A at market and log the slippage cost.
4. **Pre-checks**: balance, margin, IP whitelist, symbol tradable, exchange-side risk limits — all checked before sending.
5. **Idempotency keys** (`clientOrderId`) on every order to survive transient retries.
6. **Kill switch**: a single Telegram command (`/halt`) sets a flag that blocks all new opens and (optionally) market-closes everything.

---

## Tech Stack

| Layer | Choice | Rationale |
|---|---|---|
| **Language / Runtime** | **Go 1.23+** | Low-latency, low-memory, single static binary. Goroutines/channels map cleanly to "one WS feed per venue + fan-in to engine." Mature ecosystem for exchange SDKs. No GC pauses at trading-relevant scales here. |
| **Project layout** | **Single binary** `cmd/fab-bot` with internal packages: `internal/venue`, `internal/engine`, `internal/strategy`, `internal/telegram`, `internal/executor`, `internal/store` | One module, one binary. Packages give modularity without operational complexity. |
| **Process model** | One **systemd** unit, auto-restart, logs to journald | One process to start, stop, monitor. No orchestrator. |
| **Inter-stage messaging** | **Go channels** (`chan Tick`, `chan Candidate`, `chan Proposal`, `chan Approval`) — bounded, in-process | Replaces Redis entirely. Zero serialization cost. Backpressure is automatic (full channel blocks producer). Trade-off: if the process crashes, in-flight messages are lost — acceptable because no position is opened without an explicit user approval. |
| **Concurrency** | Goroutines per venue feed; **`context.Context`** for cancellation; **`golang.org/x/sync/errgroup`** for supervised goroutine groups inside `main` | Standard idiom. `errgroup` lets one failed stage cleanly tear down all the others. |
| **Exchange connectivity** | Venue-native Go SDKs: **`go-binance/v2`**, **`hirokisan/bybit`**, official OKX SDK, **`Logarithm-Labs/hyperliquid-go-sdk`** | Each is wrapped behind an internal `Venue` interface (see code slide). Avoid `ccxt/go` for the hot path — too generic and lags behind. |
| **WebSockets** | **`coder/websocket`** | Modern context-aware WS lib; successor to `nhooyr.io/websocket`. |
| **Telegram** | **`mymmrac/telego`** | Generics-based, clean inline-keyboard + callback API. |
| **Database** | **SQLite** via **`modernc.org/sqlite`** (pure-Go driver, no CGO) in **WAL mode**, `_journal_mode=WAL&_synchronous=NORMAL&_busy_timeout=5000` | Single file (`bot.db`), no daemon, no install step. WAL gives concurrent readers + one writer — exactly the access pattern here. Pure-Go driver keeps cross-compilation trivial (`GOOS=linux GOARCH=amd64 go build`). Use **`sqlc`** to generate type-safe query code from raw SQL — same workflow you'd use with Postgres. |
| **Pub/sub** | **None.** All decoupling is in-process channels. | If you ever genuinely need durable queues (e.g. retry execution across restarts), revisit with [`river`](https://riverqueue.com) or just a SQLite-backed job table. Don't pre-build it. |
| **Schema validation** | **`go-playground/validator/v10`** for struct tags + custom decoders per venue | Each exchange's JSON shape lives in a typed struct; validator catches missing/changed fields fast. |
| **HTTP layer (admin / metrics)** | Std lib **`net/http`** + **`go-chi/chi/v5`** if routing grows | Just `/metrics`, `/healthz`, and maybe `/positions`. Don't reach for a framework. |
| **Frontend dashboard (optional, later)** | Skip until needed. The Telegram bot is the UI. | If you later want a web view, a single static HTML page reading SQLite via a small Go HTTP handler beats any framework. |
| **Observability** | **`prometheus/client_golang`** exposing `/metrics`; **`log/slog`** (std lib) JSON logs to stdout | View live with `journalctl -fu fab-bot`. Plumb to Prometheus/Grafana only if/when you actually run them. |
| **Decimal math** | **`shopspring/decimal`** — never use `float64` for prices, sizes, or PnL | Float rounding bugs in money math are inevitable; use decimals from day one. |
| **Backtesting** | Go replay harness in the same binary (`./fab-bot replay --from=… --to=…`) reading historical snapshots from SQLite's `funding_history` table | Reuses the live engine code path. Zero external infra. |
| **Secrets** | `.env` via **`joho/godotenv`** loaded at startup; file mode `0600`; exchange keys scoped to **trade-only, no withdrawal** | Withdrawal-disabled keys are the single highest-leverage safety control. |
| **Testing** | Std `testing` + **`stretchr/testify`** for assertions; SQLite tests use an in-memory DB (`:memory:`) or a temp file | No `testcontainers` needed — SQLite has no daemon. Tests stay fast and hermetic. |
| **CI / Deploy** | GitHub Actions: `go test ./... && go build` on amd64 Linux → SCP single binary → `systemctl restart fab-bot` on the VPS | One static binary + `bot.db` next to it. Backup = `cp bot.db bot.db.bak`. Rollback = swap the binary back. |

---

## Data Flow (single opportunity, end-to-end)

```
[T=0]    Binance WS pushes funding tick for BTCUSDT → ingestor goroutine
[T<1ms]  Normalized → sent on `chan Tick` (in-process, zero copy)
[T=2ms]  Engine reads tick; recomputes spread vs cached Bybit funding
         net_edge = (rate_short − rate_long) × ttf − (fee_in + fee_out + slip)
[T=3ms]  net_edge > threshold → sent on `chan Candidate`
[T=4ms]  Strategy checks in-RAM state: no existing position, caps OK, cooldown OK
[T=5ms]  Telegram message sent with [Approve][Reject][Snooze 10m] buttons
[T+...]  User taps Approve  → telego callback → `chan Approval`
[T+50ms] Executor: pre-check balances, place leg A (smaller venue) limit-IoC
[T+150ms]Filled → place leg B market on larger venue
[T+200ms]Both filled → strategy writes position row to SQLite (single INSERT)
                       monitor goroutine spawned
[T+ ∞ ]  Monitor: every funding tick recomputes live edge & delta drift
         When exit condition triggers → Telegram warning → user approves close
```

Note: the channels are bounded (e.g. `chan Tick` with capacity 1024). If the engine ever falls behind, ingestors block — that's the desired backpressure signal. Don't grow the buffer; fix the slow stage.

---

## Risks & Guardrails

- **Funding flip during settlement** — abort entries within configurable window (e.g. last 60s) before funding.
- **Stale WS feed** — heartbeat watchdog; if no tick for X seconds, mark venue stale, halt opens involving it.
- **Withdrawal/transfer needs** — bot only trades with **pre-funded** balances on both venues; rebalancing capital across venues stays manual at MVP (flag-only).
- **Symbol mismatch** — strict allowlist of (venue, symbol) pairs that are confirmed delta-equivalent.
- **API rate limits** — token-bucket per venue in front of every REST call.
- **Single-writer for positions** — only the strategy goroutine writes the SQLite `positions` table. Other components (telegram, admin HTTP) read with `SELECT` only. SQLite WAL guarantees readers never block the writer.
- **Capital cap** — hard ceiling per asset, per venue, and global, enforced in code, not config alone.

---

## Verification

1. **Paper trading mode**: env flag `EXECUTION_MODE=paper` routes Executor to a simulator that uses live data but writes synthetic fills. Run for ≥ 1 week before live.
2. **Single-pair live with $100**: enable one pair (e.g. BTC Binance↔Bybit) at minimum size; observe ≥ 3 full open→funding→close cycles.
3. **Chaos checks**: kill the bot process mid-position — verify on restart it rehydrates open positions from SQLite and resumes monitoring. Test SQLite WAL recovery by power-cycling the VPS during a write.
4. **Funding settlement test**: confirm bot refuses to open within configured pre-settlement window.
5. **Telegram approval timeout test**: send proposal, do not approve — confirm it auto-expires and no order is placed.
6. **Backtest replay**: feed last 90 days of funding history through the engine offline; confirm trigger counts and theoretical PnL match expectations within tolerance.

---

## Open questions for the user

1. **The link** — please share it; I'll fold the specific metrics it surfaces into the **Opportunity Engine** signal definition.
2. **Starting capital & venue list** — which 2-4 exchanges do you already have funded accounts on?
3. **Tradeable universe** — top-10 perps by volume only, or include longer-tail (often higher funding, also higher risk)?
4. **Backtesting expectations** — do you want a built-in backtester from day one, or is paper-trade-live acceptable?
