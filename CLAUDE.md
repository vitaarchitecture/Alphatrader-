# AlphaTrader — Project Memory
*Last updated: August 21, 2026*

## Owner
Ben (UK, BST timezone, non-technical background). Prefers plain English explanations throughout. Building a fully automated 24/7 algorithmic trading bot for US stocks and cryptocurrency.

## Goal
Run paper trading for a validation period before deploying real capital (planned: £200 initial stake, scaling if profitable). Primary goal: stable, 24/7 bot that executes a rules-based strategy reliably, collects data, and improves systematically.

---

## Current State

**Bot version: v20** (deployed on Railway, running, paper trading)
**Paper trading start: August 18, 2026** (first clean run after plumbing rebuilt)
**Two-week validation target: September 1, 2026**
**Railway trial: ~$4.95 credit remaining (expires ~mid-September)**

---

## Infrastructure

| Component | Detail |
|---|---|
| Cloud server | Railway worker service (free trial) |
| Broker | Alpaca paper trading |
| Repo | vitaarchitecture/Alphatrader- (main branch) |
| File | trading_bot.py (Procfile: `worker: python trading_bot.py`) |
| Requirements | requests==2.31.0, websocket-client==1.7.0 |
| Alerts | Telegram bot: Vitrading (@Vitading_bot) |

### Railway Environment Variables (9 required)
```
ALPACA_KEY_ID
ALPACA_SECRET_KEY
TELEGRAM_BOT_TOKEN
TELEGRAM_CHAT_ID
PAPER_TRADING=true
ANTHROPIC_API_KEY
POLYGON_API_KEY
FMP_API_KEY
AV_API_KEY
```

---

## Trading Strategy

### Entry Signals
- **Mandatory (both required):** RSI 38-58 (weight 25) AND MACD histogram positive (weight 25)
- **Confirmers (need 2+ of 4):** Above VWAP (w20), EMA9 over EMA21 (w15), Stoch under 78 (w15), Volume confirmed (w10)
- **Minimum score:** 70 points
- **Signal cooldown:** 300 seconds per symbol

### Indicators
- All computed on **1-minute OHLC bars** (not raw ticks — tick-RSI was degenerate)
- Bars backfilled via REST at startup (100 bars per symbol — signals from minute one)
- Volume from bar volumes not tick sizes

### Exit Rules
- **Take profit:** ATR-derived per symbol (+0.8% to +4.0% range)
- **Stop loss:** ATR-derived per symbol (-0.4% to -2.0% range)
- **Risk per trade:** ~$0.25 constant regardless of symbol
- **Trailing stop:** activates at +1.5%, trails by 0.5%
- **Time stop:** 2 hours CONDITIONAL — only cuts if P&L < +0.4%; hard ceiling at 4 hours
- **EOD close:** 3:45pm ET stocks only (crypto exempt), fires exactly once per day

### Position Sizing
- **Notional fractional orders** — buys $X of a stock, any fraction
- Fallback to whole shares if notional rejected (whole-share fallback refuses symbols where 1 share > 3× risk budget)
- Base sizing: ~$28 (from ATR, scales with score and consecutive losses)
- After 2 consecutive losses: size halved

### Stocks (24 symbols, 23 tradeable)
Semiconductors: NVDA, AMD, SMCI, MU, INTC
Big tech: AAPL, MSFT, GOOGL, META, AMZN
Software: PLTR
EV: TSLA
Streaming: NFLX, DIS
Banks: JPM, BAC, GS
Pharma: LLY
Energy: XOM, CVX
Market filter only: SPY
Hedges: GLD, TLT, USO

**Symbol groups for A/B tracking:**
- HIGHVOL_GROUP: SMCI, PLTR, TSLA, MU
- MEGA_GROUP: AAPL, MSFT, GOOGL, META, AMZN, NVDA, NFLX

### Crypto (paused — entries off, exits still managed)
- BTC/USD, ETH/USD via Alpaca
- MAX_CRYPTO_POSITIONS = 0 currently
- Reason: data feed was degenerate (30s snapshot RSI pinned to 100)
- Fix: 1-minute bars now in place — re-enable when validated
- Parameters when re-enabled: TP +3.0%, SL -1.5%, time stop 4h

### Protections
- Portfolio hard stop: -10% from starting value (halts all trading)
- Daily loss limit: $10
- VIX pause: above 25
- Fear & Greed pause: below 20
- Earnings blackout: 3 days before earnings
- SPY trend filter: no tech longs if SPY below worse of open or prior close by >0.3%
- 50MA filter: only trade above 50-period moving average
- Sector surge check: skip if same-sector stock already moved 0.8%+ today
- Momentum filter: stock must be up 0.05%+ from open
- Pre-market gap skip: if gap >2%
- No shorts (long only)
- No averaging down
- No overnight stock positions

### Slot Allocation
- Max 6 positions total: 4 stocks, 2 crypto (crypto currently 0)
- Trading window: 10:15am–3:15pm ET (avoids first/last 45 mins of session)

---

## Architecture (v16+ — Robust Core)

### Key design principles
1. **Zero resting orders** — no GTC stops/targets at broker ever. All exits managed in software from live prices. Any open order found at startup is cancelled as foreign.
2. **Single exit path** — `close_position()` only. Verifies broker position before selling. Selling a flat position is structurally impossible.
3. **Idempotent entries** — deterministic `client_order_id` per signal window. Duplicate buys blocked locally (in_flight set) and at broker (422 on duplicate coid).
4. **60-second reconciler** — diffs internal state against Alpaca every minute: adopts unknown longs, purges vanished positions, corrects qty drift, auto-flattens shorts.
5. **Short auto-flatten** — if a short is detected: cancel sell-side strays only, place GTC buy-back (not DAY — works when market closed), verify fill, confirm once. 10-minute alert cooldown to prevent spam.
6. **Fill verification** — every entry polls for actual filled qty/price. Records real data not estimates.
7. **EOD fires exactly once** per calendar day.

### Telegram commands (send to @Vitading_bot)
- `/balance` — equity, cash, buying power, today's P&L vs start
- `/positions` — all open trades with live P&L and ATR exits
- `/summary` — today's completed trades
- `/status` — bot state, VIX, Fear & Greed, slots used
- `/help` — command list

---

## Version History

| Version | Key change |
|---|---|
| v1-v6 | Early builds, React artifact → Python, v6 added all 4 data APIs |
| v7 | Minimum Loss Edition: portfolio hard stop, 50MA, sector surge, consecutive loss sizing |
| v8 | Order architecture fix: bracket orders → separate market buy + stops |
| v9 | Sentiment crash fix (score returned as string), momentum loosened |
| v10 | Wide market: 28 symbols across 8 sectors |
| v11 | Crypto added (BTC/ETH), fixed exits, conditional time stop |
| v12 | Sizing back to $20, crypto paused, fill reconciliation |
| v13 | Stop loss reliability: retry x3, real entry timestamps, re-arm on startup |
| v14 | Crypto symbol matching (BTCUSD vs BTC/USD), short position refusal, orphan order cleanup |
| v15 | Telegram HTML fix (Stoch<78 and EMA9>EMA21 broke parse_mode) |
| v16 | **Full rebuild** — robust core: zero resting orders, single exit path, idempotent orders, reconciler, verified fills. 19/19 tests. |
| v17 | Notional fractional sizing, blocked-signal telemetry, volatility A/B groups, advisory sentiment, real VIX via FMP, daily reset fix |
| v18 | 1-minute bars for all indicators, ATR-scaled exits, constant-risk sizing, crypto re-enabled on bars, Telegram commands |
| v19 | Short flatten fix (GTC not DAY, alert cooldown) — SUPERSEDED by v20 |
| v20 | **Current** — audited flatten: existing queued buy preserved, sell-side only cancellation, closed-market confirmation path, dust position guard. 52/52 tests. |

---

## Bugs Fixed (Key Learnings)

1. **Alpaca paper rejects bracket orders** — split into market buy + separate orders (v8)
2. **Sentiment score as string** crashed buys with TypeError (v9)
3. **Momentum 0.3% threshold** blocked nearly all trades — loosened to 0.05% (v9)
4. **Crypto blocked by stock trading-window guards** — crypto must skip stock-only filters (v11, v14)
5. **Websocket symbol limit** — Alpaca paper caps at 30; quotes subscription doubled count; dropped quotes, trades only (v11)
6. **BASE_TRADE_SIZE accidentally 200 not 20** — lost in v7 rebuild, recurred multiple times (v12, v17)
7. **Bracket/GTC orders creating accidental SHORTS** — orphaned sell orders fired into flat positions (SMCI -8, XOM -1). Root cause: resting GTC orders. Fix: zero resting orders ever (v16)
8. **Duplicate buys 1 second apart** — in_flight guard + idempotent client_order_id (v16)
9. **EOD close spamming** — loop re-firing, fixed with date-gate (v16)
10. **SMCI held overnight** — entry_time reset on redeploy so time stop never fired; fix: recover real fill timestamp from orders API (v13/v16)
11. **Crypto RSI degenerate** — 30s snapshots of flat price pin RSI to 100; fix: 1-minute OHLC bars (v18)
12. **Telegram alerts missing** — Stoch<78 and EMA9>EMA21 in criteria broke HTML parse_mode, API returned 400s (v15)
13. **Short flatten loop** — v19's GTC flatten order was cancelled by wait_for_fill timeout, then re-placed with same coid causing 422s. Fix: preserve queued orders, no wait when market closed (v20)
14. **GOOGL whole-share sizing** — notional order failing silently for ~$344 stock, falling back to 1 share = $344 position vs intended $28 (known, unfixed in v20, v21 needed)

---

## Data Sources

| Source | Use | Key |
|---|---|---|
| Alpaca | Price feed (websocket + bars), trade execution | ALPACA_KEY_ID / SECRET |
| Polygon.io | Earnings dates, news, pre-market | POLYGON_API_KEY |
| FMP | Economic calendar, earnings, VIX level | FMP_API_KEY |
| Alpha Vantage | Economic indicators | AV_API_KEY |
| Anthropic | Advisory sentiment (async, non-blocking) | ANTHROPIC_API_KEY |
| CNN | Fear & Greed index | No key needed |

---

## Live Trading Results (Paper)

### Day 1 — August 20, 2026
- Trades: 13 | Wins: 7 (54%) | P&L: $+0.25
- Exits: TP:0 SL:1 Time:9 EOD:2 Trail:1
- Groups: highvol $+1.71 | mega $-1.51 | other $+0.05
- Top: SMCI $+1.37 | Worst: GOOGL $-1.51 (whole-share sizing bug)
- Blocked: no_momentum:208, outside_window:69, sector_held:58, spy_down:46
- Key finding: GOOGL 1 whole share at $344 (notional failed), not $28 fractional

### Day 2 — August 21, 2026
- Trades: 9 | Wins: 6 (67%) | P&L: $+0.29
- Exits: TP:0 SL:0 Time:6 EOD:3 Trail:0
- Groups: highvol $+0.12 | other $+0.17
- Top: TSLA $+0.27 | Worst: USO $-0.25
- Blocked: no_momentum:234, outside_window:101, below_50ma:64, sector_surge:45
- Key finding: EOD exits consistently green (+$0.38); time stops consistently flat/small loss

### 2-Day Running Total
- Trades: 22 | Wins: 13 (59%) | P&L: $+0.54
- Take profits hit: 0 out of 22 (targets still unreachable in 2h window)
- Time stops: 15 out of 22 exits (68%)
- **Hypothesis forming: 2-hour time stop is cutting winners before they develop**

---

## Key Decisions Made

- Kept stop loss at -0.9% initially (user explicitly rejected tightening to -0.6%)
- ATR exits replace fixed percentages — each symbol gets stops/targets from its own volatility
- Conditional time stop: only cuts at 2h if P&L < +0.4%; hard ceiling at 4h
- No overnight stock positions (gap risk unmanaged)
- No shorts, no averaging down, no leverage
- Paper trade 2 weeks minimum before live capital
- £200 initial live stake (requires fractional sizing to work)
- No IBKR (requires local Java gateway) — Alpaca for everything
- Crypto paused until validated on 1-minute bars

---

## Pending Improvements (Prioritised)

1. **Fix GOOGL notional bug** — investigate why ~$344 stocks fall back to whole share
2. **Remove or deprioritise mega-caps** — A/B data after 2 days shows highvol beats mega consistently
3. **Extend time stop for profitable positions** — consider 4h or EOD-only for positions above +0.4%
4. **Rolling momentum** — measure vs 15-min rolling window, not day's open (penalises recovered names)
5. **Re-enable crypto** — 1-minute bars now in place, validation needed
6. **Dynamic watchlist** — Polygon top movers daily (agreed as post-validation upgrade)

---

## Live Capital Plan (when ready)

- Starting capital: £200
- Max positions: 2-3 (not 6)
- Trade size: £20 (already calibrated)
- Daily loss limit: £10
- Platform: Switch from Railway to DigitalOcean ($4/mo) if profitable
- Realistic 8-week return expectation: 10-30% if strategy has edge (£20-60)
- Do NOT put £3,000 in until 8 weeks of validated paper data at 55%+ win rate

---

## How to Continue in a New Conversation

1. Paste this entire CLAUDE.md at the start of a new chat
2. I will immediately know the full context
3. For trading updates: paste the daily Telegram summary
4. For code changes: always increment the version number
5. Use Claude Projects (top left of claude.ai) to keep conversation history searchable

## What NOT to do
- Do not change bot parameters more than once per week (need stable data)
- Do not go live before 2 weeks of paper data shows consistent 50%+ win rate
- Do not scale to £3,000 before 8 weeks of validation
- Do not skip paper trading period regardless of results looking promising early
