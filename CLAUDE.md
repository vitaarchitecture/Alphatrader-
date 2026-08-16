# AlphaTrader — Project Memory

## Owner
- Based in UK (BST timezone)
- Non-technical — prefers plain English explanations
- Trading via phone and laptop

## Goal
Short-term algorithmic trading bot running 24/7 on a cloud server.
Target: consistent profitable trades over 2-week paper trading trial,
then switch to live with real capital.

## Current Status
- Bot version: v7 (Minimum Loss Edition)
- Mode: PAPER TRADING (2-week trial started ~August 16 2026)
- Deployed on: Railway (free trial ~$5 credit)
- After 2 weeks: move to DigitalOcean $4/month if profitable

## Infrastructure
- **Cloud server**: Railway (free tier)
- **Broker**: Alpaca (paper trading)
  - Paper API: https://paper-api.alpaca.markets
  - Data API: https://data.alpaca.markets
  - Websocket: wss://stream.data.alpaca.markets/v2/iex
- **Alerts**: Telegram bot (@Vitading_bot)
- **GitHub repo**: vitaarchitecture/Alphatrader-

## Bot File
- Main file: `trading_bot.py` (currently v7)
- Requirements: `requests==2.31.0`, `websocket-client==1.7.0`
- Procfile: `worker: python trading_bot.py`
- railway.json: restart on failure, max 10 retries

## Railway Environment Variables (9 total)
- ALPACA_KEY_ID
- ALPACA_SECRET_KEY
- TELEGRAM_BOT_TOKEN
- TELEGRAM_CHAT_ID
- PAPER_TRADING = true
- ANTHROPIC_API_KEY
- POLYGON_API_KEY
- FMP_API_KEY
- AV_API_KEY

## Symbols Traded (13 total, 12 tradeable)
Tech: NVDA, AMD, AAPL, MSFT, GOOGL, META, AMZN, TSLA, NFLX
ETF (market filter only): SPY
Non-correlated: GLD (gold), TLT (bonds), USO (oil)

## Trading Parameters
- Base trade size: $200
- Take profit: +2.0%
- Stop loss: -0.9% (user chose NOT to tighten to -0.6%)
- Time stop: 120 minutes
- Max positions: 3
- Daily loss limit: $50
- Portfolio hard stop: -10% from starting value

## Entry Criteria (must hit both mandatory + 2 of remaining 4)
Mandatory (both required):
  - RSI 38-58 (weight 25)
  - MACD histogram positive (weight 25)
Confirmers (need 2+):
  - Price above VWAP (weight 20)
  - EMA9 > EMA21 (weight 15)
  - Stochastic < 78 (weight 15)
  - Volume confirmed (weight 10)
Minimum score: 70 points

## Exit Rules
- Take profit: +2.0%
- Stop loss: -0.9%
- Time stop: 2 hours
- Trailing stop: activates at +1.5%, trails by 0.5%
- Trim: consider at +1.3%
- EOD close: 3:45pm ET (no overnight positions)

## Active Protections (v7)
- Portfolio hard stop at -10%
- 50MA filter (only trade above 50-period moving average)
- Sector surge check (skip if same sector already moved 0.8%+)
- Trade window: 10:15am-3:15pm ET only (avoids first/last 45 mins)
- Momentum filter: stock must be up 0.3%+ from open
- Consecutive loss protection: size drops to $100 after 2 losses in a row
- SPY trend filter: no tech longs if SPY down more than -0.3%
- VIX pause: above 25
- Fear & Greed pause: below 20
- Earnings blackout: 3 days before earnings
- Economic event caution: halves size on Fed/CPI days
- Pre-market gap skip: if gap > 2%
- No overnight positions
- No averaging down
- Max 2 positions per sector

## Data Sources
- Alpaca: price feed (websocket), news, bars
- Polygon.io: earnings dates, better news, pre-market snapshots
- FMP: economic calendar, earnings calendar
- Alpha Vantage: Fed rate, CPI data
- CNN: Fear & Greed index

## Signal Flow
1. Websocket tick received
2. Price history updated
3. Check all entry criteria
4. If BUY signal: run Claude sentiment analysis on latest news
5. If sentiment not negative: execute limit order with bracket (stop + target)
6. Monitor every tick for trailing stop / exit conditions
7. Close all positions by 3:45pm ET

## Key Decisions Made
- Kept stop loss at -0.9% (not tightened to -0.6% — too much noise)
- Paper trading 2 weeks before going live
- No overnight positions — too much gap risk
- No IBKR (requires local gateway) — using Alpaca for stocks
- No Binance (blocks UK/US) — Kraken for future crypto addition
- Max 3 positions simultaneously
- Dynamic sizing: $300 high confidence / $200 standard / $100 low
- NVDA earnings blackout: August 26 2026

## Broker Notes
- Alpaca: free, US stocks only, paper + live, bracket orders supported
- IBKR: international stocks but requires local Java gateway — future option
- Kraken: planned for weekend crypto trading (0.26% fees)
- Coinbase: too expensive (0.6%) — avoid

## User Preferences
- Wants to understand every decision — explain in plain English
- Prefers Telegram alerts over dashboard monitoring
- Wants to be informed but not bottleneck — fully automated
- Willing to pay $4/month DigitalOcean after trial if profitable
- Target return: realistic 2-4% in 2 weeks on paper, not 10%
- Risk tolerance: small account, fee-conscious, no overnight risk

## Next Steps
1. Monitor paper trading results from Aug 17 2026 (market hours 2:30-9pm UK)
2. Check daily Telegram summary each evening
3. After 2 weeks: review results together, decide on live trading
4. If going live: regenerate Alpaca keys (live), move to DigitalOcean
5. Future: add Kraken for weekend crypto trading
