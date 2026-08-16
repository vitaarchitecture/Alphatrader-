"""
AlphaTrader Bot — Paper Trading Edition
Deploy to Railway: connect GitHub repo, set environment variables, done.

Environment variables to set in Railway:
  ALPACA_KEY_ID       — your Alpaca paper trading API key
  ALPACA_SECRET_KEY   — your Alpaca paper trading secret key
  TELEGRAM_BOT_TOKEN  — from @BotFather on Telegram
  TELEGRAM_CHAT_ID    — your Telegram chat ID (message @userinfobot)
  PAPER_TRADING       — set to "true" (always, for now)
"""

import os
import time
import math
import logging
import requests
from datetime import datetime, timezone
from collections import deque

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("alphatrader")

# ── Config from environment ───────────────────────────────────────────────────
ALPACA_KEY    = os.environ.get("ALPACA_KEY_ID", "")
ALPACA_SECRET = os.environ.get("ALPACA_SECRET_KEY", "")
TG_TOKEN      = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TG_CHAT_ID    = os.environ.get("TELEGRAM_CHAT_ID", "")
IS_PAPER      = os.environ.get("PAPER_TRADING", "true").lower() == "true"

ALPACA_BASE   = "https://paper-api.alpaca.markets" if IS_PAPER else "https://api.alpaca.markets"
ALPACA_DATA   = "https://data.alpaca.markets"

# ── Trading parameters ────────────────────────────────────────────────────────
SYMBOLS        = ["NVDA", "AAPL", "SPY"]   # US stocks via Alpaca
ANALYSIS_INTERVAL = 300                     # seconds between analyses (5 min)
TRADE_SIZE_USD    = 200                     # dollars per trade
TAKE_PROFIT_PCT   = 2.0                     # % gain to exit
STOP_LOSS_PCT     = 0.9                     # % loss to exit
TIME_STOP_MINS    = 120                     # minutes before time-stop exit
MIN_CRITERIA      = 3                       # of 5 criteria needed to buy
MIN_CONFIDENCE    = 60                      # minimum score to buy

# Market hours (NYSE) — no overnight positions
MARKET_OPEN_HOUR  = 9
MARKET_OPEN_MIN   = 30
MARKET_CLOSE_HOUR = 15
MARKET_CLOSE_MIN  = 45   # close all by 3:45pm to avoid end-of-day noise

# Price history buffer per symbol
price_history = {sym: deque(maxlen=150) for sym in SYMBOLS}

# Active position tracking
positions = {}   # sym -> { entry_price, qty, entry_time, cost, order_id }
session_trades = []
session_start  = datetime.now(timezone.utc)

# ── Alpaca helpers ────────────────────────────────────────────────────────────
def alpaca_get(path):
    r = requests.get(
        f"{ALPACA_BASE}{path}",
        headers={"APCA-API-KEY-ID": ALPACA_KEY, "APCA-API-SECRET-KEY": ALPACA_SECRET},
        timeout=10,
    )
    r.raise_for_status()
    return r.json()

def alpaca_post(path, body):
    r = requests.post(
        f"{ALPACA_BASE}{path}",
        headers={"APCA-API-KEY-ID": ALPACA_KEY, "APCA-API-SECRET-KEY": ALPACA_SECRET,
                 "Content-Type": "application/json"},
        json=body,
        timeout=10,
    )
    r.raise_for_status()
    return r.json()

def alpaca_delete(path):
    r = requests.delete(
        f"{ALPACA_BASE}{path}",
        headers={"APCA-API-KEY-ID": ALPACA_KEY, "APCA-API-SECRET-KEY": ALPACA_SECRET},
        timeout=10,
    )
    return r.status_code

def get_latest_price(symbol):
    try:
        r = requests.get(
            f"{ALPACA_DATA}/v2/stocks/{symbol}/trades/latest",
            headers={"APCA-API-KEY-ID": ALPACA_KEY, "APCA-API-SECRET-KEY": ALPACA_SECRET},
            timeout=10,
        )
        r.raise_for_status()
        return float(r.json()["trade"]["p"])
    except Exception as e:
        log.warning(f"Price fetch failed for {symbol}: {e}")
        return None

def get_account():
    try:
        return alpaca_get("/v2/account")
    except Exception as e:
        log.error(f"Account fetch failed: {e}")
        return None

# ── Telegram ──────────────────────────────────────────────────────────────────
def telegram(msg):
    if not TG_TOKEN or not TG_CHAT_ID:
        log.info(f"[TELEGRAM] {msg}")
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
            json={"chat_id": TG_CHAT_ID, "text": msg, "parse_mode": "HTML"},
            timeout=10,
        )
    except Exception as e:
        log.warning(f"Telegram send failed: {e}")

# ── Technical indicators ──────────────────────────────────────────────────────
def calc_ema(prices, n):
    prices = list(prices)
    if len(prices) < n:
        return None
    k = 2 / (n + 1)
    e = sum(prices[:n]) / n
    for p in prices[n:]:
        e = p * k + e * (1 - k)
    return e

def calc_rsi(prices, n=14):
    prices = list(prices)
    if len(prices) < n + 1:
        return None
    sl = prices[-(n + 1):]
    gains = sum(max(sl[i] - sl[i-1], 0) for i in range(1, len(sl)))
    losses = sum(max(sl[i-1] - sl[i], 0) for i in range(1, len(sl)))
    ag, al = gains / n, losses / n
    return 100 if al == 0 else 100 - 100 / (1 + ag / al)

def calc_macd(prices):
    prices = list(prices)
    if len(prices) < 26:
        return None
    hist = []
    for i in range(26, len(prices) + 1):
        e12 = calc_ema(prices[:i], 12)
        e26 = calc_ema(prices[:i], 26)
        if e12 and e26:
            hist.append(e12 - e26)
    if len(hist) < 9:
        return None
    sig = calc_ema(hist, 9)
    line = hist[-1]
    return {"line": line, "signal": sig, "histogram": line - sig if sig else None}

def calc_bollinger(prices, n=20):
    prices = list(prices)
    if len(prices) < n:
        return None
    sl = prices[-n:]
    sma = sum(sl) / n
    std = math.sqrt(sum((p - sma) ** 2 for p in sl) / n)
    return {"upper": sma + 2 * std, "middle": sma, "lower": sma - 2 * std}

def calc_stoch(prices, n=14):
    prices = list(prices)
    if len(prices) < n:
        return None
    sl = prices[-n:]
    lo, hi = min(sl), max(sl)
    return 50 if hi == lo else ((prices[-1] - lo) / (hi - lo)) * 100

def calc_vwap(prices):
    prices = list(prices)
    return sum(prices) / len(prices) if prices else None

def calc_ema9_ema21(prices):
    e9  = calc_ema(prices, min(9,  len(list(prices))))
    e21 = calc_ema(prices, min(21, len(list(prices))))
    return e9, e21

# ── Entry signal evaluator ────────────────────────────────────────────────────
def evaluate_entry(symbol):
    hist = price_history[symbol]
    if len(hist) < 30:
        return None

    price = hist[-1]
    r     = calc_rsi(hist)
    m     = calc_macd(hist)
    bb    = calc_bollinger(hist)
    st    = calc_stoch(hist)
    vw    = calc_vwap(hist)
    e9, e21 = calc_ema9_ema21(hist)

    pvwap = ((price - vw) / vw * 100) if vw else None
    bb_pct = ((price - bb["lower"]) / (bb["upper"] - bb["lower"]) * 100) if bb else None

    criteria = [
        ("RSI 38-58",           r  is not None and 38 <= r  <= 58,  25),
        ("MACD histogram +ve",  m  is not None and m.get("histogram") and m["histogram"] > 0, 25),
        ("Price above VWAP",    pvwap is not None and pvwap > 0,    20),
        ("EMA9 > EMA21",        e9 and e21 and e9 > e21,            15),
        ("Stochastic < 78",     st is not None and st < 78,         15),
    ]

    met      = [(name, w) for name, passed, w in criteria if passed]
    met_count = len(met)
    score    = sum(w for _, w in met)
    signal   = "BUY" if met_count >= MIN_CRITERIA and score >= MIN_CONFIDENCE else "WAIT"

    return {
        "symbol": symbol, "price": price, "signal": signal,
        "met_count": met_count, "score": score,
        "met": [n for n, _ in met],
        "rsi": r, "macd_hist": m["histogram"] if m else None,
        "bb_pct": bb_pct, "stoch": st, "pvwap": pvwap,
        "e9": e9, "e21": e21,
    }

# ── Exit check ────────────────────────────────────────────────────────────────
def check_exit(symbol, current_price):
    pos = positions.get(symbol)
    if not pos:
        return None
    pnl_pct  = (current_price - pos["entry_price"]) / pos["entry_price"] * 100
    mins_held = (time.time() - pos["entry_time"]) / 60

    if pnl_pct >= TAKE_PROFIT_PCT:
        return ("TAKE_PROFIT", f"+{pnl_pct:.2f}% — take profit hit 🟢")
    if pnl_pct <= -STOP_LOSS_PCT:
        return ("STOP_LOSS",   f"{pnl_pct:.2f}% — stop loss hit 🔴")
    if mins_held >= TIME_STOP_MINS:
        return ("TIME_STOP",   f"{pnl_pct:.2f}% after {mins_held:.0f}m — time stop ⏱")
    return None

# ── Market hours check ────────────────────────────────────────────────────────
def is_market_hours():
    now = datetime.now(timezone.utc)
    # NYSE is UTC-4 (EDT) or UTC-5 (EST) — use UTC-4 for summer
    ny_hour = (now.hour - 4) % 24
    ny_min  = now.minute
    ny_dow  = now.weekday()  # 0=Mon, 6=Sun

    if ny_dow >= 5:  # Weekend
        return False

    after_open  = (ny_hour > MARKET_OPEN_HOUR) or \
                  (ny_hour == MARKET_OPEN_HOUR and ny_min >= MARKET_OPEN_MIN)
    before_close = (ny_hour < MARKET_CLOSE_HOUR) or \
                   (ny_hour == MARKET_CLOSE_HOUR and ny_min <= MARKET_CLOSE_MIN)
    return after_open and before_close

def should_close_all():
    """Returns True in last 15 mins of trading day."""
    now = datetime.now(timezone.utc)
    ny_hour = (now.hour - 4) % 24
    ny_min  = now.minute
    ny_dow  = now.weekday()
    if ny_dow >= 5:
        return False
    return ny_hour == 15 and ny_min >= 45

# ── Execute buy ───────────────────────────────────────────────────────────────
def execute_buy(analysis):
    symbol = analysis["symbol"]
    price  = analysis["price"]

    if symbol in positions:
        log.info(f"{symbol}: already in position, skip buy")
        return

    acct = get_account()
    if not acct:
        return

    buying_power = float(acct.get("buying_power", 0))
    trade_value  = min(TRADE_SIZE_USD, buying_power * 0.95)

    if trade_value < 10:
        log.warning(f"{symbol}: insufficient buying power (${buying_power:.2f})")
        telegram(f"⚠️ <b>Insufficient buying power</b> — ${buying_power:.2f} available")
        return

    qty = trade_value / price

    try:
        # Bracket order: entry + stop-loss + take-profit in one atomic order
        # This lives on Alpaca's servers — survives bot crashes
        stop_price   = round(price * (1 - STOP_LOSS_PCT / 100), 2)
        target_price = round(price * (1 + TAKE_PROFIT_PCT / 100), 2)

        order = alpaca_post("/v2/orders", {
            "symbol":        symbol,
            "qty":           f"{qty:.4f}",
            "side":          "buy",
            "type":          "market",
            "time_in_force": "day",
            "order_class":   "bracket",
            "stop_loss":     {"stop_price": str(stop_price)},
            "take_profit":   {"limit_price": str(target_price)},
        })

        positions[symbol] = {
            "entry_price": price,
            "qty":         qty,
            "entry_time":  time.time(),
            "cost":        trade_value,
            "order_id":    order.get("id", ""),
            "stop":        stop_price,
            "target":      target_price,
        }

        criteria_str = " · ".join(analysis["met"])
        msg = (
            f"📥 <b>BUY {symbol}</b> {'(PAPER)' if IS_PAPER else '(LIVE)'}\n"
            f"Price: ${price:.2f} | Size: ${trade_value:.0f}\n"
            f"Stop: ${stop_price} | Target: ${target_price}\n"
            f"Signal: {analysis['met_count']}/5 criteria ({analysis['score']}pts)\n"
            f"✅ {criteria_str}\n"
            f"🛡 Bracket order active on Alpaca — protected even if bot restarts"
        )
        log.info(msg.replace("\n", " | "))
        telegram(msg)

    except Exception as e:
        log.error(f"Buy order failed for {symbol}: {e}")
        telegram(f"❌ <b>BUY failed</b> {symbol}: {str(e)[:100]}")

# ── Execute sell ──────────────────────────────────────────────────────────────
def execute_sell(symbol, reason, current_price):
    pos = positions.get(symbol)
    if not pos:
        return

    try:
        # Cancel any remaining bracket legs first
        try:
            alpaca_delete(f"/v2/orders/{pos['order_id']}")
        except Exception:
            pass  # might already be filled/cancelled

        order = alpaca_post("/v2/orders", {
            "symbol":        symbol,
            "qty":           f"{pos['qty']:.4f}",
            "side":          "sell",
            "type":          "market",
            "time_in_force": "day",
        })

        pnl_pct = (current_price - pos["entry_price"]) / pos["entry_price"] * 100
        pnl_abs = (current_price - pos["entry_price"]) * pos["qty"]

        trade_record = {
            "symbol":      symbol,
            "entry":       pos["entry_price"],
            "exit":        current_price,
            "qty":         pos["qty"],
            "pnl_pct":     pnl_pct,
            "pnl_abs":     pnl_abs,
            "reason":      reason,
            "held_mins":   (time.time() - pos["entry_time"]) / 60,
        }
        session_trades.append(trade_record)
        del positions[symbol]

        emoji = "🟢" if pnl_abs >= 0 else "🔴"
        msg = (
            f"{emoji} <b>SELL {symbol}</b> {'(PAPER)' if IS_PAPER else '(LIVE)'}\n"
            f"Entry: ${pos['entry_price']:.2f} → Exit: ${current_price:.2f}\n"
            f"P&L: {'+' if pnl_pct >= 0 else ''}{pnl_pct:.2f}% (${pnl_abs:+.2f})\n"
            f"Reason: {reason}\n"
            f"Held: {trade_record['held_mins']:.0f} minutes"
        )
        log.info(msg.replace("\n", " | "))
        telegram(msg)

    except Exception as e:
        log.error(f"Sell failed for {symbol}: {e}")
        telegram(f"❌ <b>SELL failed</b> {symbol}: {str(e)[:100]}")

# ── Daily summary ─────────────────────────────────────────────────────────────
def send_daily_summary():
    if not session_trades:
        telegram("📊 <b>Daily Summary</b>\nNo trades today.")
        return

    total_pnl  = sum(t["pnl_abs"] for t in session_trades)
    wins       = sum(1 for t in session_trades if t["pnl_abs"] > 0)
    win_rate   = wins / len(session_trades) * 100
    best       = max(session_trades, key=lambda t: t["pnl_pct"])
    worst      = min(session_trades, key=lambda t: t["pnl_pct"])

    lines = [
        f"📊 <b>Daily Summary</b> {'PAPER' if IS_PAPER else 'LIVE'}",
        f"Trades: {len(session_trades)} | Win rate: {win_rate:.0f}%",
        f"Total P&L: {'+'if total_pnl >= 0 else''}{total_pnl:.2f}",
        f"Best: {best['symbol']} +{best['pnl_pct']:.2f}%",
        f"Worst: {worst['symbol']} {worst['pnl_pct']:.2f}%",
    ]
    telegram("\n".join(lines))

# ── Startup checks ────────────────────────────────────────────────────────────
def startup():
    log.info("AlphaTrader Bot starting up...")

    if not ALPACA_KEY or not ALPACA_SECRET:
        log.error("ALPACA_KEY_ID and ALPACA_SECRET_KEY must be set")
        telegram("❌ Bot failed to start: missing Alpaca keys")
        raise SystemExit(1)

    acct = get_account()
    if not acct:
        log.error("Cannot connect to Alpaca")
        raise SystemExit(1)

    buying_power   = float(acct.get("buying_power", 0))
    portfolio_val  = float(acct.get("portfolio_value", 0))

    # Check for any existing open positions
    try:
        open_pos = alpaca_get("/v2/positions")
        for p in open_pos:
            sym = p["symbol"]
            if sym in SYMBOLS:
                positions[sym] = {
                    "entry_price": float(p["avg_entry_price"]),
                    "qty":         float(p["qty"]),
                    "entry_time":  time.time() - 1800,  # assume 30min ago
                    "cost":        float(p["cost_basis"]),
                    "order_id":    "",
                    "stop":        float(p["avg_entry_price"]) * (1 - STOP_LOSS_PCT / 100),
                    "target":      float(p["avg_entry_price"]) * (1 + TAKE_PROFIT_PCT / 100),
                }
                log.info(f"Existing position found: {sym}")
    except Exception as e:
        log.warning(f"Could not fetch existing positions: {e}")

    mode = "📄 PAPER TRADING" if IS_PAPER else "💰 LIVE TRADING"
    msg = (
        f"🚀 <b>AlphaTrader Bot Started</b>\n"
        f"Mode: {mode}\n"
        f"Symbols: {', '.join(SYMBOLS)}\n"
        f"Buying power: ${buying_power:.2f}\n"
        f"Portfolio: ${portfolio_val:.2f}\n"
        f"Trade size: ${TRADE_SIZE_USD}\n"
        f"Take profit: +{TAKE_PROFIT_PCT}% | Stop loss: -{STOP_LOSS_PCT}%\n"
        f"Analysis every: {ANALYSIS_INTERVAL//60} minutes\n"
        f"⚠️ Will not hold positions overnight"
    )
    log.info(msg.replace("\n", " | "))
    telegram(msg)

# ── Main loop ─────────────────────────────────────────────────────────────────
def main():
    startup()

    last_analysis   = 0
    last_day_summary = None
    price_tick      = 0

    telegram(f"👀 Watching {', '.join(SYMBOLS)} — analysis every 5 minutes")

    while True:
        try:
            now = datetime.now(timezone.utc)
            today = now.date()

            # ── Daily summary at market close ────────────────────────────────
            ny_hour = (now.hour - 4) % 24
            ny_min  = now.minute
            if ny_hour == 16 and ny_min < 5 and last_day_summary != today:
                send_daily_summary()
                last_day_summary = today

            # ── Close all positions before EOD ───────────────────────────────
            if should_close_all() and positions:
                log.info("Approaching market close — closing all positions")
                telegram("⏰ <b>Market close approaching</b> — closing all positions")
                for sym in list(positions.keys()):
                    price = get_latest_price(sym)
                    if price:
                        execute_sell(sym, "EOD close — no overnight positions", price)

            # ── Price refresh every 30s ──────────────────────────────────────
            price_tick += 1
            if price_tick % 3 == 0 and is_market_hours():
                for sym in SYMBOLS:
                    p = get_latest_price(sym)
                    if p:
                        price_history[sym].append(p)

                # Check exits on open positions
                for sym in list(positions.keys()):
                    current = price_history[sym][-1] if price_history[sym] else None
                    if not current:
                        continue
                    result = check_exit(sym, current)
                    if result:
                        action, reason = result
                        execute_sell(sym, reason, current)

            # ── Full analysis every 5 minutes ────────────────────────────────
            if time.time() - last_analysis >= ANALYSIS_INTERVAL:
                last_analysis = time.time()

                if not is_market_hours():
                    log.info("Outside market hours — monitoring paused")
                    time.sleep(60)
                    continue

                log.info(f"Running analysis on {', '.join(SYMBOLS)}...")
                results = []

                for sym in SYMBOLS:
                    if len(price_history[sym]) < 30:
                        log.info(f"{sym}: building price history ({len(price_history[sym])}/30 ticks)")
                        continue

                    analysis = evaluate_entry(sym)
                    if not analysis:
                        continue

                    results.append(analysis)
                    log.info(
                        f"{sym} ${analysis['price']:.2f} | "
                        f"Signal: {analysis['signal']} | "
                        f"Criteria: {analysis['met_count']}/5 ({analysis['score']}pts) | "
                        f"RSI: {analysis['rsi']:.1f if analysis['rsi'] else 'N/A'}"
                    )

                    # Buy signal — only one position per symbol, max 2 total
                    if analysis["signal"] == "BUY" and sym not in positions and len(positions) < 2:
                        execute_buy(analysis)

                # Notify if strong signals but can't trade (already in max positions)
                if len(positions) >= 2:
                    strong = [a for a in results if a["signal"] == "BUY" and a["symbol"] not in positions]
                    if strong:
                        names = ", ".join(a["symbol"] for a in strong)
                        log.info(f"Strong signals ignored (max positions held): {names}")

        except KeyboardInterrupt:
            log.info("Bot stopped by user")
            telegram("🛑 AlphaTrader Bot stopped")
            break
        except Exception as e:
            log.error(f"Main loop error: {e}")
            telegram(f"⚠️ <b>Bot error</b>: {str(e)[:150]}\nRestarting in 60s...")
            time.sleep(60)

        time.sleep(10)  # 10-second base tick

if __name__ == "__main__":
    main()
