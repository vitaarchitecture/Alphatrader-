"""
AlphaTrader Bot v7 — Minimum Loss Edition
New protections over v6:
  1. Portfolio hard stop — bot halts if account drops 10% from start
  2. Real-time correlation check — skips if same-sector stock already surging
  3. Wider open/close buffer — no trades first or last 45 mins of session
  4. 50-period MA trend filter — only buy above 50MA
  5. No averaging down — enforced one position per symbol
  6. Reduced size after consecutive losses — drops to $100 after 2 losses in a row
  (Stop loss kept at -0.9% per user preference)
"""

import os
import time
import math
import json
import logging
import threading
import requests
import websocket
from datetime import datetime, timezone, timedelta
from collections import deque

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("alphatrader")

# ── Config ────────────────────────────────────────────────────────────────────
ALPACA_KEY    = os.environ.get("ALPACA_KEY_ID", "")
ALPACA_SECRET = os.environ.get("ALPACA_SECRET_KEY", "")
TG_TOKEN      = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TG_CHAT_ID    = os.environ.get("TELEGRAM_CHAT_ID", "")
ANTHROPIC_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
POLYGON_KEY   = os.environ.get("POLYGON_API_KEY", "")
FMP_KEY       = os.environ.get("FMP_API_KEY", "")
AV_KEY        = os.environ.get("AV_API_KEY", "")
IS_PAPER      = os.environ.get("PAPER_TRADING", "true").lower() == "true"

ALPACA_BASE   = "https://paper-api.alpaca.markets" if IS_PAPER else "https://api.alpaca.markets"
ALPACA_DATA   = "https://data.alpaca.markets"
WS_URL        = "wss://stream.data.alpaca.markets/v2/iex"

# ── Symbols ───────────────────────────────────────────────────────────────────
SYMBOLS = {
    "NVDA":  {"sector": "semiconductors", "corr": "tech"},
    "AMD":   {"sector": "semiconductors", "corr": "tech"},
    "AAPL":  {"sector": "big_tech",       "corr": "tech"},
    "MSFT":  {"sector": "big_tech",       "corr": "tech"},
    "GOOGL": {"sector": "big_tech",       "corr": "tech"},
    "META":  {"sector": "big_tech",       "corr": "tech"},
    "AMZN":  {"sector": "big_tech",       "corr": "tech"},
    "TSLA":  {"sector": "ev_auto",        "corr": "tech"},
    "NFLX":  {"sector": "streaming",      "corr": "tech"},
    "SPY":   {"sector": "etf",            "corr": "market"},
    "GLD":   {"sector": "gold",           "corr": "hedge"},
    "TLT":   {"sector": "bonds",          "corr": "hedge"},
    "USO":   {"sector": "oil",            "corr": "commodity"},
}
TRADEABLE = {k: v for k, v in SYMBOLS.items() if k != "SPY"}

# ── Parameters ────────────────────────────────────────────────────────────────
BASE_TRADE_SIZE         = 200
TAKE_PROFIT_PCT         = 2.0
STOP_LOSS_PCT           = 0.9      # kept at 0.9% per user preference
TIME_STOP_MINS          = 120
MAX_POSITIONS           = 3
DAILY_LOSS_LIMIT        = 50
VIX_PAUSE_LEVEL         = 25
EARNINGS_BLACKOUT       = 3
TRAILING_ACTIVATE       = 1.5
TRAILING_DISTANCE       = 0.5
SIGNAL_COOLDOWN         = 300
LIMIT_OFFSET            = 0.05
SPY_BULL_THRESHOLD      = -0.3
MOMENTUM_MIN_PCT        = 0.3
MANDATORY_SIGNALS       = {"MACD +ve", "RSI 38-58"}
MIN_CONFIRMING          = 2
MIN_SCORE               = 70
FEAR_GREED_PAUSE        = 20
PREMARKET_GAP_LIMIT     = 2.0

# ── New v7 parameters ─────────────────────────────────────────────────────────
PORTFOLIO_HARD_STOP_PCT = 10.0     # halt bot if down 10% from starting value
TRADE_START_HOUR_ET     = 10       # wider buffer — was 10:00, now 10:00 (unchanged open)
TRADE_START_MIN_ET      = 15       # skip first 45 mins (was 30)
TRADE_END_HOUR_ET       = 15       # close buffer
TRADE_END_MIN_ET        = 15       # skip last 45 mins (was 30)
MA50_FILTER             = True     # only trade above 50-period MA
CORR_SURGE_THRESHOLD    = 0.8      # % move that counts as "surging" in same sector
CONSEC_LOSS_THRESHOLD   = 2        # after this many losses in a row, halve size
REDUCED_TRADE_SIZE      = 100      # size after consecutive losses

# ── State ─────────────────────────────────────────────────────────────────────
price_history   = {sym: deque(maxlen=200) for sym in SYMBOLS}
volume_history  = {sym: deque(maxlen=50)  for sym in SYMBOLS}
open_price      = {sym: None for sym in SYMBOLS}
prev_close      = {sym: None for sym in SYMBOLS}
last_signal     = {sym: 0    for sym in SYMBOLS}
positions       = {}
session_trades  = []
daily_pnl       = 0.0
ws_connected    = False
trade_lock      = threading.Lock()

# Portfolio hard stop state
starting_portfolio_value = None
portfolio_halted         = False

# Consecutive loss tracker
consecutive_losses  = 0
last_trade_result   = None   # "win" or "loss"

# Data cache
earnings_cache    = {}
economic_events   = []
fear_greed_score  = None
premarket_data    = {}
economic_blackout = False
data_last_refresh = 0

# ── Alpaca helpers ────────────────────────────────────────────────────────────
def alpaca_get(path):
    r = requests.get(f"{ALPACA_BASE}{path}",
        headers={"APCA-API-KEY-ID": ALPACA_KEY, "APCA-API-SECRET-KEY": ALPACA_SECRET},
        timeout=10)
    r.raise_for_status()
    return r.json()

def alpaca_post(path, body):
    r = requests.post(f"{ALPACA_BASE}{path}",
        headers={"APCA-API-KEY-ID": ALPACA_KEY, "APCA-API-SECRET-KEY": ALPACA_SECRET,
                 "Content-Type": "application/json"},
        json=body, timeout=10)
    r.raise_for_status()
    return r.json()

def alpaca_delete(path):
    requests.delete(f"{ALPACA_BASE}{path}",
        headers={"APCA-API-KEY-ID": ALPACA_KEY, "APCA-API-SECRET-KEY": ALPACA_SECRET},
        timeout=10)

def get_account():
    try: return alpaca_get("/v2/account")
    except: return None

def telegram(msg):
    if not TG_TOKEN or not TG_CHAT_ID:
        log.info(f"[TG] {msg}"); return
    try:
        requests.post(f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
            json={"chat_id": TG_CHAT_ID, "text": msg, "parse_mode": "HTML"},
            timeout=10)
    except Exception as e:
        log.warning(f"Telegram: {e}")

# ── NEW: Portfolio hard stop ──────────────────────────────────────────────────
def check_portfolio_hard_stop():
    """Halt all trading if portfolio drops 10% from starting value."""
    global portfolio_halted
    if portfolio_halted: return True
    if not starting_portfolio_value: return False

    acct = get_account()
    if not acct: return False

    current = float(acct.get("portfolio_value", starting_portfolio_value))
    drop_pct = (starting_portfolio_value - current) / starting_portfolio_value * 100

    if drop_pct >= PORTFOLIO_HARD_STOP_PCT:
        portfolio_halted = True
        telegram(
            f"🛑 <b>PORTFOLIO HARD STOP TRIGGERED</b>\n"
            f"Started: ${starting_portfolio_value:.2f}\n"
            f"Now: ${current:.2f}\n"
            f"Drop: -{drop_pct:.1f}%\n"
            f"All trading halted. Review strategy before restarting.\n"
            f"To restart: redeploy on Railway."
        )
        log.error(f"Portfolio hard stop — down {drop_pct:.1f}%")
        return True
    return False

# ── NEW: Dynamic trade size based on consecutive losses ───────────────────────
def get_trade_size(confidence):
    """Reduce size after consecutive losses."""
    if consecutive_losses >= CONSEC_LOSS_THRESHOLD:
        base = REDUCED_TRADE_SIZE
        log.info(f"Reduced size active — {consecutive_losses} consecutive losses")
    else:
        if confidence >= 85: base = BASE_TRADE_SIZE * 1.5
        elif confidence >= 75: base = BASE_TRADE_SIZE
        else: base = BASE_TRADE_SIZE * 0.5
    return base

# ── NEW: 50-period MA filter ──────────────────────────────────────────────────
def above_50ma(sym):
    """Returns True if current price is above 50-period moving average."""
    if not MA50_FILTER: return True
    hist = list(price_history[sym])
    if len(hist) < 50: return True  # not enough data — allow
    ma50 = sum(hist[-50:]) / 50
    return hist[-1] > ma50

# ── NEW: Sector surge correlation check ──────────────────────────────────────
def sector_already_surging(sym):
    """
    Returns True if another stock in the same sector is already up
    significantly — meaning the move may be mature, not fresh.
    """
    my_sector = SYMBOLS[sym]["sector"]
    for other_sym, info in SYMBOLS.items():
        if other_sym == sym: return False
        if info["sector"] != my_sector: continue
        op = open_price.get(other_sym)
        hist = list(price_history[other_sym])
        if not op or not hist: continue
        move_pct = abs((hist[-1] - op) / op * 100)
        if move_pct >= CORR_SURGE_THRESHOLD:
            log.info(f"{sym} skipped — {other_sym} already moved {move_pct:.2f}% (sector surge)")
            return True
    return False

# ── Data fetchers ─────────────────────────────────────────────────────────────
def fetch_polygon_earnings():
    if not POLYGON_KEY: return {}
    try:
        today  = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        future = (datetime.now(timezone.utc) + timedelta(days=60)).strftime("%Y-%m-%d")
        r = requests.get("https://api.polygon.io/vX/reference/financials",
            params={"ticker": ",".join(TRADEABLE.keys()),
                    "filing_date.gte": today, "filing_date.lte": future,
                    "limit": 50, "apiKey": POLYGON_KEY}, timeout=10)
        dates = {}
        if r.ok:
            for item in r.json().get("results", []):
                sym = item.get("tickers", [""])[0]
                date = item.get("filing_date", "")
                if sym and date and sym not in dates: dates[sym] = date
        return dates
    except: return {}

def fetch_polygon_news(symbol):
    if not POLYGON_KEY: return []
    try:
        r = requests.get("https://api.polygon.io/v2/reference/news",
            params={"ticker": symbol, "limit": 10, "order": "desc",
                    "sort": "published_utc", "apiKey": POLYGON_KEY}, timeout=10)
        return [a.get("title","") for a in r.json().get("results",[])] if r.ok else []
    except: return []

def fetch_polygon_premarket(symbol):
    if not POLYGON_KEY: return None
    try:
        r = requests.get(
            f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers/{symbol}",
            params={"apiKey": POLYGON_KEY}, timeout=10)
        if r.ok:
            snap = r.json().get("ticker", {})
            prev = snap.get("prevDay", {})
            pm   = snap.get("lastQuote", {}).get("P")
            if pm and prev.get("c"):
                gap = (float(pm) - float(prev["c"])) / float(prev["c"]) * 100
                return {"price": float(pm), "gap_pct": gap}
        return None
    except: return None

def fetch_economic_calendar():
    if not FMP_KEY: return []
    try:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        r = requests.get("https://financialmodelingprep.com/api/v3/economic_calendar",
            params={"from": today, "to": today, "apikey": FMP_KEY}, timeout=10)
        if not r.ok: return []
        keywords = ["fed","fomc","interest rate","cpi","inflation","nonfarm","payroll","gdp","unemployment","powell"]
        return [{"event": e.get("event"), "time": e.get("date"), "impact": e.get("impact","")}
                for e in r.json()
                if e.get("impact","").lower()=="high" or any(k in e.get("event","").lower() for k in keywords)]
    except: return []

def fetch_fmp_earnings():
    if not FMP_KEY: return {}
    try:
        today  = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        future = (datetime.now(timezone.utc) + timedelta(days=60)).strftime("%Y-%m-%d")
        r = requests.get("https://financialmodelingprep.com/api/v3/earning_calendar",
            params={"from": today, "to": future, "apikey": FMP_KEY}, timeout=10)
        return {item["symbol"]: item["date"] for item in r.json()
                if item.get("symbol") in TRADEABLE and item.get("date")} if r.ok else {}
    except: return {}

def fetch_fear_greed():
    try:
        r = requests.get("https://production.dataviz.cnn.io/index/fearandgreed/graphdata",
            headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
        if r.ok:
            score = r.json().get("fear_and_greed", {}).get("score")
            return float(score) if score else None
        return None
    except: return None

def fetch_alpaca_premarket(symbol):
    try:
        r = requests.get(f"{ALPACA_DATA}/v2/stocks/{symbol}/snapshot",
            headers={"APCA-API-KEY-ID": ALPACA_KEY, "APCA-API-SECRET-KEY": ALPACA_SECRET},
            timeout=10)
        if r.ok:
            snap = r.json()
            pm   = snap.get("minuteBar", {})
            prev = snap.get("prevDailyBar", {})
            if pm and prev:
                pmp = float(pm.get("c", 0)); pcp = float(prev.get("c", 0))
                if pmp and pcp:
                    return {"price": pmp, "gap_pct": (pmp-pcp)/pcp*100}
        return None
    except: return None

def refresh_all_data():
    global earnings_cache, economic_events, fear_greed_score
    global premarket_data, economic_blackout, data_last_refresh

    log.info("Refreshing all external data sources...")

    poly_earnings = fetch_polygon_earnings()
    fmp_earnings  = fetch_fmp_earnings()
    hardcoded = {
        "NVDA":"2026-08-26","AAPL":"2026-10-30","TSLA":"2026-10-21",
        "MSFT":"2026-10-28","GOOGL":"2026-10-28","META":"2026-10-28",
        "AMZN":"2026-10-29","AMD":"2026-10-28","NFLX":"2026-10-14",
    }
    earnings_cache = {**hardcoded, **fmp_earnings, **poly_earnings}

    economic_events  = fetch_economic_calendar()
    economic_blackout = len(economic_events) > 0
    if economic_blackout:
        names = ", ".join(e["event"] for e in economic_events)
        telegram(f"📅 <b>High-impact events today:</b> {names}\n⚠️ Reduced position sizes active")

    fear_greed_score = fetch_fear_greed()
    if fear_greed_score is not None:
        if fear_greed_score <= FEAR_GREED_PAUSE:
            telegram(f"😨 <b>Fear & Greed: {fear_greed_score:.0f} — Extreme Fear</b>\n⚠️ New entries paused")
        elif fear_greed_score >= 80:
            telegram(f"🤑 <b>Fear & Greed: {fear_greed_score:.0f} — Extreme Greed</b>\n⚠️ Market may be overextended")

    for sym in TRADEABLE:
        pm = fetch_polygon_premarket(sym) or fetch_alpaca_premarket(sym)
        if pm: premarket_data[sym] = pm

    data_last_refresh = time.time()
    log.info("Data refresh complete")

# ── Claude sentiment ──────────────────────────────────────────────────────────
def claude_sentiment(symbol):
    if not ANTHROPIC_KEY:
        return {"sentiment": "neutral", "score": 50, "reason": "No API key"}
    try:
        headlines = fetch_polygon_news(symbol)
        if not headlines:
            r = requests.get(f"{ALPACA_DATA}/v1beta1/news?symbols={symbol}&limit=5",
                headers={"APCA-API-KEY-ID": ALPACA_KEY, "APCA-API-SECRET-KEY": ALPACA_SECRET},
                timeout=10)
            if r.ok: headlines = [a.get("headline","") for a in r.json().get("news",[])]
        if not headlines:
            return {"sentiment": "neutral", "score": 50, "reason": "No recent news"}

        econ = ""
        if economic_events: econ = f"\nHigh-impact events today: {', '.join(e['event'] for e in economic_events)}"
        if fear_greed_score: econ += f"\nFear & Greed: {fear_greed_score:.0f}/100"

        r = requests.post("https://api.anthropic.com/v1/messages",
            headers={"Content-Type":"application/json","x-api-key":ANTHROPIC_KEY,
                     "anthropic-version":"2023-06-01"},
            json={"model":"claude-sonnet-4-6","max_tokens":200,
                  "system":"Return ONLY valid JSON: {\"sentiment\":\"positive\"|\"negative\"|\"neutral\",\"score\":0-100,\"reason\":\"one sentence\"}",
                  "messages":[{"role":"user","content":
                      f"Short-term trading sentiment for {symbol}:\n"
                      + "\n".join(f"- {h}" for h in headlines[:8]) + econ}]},
            timeout=15)
        text = r.json()["content"][0]["text"].replace("```json","").replace("```","").strip()
        return json.loads(text)
    except Exception as e:
        log.warning(f"Sentiment failed {symbol}: {e}")
        return {"sentiment": "neutral", "score": 50, "reason": "Unavailable"}

# ── Technical indicators ──────────────────────────────────────────────────────
def ema(arr, n):
    arr=list(arr)
    if not arr: return None
    k=2/(n+1); e=sum(arr[:min(n,len(arr))])/min(n,len(arr))
    for p in arr[min(n,len(arr)):]: e=p*k+e*(1-k)
    return e

def rsi(arr, n=14):
    arr=list(arr)
    if len(arr)<n+1: return None
    sl=arr[-(n+1):]
    g=sum(max(sl[i]-sl[i-1],0) for i in range(1,len(sl)))
    l=sum(max(sl[i-1]-sl[i],0) for i in range(1,len(sl)))
    ag,al=g/n,l/n
    return 100 if al==0 else 100-100/(1+ag/al)

def macd_ind(arr):
    arr=list(arr)
    if len(arr)<26: return None
    hist=[]
    for i in range(26,len(arr)+1):
        e12=ema(arr[:i],12); e26=ema(arr[:i],26)
        if e12 and e26: hist.append(e12-e26)
    if len(hist)<9: return None
    sig=ema(hist,9); line=hist[-1]
    return {"line":line,"signal":sig,"histogram":line-sig}

def bollinger(arr, n=20):
    arr=list(arr)
    if len(arr)<n: return None
    sl=arr[-n:]; sma=sum(sl)/n
    std=math.sqrt(sum((p-sma)**2 for p in sl)/n)
    return {"upper":sma+2*std,"middle":sma,"lower":sma-2*std}

def stoch(arr, n=14):
    arr=list(arr)
    if len(arr)<n: return None
    sl=arr[-n:]; lo,hi=min(sl),max(sl)
    return 50 if hi==lo else ((arr[-1]-lo)/(hi-lo))*100

def vwap_calc(arr):
    arr=list(arr); return sum(arr)/len(arr) if arr else None

def volume_ok(sym):
    vols=list(volume_history[sym])
    if len(vols)<5: return True
    return vols[-1]>sum(vols[:-1])/(len(vols)-1)*0.9

# ── Market state ──────────────────────────────────────────────────────────────
def spy_trend():
    hist=list(price_history["SPY"]); op=open_price.get("SPY")
    if not hist or not op: return None
    return (hist[-1]-op)/op*100

def market_allows_tech_long():
    t=spy_trend(); return True if t is None else t>=-0.3

def in_trading_window():
    """Wider buffer — avoid first and last 45 mins."""
    now=datetime.now(timezone.utc); h=(now.hour-4)%24; m=now.minute
    after  = h>TRADE_START_HOUR_ET or (h==TRADE_START_HOUR_ET and m>=TRADE_START_MIN_ET)
    before = h<TRADE_END_HOUR_ET   or (h==TRADE_END_HOUR_ET   and m<=TRADE_END_MIN_ET)
    return after and before

def has_momentum(sym):
    hist=list(price_history[sym]); op=open_price.get(sym)
    if not hist or not op: return True
    return (hist[-1]-op)/op*100>=MOMENTUM_MIN_PCT

def is_market_hours():
    now=datetime.now(timezone.utc); h=(now.hour-4)%24; m=now.minute
    if now.weekday()>=5: return False
    return (h>9 or (h==9 and m>=30)) and (h<15 or (h==15 and m<=45))

def should_close_all():
    now=datetime.now(timezone.utc); h=(now.hour-4)%24; m=now.minute
    if now.weekday()>=5: return False
    return h==15 and m>=45

def near_earnings(sym):
    ed=earnings_cache.get(sym)
    if not ed: return False
    try:
        d=datetime.strptime(ed,"%Y-%m-%d").replace(tzinfo=timezone.utc)
        return 0<=(d-datetime.now(timezone.utc)).days<=EARNINGS_BLACKOUT
    except: return False

def sector_held(sym):
    sector=SYMBOLS[sym]["sector"]
    return any(SYMBOLS[s]["sector"]==sector and s!=sym for s in positions)

def get_premarket_gap(sym):
    pm=premarket_data.get(sym)
    if pm: return pm.get("gap_pct",0)
    pc=prev_close.get(sym); hist=list(price_history[sym])
    if not pc or not hist: return 0
    return (hist[-1]-pc)/pc*100

# ── Signal evaluator ──────────────────────────────────────────────────────────
def evaluate(sym):
    hist=price_history[sym]
    if len(hist)<30: return None
    price=hist[-1]; r=rsi(hist); m=macd_ind(hist); bb=bollinger(hist)
    st=stoch(hist); vw=vwap_calc(hist); e9=ema(hist,9); e21=ema(hist,21)
    pvwap=((price-vw)/vw*100) if vw else None

  
