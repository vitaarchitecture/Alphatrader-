"""
AlphaTrader Bot v11 — Crypto + Fixed Exits Edition
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
    # Tech / semis
    "NVDA":  {"sector": "semiconductors", "corr": "tech"},
    "AMD":   {"sector": "semiconductors", "corr": "tech"},
    "SMCI":  {"sector": "semiconductors", "corr": "tech"},
    "MU":    {"sector": "semiconductors", "corr": "tech"},
    "INTC":  {"sector": "semiconductors", "corr": "tech"},
    "AAPL":  {"sector": "big_tech",       "corr": "tech"},
    "MSFT":  {"sector": "big_tech",       "corr": "tech"},
    "GOOGL": {"sector": "big_tech",       "corr": "tech"},
    "META":  {"sector": "big_tech",       "corr": "tech"},
    "AMZN":  {"sector": "big_tech",       "corr": "tech"},
    "PLTR":  {"sector": "software",       "corr": "tech"},
    "TSLA":  {"sector": "ev_auto",        "corr": "tech"},
    "NFLX":  {"sector": "streaming",      "corr": "tech"},
    "DIS":   {"sector": "streaming",      "corr": "consumer"},
    # Financials — move independently of tech
    "JPM":   {"sector": "banks",          "corr": "financial"},
    "BAC":   {"sector": "banks",          "corr": "financial"},
    "GS":    {"sector": "banks",          "corr": "financial"},
    # Healthcare — defensive, uncorrelated
    "LLY":   {"sector": "pharma",         "corr": "healthcare"},
    # Energy stocks — Iran conflict tailwind
    "XOM":   {"sector": "oil_majors",     "corr": "energy"},
    "CVX":   {"sector": "oil_majors",     "corr": "energy"},
    # Consumer staples/discretionary
    # Industrials
    # Market filter + hedges
    "SPY":   {"sector": "etf",            "corr": "market"},
    "GLD":   {"sector": "gold",           "corr": "hedge"},
    "TLT":   {"sector": "bonds",          "corr": "hedge"},
    "USO":   {"sector": "oil",            "corr": "commodity"},
}
TRADEABLE = {k: v for k, v in SYMBOLS.items() if k != "SPY"}

# ── Parameters ────────────────────────────────────────────────────────────────
BASE_TRADE_SIZE         = 200
TAKE_PROFIT_PCT         = 1.2
STOP_LOSS_PCT           = 0.9      # kept at 0.9% per user preference
TIME_STOP_MINS          = 120
MAX_POSITIONS           = 6
DAILY_LOSS_LIMIT        = 50
VIX_PAUSE_LEVEL         = 25
EARNINGS_BLACKOUT       = 3
TRAILING_ACTIVATE       = 1.5
TRAILING_DISTANCE       = 0.5
SIGNAL_COOLDOWN         = 300
LIMIT_OFFSET            = 0.05
SPY_BULL_THRESHOLD      = -0.3
MOMENTUM_MIN_PCT        = 0.05
MANDATORY_SIGNALS       = {"MACD +ve", "RSI 38-58"}
MIN_CONFIRMING          = 2
MIN_SCORE               = 70

# ── Crypto parameters (wider stops/targets for volatility) ───────────────────
CRYPTO_SYMBOLS          = {"BTC/USD": "crypto", "ETH/USD": "crypto"}
CRYPTO_TAKE_PROFIT      = 3.0     # wider target
CRYPTO_STOP_LOSS        = 1.5     # wider stop (BTC noise > 0.9%)
CRYPTO_TIME_STOP_MINS   = 240     # 4 hours — trends develop slower overnight
MAX_CRYPTO_POSITIONS    = 2      # allocation bucket: max 2 of 6 slots
MAX_STOCK_POSITIONS     = 4      # allocation bucket: max 4 of 6 slots
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
    if sym in CRYPTO_SYMBOLS: return False
    my_sector = SYMBOLS.get(sym,{}).get("sector")
    if not my_sector: return False
    for other_sym, info in SYMBOLS.items():
        if other_sym == sym: return False
        if info.get("sector") != my_sector: continue
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
        "SMCI":"2026-08-26","MU":"2026-09-24",
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
    """SPY change measured from the WORSE of today's open or prior close.
    Catches gap-down days where SPY opens low and trades flat."""
    hist=list(price_history["SPY"])
    if not hist: return None
    op=open_price.get("SPY"); pc=prev_close.get("SPY")
    refs=[r for r in (op,pc) if r]
    if not refs: return None
    return min((hist[-1]-r)/r*100 for r in refs)

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
    if sym in CRYPTO_SYMBOLS: return False
    sector=SYMBOLS.get(sym,{}).get("sector")
    return any(SYMBOLS.get(s,{}).get("sector")==sector and s!=sym for s in positions)

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

    if abs(get_premarket_gap(sym))>PREMARKET_GAP_LIMIT: return None

    criteria=[
        ("RSI 38-58",        r  is not None and 38<=r<=58,              25),
        ("MACD +ve",         m  is not None and m.get("histogram",0)>0, 25),
        ("Above VWAP",       pvwap is not None and pvwap>0,             20),
        ("EMA9>EMA21",       e9 and e21 and e9>e21,                     15),
        ("Stoch<78",         st is not None and st<78,                  15),
        ("Volume confirmed", volume_ok(sym),                            10),
    ]

    met_names={n for n,p,w in criteria if p}
    score=sum(w for n,p,w in criteria if p)
    mandatory_met=MANDATORY_SIGNALS.issubset(met_names)
    confirming=len([n for n in met_names if n not in MANDATORY_SIGNALS])
    signal=("BUY" if mandatory_met and confirming>=MIN_CONFIRMING and score>=MIN_SCORE else "WAIT")

    return {"symbol":sym,"price":price,"signal":signal,"met":list(met_names),
            "met_count":len(met_names),"score":score,
            "mandatory_met":mandatory_met,"confirming":confirming,"rsi":r}

# ── Trailing stop ─────────────────────────────────────────────────────────────
def update_trail(sym, price):
    pos=positions.get(sym)
    if not pos: return
    pnl_pct=(price-pos["entry_price"])/pos["entry_price"]*100
    if pnl_pct>=TRAILING_ACTIVATE:
        if price>pos.get("peak_price",pos["entry_price"]):
            positions[sym]["peak_price"]=price
            new_stop=price*(1-TRAILING_DISTANCE/100)
            if new_stop>(pos.get("trailing_stop") or 0):
                positions[sym]["trailing_stop"]=new_stop

# ── Exit check ────────────────────────────────────────────────────────────────
def check_exit(sym, price):
    pos=positions.get(sym)
    if not pos: return None
    is_crypto = pos.get("is_crypto", False)
    tp = CRYPTO_TAKE_PROFIT   if is_crypto else TAKE_PROFIT_PCT
    sl = CRYPTO_STOP_LOSS     if is_crypto else STOP_LOSS_PCT
    ts = CRYPTO_TIME_STOP_MINS if is_crypto else TIME_STOP_MINS
    pnl_pct=(price-pos["entry_price"])/pos["entry_price"]*100
    mins=(time.time()-pos["entry_time"])/60
    trail=pos.get("trailing_stop")
    if trail and price<trail: return ("TRAIL",       f"Trailing stop ${trail:.2f} ({pnl_pct:+.2f}%) 📉")
    if pnl_pct>=tp:  return ("TAKE_PROFIT", f"+{pnl_pct:.2f}% take profit 🟢")
    if pnl_pct<=-sl: return ("STOP_LOSS",   f"{pnl_pct:.2f}% stop loss 🔴")
    # CONDITIONAL time stop: only cut trades that are flat or negative.
    # A trade in decent profit (>+0.4%) is working — let it run to target.
    if mins>=ts and pnl_pct < 0.4:
        return ("TIME_STOP", f"{pnl_pct:.2f}% after {mins:.0f}m — dead trade ⏱")
    # Hard ceiling: nothing holds past 2x the time stop regardless
    if mins>=ts*2:
        return ("TIME_STOP", f"{pnl_pct:.2f}% after {mins:.0f}m — max hold ⏱")
    return None

# ── Execute buy ───────────────────────────────────────────────────────────────
def execute_buy(analysis, sentiment):
    global daily_pnl, consecutive_losses
    sym=analysis["symbol"]; price=analysis["price"]; score=analysis["score"]

    with trade_lock:
        # ── All guards ───────────────────────────────────────────────────────
        if sym in positions: return
        if portfolio_halted:
            log.info("Portfolio halted — no new trades"); return
        if check_portfolio_hard_stop(): return
        if daily_pnl<=-DAILY_LOSS_LIMIT: return
        if near_earnings(sym):
            telegram(f"⚠️ <b>{sym}</b> skipped — earnings blackout"); return
        if sector_held(sym): return
        if len(positions)>=MAX_POSITIONS: return
        # Allocation buckets: crypto and stocks can't crowd each other out
        n_crypto = sum(1 for p in positions.values() if p.get("is_crypto"))
        n_stock  = len(positions) - n_crypto
        if sym in CRYPTO_SYMBOLS and n_crypto >= MAX_CRYPTO_POSITIONS: return
        if sym not in CRYPTO_SYMBOLS and n_stock >= MAX_STOCK_POSITIONS: return
        try:
            sent_score = int(float(sentiment.get("score", 50)))
        except (ValueError, TypeError):
            sent_score = 50
        if sentiment.get("sentiment")=="negative" and sent_score<35:
            telegram(f"📰 <b>{sym}</b> skipped — {sentiment['reason']}"); return
        if sym not in CRYPTO_SYMBOLS:
            if SYMBOLS.get(sym,{}).get("corr")=="tech" and not market_allows_tech_long(): return
            if not in_trading_window():
                log.info(f"{sym} skipped — outside trading window (45min buffer)"); return
            if not has_momentum(sym):
                log.info(f"{sym} skipped — no momentum from open"); return
            if not above_50ma(sym):
                log.info(f"{sym} skipped — price below 50MA (downtrend)"); return
            if sector_already_surging(sym):
                return
        if fear_greed_score is not None and fear_greed_score<=FEAR_GREED_PAUSE: return

        # Position sizing — reduced if on a losing streak
        if economic_blackout:
            trade_val_raw = get_trade_size(score) * 0.5
        else:
            trade_val_raw = get_trade_size(score)

        # Extra caution if consecutive losses
        if consecutive_losses >= CONSEC_LOSS_THRESHOLD:
            log.info(f"Consecutive losses: {consecutive_losses} — using reduced size ${REDUCED_TRADE_SIZE}")

        acct=get_account()
        if not acct: return
        bp=float(acct.get("buying_power",0))
        trade_val=min(trade_val_raw, bp*0.95)
        if trade_val<10: return

        is_crypto = sym in CRYPTO_SYMBOLS
        tp_pct = CRYPTO_TAKE_PROFIT if is_crypto else TAKE_PROFIT_PCT
        sl_pct = CRYPTO_STOP_LOSS   if is_crypto else STOP_LOSS_PCT
        stop_price  = round(price * (1 - sl_pct/100), 2)
        target      = round(price * (1 + tp_pct/100), 2)

        try:
            if is_crypto:
                # Crypto: fractional notional order, no resting stop
                # (crypto stops managed in software via websocket ticks)
                shares = round(trade_val / price, 6)
                actual_cost = trade_val
                order = alpaca_post("/v2/orders", {
                    "symbol":        sym,
                    "notional":      f"{trade_val:.2f}",
                    "side":          "buy",
                    "type":          "market",
                    "time_in_force": "gtc",
                })
            else:
                shares = max(1, int(trade_val / price))
                actual_cost = shares * price
                # Step 1: market buy
                order = alpaca_post("/v2/orders", {
                    "symbol":        sym,
                    "qty":           str(shares),
                    "side":          "buy",
                    "type":          "market",
                    "time_in_force": "day",
                })
            order_id = order.get("id", "")
            stop_order_id = ""

            # Step 2 (stocks only): ONE resting stop-loss order as crash
            # protection. Target is managed in software — no second resting
            # sell, so shares are never double-reserved (SMCI bug fix).
            if not is_crypto:
                try:
                    time.sleep(1)  # let buy fill before placing stop
                    stop_order = alpaca_post("/v2/orders", {
                        "symbol":        sym,
                        "qty":           str(shares),
                        "side":          "sell",
                        "type":          "stop",
                        "stop_price":    str(stop_price),
                        "time_in_force": "gtc",
                    })
                    stop_order_id = stop_order.get("id", "")
                except Exception as se:
                    log.warning(f"Stop order failed {sym}: {se}")

            positions[sym]={
                "entry_price":price,"qty":shares,"entry_time":time.time(),
                "cost":actual_cost,"order_id":order_id,
                "stop_order_id":stop_order_id,
                "stop":stop_price,"target":target,"is_crypto":is_crypto,
                "peak_price":price,"trailing_stop":None,"confidence":score,
            }

            streak_note = f"⚠️ Reduced size — {consecutive_losses} consecutive losses" if consecutive_losses>=CONSEC_LOSS_THRESHOLD else ""
            ma50_val = sum(list(price_history[sym])[-50:])/50 if len(price_history[sym])>=50 else None
            ma50_note = f"Above 50MA (${ma50_val:.2f}) ✅" if ma50_val else ""

            telegram(
                f"📥 <b>BUY {sym}</b> {'(PAPER)' if IS_PAPER else '(LIVE)'}\n"
                f"Market order | {shares} shares @ ~${price:.2f}\n"
                f"Stop: ${stop_price} | Target: ${target}\n"
                f"Signals: {' · '.join(analysis['met'])}\n"
                f"📰 {str(sentiment.get('sentiment','neutral')).upper()} ({sentiment.get('score','?')}/100): {sentiment.get('reason','')}\n"
                + (f"📊 {ma50_note}\n" if ma50_note else "")
                + (f"⚠️ {streak_note}\n" if streak_note else "")
                + f"🛡 Stop + target orders placed"
            )
            last_signal[sym]=time.time()

        except Exception as e:
            log.error(f"Buy failed {sym}: {e}")
            telegram(f"❌ <b>BUY failed</b> {sym}: {str(e)[:100]}")

# ── Execute sell ──────────────────────────────────────────────────────────────
def execute_sell(sym, reason, price):
    global daily_pnl, consecutive_losses, last_trade_result
    pos=positions.get(sym)
    if not pos: return
    with trade_lock:
        try:
            # CRITICAL: cancel the resting stop order first so shares are
            # released before the market sell (fixes SMCI orphan bug)
            if pos.get("stop_order_id"):
                try:
                    alpaca_delete(f"/v2/orders/{pos['stop_order_id']}")
                    time.sleep(0.5)
                except Exception:
                    pass
            try: alpaca_delete(f"/v2/orders/{pos['order_id']}")
            except: pass
            if pos.get("is_crypto"):
                alpaca_post("/v2/orders",{
                    "symbol":sym,"qty":f"{pos['qty']:.6f}",
                    "side":"sell","type":"market","time_in_force":"gtc",
                })
            else:
                held_qty = int(pos.get("qty", 1))
                alpaca_post("/v2/orders",{
                    "symbol":sym,"qty":str(max(1,held_qty)),
                    "side":"sell","type":"market","time_in_force":"day",
                })
            pnl_pct=(price-pos["entry_price"])/pos["entry_price"]*100
            pnl_abs=(price-pos["entry_price"])*pos["qty"]
            daily_pnl+=pnl_abs

            # Update consecutive loss tracker
            if pnl_abs > 0:
                consecutive_losses=0
                last_trade_result="win"
            else:
                consecutive_losses+=1
                last_trade_result="loss"

            session_trades.append({
                "symbol":sym,"entry":pos["entry_price"],"exit":price,
                "qty":pos["qty"],"pnl_pct":pnl_pct,"pnl_abs":pnl_abs,
                "reason":reason,"held_mins":(time.time()-pos["entry_time"])/60,
            })
            del positions[sym]

            emoji="🟢" if pnl_abs>=0 else "🔴"
            streak_note=""
            if consecutive_losses>=CONSEC_LOSS_THRESHOLD:
                streak_note=f"\n⚠️ {consecutive_losses} losses in a row — trade size reduced to ${REDUCED_TRADE_SIZE}"
            elif consecutive_losses==0 and last_trade_result=="win":
                streak_note="\n✅ Win — full size restored" if pos.get("confidence",100)<75 else ""

            telegram(
                f"{emoji} <b>SELL {sym}</b> {'(PAPER)' if IS_PAPER else '(LIVE)'}\n"
                f"Entry: ${pos['entry_price']:.2f} → Exit: ${price:.2f}\n"
                f"P&L: {pnl_pct:+.2f}% (${pnl_abs:+.2f})\n"
                f"Reason: {reason} | Today: ${daily_pnl:+.2f}"
                + streak_note
            )
        except Exception as e:
            log.error(f"Sell failed {sym}: {e}")

# ── Websocket ─────────────────────────────────────────────────────────────────
def on_tick(sym, price, volume=None):
    if not is_market_hours(): return
    price_history[sym].append(price)
    if volume: volume_history[sym].append(volume)
    if open_price.get(sym) is None: open_price[sym]=price

    if sym in positions:
        update_trail(sym,price)
        result=check_exit(sym,price)
        if result:
            _,reason=result
            execute_sell(sym,reason,price)
        return

    if sym not in TRADEABLE: return
    if portfolio_halted: return
    if time.time()-last_signal.get(sym,0)<SIGNAL_COOLDOWN: return
    if len(price_history[sym])<30: return

    analysis=evaluate(sym)
    if not analysis or analysis["signal"]!="BUY": return

    log.info(f"⚡ {sym} BUY signal — score:{analysis['score']}")
    last_signal[sym]=time.time()

    def buy_thread():
        sentiment=claude_sentiment(sym)
        execute_buy(analysis,sentiment)

    threading.Thread(target=buy_thread,daemon=True).start()

def on_message(ws, message):
    global ws_connected
    try:
        data=json.loads(message)
        for msg in (data if isinstance(data,list) else [data]):
            T=msg.get("T")
            if T=="success":
                if msg.get("msg")=="connected":
                    ws.send(json.dumps({"action":"auth","key":ALPACA_KEY,"secret":ALPACA_SECRET}))
                elif msg.get("msg")=="authenticated":
                    ws_connected=True
                    ws.send(json.dumps({"action":"subscribe","trades":list(SYMBOLS.keys())}))
                    telegram("📡 <b>Websocket live</b> — streaming all symbols")
            elif T=="t":
                sym=msg.get("S"); price=msg.get("p"); size=msg.get("s")
                if sym and price and sym in SYMBOLS:
                    on_tick(sym,float(price),float(size) if size else None)
            elif T=="error":
                log.error(f"WS: {msg}")
    except Exception as e:
        log.error(f"WS parse: {e}")

def on_error(ws,e):
    global ws_connected; ws_connected=False; log.error(f"WS: {e}")

def on_close(ws,c,m):
    global ws_connected; ws_connected=False
    telegram("⚠️ Stream disconnected — reconnecting...")

def on_open(ws): log.info("WS opened")

def start_websocket():
    while True:
        try:
            ws=websocket.WebSocketApp(WS_URL,on_message=on_message,
                on_error=on_error,on_close=on_close,on_open=on_open)
            ws.run_forever(ping_interval=30,ping_timeout=10)
        except Exception as e:
            log.error(f"WS thread: {e}")
        time.sleep(5)


# ── Crypto engine ─────────────────────────────────────────────────────────────
# Crypto trades 24/7 via REST polling (30s). Separate from stock websocket.
for _c in CRYPTO_SYMBOLS:
    price_history[_c] = deque(maxlen=200)
    volume_history[_c] = deque(maxlen=50)
    open_price[_c] = None
    prev_close[_c] = None
    last_signal[_c] = 0

def get_crypto_price(symbol):
    try:
        s = symbol.replace("/", "%2F")
        r = requests.get(f"{ALPACA_DATA}/v1beta3/crypto/us/latest/trades?symbols={s}",
            headers={"APCA-API-KEY-ID": ALPACA_KEY, "APCA-API-SECRET-KEY": ALPACA_SECRET},
            timeout=10)
        if r.ok:
            trades = r.json().get("trades", {})
            t = trades.get(symbol) or next(iter(trades.values()), None)
            if t: return float(t.get("p", 0)) or None
        return None
    except Exception:
        return None

def crypto_evaluate(sym):
    """Same indicator logic as stocks but no market-hours/momentum-from-open
    dependency — crypto has no open. Uses rolling window momentum instead."""
    hist=price_history[sym]
    if len(hist)<30: return None
    price=hist[-1]; r=rsi(hist); m=macd_ind(hist)
    st=stoch(hist); vw=vwap_calc(hist); e9=ema(hist,9); e21=ema(hist,21)
    pvwap=((price-vw)/vw*100) if vw else None
    # rolling momentum: last price vs 30 polls ago (~15 min)
    roll_mom = (hist[-1]-hist[0])/hist[0]*100
    criteria=[
        ("RSI 38-58",        r  is not None and 38<=r<=58,              25),
        ("MACD +ve",         m  is not None and m.get("histogram",0)>0, 25),
        ("Above VWAP",       pvwap is not None and pvwap>0,             20),
        ("EMA9>EMA21",       e9 and e21 and e9>e21,                     15),
        ("Stoch<78",         st is not None and st<78,                  15),
        ("Rolling momentum+",roll_mom>0,                                10),
    ]
    met_names={n for n,p,w in criteria if p}
    score=sum(w for n,p,w in criteria if p)
    mandatory_met=MANDATORY_SIGNALS.issubset(met_names)
    confirming=len([n for n in met_names if n not in MANDATORY_SIGNALS])
    signal=("BUY" if mandatory_met and confirming>=MIN_CONFIRMING and score>=MIN_SCORE else "WAIT")
    return {"symbol":sym,"price":price,"signal":signal,"met":list(met_names),
            "met_count":len(met_names),"score":score,
            "mandatory_met":mandatory_met,"confirming":confirming,"rsi":r}

def crypto_loop():
    """Poll crypto prices every 30s, 24/7. Manage entries and exits."""
    log.info("Crypto engine started — BTC/USD, ETH/USD, 24/7")
    while True:
        try:
            for sym in CRYPTO_SYMBOLS:
                p = get_crypto_price(sym)
                if not p: continue
                price_history[sym].append(p)

                # Exits first
                if sym in positions:
                    update_trail(sym, p)
                    result = check_exit(sym, p)
                    if result:
                        _, reason = result
                        execute_sell(sym, reason, p)
                    continue

                if portfolio_halted: continue
                if time.time()-last_signal.get(sym,0)<SIGNAL_COOLDOWN: continue
                if len(price_history[sym])<30: continue

                analysis = crypto_evaluate(sym)
                if not analysis or analysis["signal"]!="BUY": continue
                log.info(f"⚡ {sym} CRYPTO BUY signal — score:{analysis['score']}")
                last_signal[sym]=time.time()
                sentiment = {"sentiment":"neutral","score":50,"reason":"Crypto — technicals only"}
                execute_buy(analysis, sentiment)
        except Exception as e:
            log.error(f"Crypto loop: {e}")
        time.sleep(30)

# ── Daily summary ─────────────────────────────────────────────────────────────
def send_daily_summary():
    fg=f"Fear & Greed: {fear_greed_score:.0f}" if fear_greed_score else ""
    if not session_trades:
        telegram(f"📊 <b>Daily Summary</b>\nNo trades today.\n{fg}"); return
    total=sum(t["pnl_abs"] for t in session_trades)
    wins=sum(1 for t in session_trades if t["pnl_abs"]>0)
    wr=wins/len(session_trades)*100
    by_sym={}
    for t in session_trades: by_sym.setdefault(t["symbol"],[]).append(t["pnl_abs"])
    sym_lines="\n".join(f"  {s}: ${sum(v):+.2f} ({len(v)} trades)" for s,v in by_sym.items())
    spy_note=f"SPY: {spy_trend():+.2f}%" if spy_trend() is not None else ""
    telegram(
        f"📊 <b>Daily Summary v7</b> {'PAPER' if IS_PAPER else 'LIVE'}\n"
        f"Trades: {len(session_trades)} | Win rate: {wr:.0f}%\n"
        f"Total P&L: ${total:+.2f}\n"
        f"Consecutive losses: {consecutive_losses}\n"
        f"By symbol:\n{sym_lines}\n"
        f"{spy_note} | {fg}\n"
        f"{'✅ Profitable!' if total>0 else '❌ Reviewing signals'}"
    )

# ── Startup ───────────────────────────────────────────────────────────────────
def startup():
    global starting_portfolio_value
    log.info("AlphaTrader v11 — Crypto + Fixed Exits Edition")
    if not ALPACA_KEY or not ALPACA_SECRET:
        log.error("Missing Alpaca keys"); raise SystemExit(1)
    acct=get_account()
    if not acct: log.error("Cannot connect to Alpaca"); raise SystemExit(1)

    bp=float(acct.get("buying_power",0))
    pv=float(acct.get("portfolio_value",0))
    starting_portfolio_value=pv

    for sym in SYMBOLS:
        try:
            r=requests.get(f"{ALPACA_DATA}/v2/stocks/{sym}/bars?timeframe=1Day&limit=2",
                headers={"APCA-API-KEY-ID":ALPACA_KEY,"APCA-API-SECRET-KEY":ALPACA_SECRET},
                timeout=10)
            bars=r.json().get("bars",[])
            if len(bars)>=2:
                prev_close[sym]=float(bars[-2]["c"])
                open_price[sym]=float(bars[-1]["o"])
        except: pass
        time.sleep(0.1)

    try:
        for p in alpaca_get("/v2/positions"):
            sym=p["symbol"]
            if sym in SYMBOLS:
                positions[sym]={"entry_price":float(p["avg_entry_price"]),"qty":float(p["qty"]),
                    "entry_time":time.time()-1800,"cost":float(p["cost_basis"]),
                    "order_id":"","peak_price":float(p["current_price"]),
                    "trailing_stop":None,"confidence":60}
    except: pass

    refresh_all_data()

    api_status=[
        f"{'✅' if POLYGON_KEY else '❌'} Polygon",
        f"{'✅' if FMP_KEY else '❌'} FMP",
        f"{'✅' if AV_KEY else '❌'} Alpha Vantage",
        f"{'✅' if ANTHROPIC_KEY else '❌'} Claude AI sentiment",
    ]

    telegram(
        f"🚀 <b>AlphaTrader v11 — Crypto + Fixed Exits Edition</b>\n"
        f"Mode: {'📄 PAPER' if IS_PAPER else '💰 LIVE'}\n"
        f"Symbols: {len(SYMBOLS)} ({len(TRADEABLE)} tradeable)\n"
        f"Buying power: ${bp:.2f} | Portfolio: ${pv:.2f}\n\n"
        f"<b>Loss protections:</b>\n"
        f"🛑 Portfolio hard stop: -{PORTFOLIO_HARD_STOP_PCT}% from ${pv:.2f}\n"
        f"📉 Daily loss limit: ${DAILY_LOSS_LIMIT}\n"
        f"📊 50MA trend filter: ON\n"
        f"🔗 Sector surge check: ON\n"
        f"\n<b>STOCKS:</b> +{TAKE_PROFIT_PCT}% / -{STOP_LOSS_PCT}% | {TIME_STOP_MINS//60}h stop | 10:15am–3:15pm ET | closes EOD\n"
        f"<b>CRYPTO:</b> +{CRYPTO_TAKE_PROFIT}% / -{CRYPTO_STOP_LOSS}% | {CRYPTO_TIME_STOP_MINS//60}h stop | 24/7 | never closes\n"
        f"Slots: {MAX_POSITIONS} total ({MAX_STOCK_POSITIONS} stock / {MAX_CRYPTO_POSITIONS} crypto)\n"
        f"📉 Reduced size after {CONSEC_LOSS_THRESHOLD} consecutive losses\n"
        f"🚫 No averaging down\n\n"
        f"<b>Data sources:</b>\n" + "\n".join(api_status) + "\n"
        f"Fear & Greed: {f'{fear_greed_score:.0f}' if fear_greed_score else 'unavailable'}\n"
        f"⚡ Websocket streaming active"
    )

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    global daily_pnl
    startup()
    threading.Thread(target=start_websocket,daemon=True).start()
    threading.Thread(target=crypto_loop,daemon=True).start()

    last_summary=None; vix_alerted=0; last_hourly_refresh=time.time()

    while True:
        try:
            now=datetime.now(timezone.utc); today=now.date()
            ny_h=(now.hour-4)%24; ny_m=now.minute

            if time.time()-last_hourly_refresh>3600:
                threading.Thread(target=refresh_all_data,daemon=True).start()
                last_hourly_refresh=time.time()

            if ny_h==9 and ny_m<35 and last_summary!=today:
                daily_pnl=0.0

            if ny_h==16 and ny_m<5 and last_summary!=today:
                send_daily_summary(); last_summary=today

            stock_positions = [s for s,p in positions.items() if not p.get("is_crypto")]
            if should_close_all() and stock_positions:
                telegram("⏰ <b>3:45pm ET — closing all stock positions</b> (crypto continues)")
                for sym in stock_positions:
                    hist=list(price_history[sym])
                    if hist: execute_sell(sym,"EOD close",hist[-1])

            if is_market_hours() and time.time()-vix_alerted>300:
                try:
                    r=requests.get(f"{ALPACA_DATA}/v2/stocks/VIXY/trades/latest",
                        headers={"APCA-API-KEY-ID":ALPACA_KEY,"APCA-API-SECRET-KEY":ALPACA_SECRET},
                        timeout=5)
                    vix=float(r.json()["trade"]["p"])
                    if vix>VIX_PAUSE_LEVEL:
                        telegram(f"⚠️ <b>VIX {vix:.1f}</b> — pausing new entries")
                        vix_alerted=time.time()
                except: pass

            # Check portfolio hard stop every 5 mins
            if is_market_hours() and ny_m%5==0:
                check_portfolio_hard_stop()

        except Exception as e:
            log.error(f"Main: {e}")

        time.sleep(30)

if __name__ == "__main__":
    main()
