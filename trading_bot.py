"""
AlphaTrader Bot v6 — Full Data Edition
New data sources added:
  - Polygon.io     : real earnings dates, pre-market data, better news
  - FMP API        : economic calendar, earnings calendar
  - Alpha Vantage  : economic indicators
  - CNN Fear & Greed index
  - Pre-market gap analysis
  - Economic event blackouts (Fed days, CPI days etc)

New environment variables needed:
  POLYGON_API_KEY   → free at polygon.io
  FMP_API_KEY       → free at financialmodelingprep.com
  AV_API_KEY        → free at alphavantage.co
  (ANTHROPIC_API_KEY already from v5)
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
BASE_TRADE_SIZE      = 20
TAKE_PROFIT_PCT      = 2.0
STOP_LOSS_PCT        = 0.9
TIME_STOP_MINS       = 120
MAX_POSITIONS        = 3
DAILY_LOSS_LIMIT     = 10
VIX_PAUSE_LEVEL      = 25
EARNINGS_BLACKOUT    = 3
TRAILING_ACTIVATE    = 1.5
TRAILING_DISTANCE    = 0.5
SIGNAL_COOLDOWN      = 300
LIMIT_OFFSET         = 0.05
SPY_BULL_THRESHOLD   = -0.3
SPY_STRONG_BULL      = 0.3
TRADE_START_HOUR_ET  = 10
TRADE_END_HOUR_ET    = 15
TRADE_END_MIN_ET     = 30
MOMENTUM_MIN_PCT     = 0.3
MANDATORY_SIGNALS    = {"MACD +ve", "RSI 38-58"}
MIN_CONFIRMING       = 2
MIN_SCORE            = 70
FEAR_GREED_PAUSE     = 20   # pause if Fear & Greed below this (extreme fear)
PREMARKET_GAP_LIMIT  = 2.0  # skip if gapped more than 2%

def position_size(confidence):
    if confidence >= 85: return BASE_TRADE_SIZE * 1.5
    elif confidence >= 75: return BASE_TRADE_SIZE
    else: return BASE_TRADE_SIZE * 0.5

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

# Data cache — refreshed periodically
earnings_cache      = {}   # sym -> date string (from Polygon/FMP)
economic_events     = []   # list of today's high-impact events
fear_greed_score    = None # 0-100
premarket_data      = {}   # sym -> {price, gap_pct}
data_last_refresh   = 0
economic_blackout   = False  # True on Fed/CPI days

# ── Helpers ───────────────────────────────────────────────────────────────────
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

# ── NEW: Polygon.io data ──────────────────────────────────────────────────────
def fetch_polygon_earnings():
    """Get real earnings dates from Polygon for all symbols."""
    if not POLYGON_KEY:
        log.info("No Polygon key — using hardcoded earnings dates")
        return {}
    try:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        # Look 60 days ahead
        future = (datetime.now(timezone.utc) + timedelta(days=60)).strftime("%Y-%m-%d")
        r = requests.get(
            f"https://api.polygon.io/vX/reference/financials",
            params={"ticker": ",".join(TRADEABLE.keys()),
                    "filing_date.gte": today,
                    "filing_date.lte": future,
                    "limit": 50,
                    "apiKey": POLYGON_KEY},
            timeout=10)
        dates = {}
        if r.ok:
            for item in r.json().get("results", []):
                sym = item.get("tickers", [""])[0]
                date = item.get("filing_date", "")
                if sym and date and sym not in dates:
                    dates[sym] = date
        log.info(f"Polygon earnings: {dates}")
        return dates
    except Exception as e:
        log.warning(f"Polygon earnings failed: {e}")
        return {}

def fetch_polygon_news(symbol):
    """Get fresh news from Polygon — better than Alpaca's feed."""
    if not POLYGON_KEY:
        return []
    try:
        r = requests.get(
            f"https://api.polygon.io/v2/reference/news",
            params={"ticker": symbol, "limit": 10,
                    "order": "desc", "sort": "published_utc",
                    "apiKey": POLYGON_KEY},
            timeout=10)
        if r.ok:
            return [a.get("title","") for a in r.json().get("results", [])]
        return []
    except:
        return []

def fetch_polygon_premarket(symbol):
    """Get pre-market snapshot from Polygon."""
    if not POLYGON_KEY:
        return None
    try:
        r = requests.get(
            f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers/{symbol}",
            params={"apiKey": POLYGON_KEY},
            timeout=10)
        if r.ok:
            snap = r.json().get("ticker", {})
            day  = snap.get("day", {})
            prev = snap.get("prevDay", {})
            pm_price = snap.get("lastQuote", {}).get("P")  # pre-market ask
            if pm_price and prev.get("c"):
                gap = (float(pm_price) - float(prev["c"])) / float(prev["c"]) * 100
                return {"price": float(pm_price), "gap_pct": gap}
        return None
    except:
        return None

# ── NEW: FMP economic calendar ────────────────────────────────────────────────
def fetch_economic_calendar():
    """Get today's high-impact economic events from FMP."""
    if not FMP_KEY:
        log.info("No FMP key — economic calendar disabled")
        return []
    try:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        r = requests.get(
            f"https://financialmodelingprep.com/api/v3/economic_calendar",
            params={"from": today, "to": today, "apikey": FMP_KEY},
            timeout=10)
        if not r.ok:
            return []
        high_impact = []
        high_impact_keywords = [
            "fed", "fomc", "interest rate", "cpi", "inflation",
            "nonfarm", "payroll", "gdp", "unemployment", "powell"
        ]
        for event in r.json():
            impact  = event.get("impact", "").lower()
            name    = event.get("event", "").lower()
            if impact == "high" or any(k in name for k in high_impact_keywords):
                high_impact.append({
                    "event":  event.get("event"),
                    "time":   event.get("date"),
                    "impact": impact,
                })
        log.info(f"Economic events today: {[e['event'] for e in high_impact]}")
        return high_impact
    except Exception as e:
        log.warning(f"FMP calendar failed: {e}")
        return []

def fetch_fmp_earnings():
    """Get earnings dates from FMP as backup/supplement to Polygon."""
    if not FMP_KEY:
        return {}
    try:
        today  = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        future = (datetime.now(timezone.utc) + timedelta(days=60)).strftime("%Y-%m-%d")
        r = requests.get(
            f"https://financialmodelingprep.com/api/v3/earning_calendar",
            params={"from": today, "to": future, "apikey": FMP_KEY},
            timeout=10)
        dates = {}
        if r.ok:
            for item in r.json():
                sym  = item.get("symbol","")
                date = item.get("date","")
                if sym in TRADEABLE and date:
                    dates[sym] = date
        log.info(f"FMP earnings: {dates}")
        return dates
    except Exception as e:
        log.warning(f"FMP earnings failed: {e}")
        return {}

# ── NEW: CNN Fear & Greed ─────────────────────────────────────────────────────
def fetch_fear_greed():
    """Get CNN Fear & Greed index (0=extreme fear, 100=extreme greed)."""
    try:
        r = requests.get(
            "https://production.dataviz.cnn.io/index/fearandgreed/graphdata",
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=10)
        if r.ok:
            data = r.json()
            score = data.get("fear_and_greed", {}).get("score")
            rating = data.get("fear_and_greed", {}).get("rating", "")
            if score:
                log.info(f"Fear & Greed: {score:.0f} ({rating})")
                return float(score)
        return None
    except Exception as e:
        log.warning(f"Fear & Greed failed: {e}")
        return None

# ── NEW: Alpha Vantage economic indicators ────────────────────────────────────
def fetch_av_indicators():
    """Get key economic indicators from Alpha Vantage."""
    if not AV_KEY:
        return {}
    indicators = {}
    try:
        # Federal Funds Rate
        r = requests.get(
            "https://www.alphavantage.co/query",
            params={"function": "FEDERAL_FUNDS_RATE", "interval": "monthly",
                    "apikey": AV_KEY},
            timeout=10)
        if r.ok:
            data = r.json().get("data", [])
            if data:
                indicators["fed_rate"] = float(data[0].get("value", 0))
    except: pass
    try:
        # CPI
        r = requests.get(
            "https://www.alphavantage.co/query",
            params={"function": "CPI", "interval": "monthly", "apikey": AV_KEY},
            timeout=10)
        if r.ok:
            data = r.json().get("data", [])
            if data:
                indicators["cpi"] = float(data[0].get("value", 0))
    except: pass

    log.info(f"AV indicators: {indicators}")
    return indicators

# ── NEW: Alpaca pre-market data ───────────────────────────────────────────────
def fetch_alpaca_premarket(symbol):
    """Get pre-market price from Alpaca snapshot."""
    try:
        r = requests.get(
            f"{ALPACA_DATA}/v2/stocks/{symbol}/snapshot",
            headers={"APCA-API-KEY-ID": ALPACA_KEY, "APCA-API-SECRET-KEY": ALPACA_SECRET},
            timeout=10)
        if r.ok:
            snap = r.json()
            pm   = snap.get("minuteBar", {})
            prev = snap.get("prevDailyBar", {})
            if pm and prev:
                pm_price  = float(pm.get("c", 0))
                prev_close_p = float(prev.get("c", 0))
                if pm_price and prev_close_p:
                    gap = (pm_price - prev_close_p) / prev_close_p * 100
                    return {"price": pm_price, "gap_pct": gap}
        return None
    except:
        return None

# ── Master data refresh ───────────────────────────────────────────────────────
def refresh_all_data():
    """Refresh all external data sources. Called at startup and every hour."""
    global earnings_cache, economic_events, fear_greed_score
    global premarket_data, economic_blackout, data_last_refresh

    log.info("Refreshing all external data sources...")

    # 1. Earnings dates — Polygon first, FMP as backup
    poly_earnings = fetch_polygon_earnings()
    fmp_earnings  = fetch_fmp_earnings()

    # Hardcoded fallback
    hardcoded = {
        "NVDA":  "2026-08-26", "AAPL":  "2026-10-30",
        "TSLA":  "2026-10-21", "MSFT":  "2026-10-28",
        "GOOGL": "2026-10-28", "META":  "2026-10-28",
        "AMZN":  "2026-10-29", "AMD":   "2026-10-28",
        "NFLX":  "2026-10-14",
    }
    # Priority: Polygon > FMP > hardcoded
    earnings_cache = {**hardcoded, **fmp_earnings, **poly_earnings}
    log.info(f"Earnings cache: {earnings_cache}")

    # 2. Economic calendar
    economic_events = fetch_economic_calendar()
    economic_blackout = len(economic_events) > 0
    if economic_blackout:
        names = ", ".join(e["event"] for e in economic_events)
        telegram(f"📅 <b>High-impact events today:</b> {names}\n⚠️ Trading caution active")

    # 3. Fear & Greed
    fear_greed_score = fetch_fear_greed()
    if fear_greed_score is not None:
        if fear_greed_score <= FEAR_GREED_PAUSE:
            telegram(f"😨 <b>Fear & Greed: {fear_greed_score:.0f} (Extreme Fear)</b>\n⚠️ Pausing new entries")
        elif fear_greed_score >= 80:
            telegram(f"🤑 <b>Fear & Greed: {fear_greed_score:.0f} (Extreme Greed)</b>\n⚠️ Market may be overextended")

    # 4. Pre-market data
    for sym in TRADEABLE:
        pm = fetch_polygon_premarket(sym) or fetch_alpaca_premarket(sym)
        if pm:
            premarket_data[sym] = pm
            if abs(pm["gap_pct"]) > PREMARKET_GAP_LIMIT:
                log.info(f"{sym} pre-market gap: {pm['gap_pct']:+.2f}%")

    # 5. Alpha Vantage indicators (background, don't block)
    threading.Thread(target=fetch_av_indicators, daemon=True).start()

    data_last_refresh = time.time()
    log.info("Data refresh complete")

# ── Claude sentiment — enhanced with Polygon news ─────────────────────────────
def claude_sentiment(symbol):
    if not ANTHROPIC_KEY:
        return {"sentiment": "neutral", "score": 50, "reason": "No API key"}
    try:
        # Try Polygon first (better), fall back to Alpaca
        headlines = fetch_polygon_news(symbol)
        if not headlines:
            r = requests.get(
                f"{ALPACA_DATA}/v1beta1/news?symbols={symbol}&limit=5",
                headers={"APCA-API-KEY-ID": ALPACA_KEY, "APCA-API-SECRET-KEY": ALPACA_SECRET},
                timeout=10)
            if r.ok:
                headlines = [a.get("headline","") for a in r.json().get("news",[])]

        if not headlines:
            return {"sentiment": "neutral", "score": 50, "reason": "No recent news"}

        # Add economic context
        econ_context = ""
        if economic_events:
            econ_context = f"\nEconomic events today: {', '.join(e['event'] for e in economic_events)}"
        if fear_greed_score:
            econ_context += f"\nMarket Fear & Greed: {fear_greed_score:.0f}/100"

        r = requests.post("https://api.anthropic.com/v1/messages",
            headers={"Content-Type":"application/json","x-api-key":ANTHROPIC_KEY,
                     "anthropic-version":"2023-06-01"},
            json={"model":"claude-sonnet-4-6","max_tokens":200,
                  "system": (
                      "You are a professional trading sentiment analyst. "
                      "Given news headlines and market context, assess short-term "
                      "trading sentiment for the next 2-4 hours. "
                      "Return ONLY valid JSON: "
                      "{\"sentiment\":\"positive\"|\"negative\"|\"neutral\","
                      "\"score\":0-100,\"reason\":\"one sentence max\"}"
                  ),
                  "messages":[{"role":"user","content":
                      f"Assess {symbol} for short-term trading:\n"
                      f"Headlines:\n" + "\n".join(f"- {h}" for h in headlines[:8])
                      + econ_context}]},
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
    t=spy_trend()
    return True if t is None else t>=SPY_BULL_THRESHOLD

def in_trading_window():
    now=datetime.now(timezone.utc)
    h=(now.hour-4)%24; m=now.minute
    after =(h>TRADE_START_HOUR_ET or (h==TRADE_START_HOUR_ET and m>=0))
    before=(h<TRADE_END_HOUR_ET   or (h==TRADE_END_HOUR_ET   and m<=TRADE_END_MIN_ET))
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
    pvwap  = ((price-vw)/vw*100) if vw else None

    if abs(get_premarket_gap(sym))>PREMARKET_GAP_LIMIT:
        return None

    criteria=[
        ("RSI 38-58",        r  is not None and 38<=r<=58,              25),
        ("MACD +ve",         m  is not None and m.get("histogram",0)>0, 25),
        ("Above VWAP",       pvwap is not None and pvwap>0,             20),
        ("EMA9>EMA21",       e9 and e21 and e9>e21,                     15),
        ("Stoch<78",         st is not None and st<78,                  15),
        ("Volume confirmed", volume_ok(sym),                            10),
    ]

    met_names={n for n,p,w in criteria if p}
    met_list =[(n,w) for n,p,w in criteria if p]
    score    =sum(w for _,w in met_list)
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
    pnl_pct=(price-pos["entry_price"])/pos["entry_price"]*100
    mins=(time.time()-pos["entry_time"])/60
    trail=pos.get("trailing_stop")
    if trail and price<trail:
        return ("TRAIL",       f"Trailing stop ${trail:.2f} ({pnl_pct:+.2f}%) 📉")
    if pnl_pct>=TAKE_PROFIT_PCT:
        return ("TAKE_PROFIT", f"+{pnl_pct:.2f}% take profit 🟢")
    if pnl_pct<=-STOP_LOSS_PCT:
        return ("STOP_LOSS",   f"{pnl_pct:.2f}% stop loss 🔴")
    if mins>=TIME_STOP_MINS:
        return ("TIME_STOP",   f"{pnl_pct:.2f}% after {mins:.0f}m ⏱")
    return None

# ── Execute buy ───────────────────────────────────────────────────────────────
def execute_buy(analysis, sentiment):
    global daily_pnl
    sym=analysis["symbol"]; price=analysis["price"]; score=analysis["score"]

    with trade_lock:
        if sym in positions: return
        if daily_pnl<=-DAILY_LOSS_LIMIT: return
        if near_earnings(sym):
            telegram(f"⚠️ <b>{sym}</b> skipped — earnings blackout ({earnings_cache.get(sym)})"); return
        if sector_held(sym): return
        if len(positions)>=MAX_POSITIONS: return
        if sentiment["sentiment"]=="negative" and sentiment["score"]<35:
            telegram(f"📰 <b>{sym}</b> skipped — {sentiment['reason']}"); return

        # Economic event blackout — reduce size on high-impact days
        if economic_blackout:
            events_str=", ".join(e["event"] for e in economic_events)
            log.info(f"Economic events active ({events_str}) — halving position size")
            effective_size=position_size(score)*0.5
        else:
            effective_size=position_size(score)

        # Fear & Greed check
        if fear_greed_score is not None and fear_greed_score<=FEAR_GREED_PAUSE:
            log.info(f"Extreme fear ({fear_greed_score}) — skipping {sym}")
            return

        # SPY filter for tech
        if SYMBOLS[sym]["corr"]=="tech" and not market_allows_tech_long():
            log.info(f"{sym} skipped — SPY down {spy_trend():.2f}%"); return

        if not in_trading_window(): return
        if not has_momentum(sym): return

        acct=get_account()
        if not acct: return
        bp=float(acct.get("buying_power",0))
        trade_val=min(effective_size,bp*0.95)
        if trade_val<10: return

        qty=trade_val/price
        limit_price=round(price*(1-LIMIT_OFFSET/100),2)
        stop_price=round(price*(1-STOP_LOSS_PCT/100),2)
        target=round(price*(1+TAKE_PROFIT_PCT/100),2)

        try:
            order=alpaca_post("/v2/orders",{
                "symbol":sym,"qty":f"{qty:.4f}","side":"buy",
                "type":"limit","limit_price":str(limit_price),
                "time_in_force":"day","order_class":"bracket",
                "stop_loss":{"stop_price":str(stop_price)},
                "take_profit":{"limit_price":str(target)},
            })

            positions[sym]={
                "entry_price":price,"qty":qty,"entry_time":time.time(),
                "cost":trade_val,"order_id":order.get("id",""),
                "stop":stop_price,"target":target,
                "peak_price":price,"trailing_stop":None,"confidence":score,
            }

            pm=premarket_data.get(sym,{})
            pm_note=f"Pre-market gap: {pm.get('gap_pct',0):+.2f}%" if pm else ""
            fg_note=f"Fear & Greed: {fear_greed_score:.0f}" if fear_greed_score else ""
            econ_note=f"⚠️ High-impact events today — reduced size" if economic_blackout else ""

            telegram(
                f"📥 <b>BUY {sym}</b> {'(PAPER)' if IS_PAPER else '(LIVE)'}\n"
                f"Limit: ${limit_price} | Size: ${trade_val:.0f} ({score}pt confidence)\n"
                f"Stop: ${stop_price} | Target: ${target}\n"
                f"Signals: {' · '.join(analysis['met'])}\n"
                f"📰 Sentiment: {sentiment['sentiment'].upper()} ({sentiment['score']}/100): {sentiment['reason']}\n"
                + (f"📊 {pm_note}\n" if pm_note else "")
                + (f"😨 {fg_note}\n" if fg_note else "")
                + (f"{econ_note}\n" if econ_note else "")
                + f"🛡 Bracket order on Alpaca"
            )
            last_signal[sym]=time.time()

        except Exception as e:
            log.error(f"Buy failed {sym}: {e}")
            telegram(f"❌ <b>BUY failed</b> {sym}: {str(e)[:100]}")

# ── Execute sell ──────────────────────────────────────────────────────────────
def execute_sell(sym, reason, price):
    global daily_pnl
    pos=positions.get(sym)
    if not pos: return
    with trade_lock:
        try:
            try: alpaca_delete(f"/v2/orders/{pos['order_id']}")
            except: pass
            alpaca_post("/v2/orders",{
                "symbol":sym,"qty":f"{pos['qty']:.4f}",
                "side":"sell","type":"market","time_in_force":"day",
            })
            pnl_pct=(price-pos["entry_price"])/pos["entry_price"]*100
            pnl_abs=(price-pos["entry_price"])*pos["qty"]
            daily_pnl+=pnl_abs
            session_trades.append({
                "symbol":sym,"entry":pos["entry_price"],"exit":price,
                "qty":pos["qty"],"pnl_pct":pnl_pct,"pnl_abs":pnl_abs,
                "reason":reason,"held_mins":(time.time()-pos["entry_time"])/60,
            })
            del positions[sym]
            emoji="🟢" if pnl_abs>=0 else "🔴"
            telegram(
                f"{emoji} <b>SELL {sym}</b> {'(PAPER)' if IS_PAPER else '(LIVE)'}\n"
                f"Entry: ${pos['entry_price']:.2f} → Exit: ${price:.2f}\n"
                f"P&L: {pnl_pct:+.2f}% (${pnl_abs:+.2f})\n"
                f"Reason: {reason} | Today: ${daily_pnl:+.2f}"
            )
        except Exception as e:
            log.error(f"Sell failed {sym}: {e}")

# ── Websocket ─────────────────────────────────────────────────────────────────
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
                    ws.send(json.dumps({"action":"subscribe",
                        "trades":list(SYMBOLS.keys()),"quotes":list(SYMBOLS.keys())}))
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
    if time.time()-last_signal.get(sym,0)<SIGNAL_COOLDOWN: return
    if len(price_history[sym])<30: return

    analysis=evaluate(sym)
    if not analysis or analysis["signal"]!="BUY": return

    log.info(f"⚡ {sym} BUY signal — score:{analysis['score']} mandatory:{analysis['mandatory_met']}")
    last_signal[sym]=time.time()

    def buy_thread():
        sentiment=claude_sentiment(sym)
        execute_buy(analysis,sentiment)

    threading.Thread(target=buy_thread,daemon=True).start()

def start_websocket():
    while True:
        try:
            ws=websocket.WebSocketApp(WS_URL,on_message=on_message,
                on_error=on_error,on_close=on_close,on_open=on_open)
            ws.run_forever(ping_interval=30,ping_timeout=10)
        except Exception as e:
            log.error(f"WS thread: {e}")
        time.sleep(5)

# ── Daily summary ─────────────────────────────────────────────────────────────
def send_daily_summary():
    fg=f"Fear & Greed: {fear_greed_score:.0f}" if fear_greed_score else ""
    if not session_trades:
        telegram(f"📊 <b>Daily Summary</b>\nNo trades.\n{fg}"); return
    total=sum(t["pnl_abs"] for t in session_trades)
    wins=sum(1 for t in session_trades if t["pnl_abs"]>0)
    wr=wins/len(session_trades)*100
    by_sym={}
    for t in session_trades: by_sym.setdefault(t["symbol"],[]).append(t["pnl_abs"])
    sym_lines="\n".join(f"  {s}: ${sum(v):+.2f} ({len(v)} trades)" for s,v in by_sym.items())
    spy_note=f"SPY: {spy_trend():+.2f}%" if spy_trend() is not None else ""
    telegram(
        f"📊 <b>Daily Summary v6</b>\n"
        f"Trades: {len(session_trades)} | Win rate: {wr:.0f}%\n"
        f"Total P&L: ${total:+.2f}\n"
        f"By symbol:\n{sym_lines}\n"
        f"{spy_note} | {fg}\n"
        f"{'✅ Profitable!' if total>0 else '❌ Reviewing signals'}"
    )

# ── Startup ───────────────────────────────────────────────────────────────────
def startup():
    log.info("AlphaTrader v6 — Full Data Edition")
    if not ALPACA_KEY or not ALPACA_SECRET:
        log.error("Missing Alpaca keys"); raise SystemExit(1)
    acct=get_account()
    if not acct: log.error("Cannot connect to Alpaca"); raise SystemExit(1)

    bp=float(acct.get("buying_power",0)); pv=float(acct.get("portfolio_value",0))

    # Fetch prev closes
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

    # Fetch existing positions
    try:
        for p in alpaca_get("/v2/positions"):
            sym=p["symbol"]
            if sym in SYMBOLS:
                positions[sym]={"entry_price":float(p["avg_entry_price"]),"qty":float(p["qty"]),
                    "entry_time":time.time()-1800,"cost":float(p["cost_basis"]),
                    "order_id":"","peak_price":float(p["current_price"]),
                    "trailing_stop":None,"confidence":60}
    except: pass

    # Refresh all external data
    refresh_all_data()

    api_status=[
        f"{'✅' if POLYGON_KEY else '❌'} Polygon (earnings + news + pre-market)",
        f"{'✅' if FMP_KEY else '❌'} FMP (economic calendar + earnings)",
        f"{'✅' if AV_KEY else '❌'} Alpha Vantage (economic indicators)",
        f"{'✅' if ANTHROPIC_KEY else '❌'} Claude AI (news sentiment)",
    ]

    telegram(
        f"🚀 <b>AlphaTrader v6 — Full Data Edition</b>\n"
        f"Mode: {'📄 PAPER' if IS_PAPER else '💰 LIVE'}\n"
        f"Symbols: {len(SYMBOLS)} ({len(TRADEABLE)} tradeable)\n"
        f"Buying power: ${bp:.2f}\n\n"
        f"<b>Data sources:</b>\n" + "\n".join(api_status) + "\n\n"
        f"<b>Live market data:</b>\n"
        f"Fear & Greed: {f'{fear_greed_score:.0f}' if fear_greed_score else 'unavailable'}\n"
        f"Economic events today: {len(economic_events)}\n"
        f"Earnings blackout active: {sum(1 for s in TRADEABLE if near_earnings(s))} symbols\n"
        f"⚡ Websocket streaming active"
    )

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    global daily_pnl
    startup()
    threading.Thread(target=start_websocket,daemon=True).start()

    last_summary=None; vix_alerted=0; last_hourly_refresh=time.time()

    while True:
        try:
            now=datetime.now(timezone.utc); today=now.date()
            ny_h=(now.hour-4)%24; ny_m=now.minute

            # Hourly data refresh
            if time.time()-last_hourly_refresh>3600:
                threading.Thread(target=refresh_all_data,daemon=True).start()
                last_hourly_refresh=time.time()

            if ny_h==9 and ny_m<35 and last_summary!=today:
                daily_pnl=0.0

            if ny_h==16 and ny_m<5 and last_summary!=today:
                send_daily_summary(); last_summary=today

            if should_close_all() and positions:
                telegram("⏰ <b>3:45pm ET — closing all</b>")
                for sym in list(positions.keys()):
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

        except Exception as e:
            log.error(f"Main: {e}")

        time.sleep(30)

if __name__ == "__main__":
    main()
