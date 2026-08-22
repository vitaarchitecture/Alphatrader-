"""
AlphaTrader Bot v21 — Time Stop Fix + Mega Drop + Crypto On
v20: full audit of v19 found its flatten fix was still wrong when the
market is closed: (a) wait_for_fill cancels unfilled orders on timeout,
killing the very GTC order meant to queue for the open; (b) the next
reconcile pass's cancel_symbol_orders would kill it anyway; (c) re-placing
reused the same client_order_id -> 422 spam. Net effect: no standing
buy-back existed overnight, only log noise.

v20 flatten protocol (tested against a closed-market mock):
  1. If an open BUY for the symbol already exists -> it IS the queued
     flatten. Leave it alone. Do nothing else that pass.
  2. Cancel only SELL-side strays (they are how shorts happen), never
     our own buy-back.
  3. Place the GTC buy with a unique id; if market open, wait+verify;
     if closed, LEAVE IT QUEUED (no wait_for_fill, no cancel).
  4. When a later pass sees the symbol flat and an alert was pending,
     send ONE '✅ flattened' confirmation.
Also: ignore dust positions (<$1) instead of adopting them; whole-share
fallback refuses symbols where 1 share costs >3x MAX_NOTIONAL.
v19 fixes a real incident: v18's reconcile() tried to flatten a short with
a DAY market order for a STOCK outside market hours. Day orders can't
execute (or even queue reliably) when the exchange is shut, so the buy
silently failed and the same "flattening now" alert repeated every 60s
forever with no way to stop.

Fixes:
  * Flatten order uses time_in_force="gtc" for stocks too — queues and
    fills at next open instead of silently failing when markets are shut.
  * The flatten VERIFIES the fill (checks broker qty afterward) before
    declaring success — no more "tried and moved on" with no confirmation.
  * Per-symbol alert cooldown (10 min): the short IS still being worked
    every reconcile pass, but you are only told about it once per window,
    not every single minute.
  * If markets are closed and the symbol is a stock, the first alert
    says so explicitly, so you know why it can't fill immediately.
v18 on top of v17 (the two 'expert review' fixes, plus visibility):
  * ALL indicators now computed on 1-MINUTE BARS, not raw ticks.
    Ticks arrive at wildly different rates per symbol, so tick-RSI meant
    something different for every stock at every moment. Bars are
    aggregated live from the websocket and BACKFILLED via REST at startup
    — 100 bars of history from minute one, no more 30-min warm-up.
  * ATR-SCALED EXITS + CONSTANT-RISK SIZING: each entry gets a stop and
    target derived from that symbol's own measured volatility (ATR14 on
    1-min bars, scaled to the hold horizon, clamped). Position size is
    then set so every trade risks the same ~$0.25. SMCI's stop is wide,
    AAPL's is tight, and a stop-out costs the same either way.
  * CRYPTO RE-ENABLED (2 slots): the pause reason — degenerate snapshot
    RSI — is gone. Crypto indicators come from real 1-min OHLC bars.
  * TELEGRAM COMMANDS: message the bot /balance /summary /positions
    /status /help any time for live account state on demand.
v17 on top of v16:
  * TRUE $20 sizing: stocks try notional (fractional) market orders first,
    auto-fallback to whole-share if the account rejects notional.
  * Blocked-signal telemetry: every rejected signal is counted by reason
    and reported in the daily summary — filter tuning becomes data-driven.
  * Volatility A/B in summary: P&L split high-vol vs mega-cap vs other.
  * Sentiment re-added as ASYNC ADVISORY (never blocks/vetoes entries;
    logged and attached to trades for later evaluation).
  * Real VIX level via FMP ^VIX quote (old VIXY-price proxy was wrong).
  * Daily P&L / telemetry reset once per trading day (was: repeated 9:00-9:35).
  * session_trades cleared after summary (multi-day summaries were wrong).
  * Missed-EOD alert on startup if stocks are held while market closed.
  * Exit spawn throttle via exit_pending set.
==========================================
Architectural changes (why this version exists):

  PROBLEM (v8-v15): resting GTC stop/target orders at the broker kept
  firing into already-closed positions, creating accidental SHORTS
  (SMCI -8, XOM -1). Duplicate buys slipped through 1s apart. The EOD
  close re-fired 10+ times. Internal state drifted from Alpaca reality.

  SOLUTION (v16):
  1. ZERO resting orders. All stops/targets/time-stops are software-
     managed from live prices. Any open order at Alpaca is treated as
     foreign and cancelled on sight.
  2. ONE exit path: close_position(). It cancels symbol orders, then
     VERIFIES real holdings at Alpaca, and only sells what actually
     exists. Selling a flat position is structurally impossible.
  3. IDEMPOTENT orders: deterministic client_order_id per signal +
     an in_flight guard. The same buy/sell cannot execute twice —
     locally or at the broker.
  4. RECONCILER: every 60s internal state is diffed against Alpaca.
     Longs we don't know -> adopted. Positions that vanished -> purged.
     Qty drift -> corrected. SHORTS -> alert + auto-flattened (buy back).
  5. EOD close fires exactly once per day.
  6. Entries verify their own fill (poll up to 10s) and record the
     REAL filled qty/price, not an estimate.

  Crypto remains PAUSED for entries (MAX_CRYPTO_POSITIONS=0) until the
  data feed is rebuilt on 1-minute bars — 30s snapshot RSI was degenerate.
  Any adopted crypto position is still exit-managed.
"""

import os, re, time, math, json, logging, threading
import requests, websocket
from datetime import datetime, timezone, timedelta
from collections import deque

VERSION = "v21"

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("alphatrader")

# ── Credentials & endpoints ───────────────────────────────────────────────────
ALPACA_KEY    = os.environ.get("ALPACA_KEY_ID", "")
ALPACA_SECRET = os.environ.get("ALPACA_SECRET_KEY", "")
TG_TOKEN      = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TG_CHAT_ID    = os.environ.get("TELEGRAM_CHAT_ID", "")
ANTHROPIC_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
POLYGON_KEY   = os.environ.get("POLYGON_API_KEY", "")
FMP_KEY       = os.environ.get("FMP_API_KEY", "")
IS_PAPER      = os.environ.get("PAPER_TRADING", "true").lower() == "true"

ALPACA_BASE = "https://paper-api.alpaca.markets" if IS_PAPER else "https://api.alpaca.markets"
ALPACA_DATA = "https://data.alpaca.markets"
WS_URL      = "wss://stream.data.alpaca.markets/v2/iex"

# ── Universe ──────────────────────────────────────────────────────────────────
SYMBOLS = {
    "NVDA":{"sector":"semis","corr":"tech"},   "AMD":{"sector":"semis","corr":"tech"},
    "SMCI":{"sector":"semis","corr":"tech"},   "MU":{"sector":"semis","corr":"tech"},
    "INTC":{"sector":"semis","corr":"tech"},
    "PLTR":{"sector":"software","corr":"tech"},"TSLA":{"sector":"ev","corr":"tech"},
    "NFLX":{"sector":"stream","corr":"tech"},  "DIS":{"sector":"stream","corr":"consumer"},
    "JPM":{"sector":"banks","corr":"fin"},     "BAC":{"sector":"banks","corr":"fin"},
    "GS":{"sector":"banks","corr":"fin"},      "LLY":{"sector":"pharma","corr":"health"},
    "XOM":{"sector":"oil","corr":"energy"},    "CVX":{"sector":"oil","corr":"energy"},
    "SPY":{"sector":"etf","corr":"market"},    "GLD":{"sector":"gold","corr":"hedge"},
    "TLT":{"sector":"bonds","corr":"hedge"},   "USO":{"sector":"oiletf","corr":"commodity"},
}
TRADEABLE      = {k:v for k,v in SYMBOLS.items() if k != "SPY"}
HIGHVOL_GROUP  = {"SMCI","PLTR","TSLA","MU"}
MEGA_GROUP     = {"AAPL","MSFT","GOOGL","META","AMZN","NVDA","NFLX"}
CRYPTO_SYMBOLS = {"BTC/USD":"crypto", "ETH/USD":"crypto"}

def to_alpaca(sym):   return sym.replace("/", "")
def fmt_qty(q):
    """Alpaca-safe qty string: integers plain, fractionals trimmed."""
    if abs(q-round(q))<1e-9: return str(int(round(q)))
    return f"{q:.9f}".rstrip("0").rstrip(".")
def from_alpaca(raw):
    for cs in CRYPTO_SYMBOLS:
        if to_alpaca(cs) == raw: return cs
    return raw

# ── Parameters ────────────────────────────────────────────────────────────────
BASE_TRADE_SIZE   = 20
REDUCED_TRADE_SIZE= 10
TAKE_PROFIT_PCT   = 1.2
STOP_LOSS_PCT     = 0.9
TIME_STOP_MINS    = 120
TIME_STOP_FLOOR   = 0.4      # conditional: only cut if pnl below this at time-stop
CRYPTO_TP         = 3.0
CRYPTO_SL         = 1.5
CRYPTO_TS_MINS    = 240
TRAIL_ACTIVATE    = 1.5
TRAIL_DIST        = 0.5
MAX_POSITIONS     = 6
MAX_STOCK_POS     = 4
MAX_CRYPTO_POS    = 2        # re-enabled: indicators now on 1-min bars
DAILY_LOSS_LIMIT  = 10
PORT_HARD_STOP    = 10.0
VIX_PAUSE         = 25
EARN_BLACKOUT_D   = 3
FEAR_GREED_PAUSE  = 20
PREGAP_LIMIT      = 2.0
SIGNAL_COOLDOWN   = 300
RISK_PER_TRADE    = 0.25     # dollars risked at the stop, every trade
MIN_NOTIONAL      = 5
MAX_NOTIONAL      = 60
ATR_STOP_MULT     = 1.0      # stop  = 1.0 x expected hold-horizon move
ATR_TGT_MULT      = 2.0      # target= 2.0 x  (keeps ~2:1, your 33% breakeven)
STK_STOP_MIN,STK_STOP_MAX = 0.4, 2.0
STK_TGT_MIN, STK_TGT_MAX  = 0.8, 4.0
CRY_STOP_MIN,CRY_STOP_MAX = 0.8, 3.0
CRY_TGT_MIN, CRY_TGT_MAX  = 1.6, 6.0
MANDATORY         = {"MACD +ve", "RSI 38-58"}
MIN_CONFIRM       = 2
MIN_SCORE         = 70
MOMENTUM_MIN      = 0.05
SPY_BULL_MIN      = -0.3
W_START_H, W_START_M = 10, 15    # ET trading window
W_END_H,   W_END_M   = 15, 15
FILL_TIMEOUT_S    = 10
RECONCILE_EVERY_S = 60

# ── State (all mutations under state_lock) ────────────────────────────────────
state_lock     = threading.RLock()
positions      = {}                 # sym -> dict
in_flight      = set()              # syms with an order currently working
session_trades = []
daily_pnl      = 0.0
consecutive_losses = 0
portfolio_halted   = False
starting_pv    = None
eod_done_date  = None
summary_done_date = None

price_history  = {s: deque(maxlen=200) for s in list(SYMBOLS)+list(CRYPTO_SYMBOLS)}
bars           = {s: deque(maxlen=150) for s in list(SYMBOLS)+list(CRYPTO_SYMBOLS)}
cur_bar        = {}                 # sym -> forming 1-min bar
volume_history = {s: deque(maxlen=50)  for s in SYMBOLS}
open_price     = {s: None for s in list(SYMBOLS)+list(CRYPTO_SYMBOLS)}
prev_close     = {s: None for s in list(SYMBOLS)+list(CRYPTO_SYMBOLS)}
last_signal    = {s: 0 for s in list(SYMBOLS)+list(CRYPTO_SYMBOLS)}

earnings_cache={}; economic_events=[]; fear_greed=None; premarket={}; econ_blackout=False
vix_level=None
short_alerts={}                    # sym -> last alert time (throttle spam)
SHORT_ALERT_COOLDOWN_S=600
block_counts={}                    # reason -> count (reset daily)
exit_pending=set()                 # throttle duplicate exit threads
notional_ok=True                   # stocks: flip False on first 422, fallback to shares
last_trading_date=None

# ── HTTP helpers (with basic retry) ───────────────────────────────────────────
def _hdrs(extra=None):
    h={"APCA-API-KEY-ID":ALPACA_KEY,"APCA-API-SECRET-KEY":ALPACA_SECRET}
    if extra: h.update(extra)
    return h

def alpaca_get(path):
    for i in range(3):
        try:
            r=requests.get(f"{ALPACA_BASE}{path}",headers=_hdrs(),timeout=10)
            if r.status_code==429: time.sleep(1+i); continue
            r.raise_for_status(); return r.json()
        except requests.exceptions.HTTPError:
            raise
        except Exception:
            if i==2: raise
            time.sleep(1)

def alpaca_post(path, body):
    r=requests.post(f"{ALPACA_BASE}{path}",headers=_hdrs({"Content-Type":"application/json"}),
                    json=body,timeout=10)
    r.raise_for_status(); return r.json()

def alpaca_delete(path):
    return requests.delete(f"{ALPACA_BASE}{path}",headers=_hdrs(),timeout=10)

def get_account():
    try: return alpaca_get("/v2/account")
    except Exception as e:
        log.warning(f"account fetch: {e}"); return None

# ── Telegram (HTML-safe, plain fallback, errors visible) ──────────────────────
def telegram(msg):
    if not TG_TOKEN or not TG_CHAT_ID:
        log.info(f"[TG] {msg}"); return
    url=f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    try:
        r=requests.post(url,json={"chat_id":TG_CHAT_ID,"text":msg,"parse_mode":"HTML"},timeout=10)
        if r.ok: return
        log.warning(f"TG HTML rejected {r.status_code}: {r.text[:140]}")
        plain=re.sub(r"</?b>","",msg)
        r2=requests.post(url,json={"chat_id":TG_CHAT_ID,"text":plain},timeout=10)
        if not r2.ok: log.error(f"TG plain failed {r2.status_code}: {r2.text[:140]}")
    except Exception as e:
        log.warning(f"TG send: {e}")

# ── Broker truth helpers ──────────────────────────────────────────────────────
def broker_position(sym):
    """Real position at Alpaca. Returns (qty, avg_price) — qty may be negative."""
    try:
        p=alpaca_get(f"/v2/positions/{to_alpaca(sym)}")
        return float(p.get("qty",0)), float(p.get("avg_entry_price",0))
    except requests.exceptions.HTTPError as e:
        if e.response is not None and e.response.status_code==404: return 0.0, 0.0
        raise
    except Exception:
        raise

def cancel_symbol_orders(sym, side=None):
    """Cancel open orders for a symbol (optionally one side only).
    v16+ rests no orders, so any open order is foreign — EXCEPT a
    flatten buy-back deliberately queued for the next open."""
    try:
        for o in alpaca_get(f"/v2/orders?status=open&symbols={to_alpaca(sym)}") or []:
            if side and o.get("side")!=side: continue
            try:
                alpaca_delete(f"/v2/orders/{o['id']}")
                log.warning(f"Cancelled open order on {sym}: {o.get('side')} {o.get('qty')}")
            except Exception: pass
    except Exception as e:
        log.warning(f"cancel_symbol_orders {sym}: {e}")

def open_orders_for(sym, side=None):
    try:
        oo=alpaca_get(f"/v2/orders?status=open&symbols={to_alpaca(sym)}") or []
        return [o for o in oo if not side or o.get("side")==side]
    except Exception:
        return []

def wait_for_fill(order_id, timeout=FILL_TIMEOUT_S):
    """Poll an order until filled. Returns (filled_qty, avg_price) or (0,0)."""
    t0=time.time()
    while time.time()-t0 < timeout:
        try:
            o=alpaca_get(f"/v2/orders/{order_id}")
            st=o.get("status")
            if st=="filled":
                return float(o.get("filled_qty",0)), float(o.get("filled_avg_price") or 0)
            if st in ("canceled","expired","rejected"):
                return float(o.get("filled_qty",0) or 0), float(o.get("filled_avg_price") or 0)
        except Exception: pass
        time.sleep(1)
    try: alpaca_delete(f"/v2/orders/{order_id}")     # give up: cancel remainder
    except Exception: pass
    try:
        o=alpaca_get(f"/v2/orders/{order_id}")
        return float(o.get("filled_qty",0) or 0), float(o.get("filled_avg_price") or 0)
    except Exception:
        return 0.0, 0.0

# ── Entry (idempotent, fill-verified) ─────────────────────────────────────────
def place_entry(sym, price, score, met):
    global consecutive_losses
    is_c = sym in CRYPTO_SYMBOLS
    with state_lock:
        if portfolio_halted or sym in positions or sym in in_flight: return
        n_c=sum(1 for p in positions.values() if p.get("is_crypto"))
        n_s=len(positions)-n_c
        if len(positions)>=MAX_POSITIONS: return
        if is_c and n_c>=MAX_CRYPTO_POS: return
        if not is_c and n_s>=MAX_STOCK_POS: return
        in_flight.add(sym)
    try:
        stop_pct,target_pct=atr_exits(sym,is_c)
        tv=size_for(stop_pct)                       # constant $ risk per trade
        if score>=85: tv*=1.25
        elif score<75: tv*=0.75
        if consecutive_losses>=2: tv*=0.5
        if econ_blackout: tv*=0.5
        tv=min(max(tv,MIN_NOTIONAL),MAX_NOTIONAL)

        acct=get_account()
        if not acct: return
        tv=min(tv, float(acct.get("buying_power",0))*0.95)
        if tv<5: return

        global notional_ok
        coid=f"at17-{to_alpaca(sym)}-{int(time.time())//SIGNAL_COOLDOWN}"  # broker-side dedupe window
        order=None
        if is_c:
            body={"symbol":sym,"notional":f"{tv:.2f}","side":"buy",
                  "type":"market","time_in_force":"gtc","client_order_id":coid}
            try:
                order=alpaca_post("/v2/orders",body)
            except requests.exceptions.HTTPError as e:
                txt=e.response.text[:150] if e.response is not None else str(e)
                log.warning(f"Crypto entry rejected {sym}: {txt}"); return
        else:
            # TRUE $20 sizing: try fractional notional first (v8's 422s were
            # bracket-specific; plain notional was never tested). One rejection
            # flips notional_ok and we fall back to whole shares permanently.
            if notional_ok:
                try:
                    order=alpaca_post("/v2/orders",{"symbol":sym,"notional":f"{tv:.2f}",
                        "side":"buy","type":"market","time_in_force":"day",
                        "client_order_id":coid})
                except requests.exceptions.HTTPError as e:
                    txt=e.response.text[:150] if e.response is not None else str(e)
                    if "client_order_id" in txt:
                        log.warning(f"Entry deduped {sym}"); return
                    if "notional" in txt.lower() and "not supported" in txt.lower():
                        log.warning("Account doesnt support notional — switching to whole shares")
                        notional_ok=False
                    else:
                        log.warning(f"Notional rejected for {sym} ({txt[:80]}) — trying whole shares for this symbol")
            if order is None:
                if price > MAX_NOTIONAL*3:
                    log.info(f"{sym} skipped in whole-share fallback: 1 share "
                             f"${price:.0f} >> risk budget"); return
                shares=max(1,int(tv/price))
                try:
                    order=alpaca_post("/v2/orders",{"symbol":sym,"qty":str(shares),
                        "side":"buy","type":"market","time_in_force":"day",
                        "client_order_id":coid+"q"})
                except requests.exceptions.HTTPError as e:
                    txt=e.response.text[:150] if e.response is not None else str(e)
                    if "client_order_id" in txt or (e.response is not None and e.response.status_code==422):
                        log.warning(f"Entry deduped/rejected {sym}: {txt}")
                    else:
                        log.error(f"Entry order failed {sym}: {txt}")
                    return

        fq,fp=wait_for_fill(order.get("id",""))
        if fq<=0:
            log.warning(f"Entry not filled {sym} — abandoned"); return
        fp=fp or price
        with state_lock:
            positions[sym]={"entry_price":fp,"qty":fq,"entry_time":time.time(),
                            "cost":fq*fp,"is_crypto":is_c,"peak":fp,"trail":None,
                            "confidence":score,
                            "stop_pct":stop_pct,"target_pct":target_pct,
                            "intended":price}
        log.info(f"FILLED BUY {sym}: {fq} @ ${fp:.2f} (${fq*fp:.2f})")
        slip=(fp-price)/price*100 if price else 0
        try:
            telegram(f"📥 <b>BUY {sym}</b> {'(PAPER)' if IS_PAPER else '(LIVE)'}\n"
                     f"{fmt_qty(fq)} @ ${fp:,.2f} = ${fq*fp:.2f} (slip {slip:+.03f}%)\n"
                     f"ATR exits: +{target_pct}% / -{stop_pct}% | risk ~${RISK_PER_TRADE:.2f}\n"
                     f"Score {score}: {' · '.join(met)}")
        except Exception as te:
            log.error(f"Buy note failed {sym} (POSITION OPEN): {te}")
        threading.Thread(target=advisory_sentiment,args=(sym,),daemon=True).start()
    finally:
        with state_lock: in_flight.discard(sym)

# ── THE single exit path ──────────────────────────────────────────────────────
def close_position(sym, reason, ref_price=None):
    """Only way any position is ever closed. Verifies broker truth first;
    selling a flat position is impossible by construction."""
    global daily_pnl, consecutive_losses
    with state_lock:
        pos=positions.get(sym)
        if not pos or sym in in_flight: return
        in_flight.add(sym)
    try:
        cancel_symbol_orders(sym)                      # nothing may race the exit
        try:
            bq,_=broker_position(sym)
        except Exception as e:
            log.error(f"close {sym}: cannot verify broker position ({e}) — will retry")
            return
        if bq<=0:
            log.warning(f"close {sym}: broker qty {bq} — nothing to sell; purging internal state")
            with state_lock: positions.pop(sym,None)
            return
        qty=min(pos["qty"],bq)
        if pos.get("is_crypto"):
            body={"symbol":sym,"qty":f"{qty:.9f}".rstrip('0').rstrip('.'),
                  "side":"sell","type":"market","time_in_force":"gtc",
                  "client_order_id":f"at17x-{to_alpaca(sym)}-{int(time.time())}"}
        else:
            body={"symbol":sym,"qty":fmt_qty(qty),"side":"sell","type":"market",
                  "time_in_force":"day",
                  "client_order_id":f"at17x-{to_alpaca(sym)}-{int(time.time())}"}
        try:
            order=alpaca_post("/v2/orders",body)
        except requests.exceptions.HTTPError as e:
            log.error(f"close {sym}: sell rejected: {e.response.text[:150] if e.response is not None else e}")
            return
        fq,fp=wait_for_fill(order.get("id",""))
        if fq<=0:
            log.error(f"close {sym}: sell did not fill — position kept, will retry")
            return
        exit_p=fp or ref_price or pos["entry_price"]
        pnl_abs=(exit_p-pos["entry_price"])*fq
        pnl_pct=(exit_p-pos["entry_price"])/pos["entry_price"]*100
        with state_lock:
            daily_pnl+=pnl_abs
            consecutive_losses=0 if pnl_abs>0 else consecutive_losses+1
            session_trades.append({"symbol":sym,"entry":pos["entry_price"],"exit":exit_p,
                "qty":fq,"pnl_abs":pnl_abs,"pnl_pct":pnl_pct,"reason":reason,
                "held_mins":(time.time()-pos["entry_time"])/60,
                "sentiment":pos.get("sentiment"),
                "group":"highvol" if sym in HIGHVOL_GROUP else
                        ("mega" if sym in MEGA_GROUP else "other")})
            positions.pop(sym,None)
        log.info(f"FILLED SELL {sym}: {fq} @ ${exit_p:.2f} pnl {pnl_pct:+.2f}% (${pnl_abs:+.2f}) — {reason}")
        try:
            telegram(f"{'🟢' if pnl_abs>=0 else '🔴'} <b>SELL {sym}</b>\n"
                     f"${pos['entry_price']:,.2f} → ${exit_p:,.2f} | {pnl_pct:+.2f}% (${pnl_abs:+.2f})\n"
                     f"{reason} | Today ${daily_pnl:+.2f}")
        except Exception as te:
            log.error(f"Sell note failed {sym} (POSITION CLOSED): {te}")
    finally:
        with state_lock:
            in_flight.discard(sym); exit_pending.discard(sym)

# ── Reconciler: broker is the source of truth ─────────────────────────────────
def reconcile():
    try: broker=alpaca_get("/v2/positions") or []
    except Exception as e:
        log.warning(f"reconcile fetch: {e}"); return
    seen=set()
    for p in broker:
        raw=p["symbol"]; sym=from_alpaca(raw)
        try: q=float(p.get("qty",0))
        except Exception: q=0
        seen.add(sym)
        if q<0:
            is_c=sym in CRYPTO_SYMBOLS
            can_fill_now = is_c or is_market_hours()
            # 0) confirmation path lives below (q>=0); here we work the short.
            # 1) If our buy-back is already queued, leave it be — it will fill
            #    at the open. Re-placing/cancelling was v19's failure mode.
            existing_buys=open_orders_for(sym, side="buy")
            if existing_buys and not can_fill_now:
                continue
            last_alert=short_alerts.get(sym,0)
            if time.time()-last_alert>SHORT_ALERT_COOLDOWN_S:
                short_alerts[sym]=time.time()
                note = "" if can_fill_now else " (market closed — buy-back queued for the open)"
                log.error(f"SHORT at broker: {raw} {q} — working{note}")
                try: telegram(f"🚨 <b>Short detected: {raw} ({q})</b> — "
                              f"buy-back working{note}.")
                except Exception: pass
            with state_lock:
                if sym in in_flight: continue
                in_flight.add(sym)
            try:
                # 2) kill sell-side strays only — never our own buy-back
                cancel_symbol_orders(sym, side="sell")
                if not existing_buys:
                    o=alpaca_post("/v2/orders",{"symbol":raw,"qty":str(int(abs(q))),
                        "side":"buy","type":"market","time_in_force":"gtc",
                        "client_order_id":f"at20flat-{raw}-{int(time.time()*1000)}"})
                    if can_fill_now:
                        wait_for_fill(o.get("id",""))
                    # market closed: DO NOT wait (wait cancels on timeout).
                    # The order stays queued; step 1 protects it next pass.
                if can_fill_now:
                    nq,_=broker_position(sym)
                    if nq>=0:
                        log.info(f"Flattened short {raw} (confirmed qty {nq})")
                        short_alerts.pop(sym,None)
                        try: telegram(f"✅ <b>{raw} flattened</b> — position now {nq}")
                        except Exception: pass
            except Exception as e:
                log.error(f"flatten {raw} failed: {e}")
            finally:
                with state_lock: in_flight.discard(sym)
            continue
        # queued flatten completed since last pass? confirm once.
        if sym in short_alerts and q>=0:
            short_alerts.pop(sym,None)
            log.info(f"Short {raw} confirmed flat (queued buy filled)")
            try: telegram(f"✅ <b>{raw} flattened</b> — position now {q}")
            except Exception: pass
        with state_lock:
            if sym in positions:
                if abs(positions[sym]["qty"]-q)/max(q,1e-9)>0.01:
                    log.warning(f"reconcile {sym}: qty {positions[sym]['qty']} -> {q}")
                    positions[sym]["qty"]=q
            elif (sym in SYMBOLS or sym in CRYPTO_SYMBOLS) and sym not in in_flight:
                try:
                    _px=float(p.get("current_price") or p.get("avg_entry_price") or 0)
                    if q*_px < 1.0:
                        log.info(f"Ignoring dust position {sym}: {q} (~${q*_px:.2f})")
                        continue
                except Exception: pass
                entry_ts=time.time()-1800
                try:
                    hist=alpaca_get(f"/v2/orders?symbols={raw}&status=closed&limit=5&direction=desc") or []
                    fills=[x for x in hist if x.get("side")=="buy" and x.get("filled_at")]
                    if fills:
                        entry_ts=datetime.fromisoformat(
                            fills[0]["filled_at"].replace("Z","+00:00")).timestamp()
                except Exception: pass
                positions[sym]={"entry_price":float(p["avg_entry_price"]),"qty":q,
                    "entry_time":entry_ts,"cost":float(p.get("cost_basis",0)),
                    "is_crypto":sym in CRYPTO_SYMBOLS,
                    "peak":float(p.get("current_price",p["avg_entry_price"])),
                    "trail":None,"confidence":60}
                log.info(f"Adopted position {sym}: {q} @ {p['avg_entry_price']}")
    with state_lock:
        for sym in [s for s in positions if s not in seen and s not in in_flight]:
            log.warning(f"reconcile: {sym} gone at broker — purging")
            positions.pop(sym,None)
    # a symbol with a pending short-alert that no longer appears at the broker
    # is FLAT (Alpaca omits flat positions) — confirm once and clear.
    for sym in [s for s in list(short_alerts) if s not in seen]:
        short_alerts.pop(sym,None)
        raw=to_alpaca(sym)
        log.info(f"Short {raw} confirmed flat (position gone at broker)")
        try: telegram(f"✅ <b>{raw} flattened</b> — position closed")
        except Exception: pass

def reconciler_loop():
    while True:
        time.sleep(RECONCILE_EVERY_S)
        try: reconcile()
        except Exception as e: log.error(f"reconciler: {e}")

# ── 1-minute bar engine ───────────────────────────────────────────────────────
def update_bar(sym, price, vol, minute):
    """Aggregate live ticks into 1-min OHLCV bars. Returns True on bar close."""
    b=cur_bar.get(sym)
    closed=False
    if b is None or b["m"]!=minute:
        if b is not None:
            bars[sym].append({k:b[k] for k in ("o","h","l","c","v")}); closed=True
        cur_bar[sym]={"m":minute,"o":price,"h":price,"l":price,"c":price,"v":vol or 0}
    else:
        b["h"]=max(b["h"],price); b["l"]=min(b["l"],price)
        b["c"]=price; b["v"]+=(vol or 0)
    return closed

def bar_closes(sym, live=None):
    cs=[b["c"] for b in bars.get(sym,())]
    if live is not None: cs=cs+[live]
    return cs

def parse_bars(payload_bars):
    out=[]
    for b in payload_bars or []:
        try:
            out.append({"o":float(b["o"]),"h":float(b["h"]),
                        "l":float(b["l"]),"c":float(b["c"]),
                        "v":float(b.get("v",0))})
        except Exception: pass
    return out

def backfill_bars():
    """Warm 100 x 1-min bars per symbol at startup — signals from minute one."""
    for sym in SYMBOLS:
        try:
            r=requests.get(f"{ALPACA_DATA}/v2/stocks/{sym}/bars",
                params={"timeframe":"1Min","limit":100,"feed":"iex"},
                headers=_hdrs(),timeout=10)
            if r.ok:
                for b in parse_bars(r.json().get("bars",[])): bars[sym].append(b)
        except Exception as e:
            log.warning(f"backfill {sym}: {e}")
        time.sleep(0.05)
    try:
        r=requests.get(f"{ALPACA_DATA}/v1beta3/crypto/us/bars",
            params={"timeframe":"1Min","limit":100,
                    "symbols":",".join(CRYPTO_SYMBOLS)},
            headers=_hdrs(),timeout=10)
        if r.ok:
            for sym,bl in (r.json().get("bars",{}) or {}).items():
                if sym in CRYPTO_SYMBOLS:
                    for b in parse_bars(bl): bars[sym].append(b)
    except Exception as e:
        log.warning(f"crypto backfill: {e}")
    log.info("Bar backfill: "+", ".join(f"{s}:{len(bars[s])}"
             for s in list(SYMBOLS)[:4]+list(CRYPTO_SYMBOLS)))

# ── ATR risk model ────────────────────────────────────────────────────────────
def atr_pct(sym):
    bs=list(bars.get(sym,()))
    if len(bs)<15: return None
    trs=[]
    for i in range(1,len(bs)):
        h,l,pc=bs[i]["h"],bs[i]["l"],bs[i-1]["c"]
        trs.append(max(h-l,abs(h-pc),abs(l-pc)))
    atr=sum(trs[-14:])/14
    c_=bs[-1]["c"]
    return (atr/c_*100) if c_ else None

def atr_exits(sym, is_crypto):
    """(stop_pct, target_pct) from the symbol's own volatility, scaled to the
    hold horizon (sqrt-time), clamped to sane bounds. Fallback = old fixed."""
    a=atr_pct(sym)
    if a is None:
        return ((CRY_STOP_MIN+CRY_STOP_MAX)/2 if is_crypto else STOP_LOSS_PCT,
                (CRY_TGT_MIN+CRY_TGT_MAX)/2 if is_crypto else TAKE_PROFIT_PCT)
    horizon=CRYPTO_TS_MINS if is_crypto else TIME_STOP_MINS
    move=a*math.sqrt(horizon)
    stop=move*ATR_STOP_MULT; tgt=move*ATR_TGT_MULT
    if is_crypto:
        stop=min(max(stop,CRY_STOP_MIN),CRY_STOP_MAX)
        tgt =min(max(tgt, CRY_TGT_MIN), CRY_TGT_MAX)
    else:
        stop=min(max(stop,STK_STOP_MIN),STK_STOP_MAX)
        tgt =min(max(tgt, STK_TGT_MIN), STK_TGT_MAX)
    return round(stop,2), round(tgt,2)

def size_for(stop_pct):
    """Notional so that hitting the stop loses ~RISK_PER_TRADE dollars."""
    tv=RISK_PER_TRADE/(stop_pct/100.0)
    return min(max(tv,MIN_NOTIONAL),MAX_NOTIONAL)

# ── Indicators ────────────────────────────────────────────────────────────────
def ema(a,n):
    a=list(a)
    if not a: return None
    k=2/(n+1); e=sum(a[:min(n,len(a))])/min(n,len(a))
    for p in a[min(n,len(a)):]: e=p*k+e*(1-k)
    return e
def rsi(a,n=14):
    a=list(a)
    if len(a)<n+1: return None
    s=a[-(n+1):]
    g=sum(max(s[i]-s[i-1],0) for i in range(1,len(s)))
    l=sum(max(s[i-1]-s[i],0) for i in range(1,len(s)))
    return 100 if l==0 else 100-100/(1+(g/n)/(l/n))
def macd_ind(a):
    a=list(a)
    if len(a)<26: return None
    h=[]
    for i in range(26,len(a)+1):
        e12=ema(a[:i],12); e26=ema(a[:i],26)
        if e12 and e26: h.append(e12-e26)
    if len(h)<9: return None
    return {"histogram":h[-1]-ema(h,9)}
def stoch(a,n=14):
    a=list(a)
    if len(a)<n: return None
    s=a[-n:]; lo,hi=min(s),max(s)
    return 50 if hi==lo else (a[-1]-lo)/(hi-lo)*100
def vwapc(a):
    a=list(a); return sum(a)/len(a) if a else None
def vol_ok(sym):
    v=[b["v"] for b in bars.get(sym,()) if b.get("v")]
    if len(v)<6: return True
    return v[-1]>=sum(v[:-1])/(len(v)-1)*0.9

# ── Market state & filters ────────────────────────────────────────────────────
def now_et():
    n=datetime.now(timezone.utc); return (n.hour-4)%24, n.minute, n.weekday()
def is_market_hours():
    h,m,wd=now_et()
    if wd>=5: return False
    return (h>9 or (h==9 and m>=30)) and (h<15 or (h==15 and m<=45))
def in_window():
    h,m,_=now_et()
    return ((h>W_START_H or (h==W_START_H and m>=W_START_M)) and
            (h<W_END_H or (h==W_END_H and m<=W_END_M)))
def is_eod():
    h,m,wd=now_et()
    return wd<5 and h==15 and m>=45
def spy_trend():
    hist=list(price_history["SPY"])
    if not hist: return None
    refs=[r for r in (open_price.get("SPY"),prev_close.get("SPY")) if r]
    if not refs: return None
    return min((hist[-1]-r)/r*100 for r in refs)
def momentum_ok(sym):
    hist=list(price_history[sym]); op=open_price.get(sym)
    if not hist or not op: return True
    return (hist[-1]-op)/op*100>=MOMENTUM_MIN
def above_50ma(sym):
    h=bar_closes(sym, price_history[sym][-1] if price_history[sym] else None)
    return True if len(h)<50 else h[-1]>sum(h[-50:])/50
def sector_surging(sym):
    if sym in CRYPTO_SYMBOLS: return False
    sec=SYMBOLS.get(sym,{}).get("sector")
    for o,inf in SYMBOLS.items():
        if o==sym or inf.get("sector")!=sec: continue
        op=open_price.get(o); h=list(price_history[o])
        if op and h and abs((h[-1]-op)/op*100)>=0.8: return True
    return False
def near_earnings(sym):
    ed=earnings_cache.get(sym)
    if not ed: return False
    try:
        d=datetime.strptime(ed,"%Y-%m-%d").replace(tzinfo=timezone.utc)
        return 0<=(d-datetime.now(timezone.utc)).days<=EARN_BLACKOUT_D
    except Exception: return False
def sector_held(sym):
    if sym in CRYPTO_SYMBOLS: return False
    sec=SYMBOLS.get(sym,{}).get("sector")
    with state_lock:
        return any(SYMBOLS.get(s,{}).get("sector")==sec and s!=sym for s in positions)
def pre_gap(sym):
    pm=premarket.get(sym)
    if pm: return pm.get("gap_pct",0)
    pc=prev_close.get(sym); h=list(price_history[sym])
    return 0 if not pc or not h else (h[-1]-pc)/pc*100

# ── Signal engine ─────────────────────────────────────────────────────────────
def evaluate(sym):
    live=price_history[sym][-1] if price_history[sym] else None
    hist=bar_closes(sym, live)            # 1-min bar closes + live price
    if len(hist)<30: return None
    price=hist[-1]
    r=rsi(hist); m=macd_ind(hist); st=stoch(hist); vw=vwapc(hist)
    e9=ema(hist,9); e21=ema(hist,21)
    pv=((price-vw)/vw*100) if vw else None
    if sym not in CRYPTO_SYMBOLS and abs(pre_gap(sym))>PREGAP_LIMIT: return None
    crit=[("RSI 38-58",       r is not None and 38<=r<=58,             25),
          ("MACD +ve",        m is not None and m["histogram"]>0,      25),
          ("Above VWAP",      pv is not None and pv>0,                 20),
          ("EMA9 over EMA21", bool(e9 and e21 and e9>e21),             15),
          ("Stoch under 78",  st is not None and st<78,                15),
          ("Volume confirmed",vol_ok(sym),                             10)]
    met={n for n,p,_ in crit if p}
    score=sum(w for n,p,w in crit if p)
    ok=MANDATORY.issubset(met) and len(met-MANDATORY)>=MIN_CONFIRM and score>=MIN_SCORE
    return {"symbol":sym,"price":price,"signal":"BUY" if ok else "WAIT",
            "met":sorted(met),"score":score}

def check_exit(sym, price):
    pos=positions.get(sym)
    if not pos: return None
    ic=pos.get("is_crypto")
    tp=pos.get("target_pct") or (CRYPTO_TP if ic else TAKE_PROFIT_PCT)
    sl=pos.get("stop_pct")   or (CRYPTO_SL if ic else STOP_LOSS_PCT)
    ts=CRYPTO_TS_MINS if ic else TIME_STOP_MINS
    pnl=(price-pos["entry_price"])/pos["entry_price"]*100
    mins=(time.time()-pos["entry_time"])/60
    if pnl>=TRAIL_ACTIVATE:
        if price>pos.get("peak",0):
            pos["peak"]=price
            nt=price*(1-TRAIL_DIST/100)
            if nt>(pos.get("trail") or 0): pos["trail"]=nt
    if pos.get("trail") and price<pos["trail"]:
        return f"Trailing stop ({pnl:+.2f}%) 📉"
    if pnl>=tp:  return f"+{pnl:.2f}% take profit 🟢"
    if pnl<=-sl: return f"{pnl:.2f}% stop loss 🔴"
    if mins>=ts and pnl<TIME_STOP_FLOOR: return f"{pnl:+.2f}% after {mins:.0f}m — dead trade ⏱"
    if mins>=240: return f"{pnl:+.2f}% after {mins:.0f}m — max 4h hold ⏱"
    return None

# ── Guards before entry (full stack) ──────────────────────────────────────────
def entry_allowed(sym):
    """Returns (ok, reason). Reason feeds the daily telemetry."""
    with state_lock:
        if portfolio_halted: return False,"halted"
        if daily_pnl<=-DAILY_LOSS_LIMIT: return False,"daily_loss_limit"
    if fear_greed is not None and fear_greed<=FEAR_GREED_PAUSE: return False,"extreme_fear"
    if vix_level is not None and vix_level>VIX_PAUSE: return False,"vix_high"
    if near_earnings(sym): return False,"earnings_blackout"
    if sector_held(sym): return False,"sector_held"
    if sym not in CRYPTO_SYMBOLS:
        if not in_window(): return False,"outside_window"
        if SYMBOLS.get(sym,{}).get("corr")=="tech":
            t=spy_trend()
            if t is not None and t<SPY_BULL_MIN: return False,"spy_down"
        if not momentum_ok(sym): return False,"no_momentum"
        if not above_50ma(sym): return False,"below_50ma"
        if sector_surging(sym): return False,"sector_surge"
    return True,""

def count_block(reason):
    with state_lock:
        block_counts[reason]=block_counts.get(reason,0)+1

def try_enter(sym):
    if time.time()-last_signal.get(sym,0)<SIGNAL_COOLDOWN: return
    a=evaluate(sym)
    if not a or a["signal"]!="BUY": return
    last_signal[sym]=time.time()
    ok,reason=entry_allowed(sym)
    if not ok:
        count_block(reason)
        log.info(f"{sym} signal {a['score']} — blocked: {reason}"); return
    log.info(f"⚡ {sym} BUY signal score {a['score']}")
    threading.Thread(target=place_entry,
        args=(sym,a["price"],a["score"],a["met"]),daemon=True).start()

# ── Advisory sentiment (async; informational only, no veto power) ────────────
def advisory_sentiment(sym):
    """Runs AFTER a fill, off the hot path. Attaches a sentiment read to the
    position for later analysis. Failure of any kind is silent-but-logged."""
    if sym in CRYPTO_SYMBOLS: return
    try:
        heads=[]
        try:
            r=requests.get(f"{ALPACA_DATA}/v1beta1/news?symbols={sym}&limit=5",
                           headers=_hdrs(),timeout=10)
            if r.ok: heads=[a.get("headline","") for a in r.json().get("news",[])][:5]
        except Exception: pass
        if not heads or not ANTHROPIC_KEY:
            return
        rr=requests.post("https://api.anthropic.com/v1/messages",
            headers={"Content-Type":"application/json","x-api-key":ANTHROPIC_KEY,
                     "anthropic-version":"2023-06-01"},
            json={"model":"claude-sonnet-4-6","max_tokens":80,
                  "system":'Return ONLY JSON: {"sentiment":"positive"|"negative"|"neutral","score":0-100}',
                  "messages":[{"role":"user","content":
                      f"Short-term (hours) sentiment for {sym}:\n"+"\n".join("- "+h for h in heads)}]},
            timeout=15)
        if not rr.ok:
            log.warning(f"sentiment API {sym}: HTTP {rr.status_code}"); return
        txt=rr.json()["content"][0]["text"]
        txt=re.sub(r"```(json)?","",txt).strip()
        data=json.loads(txt)
        s=str(data.get("sentiment","neutral")); sc=int(float(data.get("score",50)))
        with state_lock:
            if sym in positions: positions[sym]["sentiment"]=f"{s}:{sc}"
        log.info(f"advisory sentiment {sym}: {s} ({sc})")
    except Exception as e:
        log.warning(f"advisory sentiment {sym}: {e}")

# ── Stock websocket ───────────────────────────────────────────────────────────
def on_tick(sym, price, vol=None):
    if not is_market_hours(): return
    price_history[sym].append(price)
    update_bar(sym, price, vol, int(time.time()//60))
    if open_price.get(sym) is None: open_price[sym]=price
    if sym in positions:
        r=check_exit(sym,price)
        if r:
            with state_lock:
                if sym in exit_pending or sym in in_flight: return
                exit_pending.add(sym)
            threading.Thread(target=close_position,args=(sym,r,price),daemon=True).start()
        return
    if sym in TRADEABLE: try_enter(sym)

def ws_message(ws,message):
    try:
        for m in (json.loads(message) if isinstance(json.loads(message),list) else [json.loads(message)]):
            t=m.get("T")
            if t=="success" and m.get("msg")=="connected":
                ws.send(json.dumps({"action":"auth","key":ALPACA_KEY,"secret":ALPACA_SECRET}))
            elif t=="success" and m.get("msg")=="authenticated":
                ws.send(json.dumps({"action":"subscribe","trades":list(SYMBOLS.keys())}))
                log.info("WS authenticated + subscribed")
            elif t=="t":
                s,p=m.get("S"),m.get("p")
                if s in SYMBOLS and p: on_tick(s,float(p),m.get("s"))
            elif t=="error":
                log.error(f"WS: {m}")
    except Exception as e:
        log.error(f"WS msg: {e}")

def ws_loop():
    while True:
        try:
            w=websocket.WebSocketApp(WS_URL,on_message=ws_message,
                on_error=lambda w,e:log.error(f"WS err: {e}"),
                on_close=lambda w,c,m:log.warning("WS closed"))
            w.run_forever(ping_interval=30,ping_timeout=10)
        except Exception as e:
            log.error(f"WS thread: {e}")
        time.sleep(5)

# ── Crypto monitor (entries paused; exits + heartbeat active) ─────────────────
def crypto_price(sym):
    s=sym.replace("/","%2F")
    try:
        r=requests.get(f"{ALPACA_DATA}/v1beta3/crypto/us/latest/trades?symbols={s}",
                       headers=_hdrs(),timeout=10)
        if r.ok:
            t=r.json().get("trades",{}).get(sym)
            if t and t.get("p"): return float(t["p"])
        else:
            log.warning(f"crypto trades {sym}: HTTP {r.status_code}")
    except Exception as e:
        log.warning(f"crypto trades {sym}: {e}")
    try:
        r=requests.get(f"{ALPACA_DATA}/v1beta3/crypto/us/latest/quotes?symbols={s}",
                       headers=_hdrs(),timeout=10)
        if r.ok:
            q=r.json().get("quotes",{}).get(sym) or {}
            bp,ap=float(q.get("bp",0)),float(q.get("ap",0))
            if bp and ap: return (bp+ap)/2
        else:
            log.warning(f"crypto quotes {sym}: HTTP {r.status_code}")
    except Exception as e:
        log.warning(f"crypto quotes {sym}: {e}")
    return None

def crypto_loop():
    log.info(f"Crypto monitor started (entries {'ON' if MAX_CRYPTO_POS>0 else 'PAUSED'})")
    n=0; fails=0
    while True:
        try:
            n+=1
            try:      # refresh latest 1-min bars for indicators
                rb=requests.get(f"{ALPACA_DATA}/v1beta3/crypto/us/bars",
                    params={"timeframe":"1Min","limit":3,
                            "symbols":",".join(CRYPTO_SYMBOLS)},
                    headers=_hdrs(),timeout=10)
                if rb.ok:
                    for _s,_bl in (rb.json().get("bars",{}) or {}).items():
                        if _s not in CRYPTO_SYMBOLS: continue
                        for _b in parse_bars(_bl):
                            if not bars[_s] or abs(bars[_s][-1]["c"]-_b["c"])>1e-12 \
                               or bars[_s][-1]["v"]!=_b["v"]:
                                if bars[_s] and bars[_s][-1]["o"]==_b["o"] \
                                   and bars[_s][-1]["h"]<=_b["h"]:
                                    bars[_s][-1]=_b          # same forming bar updated
                                else:
                                    bars[_s].append(_b)
            except Exception as e:
                log.warning(f"crypto bars: {e}")
            for sym in CRYPTO_SYMBOLS:
                p=crypto_price(sym)
                if not p:
                    fails+=1
                    if fails in (1,5,20) or fails%100==0:
                        log.error(f"CRYPTO PRICE FAILING ({fails}) — {sym}")
                    continue
                price_history[sym].append(p)
                if sym in positions:
                    r=check_exit(sym,p)
                    if r:
                        with state_lock:
                            if sym in exit_pending or sym in in_flight: continue
                            exit_pending.add(sym)
                        threading.Thread(target=close_position,args=(sym,r,p),daemon=True).start()
                elif MAX_CRYPTO_POS>0:
                    try_enter(sym)
            if n%10==0:
                bits=[]
                for s in CRYPTO_SYMBOLS:
                    h=price_history[s]; a=atr_pct(s)
                    bits.append(f"{s} ${h[-1]:,.0f} [{len(bars[s])}bars"
                                f"{', ATR '+format(a,'.3f')+'%' if a else ''}]"
                                if h else f"{s} NO DATA")
                with state_lock: held=[s for s in CRYPTO_SYMBOLS if s in positions]
                log.info(f"Crypto heartbeat: {' | '.join(bits)} | holding: {held or 'none'}")
        except Exception as e:
            log.error(f"crypto loop: {e}")
        time.sleep(30)

# ── External data (unchanged behaviour, trimmed) ──────────────────────────────
def refresh_data():
    global earnings_cache,economic_events,econ_blackout,fear_greed
    try:
        hard={"NVDA":"2026-08-26","SMCI":"2026-08-26","MU":"2026-09-24",
              "TSLA":"2026-10-21",
              
              "AMD":"2026-10-28","NFLX":"2026-10-14"}
        fmp={}
        if FMP_KEY:
            try:
                t=datetime.now(timezone.utc).strftime("%Y-%m-%d")
                f=(datetime.now(timezone.utc)+timedelta(days=60)).strftime("%Y-%m-%d")
                r=requests.get("https://financialmodelingprep.com/api/v3/earning_calendar",
                    params={"from":t,"to":f,"apikey":FMP_KEY},timeout=10)
                if r.ok: fmp={i["symbol"]:i["date"] for i in r.json()
                              if i.get("symbol") in TRADEABLE and i.get("date")}
            except Exception: pass
        earnings_cache={**hard,**fmp}
        economic_events=[]
        if FMP_KEY:
            try:
                t=datetime.now(timezone.utc).strftime("%Y-%m-%d")
                r=requests.get("https://financialmodelingprep.com/api/v3/economic_calendar",
                    params={"from":t,"to":t,"apikey":FMP_KEY},timeout=10)
                kw=["fed","fomc","cpi","inflation","payroll","gdp","unemployment"]
                if r.ok: economic_events=[e for e in r.json()
                    if e.get("impact","").lower()=="high"
                    or any(k in e.get("event","").lower() for k in kw)]
            except Exception: pass
        econ_blackout=len(economic_events)>0
        global vix_level
        if FMP_KEY:
            try:
                r=requests.get("https://financialmodelingprep.com/api/v3/quote/%5EVIX",
                               params={"apikey":FMP_KEY},timeout=10)
                if r.ok and r.json():
                    vix_level=float(r.json()[0].get("price") or 0) or None
                    if vix_level: log.info(f"VIX: {vix_level:.1f}")
            except Exception: pass
        try:
            r=requests.get("https://production.dataviz.cnn.io/index/fearandgreed/graphdata",
                headers={"User-Agent":"Mozilla/5.0"},timeout=10)
            if r.ok:
                s=r.json().get("fear_and_greed",{}).get("score")
                fear_greed=float(s) if s else None
        except Exception: pass
        for sym in SYMBOLS:
            try:
                r=requests.get(f"{ALPACA_DATA}/v2/stocks/{sym}/bars?timeframe=1Day&limit=2",
                    headers=_hdrs(),timeout=10)
                b=r.json().get("bars",[])
                if len(b)>=2:
                    prev_close[sym]=float(b[-2]["c"]); open_price[sym]=float(b[-1]["o"])
            except Exception: pass
            time.sleep(0.05)
        log.info("data refresh complete")
    except Exception as e:
        log.error(f"refresh_data: {e}")

# ── Daily summary / EOD (fire exactly once) ───────────────────────────────────
def daily_summary():
    telegram(summary_text(clear=True))

# ── Telegram command interface (/balance /summary /positions /status) ────────
def balance_text():
    a=get_account() or {}
    eq=float(a.get("portfolio_value",0)); bp=float(a.get("buying_power",0))
    cash=float(a.get("cash",0))
    d=eq-starting_pv if starting_pv else 0
    with state_lock: n=len(positions)
    return (f"💰 <b>Balance</b>\n"
            f"Equity: ${eq:,.2f} ({'+' if d>=0 else ''}{d:,.2f} vs start)\n"
            f"Cash: ${cash:,.2f} | Buying power: ${bp:,.2f}\n"
            f"Open positions: {n} | Today's closed P&L: ${daily_pnl:+.2f}")

def positions_text():
    with state_lock: snap=dict(positions)
    if not snap: return "📭 No open positions."
    lines=["📌 <b>Open positions</b>"]
    for s,p in snap.items():
        h=price_history.get(s)
        cur=h[-1] if h else p["entry_price"]
        pnl=(cur-p["entry_price"])/p["entry_price"]*100
        mins=(time.time()-p["entry_time"])/60
        lines.append(f"{s}: {fmt_qty(p['qty'])} @ ${p['entry_price']:,.2f} "
                     f"→ ${cur:,.2f} ({pnl:+.2f}%) | {mins:.0f}m | "
                     f"±{p.get('stop_pct','?')}/{p.get('target_pct','?')}%")
    return "\n".join(lines)

def summary_text(clear=False):
    with state_lock:
        trades=list(session_trades); blocks=dict(block_counts)
        if clear: session_trades.clear()
    if not trades:
        bl=", ".join(f"{k}:{v}" for k,v in sorted(blocks.items(),key=lambda x:-x[1])[:5]) or "none"
        return (f"📊 <b>Summary {VERSION}</b>\nNo completed trades today.\n"
                f"Signals blocked by: {bl}")
    tot=sum(t["pnl_abs"] for t in trades); wins=sum(1 for t in trades if t["pnl_abs"]>0)
    by={}
    for t in trades: by.setdefault(t["symbol"],[]).append(t["pnl_abs"])
    lines="\n".join(f"  {s}: ${sum(v):+.2f} ({len(v)})" for s,v in by.items())
    rx={"take profit":0,"stop loss":0,"⏱":0,"EOD":0,"Trailing":0}
    for t in trades:
        for k in rx:
            if k in (t.get("reason") or ""): rx[k]+=1; break
    grp={}
    for t in trades:
        g=t.get("group","other"); grp.setdefault(g,[0,0])
        grp[g][0]+=t["pnl_abs"]; grp[g][1]+=1
    gl=" | ".join(f"{g}: ${v[0]:+.2f} ({v[1]})" for g,v in grp.items())
    bl=", ".join(f"{k}:{v}" for k,v in sorted(blocks.items(),key=lambda x:-x[1])[:5]) or "none"
    return (f"📊 <b>Summary {VERSION}</b>\n"
            f"Trades: {len(trades)} | Wins: {wins} ({wins/len(trades)*100:.0f}%) | P&L ${tot:+.2f}\n"
            f"Exits — TP:{rx['take profit']} SL:{rx['stop loss']} Time:{rx['⏱']} "
            f"EOD:{rx['EOD']} Trail:{rx['Trailing']}\n"
            f"Groups — {gl}\n{lines}\nBlocked: {bl}")

def status_text():
    with state_lock:
        n=len(positions); b=sum(block_counts.values())
    fg=f"{fear_greed:.0f}" if fear_greed is not None else "?"
    vx=f"{vix_level:.1f}" if vix_level is not None else "?"
    return (f"🤖 <b>AlphaTrader {VERSION}</b> {'PAPER' if IS_PAPER else 'LIVE'}\n"
            f"Positions: {n}/{MAX_POSITIONS} | Blocked today: {b}\n"
            f"VIX: {vx} | Fear&Greed: {fg}\n"
            f"Halted: {portfolio_halted} | Notional sizing: {notional_ok}")

def help_text():
    return ("Commands:\n/balance — equity & buying power\n"
            "/positions — open trades live P&L\n"
            "/summary — today's trades so far\n/status — bot state")

def handle_command(txt):
    t=(txt or "").strip().lower()
    if t.startswith("/balance"):   return balance_text()
    if t.startswith("/pos"):       return positions_text()
    if t.startswith("/summary"):   return summary_text(clear=False)
    if t.startswith("/status"):    return status_text()
    if t.startswith("/help") or t=="/start": return help_text()
    return None

def telegram_listener():
    if not TG_TOKEN or not TG_CHAT_ID:
        log.info("TG listener disabled (no token)"); return
    offset=None
    log.info("Telegram command listener started (/help)")
    while True:
        try:
            r=requests.get(f"https://api.telegram.org/bot{TG_TOKEN}/getUpdates",
                params={"timeout":25,**({"offset":offset} if offset else {})},
                timeout=35)
            if not r.ok:
                time.sleep(5); continue
            for u in r.json().get("result",[]):
                offset=u["update_id"]+1
                msg=u.get("message") or {}
                if str(msg.get("chat",{}).get("id"))!=str(TG_CHAT_ID): continue
                out=handle_command(msg.get("text"))
                if out: telegram(out)
        except Exception as e:
            log.warning(f"TG listener: {e}"); time.sleep(5)

# ── Portfolio protections ─────────────────────────────────────────────────────
def hard_stop_check():
    global portfolio_halted
    if portfolio_halted or not starting_pv: return
    a=get_account()
    if not a: return
    pv=float(a.get("portfolio_value",starting_pv))
    dd=(starting_pv-pv)/starting_pv*100
    if dd>=PORT_HARD_STOP:
        portfolio_halted=True
        telegram(f"🛑 <b>HARD STOP</b> — portfolio -{dd:.1f}% from ${starting_pv:,.0f}. "
                 f"Trading halted; redeploy to reset.")

# ── Startup ───────────────────────────────────────────────────────────────────
def startup():
    global starting_pv
    log.info(f"AlphaTrader {VERSION} starting")
    if not ALPACA_KEY or not ALPACA_SECRET: raise SystemExit("missing keys")
    a=get_account()
    if not a: raise SystemExit("cannot reach Alpaca")
    starting_pv=float(a.get("portfolio_value",0))

    # v16 rests NO orders: anything open at the broker is stale/foreign.
    try:
        alpaca_delete("/v2/orders")
        log.info("Startup: cancelled ALL open orders")
    except Exception as e:
        log.warning(f"cancel-all failed: {e}")

    backfill_bars()      # 100 x 1-min bars per symbol — indicators warm now
    reconcile()          # adopt longs, flatten shorts, sync state
    with state_lock:
        overnight=[s for s,p in positions.items() if not p.get("is_crypto")]
    if overnight and not is_market_hours():
        telegram(f"⚠️ <b>Stock positions held while market closed:</b> "
                 f"{', '.join(overnight)}\nEOD close was missed (downtime?). "
                 f"They will be exit-managed from the next open.")
    refresh_data()

    with state_lock:
        held=", ".join(f"{s}({positions[s]['qty']})" for s in positions) or "none"
    telegram(f"🚀 <b>AlphaTrader {VERSION} — Bars + ATR Risk</b> "
             f"{'📄 PAPER' if IS_PAPER else '💰 LIVE'}\n"
             f"Indicators on 1-min bars (backfilled) | ATR exits, ~${RISK_PER_TRADE:.2f} risk/trade\n"
             f"Crypto entries: {'ON (bars-based)' if MAX_CRYPTO_POS>0 else 'PAUSED'}\n"
             f"Slots: {MAX_POSITIONS} ({MAX_STOCK_POS} stock/{MAX_CRYPTO_POS} crypto) | size ${BASE_TRADE_SIZE}\n"
             f"Exits: software-managed, zero resting orders, 60s reconciler\n"
             f"💬 Message /help for live commands\n"
             f"Recovered positions: {held}\n"
             f"Portfolio ${starting_pv:,.0f} | hard stop -{PORT_HARD_STOP}%")

def main():
    global eod_done_date, summary_done_date, last_trading_date
    startup()
    threading.Thread(target=ws_loop,daemon=True).start()
    threading.Thread(target=crypto_loop,daemon=True).start()
    threading.Thread(target=reconciler_loop,daemon=True).start()
    threading.Thread(target=telegram_listener,daemon=True).start()
    last_refresh=time.time()
    while True:
        try:
            h,m,_=now_et(); today=datetime.now(timezone.utc).date()
            if time.time()-last_refresh>3600:
                threading.Thread(target=refresh_data,daemon=True).start()
                last_refresh=time.time()
            # once-per-trading-day reset (first weekday pass after 09:00 ET)
            global last_trading_date
            _,_,wd=now_et()
            if wd<5 and h>=9 and last_trading_date!=today:
                last_trading_date=today
                with state_lock:
                    globals()['daily_pnl']=0.0
                    block_counts.clear()
                log.info(f"New trading day {today} — daily P&L and telemetry reset")
            if is_eod() and eod_done_date!=today:
                eod_done_date=today
                with state_lock:
                    stocks=[s for s,p in positions.items() if not p.get("is_crypto")]
                if stocks:
                    telegram("⏰ <b>EOD</b> — closing stock positions (once)")
                    for s in stocks:
                        hst=list(price_history[s])
                        close_position(s,"EOD close",hst[-1] if hst else None)
            if h==16 and m<5 and summary_done_date!=today:
                summary_done_date=today; daily_summary()
            if is_market_hours() and m%5==0: hard_stop_check()
        except Exception as e:
            log.error(f"main: {e}")
        time.sleep(20)

if __name__=="__main__":
    main()
