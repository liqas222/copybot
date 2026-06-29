#!/usr/bin/env python3
"""
Hyperliquid Copy-Trading Bot  ·  v3  (PAPER mode + 24/7 service + dashboard)
============================================================================
Reads the tracked whale from MAINNET and copies its trades. It serves the
dashboard + a live status panel, reachable from any device via a
Tailscale Funnel (a fixed …ts.net URL).

Three modes (set below):
  * PAPER_MODE = True  -> simulates copying into a virtual $1000 account
                          (no keys, no funds, no exchange). Best for testing.
  * PAPER_MODE = False + EXEC_URL = TESTNET  -> real test trades (needs config.json)
  * PAPER_MODE = False + EXEC_URL = MAINNET  -> LIVE real money (needs config.json)

Rules (all modes): copy opens & exits · 1/5 equity margin/trade · max 5 positions
· leverage fixed 6x (cross & isolated) · TP +12% ROE ·
no SL · daily -25% kill-switch · all coins.

RUN:
  python bot.py           -> run in this window (stops when window closes)
  python bot.py install   -> install as a 24/7 service (survives reboot / Ctrl+C)
  cat link.txt            -> show the current dashboard link any time
"""

import os, sys, json, time, math, datetime, threading, secrets, subprocess, re, queue
import urllib.request, urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from concurrent.futures import ThreadPoolExecutor

# ============================ CONFIG ============================
WHALE            = "0x0c349d9b92fbd172bbb5a17a9db0a673a6a10ad3"
SOURCE_URL       = "https://api.hyperliquid.xyz"        # read whale from MAINNET

# Your own wallet — NOT copied, only logged: the bot records each of your real
# trades with the actual leverage/margin it sees while the position is open, so
# the dashboard can show exact ROE (Hyperliquid doesn't report leverage for
# already-closed trades). Logging only covers trades opened while the bot runs.
MY_WALLET        = "0x2a62f939cd2E36293c9e1ef73FdCA33daC8bcf95"

PAPER_MODE       = True          # <- keep True: this process hosts the dashboard + LIVE engine + Smart Money
PAPER_TRADING    = False         # paper copy-bot retired (we trade live now). The loop still hosts
                                 # the server, the live engine, Smart Money and My-Account logging,
                                 # but no longer simulates virtual whale-copy trades.
PAPER_START      = 1000.0        # virtual starting capital for paper mode

CAPITAL_FRACTION = 0.20
MAX_POSITIONS    = 5
FIXED_LEV        = 6       # every copied trade uses exactly this leverage (cross & isolated)
TP_ROE           = 0.12      # default; live values held in SET (settable from dashboard)
SET = {"tp_crypto": TP_ROE, "tp_stock": TP_ROE}   # take-profit ROE per asset class
DAILY_LOSS_LIMIT = 0.25
POLL_SECONDS     = 3
CLOSE_CONFIRM    = 2          # whale-exit must be confirmed this many polls before closing a copy
OPEN_CONFIRM     = 2          # a new whale open must be confirmed this many polls before copying
COIN_WHITELIST   = None      # None = all coins; or {"BTC","HYPE"}

TG = {"token": os.environ.get("TELEGRAM_BOT_TOKEN", ""), "chat": "1253059682"}

# Tracked wallets shown on the dashboard (editable + saved from the UI). The bot
# copies WHALE; the others are watch-only.
WALLETS = [
    {"addr": "0x0c349d9b92fbd172bbb5a17a9db0a673a6a10ad3", "label": "Wallet 1"},
    {"addr": "0x1aa780bb10425b86bcf05ecbb7953f9a93729ed9", "label": "Wallet 2"},
]
ADDR_RE = re.compile(r"^0x[0-9a-fA-F]{40}$")
KEY_RE  = re.compile(r"^0x[0-9a-fA-F]{64}$")   # an agent/API private key: 0x + 32 bytes hex

def wallets_payload():
    return [{"addr": w["addr"], "label": w["label"],
             "copied": w["addr"].lower() == WHALE.lower()} for w in WALLETS]

DASH_HOST  = "0.0.0.0"
DASH_PORT  = 80
START_CF_TUNNEL = False    # Tailscale Funnel gives a FIXED url now; the cloudflared
                           # quick-tunnel made a new random link on every restart -> off.
DASH_TOKEN = os.environ.get("DASH_TOKEN") or secrets.token_urlsafe(12)
# ===============================================================

# real-exchange URL only loaded when not in paper mode (keeps paper deps light)
EXEC_URL = None
if not PAPER_MODE:
    from eth_account import Account
    from hyperliquid.exchange import Exchange
    from hyperliquid.utils import constants
    EXEC_URL = constants.TESTNET_API_URL          # execute on TESTNET
    # EXEC_URL = constants.MAINNET_API_URL         # <- switch to GO LIVE

STATE = {"running": False, "paused": False, "killed": False, "net": "",
         "equity": 0.0, "day_start_equity": 0.0, "slots_used": 0,
         "max_positions": MAX_POSITIONS, "log": []}
LOCK = threading.Lock()
EX   = {"ex": None}

# ============================ LIVE (real money) ============================
# A separate, opt-in engine that mirrors the SAME whale with the SAME rules as
# the paper copy-bot, but places REAL orders on Hyperliquid MAINNET. It is OFF
# by default and only ever trades when ALL of these hold:
#   * LIVE["enabled"] is True (master switch, toggled from the dashboard)
#   * a trade-only agent key is present in config.json
#   * the Hyperliquid SDK (eth_account + hyperliquid) imports successfully
# It only ever CLOSES positions it opened itself (tracked in LIVE["owned"]),
# never the user's other/manual positions. Settings live in config.json (gitignored).
CONFIG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.json")
LIVE_STATE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "live_state.json")
LIVE = {
    "enabled": False,        # master on/off (persisted)
    "max_lev": FIXED_LEV,    # hard leverage cap for live trades (persisted, settable)
    "killswitch": True,      # daily -25% loss halt; can be switched off from the dashboard (persisted)
    "ready": False,          # key loaded + SDK importable + Exchange built
    "err": "",               # last init/runtime error (shown on the dashboard)
    "addr": "",              # account address the agent trades for
    "net": "MAINNET",
    "equity": 0.0,          # total portfolio value (perp + spot), matches the Hyperliquid UI
    "perp_equity": 0.0,     # perp account value only (this is what's usable as perp margin)
    "spot_equity": 0.0,
    "day_start_eq": 0.0,
    "killed": False,
    "started_ms": 0,
    "ex": None,              # hyperliquid Exchange instance
    "owned": {},             # key (dex,bare) -> meta {side,entry,margin,sz,lev,opened_ms}; only these are managed/closed
    "pos": [],               # snapshot of current account positions (for the dashboard)
    "closed": [],            # closed-trade log (for the dashboard)
    "log": [],               # engine event log (for the dashboard)
    "hist": [],              # [[t_ms, account_value], ...] equity curve for the My-Account chart
}
LIVE_HIST_EVERY = 60         # seconds between live equity snapshots for the chart

# Active trading mode — exactly ONE engine may open new trades at a time.
#   "live"  -> the real-money Live engine may be armed; Smart Money opens nothing.
#   "smart" -> Smart Money opens (paper for now); the Live engine is forced off.
# A mode switch is only allowed when the CURRENT mode is flat (no open bot positions).
MODE = {"v": "live"}

# Active trading mode — exactly ONE engine may open new trades at a time.
#   "live"  -> the real-money Live engine may be armed; Smart Money opens nothing.
#   "smart" -> Smart Money opens (paper for now); the Live engine is forced off.
# A mode switch is only allowed when the CURRENT mode is flat (no open bot positions).
MODE = {"v": "live"}

# virtual book for paper mode
PAPER = {"cash": PAPER_START, "pos": {}, "hist": [], "closed": []}   # pos: key(tuple) -> dict; hist: [[t_ms, equity], ...]; closed: [trade dicts]
HIST_EVERY = 60       # seconds between equity snapshots for the chart

# log of YOUR real trades (MY_WALLET): open snapshot -> closed trade with exact ROE
MINE = {"pos": {}, "closed": []}   # pos: key(tuple) -> snapshot dict; closed: [trade dicts]

# manual "adopt": set by the dashboard -> the loop copies currently-open whale
# positions we don't hold yet, entered at the whale's OWN entry price.
ADOPT = {"pending": False}


# ---------------- notifications + log ----------------
# All server-generated times use Swiss local time (CET/CEST, DST-aware) instead of
# the server's UTC clock — so Telegram messages, the dashboard log and position
# open-times match your wall clock in Switzerland.
try:
    from zoneinfo import ZoneInfo
    LOCAL_TZ = ZoneInfo(os.environ.get("BOT_TZ", "Europe/Zurich"))
except Exception:
    LOCAL_TZ = None      # tz database missing -> fall back to the server clock

def now_hms():
    if LOCAL_TZ is not None:
        return datetime.datetime.now(LOCAL_TZ).strftime("%H:%M:%S")
    return time.strftime("%H:%M:%S")

_TGQ = queue.Queue()
def _tg_send(msg):
    tok = TG["token"]
    if not tok:
        return
    try:
        data = urllib.parse.urlencode({"chat_id": TG["chat"], "text": msg}).encode()
        urllib.request.urlopen("https://api.telegram.org/bot%s/sendMessage" % tok,
                               data=data, timeout=10)
    except Exception as e:
        print("TG error:", e)

def _tg_worker():
    while True:
        msg = _TGQ.get()
        try:
            _tg_send(msg)
        finally:
            _TGQ.task_done()

threading.Thread(target=_tg_worker, daemon=True).start()

def tg(msg):
    # Log instantly for the dashboard, then hand the Telegram send to a
    # background worker so the poll loop never blocks on the network.
    stamp = now_hms()
    with LOCK:
        STATE["log"].append({"t": stamp, "text": msg})
        if len(STATE["log"]) > 200:
            STATE["log"] = STATE["log"][-200:]
    print(msg)
    if TG["token"]:
        _TGQ.put("🕒 %s\n%s" % (stamp[:5], msg))   # local-time stamp on every Telegram message

def _tg_reply(text):
    if TG["token"]:
        _TGQ.put(text)

def _fmt_money(x):
    try: x = float(x)
    except Exception: x = 0.0
    return ("+" if x >= 0 else "-") + "$%.2f" % abs(x)

def _window_starts():
    """(today_local_midnight, 7d_ago, 30d_ago) in epoch-ms."""
    now_ms = time.time() * 1000
    try:
        base = datetime.datetime.now(LOCAL_TZ) if LOCAL_TZ else datetime.datetime.now()
        today = base.replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000
    except Exception:
        today = now_ms - 86400000
    return today, now_ms - 7 * 86400000, now_ms - 30 * 86400000

def _stats_block(closed):
    tday, w7, w30 = _window_starts()
    def line(lbl, start):
        rows = [t for t in closed if (t.get("closed_ms") or 0) >= start]
        pnl = sum(float(t.get("pnl", 0)) for t in rows); n = len(rows)
        wins = sum(1 for t in rows if float(t.get("pnl", 0)) > 0)
        wr = round(100 * wins / n) if n else 0
        return "%s: %s · %d Trades · %d%% WR" % (lbl, _fmt_money(pnl), n, wr)
    return "\n".join([line("Heute", tday), line("7 Tage", w7), line("30 Tage", w30)])

def _live_stats_text():
    on = bool(LIVE.get("enabled") and LIVE.get("ready"))
    total = sum(float(t.get("pnl", 0)) for t in LIVE.get("closed", []))
    return ("🔴 LIVE-BOT (echtes Geld)\nAccount: $%.2f · offene Bot-Pos: %d · Engine: %s\n%s\nGesamt realisiert: %s"
            % (LIVE.get("equity", 0), len(LIVE.get("owned", {})), "🟢 AN" if on else "⚪ AUS",
               _stats_block(LIVE.get("closed", [])), _fmt_money(total)))

def _smart_stats_text():
    sm = STATE.get("smart", {})
    total = sum(float(t.get("pnl", 0)) for t in SMART.get("closed", []))
    return ("🧠 SMART-MONEY (Paper)\nEquity: $%.2f · offene Pos: %d\n%s\nGesamt realisiert: %s"
            % (sm.get("equity", 0), len(sm.get("pos", [])), _stats_block(SMART.get("closed", [])), _fmt_money(total)))

# ---- Telegram inline-keyboard menus (tap, don't type) ----
def _tg_api(method, params):
    tok = TG["token"]
    if not tok:
        return None
    try:
        data = urllib.parse.urlencode(params).encode()
        r = urllib.request.urlopen("https://api.telegram.org/bot%s/%s" % (tok, method), data=data, timeout=10)
        return json.loads(r.read())
    except Exception as e:
        print("tg api error:", e); return None

def _kb(rows): return json.dumps({"inline_keyboard": rows})
_MENU_KB = [[{"text": "🔴 Live (echtes Geld)", "callback_data": "e:live"}],
            [{"text": "🧠 Smart Money", "callback_data": "e:smart"}]]
def _period_kb(eng):
    return [[{"text": "Heute", "callback_data": "s:%s:t" % eng}, {"text": "7 Tage", "callback_data": "s:%s:7" % eng}, {"text": "30 Tage", "callback_data": "s:%s:30" % eng}],
            [{"text": "Gesamt", "callback_data": "s:%s:all" % eng}],
            [{"text": "↩︎ Bot wechseln", "callback_data": "m"}]]
_PERIOD_LABEL = {"t": "Heute", "7": "7 Tage", "30": "30 Tage", "all": "Gesamt"}
def _period_start(per):
    tday, w7, w30 = _window_starts()
    return {"t": tday, "7": w7, "30": w30, "all": 0}.get(per, 0)

def _stat_one(eng, per):
    start = _period_start(per); lbl = _PERIOD_LABEL.get(per, per)
    if eng == "live":
        on = bool(LIVE.get("enabled") and LIVE.get("ready"))
        head = "🔴 LIVE-BOT (echtes Geld)\nAccount: $%.2f · offen: %d · %s" % (LIVE.get("equity", 0), len(LIVE.get("owned", {})), "🟢 AN" if on else "⚪ AUS")
        closed = LIVE.get("closed", [])
    else:
        sm = STATE.get("smart", {})
        head = "🧠 SMART-MONEY (Paper)\nEquity: $%.2f · offen: %d" % (sm.get("equity", 0), len(sm.get("pos", [])))
        closed = SMART.get("closed", [])
    rows = [t for t in closed if (t.get("closed_ms") or 0) >= start]
    pnl = sum(float(t.get("pnl", 0)) for t in rows); n = len(rows)
    wins = sum(1 for t in rows if float(t.get("pnl", 0)) > 0); wr = round(100 * wins / n) if n else 0
    return "%s\n\n📊 %s\nPnL: %s · %d Trades · %d%% Win-Rate" % (head, lbl, _fmt_money(pnl), n, wr)

def run_tg_listener():
    """Long-poll Telegram and drive a tap-only menu: pick Live/Smart, then a period."""
    offset = 0; drained = False
    while True:
        try:
            tok = TG["token"]
            if not tok:
                time.sleep(5); continue
            url = "https://api.telegram.org/bot%s/getUpdates?timeout=30&offset=%d&allowed_updates=%s" % (
                tok, offset, urllib.parse.quote('["message","callback_query"]'))
            r = urllib.request.urlopen(url, timeout=40)
            data = json.loads(r.read())
            for u in data.get("result", []):
                offset = u["update_id"] + 1
                if not drained:
                    continue                      # skip backlog on first start
                cq = u.get("callback_query")
                if cq:                            # a button was tapped
                    mm = cq.get("message") or {}
                    chat = str((mm.get("chat") or {}).get("id", "")); mid = mm.get("message_id")
                    _tg_api("answerCallbackQuery", {"callback_query_id": cq.get("id", "")})
                    if TG.get("chat") and chat and chat != str(TG["chat"]):
                        continue
                    d = cq.get("data", "")
                    if d == "m":
                        _tg_api("editMessageText", {"chat_id": chat, "message_id": mid, "text": "📊 Stats — welcher Bot?", "reply_markup": _kb(_MENU_KB)})
                    elif d.startswith("e:"):
                        eng = d[2:]; title = "🔴 Live (echtes Geld)" if eng == "live" else "🧠 Smart Money"
                        _tg_api("editMessageText", {"chat_id": chat, "message_id": mid, "text": "%s\nZeitraum wählen:" % title, "reply_markup": _kb(_period_kb(eng))})
                    elif d.startswith("s:"):
                        _, eng, per = d.split(":")
                        _tg_api("editMessageText", {"chat_id": chat, "message_id": mid, "text": _stat_one(eng, per), "reply_markup": _kb(_period_kb(eng))})
                    continue
                m = u.get("message") or u.get("channel_post") or {}
                chat = str((m.get("chat") or {}).get("id", ""))
                if not m.get("text"):
                    continue
                if TG.get("chat") and chat and chat != str(TG["chat"]):
                    continue                      # only respond to the configured chat
                _tg_api("sendMessage", {"chat_id": chat, "text": "📊 Stats — welcher Bot?", "reply_markup": _kb(_MENU_KB)})
            drained = True
        except Exception as e:
            print("tg listener error:", e); time.sleep(5)


# ---------------- HL reads (public) ----------------
def hl_post(base, body, timeout=10):
    req = urllib.request.Request(base + "/info", data=json.dumps(body).encode(),
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())

_dex_cache = None
def perp_dexes(base):
    global _dex_cache
    if _dex_cache is None:
        try:
            d = hl_post(base, {"type": "perpDexs"})
            _dex_cache = [""] + [x["name"] for x in d if x and x.get("name")]
        except Exception:
            _dex_cache = [""]
    return _dex_cache

def parse_pos(ap, dex):
    p = ap.get("position", {})
    szi = float(p.get("szi") or 0)
    if not szi:
        return None
    raw = p.get("coin", "")
    bare = raw.split(":")[1] if ":" in raw else raw
    lev = p.get("leverage", {}) or {}
    val = float(p.get("positionValue") or 0)
    return {"key": (dex, bare), "coin": raw if dex else bare, "bare": bare, "dex": dex,
            "szi": szi, "side": "LONG" if szi > 0 else "SHORT",
            "entry": float(p.get("entryPx") or 0), "lev": float(lev.get("value") or 1),
            "mode": lev.get("type") or "cross", "mark": (val / abs(szi)) if szi else 0}

def _fetch_ch(base, addr, dex):
    """Fetch one dex's clearinghouseState. Returns (dex, data|None)."""
    body = {"type": "clearinghouseState", "user": addr}
    if dex:
        body["dex"] = dex
    try:
        return dex, hl_post(base, body, timeout=6)   # short timeout: a hung dex must not stall the poll
    except Exception:
        return dex, None

def _fetch_all_dexes(base, addr):
    """Fetch every perp dex's clearinghouseState in ONE parallel batch so the
    whole read takes ~one timeout regardless of how many dexes exist (HIP-3 has
    grown to dozens). Returns list of (dex, data|None)."""
    dexes = perp_dexes(base)
    workers = min(64, max(1, len(dexes)))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        return list(pool.map(lambda dx: _fetch_ch(base, addr, dx), dexes))

def get_positions_ex(base, addr):
    """Returns (positions_by_key, ok_dexes). ok_dexes = dexes that were read
    successfully this call. A dex missing from ok means its read failed, so we
    must NOT treat its absent positions as 'closed'."""
    out = {}
    ok = set()
    for dex, d in _fetch_all_dexes(base, addr):
        if d is None:
            continue
        ok.add(dex)
        for ap in d.get("assetPositions", []):
            x = parse_pos(ap, dex)
            if x:
                out[x["key"]] = x
    return out, ok

def get_positions(base, addr):
    return get_positions_ex(base, addr)[0]

def get_equity(base, addr):
    eq = 0.0
    for dex, d in _fetch_all_dexes(base, addr):
        if d is None:
            continue
        try:
            eq += float(d.get("marginSummary", {}).get("accountValue") or 0)
        except Exception:
            pass
    return eq

def get_spot_equity(base, addr):
    """USD value of SPOT balances (USDC at par + tokens at their spot mark price).
    Best-effort: any failure returns 0.0 so it can never break the perp-equity read."""
    try:
        st = hl_post(base, {"type": "spotClearinghouseState", "user": addr})
        bals = st.get("balances") or []
        if not bals:
            return 0.0
        prices = {"USDC": 1.0}
        try:
            meta, ctxs = hl_post(base, {"type": "spotMetaAndAssetCtxs"})
            toks = {t.get("index", i): t.get("name") for i, t in enumerate(meta.get("tokens", []))}
            for i, u in enumerate(meta.get("universe", [])):
                tk = u.get("tokens") or []
                if len(tk) == 2 and tk[1] == 0:                 # pair quoted in USDC (token index 0)
                    name = toks.get(tk[0]); px = float((ctxs[i] or {}).get("markPx") or 0)
                    if name and px > 0:
                        prices[name] = px
        except Exception:
            pass
        total = 0.0
        for b in bals:
            coin = b.get("coin", ""); amt = float(b.get("total") or 0)
            if amt:
                total += amt * prices.get(coin, 1.0 if coin == "USDC" else 0.0)
        return total
    except Exception:
        return 0.0

def get_portfolio_value(base, addr):
    """Total account value Hyperliquid shows = perp equity + spot equity.
    Returns (perp, spot, total)."""
    perp = get_equity(base, addr)
    spot = get_spot_equity(base, addr)
    return perp, spot, perp + spot

def get_unrealized_pnl(base, addr):
    """Sum unrealized PnL across ALL perp dexes (main + HIP-3 builder dexes)."""
    upnl = 0.0
    try:
        for dex, d in _fetch_all_dexes(base, addr):
            if not d:
                continue
            for ap in d.get("assetPositions", []):
                try:
                    upnl += float((ap.get("position") or {}).get("unrealizedPnl") or 0)
                except Exception:
                    pass
    except Exception:
        pass
    return upnl

def get_unified_value(base, addr):
    """Hyperliquid UNIFIED account Portfolio Value = your USDC/spot collateral + the
    unrealized PnL of all open positions. The per-dex perp 'accountValue' fields are just
    views of the SAME shared collateral, so they must NOT be summed (that double-counts).
    This matches the single 'Portfolio Value' number the Hyperliquid UI shows."""
    return get_spot_equity(base, addr) + get_unrealized_pnl(base, addr)


# ---------------- sizing / rounding ----------------
_meta = None
def sz_decimals(base, coin):
    global _meta
    if _meta is None:
        try:
            _meta = {a["name"]: a.get("szDecimals", 3) for a in hl_post(base, {"type": "meta"})["universe"]}
        except Exception:
            _meta = {}
    return _meta.get(coin, 3)

def round_sz(base, coin, sz):
    return round(sz, sz_decimals(base, coin))

def round_px(px):
    if px <= 0:
        return px
    digits = 5 - int(math.floor(math.log10(abs(px)))) - 1
    return round(px, max(0, digits))

def my_leverage(whale_lev, mode):
    return FIXED_LEV          # fixed leverage on every trade, regardless of the whale's

def scaled_tp(base_tp, trader_lev, our_lev):
    """We trade a fixed leverage (6x). If the source trader uses LOWER leverage than us,
    our position is more aggressive than theirs -> scale the take-profit DOWN proportionally
    so we exit at a smaller price move and don't out-risk them. Example: base 12% TP at 6x,
    trader at 3x -> TP 6%. Never scales the TP up (trader >= our leverage keeps base TP)."""
    try:
        tl = float(trader_lev or 0); ol = float(our_lev or 0)
        if tl > 0 and ol > 0 and tl < ol:
            return round(base_tp * (tl / ol), 4)
    except Exception:
        pass
    return base_tp


# ---------------- paper helpers ----------------
def _mk(key, p, whale, mids):
    """current mark for one of our paper positions"""
    if key in whale and whale[key]["mark"]:
        return whale[key]["mark"]
    try:
        v = float(mids.get(p["bare"]) or 0)
        if v > 0:
            return v
    except Exception:
        pass
    return p["entry"]

def _pnl(key, p, whale, mids):
    sign = 1 if p["side"] == "LONG" else -1
    return sign * p["sz"] * (_mk(key, p, whale, mids) - p["entry"])

def paper_equity(whale=None, mids=None):
    whale = whale or {}
    mids = mids or {}
    return PAPER["cash"] + sum(_pnl(k, p, whale, mids) for k, p in PAPER["pos"].items())


# ---------------- real order actions (non-paper) ----------------
def open_copy(ex, w, equity):
    lev = my_leverage(w["lev"], w["mode"])
    is_cross = (w["mode"] == "cross")
    is_buy = w["szi"] > 0
    coin, mark = w["coin"], (w["mark"] or 1)
    margin = equity * CAPITAL_FRACTION
    sz = round_sz(EXEC_URL, w["bare"], (margin * lev) / mark)
    if sz <= 0:
        tg("⚠️ Skipped %s: size 0 (not enough capital?)" % w["bare"]); return
    try:
        ex.update_leverage(lev, coin, is_cross)
        ex.market_open(coin, is_buy, sz)
        time.sleep(1.0)
        mine = get_positions(EXEC_URL, ex.account_address).get(w["key"])
        entry = mine["entry"] if mine else mark
        tp_roe = SET["tp_stock"] if w["dex"] else SET["tp_crypto"]
        move = tp_roe / lev
        tp = round_px(entry * (1 + move) if is_buy else entry * (1 - move))
        ex.order(coin, (not is_buy), sz, tp,
                 {"trigger": {"triggerPx": tp, "isMarket": True, "tpsl": "tp"}}, reduce_only=True)
        tg("✅ COPIED %s %s · size %.4f @ ~$%.4f · %dx %s · TP +%.0f%% ROE @ $%s"
           % (w["bare"], w["side"], sz, entry, lev, "Cross" if is_cross else "Isolated", tp_roe * 100, tp))
    except Exception as e:
        tg("❌ Error opening %s: %s" % (w["bare"], e))

def close_copy(ex, w, reason):
    try:
        ex.market_close(w["coin"]); tg("🔻 CLOSED %s (%s)" % (w["bare"], reason))
    except Exception as e:
        tg("❌ Error closing %s: %s" % (w["bare"], e))

def flatten_all():
    # paper mode
    if PAPER_MODE:
        try:    whale = get_positions(SOURCE_URL, WHALE)
        except Exception: whale = {}
        try:    mids = hl_post(SOURCE_URL, {"type": "allMids"})
        except Exception: mids = {}
        msgs = ["🧹 Flatten: closing all paper positions…"]
        with LOCK:
            for key in list(PAPER["pos"].keys()):
                p = PAPER["pos"].pop(key)
                pnl = _pnl(key, p, whale, mids)
                PAPER["cash"] += pnl
                record_closed(p, pnl, "manual")
                msgs.append("🔻 CLOSED %s (manual) — PnL $%.2f" % (p["bare"], pnl))
        for m in msgs:
            tg(m)
        return
    # real mode
    ex = EX["ex"]
    if not ex:
        return
    tg("🧹 Flatten: closing all positions…")
    for p in get_positions(EXEC_URL, ex.account_address).values():
        try:
            ex.market_close(p["coin"])
        except Exception as e:
            tg("❌ Flatten %s: %s" % (p["bare"], e))


# ---------------- web server (dashboard + status + controls) ----------------
HERE = os.path.dirname(os.path.abspath(__file__))
SECRETS_FILE = os.path.join(HERE, "secrets.json")   # local only, NOT in git

def load_secrets():
    global DASH_TOKEN
    if not os.path.exists(SECRETS_FILE):
        print("secrets.json NOT FOUND at %s — starting with %d default wallets" % (SECRETS_FILE, len(WALLETS)))
        return
    try:
        d = json.load(open(SECRETS_FILE))
        if d.get("tg_token"):
            TG["token"] = d["tg_token"]
        if d.get("tg_chat"):
            TG["chat"] = str(d["tg_chat"])
        if d.get("tp_crypto"):
            SET["tp_crypto"] = float(d["tp_crypto"])
        if d.get("tp_stock"):
            SET["tp_stock"] = float(d["tp_stock"])
        # keep the dashboard token stable across restarts (env var still wins)
        if not os.environ.get("DASH_TOKEN") and d.get("dash_token"):
            DASH_TOKEN = d["dash_token"]
        if isinstance(d.get("wallets"), list) and d["wallets"]:
            WALLETS[:] = [{"addr": w["addr"], "label": w.get("label", "Wallet")}
                          for w in d["wallets"] if w.get("addr")]
        print("secrets.json loaded: %d wallets, Telegram %s, token persisted" %
              (len(WALLETS), "on" if TG["token"] else "off"))
    except Exception as e:
        print("load_secrets error:", e)

def save_secrets():
    try:
        tmp = SECRETS_FILE + ".tmp"
        json.dump({"tg_token": TG["token"], "tg_chat": TG["chat"],
                   "tp_crypto": SET["tp_crypto"], "tp_stock": SET["tp_stock"],
                   "dash_token": DASH_TOKEN, "wallets": WALLETS},
                  open(tmp, "w"))
        os.replace(tmp, SECRETS_FILE)   # atomic write so a crash can't truncate the file
        print("saved secrets.json: %d wallets, Telegram %s" % (len(WALLETS), "on" if TG["token"] else "off"))
    except Exception as e:
        print("save_secrets error:", e)

class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _auth(self):
        q = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
        return q.get("token", [""])[0] == DASH_TOKEN

    def _send(self, code, body, ctype="application/json", no_cache=False):
        if isinstance(body, str):
            body = body.encode()
        try:
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Access-Control-Allow-Origin", "*")
            if no_cache:
                self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
                self.send_header("Pragma", "no-cache")
                self.send_header("Expires", "0")
            self.end_headers()
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
            pass   # client (browser/Tailscale) closed mid-response — harmless, don't spam the log

    def handle_one_request(self):
        try:
            super().handle_one_request()
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
            self.close_connection = True

    def do_GET(self):
        path = urllib.parse.urlparse(self.path).path
        if path in ("/", "/index.html"):
            try:
                html = open(os.path.join(HERE, "index.html"), encoding="utf-8").read()
            except Exception:
                return self._send(404, "index.html nicht gefunden (neben bot.py legen)", "text/plain")
            return self._send(200, html.replace("__DASH_TOKEN__", DASH_TOKEN), "text/html; charset=utf-8", no_cache=True)
        if path == "/status":
            if not self._auth():
                return self._send(403, json.dumps({"error": "forbidden"}))
            with LOCK:
                st = dict(STATE)
                st["day_pnl"] = round(st["equity"] - st["day_start_equity"], 2)
                st["tg_on"] = bool(TG["token"])
                st["tg_chat"] = TG["chat"] if bool(TG["token"]) else ""
                st["tp_crypto_pct"] = round(SET["tp_crypto"] * 100, 2)
                st["tp_stock_pct"] = round(SET["tp_stock"] * 100, 2)
                st["fixed_lev"] = FIXED_LEV
                st["capital_pct"] = round(CAPITAL_FRACTION * 100, 1)
                st["daily_loss_pct"] = round(DAILY_LOSS_LIMIT * 100, 1)
                st["wallets"] = wallets_payload()
                st["smart"] = STATE.get("smart", {})
                st["live"] = STATE.get("live", {})
                st["mode"] = MODE["v"]
                st["log"] = STATE["log"][-60:]
            return self._send(200, json.dumps(st))
        return self._send(404, "not found", "text/plain")

    def do_POST(self):
        path = urllib.parse.urlparse(self.path).path
        if not self._auth():
            return self._send(403, json.dumps({"error": "forbidden"}))
        if path == "/pause":
            STATE["paused"] = True;  tg("⏸ Bot paused (dashboard)")
        elif path == "/resume":
            STATE["paused"] = False; tg("▶️ Bot resumed (dashboard)")
        elif path == "/flatten":
            threading.Thread(target=flatten_all, daemon=True).start()
        elif path == "/adopt":
            ADOPT["pending"] = True
            tg("➕ Adopt requested — copying current whale positions at their entry…")
        elif path == "/settg":
            ln = int(self.headers.get("Content-Length") or 0)
            raw = self.rfile.read(ln).decode("utf-8", "ignore") if ln > 0 else ""
            body = urllib.parse.parse_qs(raw)
            tok = (body.get("tg_token", [""])[0] or "").strip()
            chat = (body.get("tg_chat", [""])[0] or "").strip()
            if tok:
                TG["token"] = tok
            if chat:
                TG["chat"] = chat
            save_secrets()
            if TG["token"]:
                threading.Thread(target=lambda: tg("✅ Telegram connected — you'll get notifications here from now on."), daemon=True).start()
            return self._send(200, json.dumps({"ok": True, "tg_on": bool(TG["token"])}))
        elif path == "/cleartg":
            TG["token"] = ""
            save_secrets()
            return self._send(200, json.dumps({"ok": True, "tg_on": False}))
        elif path == "/settp":
            ln = int(self.headers.get("Content-Length") or 0)
            raw = self.rfile.read(ln).decode("utf-8", "ignore") if ln > 0 else ""
            body = urllib.parse.parse_qs(raw)
            def _pct(name, cur):
                try:
                    v = float(body.get(name, [""])[0])
                    if 0 < v <= 1000:
                        return v / 100.0
                except Exception:
                    pass
                return cur
            SET["tp_crypto"] = _pct("tp_crypto", SET["tp_crypto"])
            SET["tp_stock"] = _pct("tp_stock", SET["tp_stock"])
            save_secrets()
            tg("⚙️ Take-profit set · Crypto +%.0f%% · Stocks +%.0f%% (applies to new trades)"
               % (SET["tp_crypto"] * 100, SET["tp_stock"] * 100))
            return self._send(200, json.dumps({"ok": True,
                              "tp_crypto_pct": round(SET["tp_crypto"] * 100, 2),
                              "tp_stock_pct": round(SET["tp_stock"] * 100, 2)}))
        elif path == "/addwallet":
            ln = int(self.headers.get("Content-Length") or 0)
            raw = self.rfile.read(ln).decode("utf-8", "ignore") if ln > 0 else ""
            body = urllib.parse.parse_qs(raw)
            addr = (body.get("addr", [""])[0] or "").strip()
            label = (body.get("label", [""])[0] or "").strip()
            if not ADDR_RE.match(addr):
                return self._send(200, json.dumps({"ok": False, "error": "invalid address"}))
            if any(w["addr"].lower() == addr.lower() for w in WALLETS):
                return self._send(200, json.dumps({"ok": False, "error": "already tracked",
                                  "wallets": wallets_payload()}))
            if not label:
                label = "Wallet %d" % (len(WALLETS) + 1)
            WALLETS.append({"addr": addr, "label": label})
            save_secrets()
            tg("➕ Now tracking %s (%s)" % (label, addr[:6] + "…" + addr[-4:]))
            return self._send(200, json.dumps({"ok": True, "wallets": wallets_payload()}))
        elif path == "/live_connect":
            ln = int(self.headers.get("Content-Length") or 0)
            raw = self.rfile.read(ln).decode("utf-8", "ignore") if ln > 0 else ""
            body = urllib.parse.parse_qs(raw)
            addr = (body.get("account_address", [""])[0] or "").strip()
            key = (body.get("agent_key", [""])[0] or "").strip()
            if key and not key.startswith("0x"):
                key = "0x" + key          # accept a key pasted without the 0x prefix
            if not ADDR_RE.match(addr):
                return self._send(200, json.dumps({"ok": False, "error": "Account-Adresse ungültig (0x + 40 Hex-Zeichen)"}))
            if not KEY_RE.match(key):
                return self._send(200, json.dumps({"ok": False, "error": "Agent-Key ungültig (0x + 64 Hex-Zeichen)"}))
            try:
                live_save_credentials(key, addr)
            except Exception as e:
                return self._send(200, json.dumps({"ok": False, "error": "Speichern fehlgeschlagen: %s" % e}))
            # force a fresh Exchange build with the new key, then verify it works
            LIVE["ready"] = False; LIVE["ex"] = None; LIVE["err"] = ""
            ok, msg = live_init()
            if not ok:
                return self._send(200, json.dumps({"ok": False, "error": msg, "connected": True}))
            tg("🔗 Hyperliquid verbunden (Agent-Key) · Account %s" % (addr[:6] + "…" + addr[-4:]))
            return self._send(200, json.dumps({"ok": True, "connected": True, "ready": LIVE["ready"], "addr": addr}))
        elif path == "/live_disconnect":
            live_clear_credentials()
            live_save_config()
            tg("🔌 Hyperliquid getrennt — Agent-Key entfernt, Live-Engine aus.")
            return self._send(200, json.dumps({"ok": True, "connected": False}))
        elif path == "/set_mode":
            ln = int(self.headers.get("Content-Length") or 0)
            raw = self.rfile.read(ln).decode("utf-8", "ignore") if ln > 0 else ""
            body = urllib.parse.parse_qs(raw)
            new = (body.get("mode", [""])[0] or "").strip()
            ok, msg = set_mode(new)
            return self._send(200, json.dumps({"ok": ok, "mode": MODE["v"], "error": "" if ok else msg}))
        elif path == "/live_toggle":
            ln = int(self.headers.get("Content-Length") or 0)
            raw = self.rfile.read(ln).decode("utf-8", "ignore") if ln > 0 else ""
            body = urllib.parse.parse_qs(raw)
            want = (body.get("on", [""])[0] or "").strip() == "1"
            if want and MODE["v"] != "live":
                return self._send(200, json.dumps({"ok": False, "error": "Modus steht auf Smart Money — erst auf Live umschalten."}))
            if want:
                ok, msg = live_init()
                if not ok:
                    return self._send(200, json.dumps({"ok": False, "error": msg}))
                LIVE["enabled"] = True
            else:
                LIVE["enabled"] = False
            live_save_config()
            return self._send(200, json.dumps({"ok": True, "enabled": LIVE["enabled"], "ready": LIVE["ready"], "err": LIVE["err"]}))
        elif path == "/live_settings":
            ln = int(self.headers.get("Content-Length") or 0)
            raw = self.rfile.read(ln).decode("utf-8", "ignore") if ln > 0 else ""
            body = urllib.parse.parse_qs(raw)
            if "max_lev" in body:
                try:
                    ml = int(float(body.get("max_lev", [""])[0]))
                    LIVE["max_lev"] = max(1, min(ml, 40))
                    tg("⚙️ LIVE Leverage-Cap = %d× (gilt für neue Live-Trades)" % LIVE["max_lev"])
                except Exception:
                    pass
            if "killswitch" in body:
                LIVE["killswitch"] = (body.get("killswitch", [""])[0] == "1")
                tg("⚙️ LIVE Kill-Switch %s" % ("AN (−%.0f%% Tagesstopp)" % (DAILY_LOSS_LIMIT * 100) if LIVE["killswitch"] else "AUS — keine automatische Verlustbremse!"))
            live_save_config()
            return self._send(200, json.dumps({"ok": True, "max_lev": LIVE["max_lev"], "lev": live_lev(), "killswitch": LIVE["killswitch"]}))
        elif path == "/live_flatten":
            threading.Thread(target=live_flatten, daemon=True).start()
            return self._send(200, json.dumps({"ok": True}))
        elif path == "/live_test_order":
            ln = int(self.headers.get("Content-Length") or 0)
            raw = self.rfile.read(ln).decode("utf-8", "ignore") if ln > 0 else ""
            body = urllib.parse.parse_qs(raw)
            coin = (body.get("coin", ["BTC"])[0] or "BTC").strip().upper()
            side = (body.get("side", ["long"])[0] or "long").strip().lower()
            try:    margin = float(body.get("margin", ["10"])[0])
            except Exception: margin = 10.0
            margin = max(1.0, min(margin, 1000.0))   # safety cap on a manual test order
            ok, msg = live_test_open(coin, side != "short", margin)
            return self._send(200, json.dumps({"ok": ok, "error": "" if ok else msg}))
        elif path == "/delwallet":
            ln = int(self.headers.get("Content-Length") or 0)
            raw = self.rfile.read(ln).decode("utf-8", "ignore") if ln > 0 else ""
            body = urllib.parse.parse_qs(raw)
            addr = (body.get("addr", [""])[0] or "").strip().lower()
            WALLETS[:] = [w for w in WALLETS if w["addr"].lower() != addr]
            save_secrets()
            return self._send(200, json.dumps({"ok": True, "wallets": wallets_payload()}))
        else:
            return self._send(404, "not found", "text/plain")
        return self._send(200, json.dumps({"ok": True}))

def start_tunnel():
    """Download cloudflared and open a free https tunnel so the dashboard
    is reachable from any network (e.g. a restricted PC)."""
    if not START_CF_TUNNEL:
        return
    link_file = os.path.join(HERE, "link.txt")
    try:
        open(link_file, "w").write("Tunnel startet… in ~10s nochmal 'cat link.txt'\n")
    except Exception:
        pass
    cf = os.path.join(HERE, "cloudflared")
    try:
        if not os.path.exists(cf):
            print("Lade Tunnel-Programm (cloudflared) ...")
            urllib.request.urlretrieve(
                "https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64", cf)
            os.chmod(cf, 0o755)
        proc = subprocess.Popen(
            [cf, "tunnel", "--url", "http://localhost:" + str(DASH_PORT), "--no-autoupdate"],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
        for line in proc.stdout:
            m = re.search(r"https://[a-z0-9-]+\.trycloudflare\.com", line)
            if m:
                url = m.group(0)
                try:
                    open(link_file, "w").write(url + "\n")
                except Exception:
                    pass
                print("\n==================================================")
                print(">>> DASHBOARD-LINK (auch fuer den PC, durch jede Firewall):")
                print(">>> " + url)
                print("==================================================\n")
                tg("🔗 Dashboard link (PC): " + url)
                break
    except Exception as e:
        print("Tunnel-Fehler:", e)


def start_server():
    srv = ThreadingHTTPServer((DASH_HOST, DASH_PORT), Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()


# ---------------- paper persistence + display helpers ----------------
PAPER_FILE = os.path.join(HERE, "paper_state.json")

def save_paper(day_start_eq, day_start_cash, day):
    try:
        data = {"cash": PAPER["cash"],
                "pos": {("%s|%s" % k): v for k, v in PAPER["pos"].items()},
                "hist": PAPER["hist"][-3000:],
                "closed": PAPER.get("closed", [])[-500:],
                "mine_pos": {("%s|%s" % k): v for k, v in MINE["pos"].items()},
                "mine_closed": MINE.get("closed", [])[-500:],
                "day_start_eq": day_start_eq,
                "day_start_cash": day_start_cash,
                "day": day.isoformat()}
        tmp = PAPER_FILE + ".tmp"
        json.dump(data, open(tmp, "w"))
        os.replace(tmp, PAPER_FILE)
    except Exception as e:
        print("save_paper error:", e)

def load_paper():
    if not os.path.exists(PAPER_FILE):
        return None
    try:
        d = json.load(open(PAPER_FILE))
        PAPER["cash"] = d.get("cash", PAPER_START)
        PAPER["pos"] = {}
        for ks, v in (d.get("pos") or {}).items():
            dex, bare = ks.split("|", 1)
            PAPER["pos"][(dex, bare)] = v
        PAPER["hist"] = d.get("hist") or []
        PAPER["closed"] = d.get("closed") or []
        MINE["pos"] = {}
        for ks, v in (d.get("mine_pos") or {}).items():
            dex, bare = ks.split("|", 1)
            MINE["pos"][(dex, bare)] = v
        MINE["closed"] = d.get("mine_closed") or []
        ds = d.get("day_start_eq", PAPER["cash"])
        dsc = d.get("day_start_cash", PAPER["cash"])
        day = (datetime.date.fromisoformat(d["day"]) if d.get("day")
               else datetime.datetime.now(datetime.timezone.utc).date())
        return ds, dsc, day
    except Exception as e:
        print("load_paper error:", e)
        return None

def snapshot(equity):
    now_ms = int(time.time() * 1000)
    h = PAPER["hist"]
    if not h or (now_ms - h[-1][0]) >= HIST_EVERY * 1000:
        h.append([now_ms, round(equity, 2)])
        if len(h) > 5000:
            del h[:len(h) - 5000]

def perf_window(equity, ms):
    h = PAPER["hist"]
    if not h:
        return equity - PAPER_START
    cutoff = (time.time() * 1000) - ms
    base = None
    for t, v in h:
        if t <= cutoff:
            base = v
        else:
            break
    if base is None:
        base = h[0][1]
    return equity - base

def record_closed(p, pnl, reason):
    """Append a structured record whenever a paper position is closed."""
    roe = (pnl / p["margin"]) if p.get("margin") else 0.0
    PAPER.setdefault("closed", []).append({
        "bare": p.get("bare"), "coin": p.get("coin", p.get("bare")),
        "kind": "Stock" if p.get("dex") else "Crypto",
        "side": p.get("side"), "entry": p.get("entry"),
        "lev": p.get("lev"), "mode": p.get("mode"), "margin": round(p.get("margin", 0), 2),
        "pnl": round(pnl, 2), "roe": round(roe, 4), "peak_roe": round(p.get("peak_roe", 0.0), 4),
        "opened_ms": p.get("opened_ms", 0), "closed_ms": int(time.time() * 1000),
        "reason": reason,
    })
    if len(PAPER["closed"]) > 500:
        del PAPER["closed"][:len(PAPER["closed"]) - 500]

def paper_normalize_tps_today(target=TP_ROE):
    """One-time (idempotent) correction: rewrite TODAY's already-closed take-profit trades
    to exactly +target ROE and refund the over-target overshoot back into cash. These only
    overshot 12% because the bot was frozen/restarted and filled late — clean up the history."""
    today = datetime.datetime.now(LOCAL_TZ).date() if LOCAL_TZ else datetime.date.today()
    fixed = 0; delta = 0.0
    for t in PAPER.get("closed", []):
        if t.get("reason") != "take-profit":
            continue
        cms = t.get("closed_ms") or 0
        if not cms:
            continue
        d = (datetime.datetime.fromtimestamp(cms / 1000, LOCAL_TZ).date() if LOCAL_TZ
             else datetime.datetime.fromtimestamp(cms / 1000).date())
        if d != today:
            continue
        m = t.get("margin") or 0
        tgt = round(target * m, 2)
        if m > 0 and float(t.get("pnl", 0)) > tgt + 0.01:
            delta += float(t["pnl"]) - tgt
            t["pnl"] = tgt; t["roe"] = target; fixed += 1
    if fixed:
        PAPER["cash"] = round(PAPER["cash"] - delta, 2)
        print("paper: normalized %d take-profits today to %.0f%% ROE (cash -$%.2f)" % (fixed, target * 100, delta))
    return fixed

def closed_summary():
    c = PAPER.get("closed", [])
    wins = sum(1 for t in c if t["pnl"] > 0)
    total = sum(t["pnl"] for t in c)
    return {"count": len(c), "wins": wins,
            "win_rate": round(100.0 * wins / len(c), 1) if c else 0.0,
            "realized": round(total, 2)}

# ---- log YOUR real trades (MY_WALLET) with the real leverage seen while open ----
_mymiss = {}        # key -> consecutive CONFIRMED-absent polls (debounced close)
_myfirst = True     # first poll = baseline: pre-existing positions have unknown open time

def my_closed_summary():
    c = MINE.get("closed", [])
    wins = sum(1 for t in c if t["pnl"] > 0)
    total = sum(t["pnl"] for t in c)
    return {"count": len(c), "wins": wins,
            "win_rate": round(100.0 * wins / len(c), 1) if c else 0.0,
            "realized": round(total, 2)}

def log_my_trades(mine_now, myok):
    """Update open snapshots and record closed trades for MY_WALLET. Call inside LOCK
    (no network here — positions are fetched in the loop). Uses the real leverage/margin
    Hyperliquid reports while a position is open, so closed-trade ROE is exact."""
    global _myfirst
    now_ms = int(time.time() * 1000)
    for key, w in mine_now.items():
        mk = w["mark"] or w["entry"] or 0
        szabs = abs(w["szi"])
        lev = w["lev"] or 1
        margin = (mk * szabs / lev) if lev else (mk * szabs)
        if key in MINE["pos"]:
            MINE["pos"][key].update(entry=w["entry"], lev=lev, mode=w["mode"], side=w["side"],
                                    sz=szabs, margin=margin, last_mark=mk)
        else:
            MINE["pos"][key] = {"bare": w["bare"], "coin": w["coin"], "dex": w["dex"],
                                "side": w["side"], "entry": w["entry"], "lev": lev,
                                "mode": w["mode"], "sz": szabs, "margin": margin, "last_mark": mk,
                                "opened_ms": 0 if _myfirst else now_ms}
        cur = MINE["pos"][key]                         # track best ROE reached (for the popup)
        sign = 1 if w["side"] == "LONG" else -1
        roe_now = (sign * szabs * (mk - w["entry"]) / margin) if margin else 0.0
        cur["peak_roe"] = max(cur.get("peak_roe", roe_now), roe_now)
        _mymiss[key] = 0
    for key in list(MINE["pos"].keys()):
        if key in mine_now:
            continue
        if key[0] not in myok:
            continue                       # that dex wasn't read this poll -> leave untouched
        _mymiss[key] = _mymiss.get(key, 0) + 1
        if _mymiss[key] >= CLOSE_CONFIRM:
            p = MINE["pos"].pop(key); _mymiss.pop(key, None)
            sign = 1 if p["side"] == "LONG" else -1
            exitpx = p.get("last_mark") or p["entry"]
            pnl = sign * p["sz"] * (exitpx - p["entry"])
            roe = (pnl / p["margin"]) if p["margin"] else 0.0
            MINE["closed"].append({
                "bare": p["bare"], "coin": p["coin"], "kind": "Stock" if p["dex"] else "Crypto",
                "side": p["side"], "entry": p["entry"], "exit": exitpx,
                "lev": int(round(p["lev"])) if p.get("lev") else 1, "mode": p["mode"], "margin": round(p["margin"], 2),
                "pnl": round(pnl, 2), "roe": round(roe, 4), "peak_roe": round(p.get("peak_roe", 0.0), 4),
                "opened_ms": p.get("opened_ms", 0), "closed_ms": now_ms})
            if len(MINE["closed"]) > 500:
                del MINE["closed"][:len(MINE["closed"]) - 500]
    _myfirst = False

def build_positions(whale, mids):
    out = []
    for key, p in PAPER["pos"].items():
        mk = _mk(key, p, whale, mids)
        sign = 1 if p["side"] == "LONG" else -1
        upnl = sign * p["sz"] * (mk - p["entry"])
        roe = (upnl / p["margin"]) if p["margin"] else 0
        peak = max(p.get("peak_roe", roe), roe)
        p["peak_roe"] = peak
        out.append({"bare": p["bare"], "coin": p.get("coin", p["bare"]),
                    "kind": "Stock" if p.get("dex") else "Crypto",
                    "side": p["side"], "sz": p["sz"], "entry": p["entry"], "mark": mk,
                    "lev": p["lev"], "mode": p["mode"], "margin": p["margin"],
                    "upnl": round(upnl, 2), "roe": round(roe, 4),
                    "peak_roe": round(peak, 4), "tp": p.get("tp"),
                    "value": abs(p["sz"]) * mk,
                    "opened": p.get("opened_ms", 0)})
    out.sort(key=lambda x: x["value"], reverse=True)
    return out

def publish_state(equity, day_start_eq, killed, whale, mids):
    """Write the full snapshot the dashboard reads. Call inside LOCK."""
    snapshot(equity)
    STATE.update(equity=round(equity, 2), day_start_equity=round(day_start_eq, 2),
                 slots_used=len(PAPER["pos"]), killed=killed,
                 cash=round(PAPER["cash"], 2), start_equity=PAPER_START,
                 positions=build_positions(whale, mids),
                 history=PAPER["hist"][-1500:],
                 closed=list(reversed(PAPER.get("closed", [])[-80:])),
                 closed_stats=closed_summary(),
                 my_closed=list(reversed(MINE.get("closed", [])[-120:])),
                 my_closed_stats=my_closed_summary(),
                 pnl_24h=round(perf_window(equity, 86400000), 2),
                 pnl_7d=round(perf_window(equity, 604800000), 2),
                 pnl_30d=round(perf_window(equity, 2592000000), 2),
                 pnl_all=round(equity - PAPER_START, 2))


# ============================ SMART-MONEY ENGINE ============================
# A second, independent paper account that tracks the week's top-100 traders
# (ranked by ROI, filtered by win-rate from real fills) 24/7 and copies each
# genuinely NEW position they open. Runs in its own thread alongside run_paper().
SMART_START   = 1000.0
SMART_FRAC    = 0.20          # 20% of equity as margin per trade
SMART_LEV     = 6            # fixed leverage
SMART_TP      = 0.12          # take-profit ROE
SMART_MAX     = 5            # max concurrent positions
SMART_TOPN    = 100          # size of the tracked list (we aim to fill all 100)
SMART_POOL    = 320          # leaderboard candidates evaluated for win-rate (bigger -> fills 100)
SMART_MINWR   = 42.0         # minimum win-rate % to qualify
SMART_MINTR   = 8            # minimum closed trades to trust the win-rate
SMART_REBUILD_MIN = 60       # rebuild the top list every N minutes (list refresh, NOT trade-scan)
SMART_CYCLE   = 60.0         # target seconds for one full pass over the list (continuous trade-scan)
LB_URL        = "https://stats-data.hyperliquid.xyz/Mainnet/leaderboard"

SMART = {"cash": SMART_START, "pos": {}, "closed": [], "hist": []}   # pos keyed by bare coin
SMART_TOP = []               # [{addr,name,roi,wr,score}]
SMART_SIG = []               # recent decision log [{t,text,act}]
_smart_last = {}             # addr -> set of bare coins open (main dex) last poll
_smart_miss = {}             # coin -> consecutive polls the source was absent
SMART_FILE = os.path.join(HERE, "smart_state.json")

def smart_log(text, act=""):
    SMART_SIG.insert(0, {"t": now_hms()[:5], "text": text, "act": act})
    del SMART_SIG[80:]

def fetch_leaderboard():
    req = urllib.request.Request(LB_URL, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=25) as r:
        return json.loads(r.read()).get("leaderboardRows", [])

def _wp(row, w):
    for a in row.get("windowPerformances", []):
        if a and a[0] == w:
            return a[1]
    return None

def smart_winrate(addr):
    try:
        fills = hl_post(SOURCE_URL, {"type": "userFills", "user": addr}, timeout=8)
    except Exception:
        return 0.0, 0
    w = l = 0
    for f in (fills or []):
        cp = float(f.get("closedPnl") or 0)
        if cp > 0: w += 1
        elif cp < 0: l += 1
    n = w + l
    return (round(100.0 * w / n, 1) if n else 0.0), n

SMART_BUILD = {"on": False, "done": 0, "total": 0}
def smart_set_build(on, done=0, total=0):
    SMART_BUILD["on"] = on; SMART_BUILD["done"] = done; SMART_BUILD["total"] = total
    with LOCK:
        base = dict(STATE.get("smart", {}))
        base["building"] = on; base["build_done"] = done; base["build_total"] = total
        base.setdefault("signals", SMART_SIG[:40]); base.setdefault("tracked", len(SMART_TOP))
        STATE["smart"] = base

def smart_build_top():
    smart_set_build(True, 0, SMART_POOL)
    try:
        rows = fetch_leaderboard()
    except Exception as e:
        print("smart leaderboard error:", e); smart_set_build(False); return
    ranked = []
    for r in rows:
        addr = r.get("ethAddress") or ""
        wp = _wp(r, "week")
        av = float(r.get("accountValue") or 0)
        if not addr or not wp:
            continue
        roi = float(wp.get("roi") or 0)
        if roi <= 0 or av < 20000:
            continue
        ranked.append((addr, r.get("displayName") or "", roi))
    ranked.sort(key=lambda x: -x[2])
    total = min(len(ranked), SMART_POOL)
    smart_set_build(True, 0, total)
    smart_log("Leaderboard: %d Kandidaten, bewerte Win-Rate…" % total, "info")
    out = []
    for i, (addr, name, roi) in enumerate(ranked[:SMART_POOL]):
        wr, n = smart_winrate(addr)
        if wr >= SMART_MINWR and n >= SMART_MINTR:
            out.append({"addr": addr, "name": name, "roi": round(roi * 100, 1),
                        "wr": wr, "score": round(roi * (wr / 100.0), 4)})
        if i % 8 == 0:
            smart_set_build(True, i + 1, total)
        time.sleep(0.1)
    out.sort(key=lambda x: -x["score"])
    global SMART_TOP
    SMART_TOP = out[:SMART_TOPN]
    smart_set_build(False, total, total)
    smart_log("✅ Top-%d aktualisiert (%d Trader, ROI+WR gefiltert)" % (SMART_TOPN, len(SMART_TOP)), "info")

def smart_fetch_main(addr):
    """Main-perp positions only (no dex param) -> liquid coins we can price."""
    try:
        d = hl_post(SOURCE_URL, {"type": "clearinghouseState", "user": addr}, timeout=6)
    except Exception:
        return None
    out = {}
    for ap in d.get("assetPositions", []):
        x = parse_pos(ap, "")
        if x:
            out[x["bare"]] = x
    return out

def smart_mark(p, mids):
    try:
        v = float(mids.get(p["coin"]) or 0)
        if v > 0: return v
    except Exception:
        pass
    return p["entry"]

def smart_pnl(p, mids):
    sign = 1 if p["side"] == "LONG" else -1
    return sign * p["sz"] * (smart_mark(p, mids) - p["entry"])

def smart_equity(mids):
    return SMART["cash"] + sum(smart_pnl(p, mids) for p in SMART["pos"].values())

def smart_tp_price(p):
    """Exact price at which our +TP ROE is reached (ROE = lev * price-move)."""
    move = p["tp"] / p["lev"]
    return p["entry"] * (1 + move) if p["side"] == "LONG" else p["entry"] * (1 - move)

def smart_record(p, pnl, reason, mids, exitpx=None):
    roe = (pnl / p["margin"]) if p.get("margin") else 0.0
    SMART["closed"].append({"coin": p["coin"], "side": p["side"], "entry": p["entry"],
        "exit": exitpx if exitpx is not None else smart_mark(p, mids), "lev": p["lev"], "margin": round(p["margin"], 2),
        "pnl": round(pnl, 2), "roe": round(roe, 4), "peak_roe": round(p.get("peak_roe", 0.0), 4),
        "src_name": p.get("src_name", ""), "src": p.get("src", ""), "opened_ms": p.get("opened_ms", 0),
        "closed_ms": int(time.time() * 1000), "reason": reason})
    if len(SMART["closed"]) > 300:
        del SMART["closed"][:len(SMART["closed"]) - 300]

def smart_consider(coin, w, tr, mids):
    """Decision (option 2): the trader is already top-100 by ROI+win-rate, the coin
    is a liquid main perp -> copy unless we already hold it or are at max slots."""
    # NOTE: Smart Money is still PAPER, so it always runs — even alongside the Live
    # engine. Re-enable this mutual-exclusion guard once Smart Money trades real money:
    #   if MODE["v"] != "smart": return
    entry = w["mark"] or w["entry"] or 0
    nm = tr["name"] or (tr["addr"][:6] + "…")
    with LOCK:
        if coin in SMART["pos"]:
            return
        if entry <= 0:
            return
        if len(SMART["pos"]) >= SMART_MAX:
            smart_log("⏭️ %s %s von %s — 5 Slots voll" % (coin, w["side"], nm), "skip"); return
        eq = smart_equity(mids)
        margin = eq * SMART_FRAC
        sz = (margin * SMART_LEV) / entry
        tp = scaled_tp(SMART_TP, w.get("lev"), SMART_LEV)   # smaller TP if the trader's leverage < ours
        SMART["pos"][coin] = {"coin": coin, "side": w["side"], "entry": entry, "lev": SMART_LEV,
            "margin": margin, "sz": sz, "tp": tp, "src": tr["addr"], "src_name": nm,
            "opened_ms": int(time.time() * 1000), "opened": now_hms(), "peak_roe": 0.0}
    smart_log("✅ KOPIERT %s %s · %s · WR %.0f%% · TP +%.0f%%" % (coin, w["side"], nm, tr["wr"], tp * 100), "copy")
    tg("🧠 SMART ✅ COPIED %s %s · von %s · WR %.0f%% · margin $%.2f · %d× · TP +%.0f%%"
       % (coin, w["side"], nm, tr["wr"], margin, SMART_LEV, tp * 100))

def smart_publish(mids):
    eq = smart_equity(mids)
    h = SMART["hist"]; nowms = int(time.time() * 1000)
    if not h or nowms - h[-1][0] >= 60000:
        h.append([nowms, round(eq, 2)]); 
        if len(h) > 5000: del h[:len(h) - 5000]
    pos = []
    for c, p in SMART["pos"].items():
        mk = smart_mark(p, mids); sign = 1 if p["side"] == "LONG" else -1
        upnl = sign * p["sz"] * (mk - p["entry"]); roe = upnl / p["margin"] if p["margin"] else 0
        lev = p["lev"] or 1
        # isolated-margin liquidation: where the loss equals the margin (our paper engine
        # liquidates at pnl <= -margin). long: entry*(1-1/lev), short: entry*(1+1/lev).
        liq = p["entry"] * (1 - sign / lev) if lev else 0
        pos.append({"coin": c, "side": p["side"], "entry": p["entry"], "mark": mk, "lev": p["lev"],
            "liq": round(liq, 6), "margin": round(p["margin"], 2), "upnl": round(upnl, 2), "roe": round(roe, 4),
            "src_name": p.get("src_name", ""), "src": p.get("src", ""), "opened": p.get("opened_ms", 0)})
    pos.sort(key=lambda x: -abs(x["margin"]))
    wins = sum(1 for t in SMART["closed"] if t["pnl"] > 0); tot = sum(t["pnl"] for t in SMART["closed"]); n = len(SMART["closed"])
    sum_roe = sum((t.get("roe") or 0) for t in SMART["closed"]) * 100.0   # added-up % of every trade
    start_ms = SMART["hist"][0][0] if SMART["hist"] else nowms             # engine start ≈ first snapshot
    STATE["smart"] = {"equity": round(eq, 2), "cash": round(SMART["cash"], 2), "start": SMART_START,
        "pos": pos, "closed": list(reversed(SMART["closed"][-60:])), "start_ms": start_ms,
        "stats": {"count": n, "win_rate": round(100.0 * wins / n, 1) if n else 0.0,
                  "realized": round(tot, 2), "sum_roe": round(sum_roe, 1)},
        "top": SMART_TOP[:100], "signals": SMART_SIG[:40], "tracked": len(SMART_TOP),
        "lev": SMART_LEV, "tp_pct": round(SMART_TP * 100), "frac_pct": round(SMART_FRAC * 100),
        "history": SMART["hist"][-1500:], "pnl_all": round(eq - SMART_START, 2),
        "building": SMART_BUILD["on"], "build_done": SMART_BUILD["done"], "build_total": SMART_BUILD["total"],
        "next_build_ms": int((SMART_BUILD.get("last", 0) + SMART_REBUILD_MIN * 60) * 1000) if SMART_BUILD.get("last") else 0,
        "interval_min": SMART_REBUILD_MIN}

def smart_save():
    try:
        data = {"cash": SMART["cash"], "pos": SMART["pos"], "closed": SMART["closed"][-300:],
                "hist": SMART["hist"][-3000:], "top": SMART_TOP}
        tmp = SMART_FILE + ".tmp"; json.dump(data, open(tmp, "w")); os.replace(tmp, SMART_FILE)
    except Exception as e:
        print("smart_save error:", e)

def smart_load():
    global SMART_TOP
    if not os.path.exists(SMART_FILE):
        return False
    try:
        d = json.load(open(SMART_FILE))
        SMART["cash"] = d.get("cash", SMART_START)
        SMART["pos"] = d.get("pos") or {}
        SMART["closed"] = d.get("closed") or []
        SMART["hist"] = d.get("hist") or []
        SMART_TOP = d.get("top") or []
        smart_normalize_tps()   # retro-fix any take-profit that overshot +12% (idempotent)
        return True
    except Exception as e:
        print("smart_load error:", e); return False

def smart_normalize_tps():
    """One-time (idempotent) correction: rewrite already-closed take-profit trades to
    exactly +SMART_TP ROE and refund the over-target overshoot back out of cash, so the
    history is consistent (these only overshot because the bot was frozen/restarted)."""
    fixed = 0; delta = 0.0
    for t in SMART["closed"]:
        if t.get("reason") != "take-profit":
            continue
        m = t.get("margin") or 0
        target = round(SMART_TP * m, 2)
        if m > 0 and float(t.get("pnl", 0)) > target + 0.01:
            delta += float(t["pnl"]) - target
            move = SMART_TP / (t.get("lev") or SMART_LEV)
            t["exit"] = round(t["entry"] * (1 + move) if t["side"] == "LONG" else t["entry"] * (1 - move), 6)
            t["pnl"] = target; t["roe"] = SMART_TP; fixed += 1
    if fixed:
        SMART["cash"] = round(SMART["cash"] - delta, 2)
        print("smart: normalized %d take-profits to %.0f%% ROE (cash -$%.2f)" % (fixed, SMART_TP * 100, delta))

def run_smart():
    smart_load()
    if not SMART_TOP:
        smart_build_top()
    SMART_BUILD["last"] = time.time(); idx = 0
    while True:
        try:
            if not SMART_TOP:
                smart_build_top(); SMART_BUILD["last"] = time.time(); time.sleep(5); continue
            if time.time() - SMART_BUILD.get("last", 0) > SMART_REBUILD_MIN * 60:
                smart_build_top(); SMART_BUILD["last"] = time.time()
            try:
                mids = hl_post(SOURCE_URL, {"type": "allMids"})
            except Exception:
                mids = {}
            # TP / liquidation on our own positions every tick.
            # IMPORTANT: never call tg() inside `with LOCK` — tg() itself takes LOCK
            # and threading.Lock isn't reentrant -> deadlock. Collect msgs, send after.
            sm_events = []
            with LOCK:
                for coin in list(SMART["pos"].keys()):
                    p = SMART["pos"][coin]; pnl = smart_pnl(p, mids); roe = pnl / p["margin"] if p["margin"] else 0
                    p["peak_roe"] = max(p.get("peak_roe", roe), roe)
                    if pnl <= -p["margin"]:
                        SMART["cash"] -= p["margin"]; SMART["pos"].pop(coin); smart_record(p, -p["margin"], "liquidated", mids)
                        smart_log("💥 LIQ %s — -$%.2f" % (coin, p["margin"]), "liq")
                        sm_events.append("🧠 SMART 💥 LIQUIDATED %s — -$%.2f" % (coin, p["margin"]))
                    elif roe >= p["tp"]:
                        tppx = smart_tp_price(p)              # fill exactly at the +12% target, not the overshot mark
                        tp_pnl = p["tp"] * p["margin"]        # => PnL is exactly TP% of margin
                        SMART["cash"] += tp_pnl; SMART["pos"].pop(coin); smart_record(p, tp_pnl, "take-profit", mids, exitpx=tppx)
                        smart_log("🎯 TP %s +$%.2f (%.0f%%)" % (coin, tp_pnl, p["tp"] * 100), "tp")
                        sm_events.append("🧠 SMART 🎯 TP %s +$%.2f (%.0f%% ROE)" % (coin, tp_pnl, p["tp"] * 100))
            for e in sm_events:
                tg(e)
            # poll ONE trader per tick (staggered over the whole list)
            tr = SMART_TOP[idx % len(SMART_TOP)]; idx += 1
            cur = smart_fetch_main(tr["addr"])
            if cur is not None:
                coins_now = set(cur.keys()); prev = _smart_last.get(tr["addr"]); _smart_last[tr["addr"]] = coins_now
                if prev is not None:
                    # Source trader CLOSED a coin we copied from THEM -> close ours too,
                    # UNLESS we're already in profit (then let the +TP / liquidation run).
                    sm_ev = []
                    with LOCK:
                        for coin in prev - coins_now:
                            p = SMART["pos"].get(coin)
                            if not p or p.get("src") != tr["addr"]:
                                continue
                            pnl = smart_pnl(p, mids); roe = pnl / p["margin"] if p["margin"] else 0
                            if roe > 0:
                                continue   # in profit -> keep it, our +TP caps the upside
                            SMART["cash"] += pnl; SMART["pos"].pop(coin)
                            smart_record(p, pnl, "trader exit", mids)
                            smart_log("🚪 EXIT %s — Trader zu, ROE %.0f%%" % (coin, roe * 100), "close")
                            sm_ev.append("🧠 SMART 🚪 EXIT %s (Trader zu) · PnL $%.2f · %.0f%%" % (coin, pnl, roe * 100))
                    for e in sm_ev:
                        tg(e)
                    # genuine NEW opens (only after we have a baseline snapshot for this trader)
                    for coin in coins_now - prev:
                        smart_consider(coin, cur[coin], tr, mids)
            with LOCK:
                smart_publish(mids)
            smart_save()
            time.sleep(max(0.4, SMART_CYCLE / max(1, len(SMART_TOP))))
        except Exception as e:
            print("smart loop error:", e); time.sleep(2)


# ============================ PAPER LOOP ============================
# ============================ LIVE ENGINE (real money) ============================
def live_log(msg, kind="info"):
    """Append to the live engine log (shown on the dashboard) and mirror to Telegram."""
    stamp = now_hms()[:5]
    with LOCK:
        LIVE["log"].append({"t": stamp, "text": msg, "kind": kind})
        if len(LIVE["log"]) > 200:
            del LIVE["log"][:len(LIVE["log"]) - 200]
    tg("🔴 LIVE " + msg)

def live_note(msg, kind="info"):
    """Dashboard-only log entry (no Telegram) — for routine/noisy notes like skips."""
    stamp = now_hms()[:5]
    with LOCK:
        LIVE["log"].append({"t": stamp, "text": msg, "kind": kind})
        if len(LIVE["log"]) > 200:
            del LIVE["log"][:len(LIVE["log"]) - 200]
    print("LIVE note:", msg)

def live_load_config():
    """Read config.json: enabled flag + leverage cap (agent_key/account_address stay
    on disk only and are loaded in live_init). Never writes the key back out elsewhere."""
    if not os.path.exists(CONFIG_FILE):
        return
    try:
        d = json.load(open(CONFIG_FILE))
        LIVE["enabled"] = bool(d.get("live_enabled", False))
        if "live_killswitch" in d:
            LIVE["killswitch"] = bool(d.get("live_killswitch"))
        ml = d.get("live_max_lev")
        if ml:
            LIVE["max_lev"] = max(1, min(int(ml), 40))
        m = d.get("mode")
        if m in ("live", "smart"):
            MODE["v"] = m
    except Exception as e:
        print("live_load_config error:", e)

def live_save_config():
    """Atomic write of config.json preserving agent_key/account_address already on disk."""
    try:
        d = {}
        if os.path.exists(CONFIG_FILE):
            try:    d = json.load(open(CONFIG_FILE))
            except Exception: d = {}
        d["live_enabled"] = bool(LIVE["enabled"])
        d["live_max_lev"] = int(LIVE["max_lev"])
        d["live_killswitch"] = bool(LIVE["killswitch"])
        d["mode"] = MODE["v"]
        tmp = CONFIG_FILE + ".tmp"
        json.dump(d, open(tmp, "w"))
        os.replace(tmp, CONFIG_FILE)
    except Exception as e:
        print("live_save_config error:", e)

def live_has_credentials():
    """True if config.json already holds an agent_key + account_address. Used by the
    dashboard to show whether Hyperliquid is connected — without ever exposing the key."""
    if not os.path.exists(CONFIG_FILE):
        return False
    try:
        d = json.load(open(CONFIG_FILE))
        return bool((d.get("agent_key") or "").strip()) and bool((d.get("account_address") or "").strip())
    except Exception:
        return False

def live_save_credentials(key, addr):
    """Atomic write of the agent_key + account_address into config.json, preserving the
    other settings (enabled/max_lev/killswitch) already on disk. The key is stored ONLY
    here (gitignored) and is never sent back to the browser or logged."""
    d = {}
    if os.path.exists(CONFIG_FILE):
        try:    d = json.load(open(CONFIG_FILE))
        except Exception: d = {}
    d["agent_key"] = key
    d["account_address"] = addr
    tmp = CONFIG_FILE + ".tmp"
    json.dump(d, open(tmp, "w"))
    os.replace(tmp, CONFIG_FILE)
    try:    os.chmod(CONFIG_FILE, 0o600)   # key file: owner read/write only
    except Exception: pass

def live_clear_credentials():
    """Remove the agent_key + account_address from config.json and tear down the live
    Exchange so the engine can no longer trade. Keeps the other settings intact."""
    if os.path.exists(CONFIG_FILE):
        try:
            d = json.load(open(CONFIG_FILE))
        except Exception:
            d = {}
        d.pop("agent_key", None)
        d.pop("account_address", None)
        d["live_enabled"] = False
        tmp = CONFIG_FILE + ".tmp"
        json.dump(d, open(tmp, "w"))
        os.replace(tmp, CONFIG_FILE)
    LIVE["enabled"] = False
    LIVE["ready"] = False
    LIVE["ex"] = None
    LIVE["addr"] = ""
    LIVE["err"] = ""

def set_mode(new):
    """Switch the active trading mode. Allowed ONLY when the current mode is flat
    (no open bot positions) so the two engines can never overlap. Returns (ok, msg)."""
    if new not in ("live", "smart"):
        return False, "ungültiger Modus"
    cur = MODE["v"]
    if new == cur:
        return True, "ok"
    # Only the Live engine (real money) blocks a switch while it holds open positions.
    # Smart Money is paper and runs continuously, so its positions don't block anything.
    if cur == "live" and LIVE.get("owned"):
        return False, "Live-Engine hat noch %d offene Bot-Position(en) — erst schließen (PANIC)." % len(LIVE["owned"])
    MODE["v"] = new
    if new != "live":
        LIVE["enabled"] = False          # leaving live -> engine can no longer trade
    live_save_config()
    tg("🔀 Modus gewechselt → %s" % ("🔴 LIVE (echtes Geld)" if new == "live" else "🧠 Smart Money"))
    return True, "ok"

def live_save_state():
    """Persist which positions the engine opened (owned) + the closed-trade log, so a
    restart doesn't forget them. Without this, a position opened before a restart and
    take-profited after it would never be recorded or notified."""
    try:
        owned = [{"dex": k[0], "bare": k[1], "meta": m} for k, m in LIVE["owned"].items()]
        tmp = LIVE_STATE_FILE + ".tmp"
        json.dump({"owned": owned, "closed": LIVE["closed"][-100:], "hist": LIVE["hist"][-3000:]}, open(tmp, "w"))
        os.replace(tmp, LIVE_STATE_FILE)
    except Exception as e:
        print("live_save_state error:", e)

def live_load_state():
    if not os.path.exists(LIVE_STATE_FILE):
        return
    try:
        d = json.load(open(LIVE_STATE_FILE))
        LIVE["owned"] = {(o["dex"], o["bare"]): (o.get("meta") or {}) for o in d.get("owned", [])}
        LIVE["closed"] = d.get("closed", []) or []
        LIVE["hist"] = d.get("hist", []) or []
        if LIVE["owned"] or LIVE["closed"]:
            print("live_state.json loaded: %d owned, %d closed, %d hist" % (len(LIVE["owned"]), len(LIVE["closed"]), len(LIVE["hist"])))
    except Exception as e:
        print("live_load_state error:", e)

def live_snapshot(equity):
    """Append an accurate account-value point to the live equity curve, at most every
    LIVE_HIST_EVERY seconds. Skips non-positive/missing readings so a failed API read
    can never poison the chart."""
    try:
        eq = float(equity)
    except Exception:
        return
    if eq <= 0:
        return
    h = LIVE["hist"]; nowms = int(time.time() * 1000)
    if not h or (nowms - h[-1][0]) >= LIVE_HIST_EVERY * 1000:
        h.append([nowms, round(eq, 2)])
        if len(h) > 3000:
            del h[:len(h) - 3000]

def live_init():
    """Lazily import the SDK and build the Exchange from the agent key in config.json.
    Returns (ok, message). Safe to call repeatedly; only builds once."""
    global EXEC_URL
    if LIVE["ready"] and LIVE["ex"]:
        return True, "ready"
    if not os.path.exists(CONFIG_FILE):
        LIVE["err"] = "config.json fehlt (Agent-Key eintragen)"; return False, LIVE["err"]
    try:
        cfg = json.load(open(CONFIG_FILE))
    except Exception as e:
        LIVE["err"] = "config.json unlesbar: %s" % e; return False, LIVE["err"]
    key = (cfg.get("agent_key") or "").strip()
    addr = (cfg.get("account_address") or "").strip()
    if not key or not addr:
        LIVE["err"] = "agent_key / account_address fehlen in config.json"; return False, LIVE["err"]
    try:
        from eth_account import Account
        from hyperliquid.exchange import Exchange
        from hyperliquid.utils import constants
    except Exception as e:
        LIVE["err"] = "SDK fehlt — 'pip install hyperliquid-python-sdk' auf dem Server (%s)" % e
        return False, LIVE["err"]
    try:
        EXEC_URL = constants.MAINNET_API_URL
        ex = Exchange(Account.from_key(key), EXEC_URL, account_address=addr)
        LIVE["ex"] = ex; LIVE["addr"] = addr; LIVE["ready"] = True; LIVE["err"] = ""
        return True, "ready"
    except Exception as e:
        LIVE["err"] = "Exchange-Init fehlgeschlagen: %s" % e; return False, LIVE["err"]

def live_lev():
    return max(1, min(FIXED_LEV, int(LIVE["max_lev"])))

def live_resp_error(resp):
    """Inspect a Hyperliquid SDK response and return an error string if the order did NOT
    succeed, else None. The SDK does NOT raise on a rejected order — it returns
    {'status':'err',...} or {'status':'ok', response:{data:{statuses:[{'error':...}]}}}.
    Without this the bot would log '✅ OPEN' even when the exchange rejected the order."""
    try:
        if resp is None:
            return "keine Antwort vom Exchange"
        if isinstance(resp, str):
            return resp if resp.lower().startswith("err") else None
        if isinstance(resp, dict):
            if resp.get("status") == "err":
                return str(resp.get("response"))
            data = resp.get("response")
            if isinstance(data, dict):
                for s in (((data.get("data") or {}).get("statuses")) or []):
                    if isinstance(s, dict) and s.get("error"):
                        return str(s["error"])
        return None
    except Exception:
        return None

def live_test_open(coin, is_buy, margin):
    """Manually open ONE small real position on the connected live account to verify the
    order path end-to-end (independent of the whale/engine). Leaves it open; records it as
    engine-owned so it shows in the dashboard and PANIC can close it. Returns (ok, msg)."""
    ok, m = live_init()
    if not ok:
        return False, m
    ex = LIVE["ex"]
    lev = live_lev()
    try:
        mids = hl_post(EXEC_URL, {"type": "allMids"})
        mark = float(mids.get(coin) or 0)
    except Exception as e:
        return False, "Preis-Read fehlgeschlagen: %s" % e
    if mark <= 0:
        return False, "kein Preis für %s" % coin
    sz = round_sz(EXEC_URL, coin, (margin * lev) / mark)
    if sz <= 0:
        return False, "Größe 0 — Margin zu klein für %s" % coin
    key = ("", coin)
    try:
        lres = ex.update_leverage(lev, coin, True)   # cross
        lerr = live_resp_error(lres)
        if lerr:
            live_note("⚠️ Test-Leverage %s: %s" % (coin, lerr), "warn")
        res = ex.market_open(coin, is_buy, sz)
        err = live_resp_error(res)
        if err:
            return False, "Order von Hyperliquid abgelehnt: %s" % err
        time.sleep(1.0)
        mine = get_positions(EXEC_URL, ex.account_address).get(key)
        if not mine:
            return False, "keine Position nach Order sichtbar (eventuell nicht gefüllt)"
        entry = mine["entry"] or mark
        with LOCK:
            LIVE["owned"][key] = {"coin": coin, "bare": coin, "side": "LONG" if is_buy else "SHORT",
                                  "entry": entry, "margin": round(margin, 2), "sz": sz, "lev": lev,
                                  "opened_ms": int(time.time() * 1000), "test": True}
        live_save_state()
        live_log("🧪 TEST-ORDER %s %s · Margin $%.2f · %d× · Entry ~$%.2f (bleibt offen — mit PANIC schließen)"
                 % (coin, "LONG" if is_buy else "SHORT", margin, lev, entry), "open")
        return True, "ok"
    except Exception as e:
        return False, "Fehler: %s" % e

def live_open(w, equity):
    """Open ONE real copy of a whale position. Mirrors open_copy but leverage-capped
    and logged into the live engine. Records the key as engine-owned."""
    ex = LIVE["ex"]
    if not ex:
        return
    lev = live_lev()
    is_cross = (w["mode"] == "cross")
    is_buy = w["szi"] > 0
    coin, mark = w["coin"], (w["mark"] or 0)
    if mark <= 0:
        live_log("⏭️ %s übersprungen — kein Preis" % w["bare"], "skip"); return
    margin = equity * CAPITAL_FRACTION
    sz = round_sz(EXEC_URL, w["bare"], (margin * lev) / mark)
    if sz <= 0:
        live_log("⏭️ %s übersprungen — Größe 0 (zu wenig Kapital?)" % w["bare"], "skip"); return
    try:
        lres = ex.update_leverage(lev, coin, is_cross)
        lerr = live_resp_error(lres)
        if lerr:
            live_log("⚠️ Leverage %s: %s" % (w["bare"], lerr), "warn")
        res = ex.market_open(coin, is_buy, sz)
        err = live_resp_error(res)
        if err:
            # The exchange REJECTED the order — do NOT pretend we opened it.
            live_log("❌ Order %s ABGELEHNT von Hyperliquid: %s" % (w["bare"], err), "err"); return
        time.sleep(1.0)
        mine = get_positions(EXEC_URL, ex.account_address).get(w["key"])
        if not mine:
            # No position showed up after a supposed fill -> treat as not opened, surface it.
            live_log("⚠️ %s: keine Position nach Order sichtbar — nicht als offen gewertet." % w["bare"], "warn"); return
        entry = mine["entry"] or mark
        base_tp = SET["tp_stock"] if w["dex"] else SET["tp_crypto"]
        tp_roe = scaled_tp(base_tp, w.get("lev"), lev)   # smaller TP if the whale's leverage < ours
        move = tp_roe / lev
        tp = round_px(entry * (1 + move) if is_buy else entry * (1 - move))
        try:
            tres = ex.order(coin, (not is_buy), sz, tp,
                            {"trigger": {"triggerPx": tp, "isMarket": True, "tpsl": "tp"}}, reduce_only=True)
            terr = live_resp_error(tres)
            if terr:
                live_log("⚠️ TP-Order %s nicht gesetzt: %s" % (w["bare"], terr), "warn")
        except Exception as te:
            live_log("⚠️ TP-Order %s fehlgeschlagen: %s" % (w["bare"], te), "warn")
        with LOCK:
            LIVE["owned"][w["key"]] = {"coin": coin, "bare": w["bare"], "side": w["side"],
                                       "entry": entry, "margin": round(margin, 2), "sz": sz,
                                       "lev": lev, "opened_ms": int(time.time() * 1000)}
        live_save_state()   # survive restarts so a later TP close is still tracked + notified
        # OPEN notification — includes the margin you asked for
        live_log("✅ OPEN %s %s · Margin $%.2f · %d× %s · Entry ~$%.4f · TP +%.0f%% ROE @ $%s"
                 % (w["bare"], w["side"], margin, lev,
                    "Cross" if is_cross else "Isolated", entry, tp_roe * 100, tp), "open")
    except Exception as e:
        live_log("❌ Fehler beim Öffnen %s: %s" % (w["bare"], e), "err")

def live_fills_pnl(addr, bare, since_ms):
    """Sum the realized closedPnl for `bare` from recent fills at/after since_ms.
    Returns (pnl, found). Best-effort; used to report exact PnL on a close."""
    try:
        fills = hl_post(EXEC_URL, {"type": "userFills", "user": addr}) or []
        tot = 0.0; found = False
        for f in fills:
            fb = f.get("coin", ""); fb = fb.split(":")[1] if ":" in fb else fb
            if fb == bare and (f.get("time") or 0) >= since_ms - 3000:
                cp = f.get("closedPnl")
                if cp is not None:
                    tot += float(cp); found = True
        return tot, found
    except Exception:
        return 0.0, False

def live_record_close(key, reason):
    """Compute realized PnL + ROE% for a just-closed engine position, log it and send a
    CLOSE notification with PnL and percent, then drop it from owned."""
    meta = LIVE["owned"].get(key) or {}
    bare = meta.get("bare", key[1]); side = meta.get("side", "")
    margin = meta.get("margin", 0) or 0
    pnl, found = live_fills_pnl(LIVE["addr"], bare, meta.get("opened_ms", 0))
    roe = (pnl / margin) if (margin and found) else 0.0
    with LOCK:
        LIVE["owned"].pop(key, None)
        LIVE["closed"].append({"coin": bare, "side": side, "pnl": round(pnl, 2),
                               "roe": round(roe, 4), "reason": reason,
                               "closed_ms": int(time.time() * 1000)})
        if len(LIVE["closed"]) > 100:
            del LIVE["closed"][:len(LIVE["closed"]) - 100]
    if found:
        live_log("🔻 CLOSE %s %s (%s) · PnL %s$%.2f · %s%.1f%%"
                 % (side, bare, reason, "+" if pnl >= 0 else "-", abs(pnl),
                    "+" if roe >= 0 else "-", abs(roe) * 100), "close")
    else:
        live_log("🔻 CLOSE %s %s (%s) · PnL wird von Hyperliquid abgerechnet" % (side, bare, reason), "close")
    live_save_state()


def live_flatten():
    """Panic: close every engine-owned position immediately and disable the engine."""
    ex = LIVE["ex"]
    LIVE["enabled"] = False; live_save_config()
    live_log("🧹 PANIC — Engine AUS, schließe alle vom Bot eröffneten Positionen…", "warn")
    if not ex:
        return
    try:
        pos = get_positions(EXEC_URL, ex.account_address)
    except Exception as e:
        live_log("❌ Flatten-Read fehlgeschlagen: %s" % e, "err"); return
    for key in list(LIVE["owned"]):
        p = pos.get(key)
        if not p:
            with LOCK: LIVE["owned"].pop(key, None)
            continue
        try:
            ex.market_close(p["coin"])
            time.sleep(1.0)
            live_record_close(key, "panic")
        except Exception as e:
            live_log("❌ Flatten %s: %s" % (p["bare"], e), "err")

def live_publish():
    """Build the STATE['live'] payload the dashboard renders."""
    ex = LIVE["ex"]; rows = []
    if ex and LIVE["ready"]:
        try:
            allpos = get_positions(EXEC_URL, ex.account_address)
            for key, p in allpos.items():
                upnl = p["szi"] * (p["mark"] - p["entry"])
                pv = abs(p["szi"]) * p["mark"]
                margin = pv / p["lev"] if p.get("lev") else 0
                roe = (upnl / margin) if margin else 0
                meta = LIVE["owned"].get(key) or {}
                rows.append({"coin": p["bare"], "side": p["side"], "entry": p["entry"],
                             "mark": p["mark"], "lev": p["lev"], "margin": round(margin, 2),
                             "upnl": round(upnl, 2), "roe": round(roe, 4),
                             "owned": key in LIVE["owned"],
                             "src": WHALE if key in LIVE["owned"] else "",
                             "opened_ms": meta.get("opened_ms", 0)})
        except Exception as e:
            LIVE["err"] = "Positions-Read: %s" % e
    rows.sort(key=lambda r: -r["margin"])
    day_pnl = round(LIVE["equity"] - LIVE["day_start_eq"], 2) if LIVE["day_start_eq"] else 0.0
    with LOCK:
        STATE["live"] = {
            "enabled": LIVE["enabled"], "ready": LIVE["ready"], "err": LIVE["err"],
            "connected": live_has_credentials(), "mode": MODE["v"],
            "net": LIVE["net"], "addr": LIVE["addr"], "whale": WHALE, "equity": round(LIVE["equity"], 2),
            "perp_equity": round(LIVE["perp_equity"], 2), "spot_equity": round(LIVE["spot_equity"], 2),
            "day_pnl": day_pnl, "killed": LIVE["killed"], "killswitch": LIVE["killswitch"], "max_lev": LIVE["max_lev"],
            "lev": live_lev(), "capital_pct": round(CAPITAL_FRACTION * 100, 1),
            "max_positions": MAX_POSITIONS, "tp_crypto_pct": round(SET["tp_crypto"] * 100, 2),
            "tp_stock_pct": round(SET["tp_stock"] * 100, 2), "daily_loss_pct": round(DAILY_LOSS_LIMIT * 100, 1),
            "owned": len(LIVE["owned"]), "open_total": len(rows), "pos": rows,
            "closed": LIVE["closed"][-40:][::-1], "log": LIVE["log"][-50:][::-1],
            "history": LIVE["hist"][-1500:],
        }

def live_tick(ctx):
    """One poll of the live engine. Returns how many seconds to sleep before the next
    tick. All cross-poll state lives in `ctx` so this is unit-testable in isolation."""
    if not LIVE["enabled"]:
        if ctx["was_enabled"]:
            live_log("⏸ Engine deaktiviert — keine neuen Trades (offene Positionen laufen weiter).", "warn")
            ctx["was_enabled"] = False
        LIVE["ready"] = LIVE["ready"] and bool(LIVE["ex"])
        # keep an accurate equity curve even while idle: if connected, sample the real
        # Hyperliquid account value every ~30s (throttled) and snapshot it for the chart.
        if LIVE["ready"] and LIVE["addr"] and (time.time() - ctx.get("idle_read", 0) >= 30):
            ctx["idle_read"] = time.time()
            try:
                eq = get_unified_value(EXEC_URL, LIVE["addr"])
                LIVE["equity"] = eq; LIVE["perp_equity"] = eq; LIVE["spot_equity"] = 0.0
                live_snapshot(eq)
            except Exception:
                pass
        live_publish(); return 2
    if not LIVE["ready"]:
        ok, _ = live_init()
        if not ok:
            live_publish(); return 5
        ctx["announced"] = False; ctx["known"] = set(); ctx["based"] = set(); ctx["miss"] = {}; ctx["openseen"] = {}
    if not ctx["was_enabled"]:
        ctx["was_enabled"] = True; LIVE["started_ms"] = int(time.time() * 1000)
        ctx["day"] = datetime.datetime.now(datetime.timezone.utc).date()
        LIVE["day_start_eq"] = get_unified_value(EXEC_URL, LIVE["addr"]); LIVE["killed"] = False
        live_log("🟢 Engine AKTIV auf %s · Equity $%.2f · %d× · %.0f%%/Trade · max %d Pos."
                 % (LIVE["net"], LIVE["day_start_eq"], live_lev(), CAPITAL_FRACTION * 100, MAX_POSITIONS), "on")

    today = datetime.datetime.now(datetime.timezone.utc).date()
    if today != ctx["day"]:
        ctx["day"] = today; LIVE["day_start_eq"] = get_unified_value(EXEC_URL, LIVE["addr"]); LIVE["killed"] = False
        live_log("🌅 Neuer Tag — Kill-Switch zurückgesetzt.", "info")

    # One unified Portfolio Value (collateral + unrealized PnL), matches the HL UI.
    # Sizing, kill-switch and headline all use this single number.
    equity = get_unified_value(EXEC_URL, LIVE["addr"])
    LIVE["equity"] = equity; LIVE["perp_equity"] = equity; LIVE["spot_equity"] = 0.0
    live_snapshot(equity)
    if LIVE["killswitch"]:
        if not LIVE["killed"] and LIVE["day_start_eq"] > 0 and equity <= LIVE["day_start_eq"] * (1 - DAILY_LOSS_LIMIT):
            LIVE["killed"] = True
            live_log("🛑 KILL-SWITCH: −%.0f%% heute. Keine neuen Trades." % (DAILY_LOSS_LIMIT * 100), "kill")
    elif LIVE["killed"]:
        LIVE["killed"] = False   # switched off -> lift any active halt

    whale, okdex = get_positions_ex(SOURCE_URL, WHALE)
    if COIN_WHITELIST is not None:
        whale = {k: v for k, v in whale.items() if v["bare"] in COIN_WHITELIST}
    livepos, okexec = get_positions_ex(EXEC_URL, LIVE["addr"])   # okexec = our dexes read OK

    known = ctx["known"]; based = ctx["based"]; miss = ctx["miss"]; openseen = ctx["openseen"]; close_miss = ctx["close_miss"]

    # establish baseline: existing whale positions are NOT copied
    new_dex = okdex - based
    if new_dex:
        for k in whale:
            if k[0] in new_dex:
                known.add(k)
        based |= new_dex
    if not ctx["announced"]:
        ctx["announced"] = True
        live_log("👀 Beobachte ab jetzt — bestehende Whale-Positionen werden NICHT kopiert. Nur NEUE, über %d Polls bestätigt (Anti-Flicker, KEIN Hebel)." % OPEN_CONFIRM, "info")
        live_publish(); return POLL_SECONDS

    # reconcile: an engine-owned position closed via TP (or by hand). Only act when
    # the position's OWN dex was actually read this poll AND it's been absent for
    # CLOSE_CONFIRM consecutive polls — otherwise a propagation delay or a single
    # failed read would falsely report a close (e.g. open + instant "take-profit").
    for key in list(LIVE["owned"]):
        if key in livepos:
            close_miss[key] = 0
        elif key[0] in okexec:
            close_miss[key] = close_miss.get(key, 0) + 1
            if close_miss[key] >= CLOSE_CONFIRM:
                close_miss.pop(key, None)
                live_record_close(key, "take-profit")
        # else: that dex wasn't read this poll -> unknown, leave the position alone

    # whale-exit: we do NOT close our copy when the whale closes — our positions
    # are managed ONLY by their own take-profit / liquidation. We just stop tracking
    # the key so a later whale re-open of the same coin can be copied again.
    for key in list(known):
        if key in whale:
            miss[key] = 0
        elif key[0] in okdex:
            miss[key] = miss.get(key, 0) + 1
            if miss[key] >= CLOSE_CONFIRM:
                known.discard(key); miss.pop(key, None)

    # Builder-dex (HIP-3) perps like stock perps on 'xyz' can't be placed via the
    # standard SDK call -> skip them cleanly (no error spam) instead of crashing.
    for k in list(whale):
        if k[0] != "" and k[0] in okdex and k not in known:
            known.add(k)
            live_note("⏭️ %s (Builder-Dex '%s') — Stock-/Builder-Perps werden live nicht gehandelt" % (k[1], k[0]), "skip")

    # confirmed NEW opens -> copy with real orders (MAIN perp dex only)
    cand = set(k for k in whale if k[0] == "" and "" in okdex and k not in known)
    for k in list(openseen):
        if k not in cand:
            openseen.pop(k, None)
    for key in cand:
        openseen[key] = openseen.get(key, 0) + 1
    for key in list(cand):
        if openseen.get(key, 0) < OPEN_CONFIRM:
            continue
        openseen.pop(key, None); known.add(key)
        if key in LIVE["owned"]:
            continue
        if LIVE["killed"]:
            live_log("⏭️ %s übersprungen — Kill-Switch." % whale[key]["bare"], "skip"); continue
        # account-wide cap: ALL open positions (your manual ones + the bot's)
        # count against the 5 — re-read live to stay current within this poll.
        try:    open_total = len(get_positions(EXEC_URL, LIVE["addr"]))
        except Exception: open_total = len(livepos)
        if open_total >= MAX_POSITIONS:
            live_log("⏭️ %s übersprungen — %d/%d Slots belegt (inkl. deiner manuellen Trades)."
                     % (whale[key]["bare"], open_total, MAX_POSITIONS), "skip"); continue
        live_open(whale[key], equity)   # unified account: full collateral backs margin

    live_publish()
    return POLL_SECONDS

def run_live():
    """Daemon loop. Idle while disabled; when enabled, mirror the whale with REAL
    orders using the same open/close confirmation as the paper engine."""
    live_load_config()
    live_load_state()   # restore engine-owned positions + closed log across restarts
    ctx = {"known": set(), "based": set(), "miss": {}, "openseen": {}, "close_miss": {},
           "announced": False, "was_enabled": False, "day": None}
    last_hist_save = 0
    while True:
        try:
            slp = live_tick(ctx)
            # persist the equity curve periodically so it survives restarts even
            # when no trades open/close (those paths already save state).
            if time.time() - last_hist_save >= LIVE_HIST_EVERY:
                last_hist_save = time.time(); live_save_state()
        except Exception as e:
            LIVE["err"] = str(e); print("live loop error:", e)
            try: live_publish()
            except Exception: pass
            slp = POLL_SECONDS
        time.sleep(slp)


def run_paper():
    STATE["net"] = "PAPER"; STATE["running"] = True
    load_secrets()
    save_secrets()          # persist a stable dash token + ensure secrets.json exists from the first run
    loaded = load_paper()
    if loaded and PAPER_TRADING:
        paper_normalize_tps_today()   # retro-fix today's take-profits that overshot +12% (idempotent)
    if not PAPER_TRADING:
        PAPER["pos"] = {}             # paper copy-bot retired -> drop any leftover simulated positions
    start_server()
    threading.Thread(target=start_tunnel, daemon=True).start()
    threading.Thread(target=run_smart, daemon=True).start()   # Smart-Money top-100 tracker (separate paper book)
    threading.Thread(target=run_live, daemon=True).start()    # LIVE real-money engine (OFF until enabled from dashboard)
    threading.Thread(target=run_tg_listener, daemon=True).start()  # Telegram command listener (live/smart stats)
    if PAPER_TRADING:
        tg("🤖 Copy-Bot in PAPER mode (simulated, no real money) · start $%.0f" % PAPER_START)
    else:
        tg("🟢 Dashboard host up · Live engine + Smart Money active · paper copy-bot retired")
    print("\n>>> Dashboard:  http://<server-ip>/   (token: %s)\n" % DASH_TOKEN)

    if loaded:
        day_start_eq, day_start_cash, day = loaded
        if PAPER_TRADING:
            tg("💾 Previous paper state loaded · cash $%.2f · %d open position(s)"
               % (PAPER["cash"], len(PAPER["pos"])))
    else:
        day = datetime.datetime.now(datetime.timezone.utc).date()
        day_start_eq = PAPER_START
        day_start_cash = PAPER_START
    killed = False
    known = set(PAPER["pos"].keys())   # whale keys we already account for (baseline + copied)
    based = set()                      # dexes whose baseline has been established (read OK once)
    miss = {}                          # key -> consecutive CONFIRMED-absent polls (debounced close)
    openseen = {}                      # key -> consecutive CONFIRMED-new polls (debounced open)
    announced = False

    while True:
        try:
            _t0 = time.time()
            today = datetime.datetime.now(datetime.timezone.utc).date()
            whale, okdex = get_positions_ex(SOURCE_URL, WHALE)
            _dt = time.time() - _t0
            if _dt > 8:   # surfaces in `journalctl -u copybot` if reads are stalling
                print("SLOW poll: whale read took %.1fs (%d/%d dexes ok)" % (_dt, len(okdex), len(perp_dexes(SOURCE_URL))))
            if COIN_WHITELIST is not None:
                whale = {k: v for k, v in whale.items() if v["bare"] in COIN_WHITELIST}
            try:    mids = hl_post(SOURCE_URL, {"type": "allMids"})
            except Exception: mids = {}

            # read YOUR wallet too (logged, not copied) — done outside the lock
            try:    mine_now, myok = get_positions_ex(SOURCE_URL, MY_WALLET)
            except Exception: mine_now, myok = {}, set()

            # First successful read of a dex = baseline: its CURRENT positions are
            # pre-existing -> mark 'known' so they are NEVER copied. Only positions
            # that appear AFTER the baseline count as genuine new opens.
            new_dex = okdex - based
            if new_dex:
                for k in whale:
                    if k[0] in new_dex:
                        known.add(k)
                based |= new_dex

            if not announced:
                announced = True
                with LOCK:
                    eq = paper_equity(whale, mids)
                    publish_state(eq, day_start_eq, killed, whale, mids)
                    save_paper(day_start_eq, day_start_cash, day)
                if PAPER_TRADING:
                    tg("👀 Watching from now — existing whale positions are NOT copied. Only NEW ones, confirmed %d× (open & close)." % CLOSE_CONFIRM)
                time.sleep(POLL_SECONDS); continue

            events = []
            with LOCK:
                if today != day:
                    day = today; killed = False
                    day_start_eq = paper_equity(whale, mids)
                    day_start_cash = PAPER["cash"]
                    events.append("🌅 New day — kill-switch reset.")

                # TP (per-position, locked at open) / liquidation
                for key in list(PAPER["pos"].keys()):
                    p = PAPER["pos"][key]
                    pnl = _pnl(key, p, whale, mids)
                    roe = pnl / p["margin"] if p["margin"] else 0
                    p["peak_roe"] = max(p.get("peak_roe", roe), roe)   # best ROE reached (for the popup)
                    tp = p.get("tp") or (SET["tp_stock"] if p.get("dex") else SET["tp_crypto"])
                    if pnl <= -p["margin"]:
                        PAPER["cash"] -= p["margin"]; PAPER["pos"].pop(key)
                        record_closed(p, -p["margin"], "liquidated")
                        events.append("💥 LIQUIDATED %s — margin lost ($%.2f)" % (p["bare"], p["margin"]))
                    elif roe >= tp:
                        tp_pnl = tp * p["margin"]    # fill exactly at the +TP target, not the overshot mark
                        PAPER["cash"] += tp_pnl; PAPER["pos"].pop(key)
                        record_closed(p, tp_pnl, "take-profit")
                        events.append("🎯 TP +%.0f%% ROE %s — profit $%.2f" % (tp * 100, p["bare"], tp_pnl))

                # confirmed whale-exit: a known key absent on a successfully-read dex
                # for CLOSE_CONFIRM polls -> forget it (and close our copy if we hold one)
                for key in list(known):
                    if key in whale:
                        miss[key] = 0
                    elif key[0] in okdex:
                        miss[key] = miss.get(key, 0) + 1
                        if miss[key] >= CLOSE_CONFIRM:
                            known.discard(key); miss.pop(key, None)
                            if key in PAPER["pos"]:
                                p = PAPER["pos"].pop(key)
                                pnl = _pnl(key, p, whale, mids)
                                PAPER["cash"] += pnl
                                record_closed(p, pnl, "whale exit")
                                events.append("🔻 CLOSED %s (whale exit) — PnL $%.2f" % (p["bare"], pnl))
                    # else: that dex wasn't read this poll -> unknown, leave untouched

                log_my_trades(mine_now, myok)
                equity = paper_equity(whale, mids)
                publish_state(equity, day_start_eq, killed, whale, mids)
                save_paper(day_start_eq, day_start_cash, day)

            for e in events:
                tg(e)

            # daily kill-switch — counts only REALIZED losses today (closed trades /
            # liquidations move PAPER["cash"]); open positions in the red don't trip it.
            realized_loss = day_start_cash - PAPER["cash"]
            if not killed and day_start_eq > 0 and realized_loss >= DAILY_LOSS_LIMIT * day_start_eq:
                killed = True
                tg("🛑 KILL-SWITCH: realized −%.0f%% today. No new trades." % (DAILY_LOSS_LIMIT * 100))

            if not PAPER_TRADING:
                # paper copy-bot retired — host keeps serving the dashboard + live/smart;
                # no virtual adopt/opens below. (My-Account logging + publish already done above.)
                time.sleep(POLL_SECONDS); continue

            # manual adoption: copy any currently-open whale position we don't hold yet,
            # entered at the whale's OWN entry (as if we'd opened it together with them).
            # Bypasses pause/kill-switch (it's an explicit user action).
            if ADOPT["pending"]:
                ADOPT["pending"] = False
                adopted = []
                with LOCK:
                    for key, w in whale.items():
                        if key[0] not in okdex or key in PAPER["pos"]:
                            continue
                        entry = w["entry"] or w["mark"] or 0
                        if entry <= 0:
                            continue
                        known.add(key)
                        if len(PAPER["pos"]) >= MAX_POSITIONS:
                            adopted.append("⏭️ %s not adopted — %d slots full." % (w["bare"], MAX_POSITIONS)); continue
                        eq = paper_equity(whale, mids)
                        margin = eq * CAPITAL_FRACTION
                        lev = my_leverage(w["lev"], w["mode"])
                        sz = (margin * lev) / entry
                        tp = SET["tp_stock"] if w["dex"] else SET["tp_crypto"]
                        PAPER["pos"][key] = {"bare": w["bare"], "coin": w["coin"], "dex": w["dex"],
                                             "side": w["side"], "entry": entry, "lev": lev,
                                             "mode": w["mode"], "sz": sz, "margin": margin, "tp": tp,
                                             "opened": now_hms(),
                                             "opened_ms": int(time.time() * 1000)}
                        adopted.append("✅ ADOPTED %s %s @ $%.4f (whale entry) · margin $%.2f · %dx %s · TP +%.0f%% ROE"
                                       % (w["bare"], w["side"], entry, margin, lev,
                                          "Cross" if w["mode"] == "cross" else "Isolated", tp * 100))
                for m in adopted:
                    tg(m)
                if not adopted:
                    tg("ℹ️ Adopt: no open whale position to copy (all already held).")

            # genuine NEW opens only: present on a read dex, not already known,
            # and confirmed for OPEN_CONFIRM consecutive polls (no single-tick blips)
            cand = set(k for k in whale if k[0] in okdex and k not in known)
            for k in list(openseen):
                if k not in cand:
                    openseen.pop(k, None)
            for key in cand:
                openseen[key] = openseen.get(key, 0) + 1
            for key in cand:
                if openseen[key] < OPEN_CONFIRM:
                    continue
                openseen.pop(key, None)
                w = whale[key]
                entry = w["mark"] or 0
                if entry <= 0:
                    continue                      # bad price data -> retry next poll
                known.add(key)                    # decision is made now (one-shot)
                if STATE["paused"]:
                    tg("⏭️ %s skipped — paused." % w["bare"]); continue
                if killed:
                    tg("⏭️ %s skipped — kill-switch." % w["bare"]); continue
                msg = None
                with LOCK:
                    if len(PAPER["pos"]) >= MAX_POSITIONS:
                        msg = "⏭️ %s skipped — 5 slots full." % w["bare"]
                    else:
                        eq = paper_equity(whale, mids)
                        margin = eq * CAPITAL_FRACTION
                        lev = my_leverage(w["lev"], w["mode"])
                        sz = (margin * lev) / entry
                        tp = SET["tp_stock"] if w["dex"] else SET["tp_crypto"]
                        PAPER["pos"][key] = {"bare": w["bare"], "coin": w["coin"], "dex": w["dex"],
                                             "side": w["side"], "entry": entry, "lev": lev,
                                             "mode": w["mode"], "sz": sz, "margin": margin, "tp": tp,
                                             "opened": now_hms(),
                                             "opened_ms": int(time.time() * 1000)}
                        msg = ("✅ COPIED %s %s · margin $%.2f · %dx %s · TP +%.0f%% ROE"
                               % (w["bare"], w["side"], margin, lev,
                                  "Cross" if w["mode"] == "cross" else "Isolated", tp * 100))
                tg(msg)
        except Exception as e:
            print("paper loop error:", e)
        time.sleep(POLL_SECONDS)


# ============================ REAL LOOP ============================
def main():
    if PAPER_MODE:
        run_paper()
        return

    load_secrets()
    save_secrets()          # persist a stable dash token + ensure secrets.json exists
    cfg_path = os.path.join(HERE, "config.json")
    if not os.path.exists(cfg_path):
        if EXEC_URL != constants.TESTNET_API_URL:
            print("FEHLER: config.json fehlt - auf Mainnet brauchst du echte Zugangsdaten.")
            return
        w = Account.create()
        json.dump({"agent_key": w.key.hex(), "account_address": w.address}, open(cfg_path, "w"))
        print("Test-Wallet erzeugt (leer, nur Testnet):", w.address)
    cfg = json.load(open(cfg_path))
    ex = Exchange(Account.from_key(cfg["agent_key"]), EXEC_URL, account_address=cfg["account_address"])
    EX["ex"] = ex
    main_addr = cfg["account_address"]

    net = "TESTNET" if EXEC_URL == constants.TESTNET_API_URL else "🔴 MAINNET"
    STATE["net"] = net; STATE["running"] = True
    start_server()
    threading.Thread(target=start_tunnel, daemon=True).start()
    tg("🤖 Copy-Bot running on %s · dashboard: http://<server-ip>/" % net)
    print("\n>>> Dashboard:  http://<server-ip>/   (token: %s)\n" % DASH_TOKEN)

    day = datetime.datetime.now(datetime.timezone.utc).date()
    day_start_eq = get_equity(EXEC_URL, main_addr)
    killed = False
    known = set()            # whale keys already accounted for (baseline + copied)
    based = set()            # dexes with an established baseline
    miss = {}                # key -> consecutive CONFIRMED-absent polls (debounced close)
    openseen = {}            # key -> consecutive CONFIRMED-new polls (debounced open)
    announced = False

    while True:
        try:
            today = datetime.datetime.now(datetime.timezone.utc).date()
            if today != day:
                day = today; day_start_eq = get_equity(EXEC_URL, main_addr); killed = False
                tg("🌅 New day — kill-switch reset.")
            equity = get_equity(EXEC_URL, main_addr)
            if not killed and day_start_eq > 0 and equity <= day_start_eq * (1 - DAILY_LOSS_LIMIT):
                killed = True
                tg("🛑 KILL-SWITCH: −%.0f%% today. No new trades." % (DAILY_LOSS_LIMIT * 100))

            whale, okdex = get_positions_ex(SOURCE_URL, WHALE)
            if COIN_WHITELIST is not None:
                whale = {k: v for k, v in whale.items() if v["bare"] in COIN_WHITELIST}
            mine = get_positions(EXEC_URL, main_addr)

            with LOCK:
                STATE.update(equity=equity, day_start_equity=day_start_eq, killed=killed,
                             slots_used=len(mine))

            # baseline existing whale positions on each dex's first good read
            new_dex = okdex - based
            if new_dex:
                for k in whale:
                    if k[0] in new_dex:
                        known.add(k)
                based |= new_dex

            if not announced:
                announced = True
                tg("👀 Watching from now — existing whale positions are NOT copied. Only NEW ones, confirmed %d× (open & close)." % CLOSE_CONFIRM)
                time.sleep(POLL_SECONDS); continue

            # confirmed whale-exit -> forget key (and close our copy if we hold it)
            for key in list(known):
                if key in whale:
                    miss[key] = 0
                elif key[0] in okdex:
                    miss[key] = miss.get(key, 0) + 1
                    if miss[key] >= CLOSE_CONFIRM:
                        known.discard(key); miss.pop(key, None)
                        if key in mine:
                            close_copy(ex, mine[key], "whale exit")

            # genuine NEW opens only, confirmed for OPEN_CONFIRM polls
            cand = set(k for k in whale if k[0] in okdex and k not in known)
            for k in list(openseen):
                if k not in cand:
                    openseen.pop(k, None)
            for key in cand:
                openseen[key] = openseen.get(key, 0) + 1
            for key in cand:
                if openseen[key] < OPEN_CONFIRM:
                    continue
                openseen.pop(key, None)
                known.add(key)
                if key in mine:
                    continue
                if STATE["paused"]:
                    tg("⏭️ %s skipped — paused." % whale[key]["bare"]); continue
                if killed:
                    tg("⏭️ %s skipped — kill-switch." % whale[key]["bare"]); continue
                if len(get_positions(EXEC_URL, main_addr)) >= MAX_POSITIONS:
                    tg("⏭️ %s skipped — 5 slots full." % whale[key]["bare"]); continue
                open_copy(ex, whale[key], equity)
        except Exception as e:
            print("loop error:", e)
        time.sleep(POLL_SECONDS)


# ============================ SERVICE INSTALL ============================
def setup_service():
    py = sys.executable
    script = os.path.abspath(__file__)
    workdir = os.path.dirname(script)
    tg_line = ("Environment=TELEGRAM_BOT_TOKEN=%s\n" % TG["token"]) if TG["token"] else ""
    unit = (
        "[Unit]\n"
        "Description=Hyperliquid Copy Bot\n"
        "After=network-online.target\n"
        "Wants=network-online.target\n\n"
        "[Service]\n"
        "Type=simple\n"
        "User=root\n"
        "WorkingDirectory=" + workdir + "\n"
        "ExecStart=" + py + " " + script + "\n"
        "Restart=always\n"
        "RestartSec=5\n"
        "Environment=PYTHONUNBUFFERED=1\n"
        + tg_line +
        "\n[Install]\n"
        "WantedBy=multi-user.target\n"
    )
    path = "/etc/systemd/system/copybot.service"
    try:
        open(path, "w").write(unit)
    except Exception as e:
        print("Could not write service file (run as root):", e)
        return
    os.system("systemctl daemon-reload")
    os.system("systemctl enable copybot")
    os.system("systemctl restart copybot")
    print("\n==================================================")
    print(">>> ALWAYS-ON ACTIVE. The bot now runs 24/7.")
    print(">>> Survives closing the console and server reboots.")
    print("==================================================")
    print(">>> Status:      systemctl status copybot")
    print(">>> Live logs:   journalctl -u copybot -f")
    print(">>> Stop:        systemctl stop copybot")
    print(">>> Dashboard:   open your fixed Tailscale URL\n")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "install":
        setup_service()
    else:
        main()
