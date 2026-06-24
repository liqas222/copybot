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
· leverage cross->min(his,20x) / isolated->exact(<=40x) · TP +20% ROE ·
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

PAPER_MODE       = True          # <- simulate (no money). Set False for real trading.
PAPER_START      = 1000.0        # virtual starting capital for paper mode

CAPITAL_FRACTION = 0.20
MAX_POSITIONS    = 5
CROSS_LEV_CAP    = 20      # cross positions: copy whale's leverage but cap at 20x
MAX_LEV          = 40      # isolated positions: copy exact, capped at 40x
TP_ROE           = 0.20      # default; live values held in SET (settable from dashboard)
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

# virtual book for paper mode
PAPER = {"cash": PAPER_START, "pos": {}, "hist": [], "closed": []}   # pos: key(tuple) -> dict; hist: [[t_ms, equity], ...]; closed: [trade dicts]
HIST_EVERY = 60       # seconds between equity snapshots for the chart

# log of YOUR real trades (MY_WALLET): open snapshot -> closed trade with exact ROE
MINE = {"pos": {}, "closed": []}   # pos: key(tuple) -> snapshot dict; closed: [trade dicts]

# manual "adopt": set by the dashboard -> the loop copies currently-open whale
# positions we don't hold yet, entered at the whale's OWN entry price.
ADOPT = {"pending": False}


# ---------------- notifications + log ----------------
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
    with LOCK:
        STATE["log"].append({"t": time.strftime("%H:%M:%S"), "text": msg})
        if len(STATE["log"]) > 200:
            STATE["log"] = STATE["log"][-200:]
    print(msg)
    if TG["token"]:
        _TGQ.put(msg)


# ---------------- HL reads (public) ----------------
def hl_post(base, body):
    req = urllib.request.Request(base + "/info", data=json.dumps(body).encode(),
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=10) as r:
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
        return dex, hl_post(base, body)
    except Exception:
        return dex, None

def _fetch_all_dexes(base, addr):
    """Fetch every perp dex's clearinghouseState in parallel (1 round-trip
    instead of N sequential ones). Returns list of (dex, data|None)."""
    dexes = perp_dexes(base)
    workers = min(12, max(1, len(dexes)))
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
    cap = CROSS_LEV_CAP if mode == "cross" else MAX_LEV
    return max(1, min(int(round(whale_lev)), cap))


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
    if not os.path.exists(SECRETS_FILE):
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
        if isinstance(d.get("wallets"), list) and d["wallets"]:
            WALLETS[:] = [{"addr": w["addr"], "label": w.get("label", "Wallet")}
                          for w in d["wallets"] if w.get("addr")]
    except Exception as e:
        print("load_secrets error:", e)

def save_secrets():
    try:
        json.dump({"tg_token": TG["token"], "tg_chat": TG["chat"],
                   "tp_crypto": SET["tp_crypto"], "tp_stock": SET["tp_stock"],
                   "wallets": WALLETS},
                  open(SECRETS_FILE, "w"))
    except Exception as e:
        print("save_secrets error:", e)

class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _auth(self):
        q = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
        return q.get("token", [""])[0] == DASH_TOKEN

    def _send(self, code, body, ctype="application/json"):
        if isinstance(body, str):
            body = body.encode()
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        path = urllib.parse.urlparse(self.path).path
        if path in ("/", "/index.html"):
            try:
                html = open(os.path.join(HERE, "index.html"), encoding="utf-8").read()
            except Exception:
                return self._send(404, "index.html nicht gefunden (neben bot.py legen)", "text/plain")
            return self._send(200, html.replace("__DASH_TOKEN__", DASH_TOKEN), "text/html; charset=utf-8")
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
                st["wallets"] = wallets_payload()
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
                                "peak_roe": 0.0, "opened_ms": 0 if _myfirst else now_ms}
        cur = MINE["pos"][key]                         # track best ROE reached (for the popup)
        sign = 1 if w["side"] == "LONG" else -1
        roe_now = (sign * szabs * (mk - w["entry"]) / margin) if margin else 0.0
        if roe_now > cur.get("peak_roe", 0.0):
            cur["peak_roe"] = roe_now
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
        out.append({"bare": p["bare"], "coin": p.get("coin", p["bare"]),
                    "kind": "Stock" if p.get("dex") else "Crypto",
                    "side": p["side"], "sz": p["sz"], "entry": p["entry"], "mark": mk,
                    "lev": p["lev"], "mode": p["mode"], "margin": p["margin"],
                    "upnl": round(upnl, 2), "roe": round(roe, 4),
                    "peak_roe": round(p.get("peak_roe", 0.0), 4),
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


# ============================ PAPER LOOP ============================
def run_paper():
    STATE["net"] = "PAPER"; STATE["running"] = True
    load_secrets()
    loaded = load_paper()
    start_server()
    threading.Thread(target=start_tunnel, daemon=True).start()
    tg("🤖 Copy-Bot in PAPER mode (simulated, no real money) · start $%.0f" % PAPER_START)
    print("\n>>> Dashboard:  http://<server-ip>/   (token: %s)\n" % DASH_TOKEN)

    if loaded:
        day_start_eq, day_start_cash, day = loaded
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
            today = datetime.datetime.now(datetime.timezone.utc).date()
            whale, okdex = get_positions_ex(SOURCE_URL, WHALE)
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
                    if roe > p.get("peak_roe", 0.0):
                        p["peak_roe"] = roe            # track best ROE reached (for the popup)
                    tp = p.get("tp") or (SET["tp_stock"] if p.get("dex") else SET["tp_crypto"])
                    if pnl <= -p["margin"]:
                        PAPER["cash"] -= p["margin"]; PAPER["pos"].pop(key)
                        record_closed(p, -p["margin"], "liquidated")
                        events.append("💥 LIQUIDATED %s — margin lost ($%.2f)" % (p["bare"], p["margin"]))
                    elif roe >= tp:
                        PAPER["cash"] += pnl; PAPER["pos"].pop(key)
                        record_closed(p, pnl, "take-profit")
                        events.append("🎯 TP +%.0f%% ROE %s — profit $%.2f" % (tp * 100, p["bare"], pnl))

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
                                             "opened": time.strftime("%H:%M:%S"),
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
                                             "opened": time.strftime("%H:%M:%S"),
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
