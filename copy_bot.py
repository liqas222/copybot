#!/usr/bin/env python3
"""
Hyperliquid Copy-Trading Bot  ·  v2 (TESTNET-FIRST, with built-in dashboard)
============================================================================
Reads the tracked whale from MAINNET and mirrors trades onto YOUR account on
TESTNET (play money). It ALSO serves the dashboard + a live status panel, so
you can open it from any device at  http://<server-ip>:8080/

Rules: copy opens & exits · 1/5 equity margin/trade · max 5 positions ·
leverage cross->min(his,10x)/isolated->exact(<=40x) · TP +20% ROE ·
no SL · daily -25% kill-switch · all coins.

SAFETY: uses an AGENT/API wallet (can trade, CANNOT withdraw).

SETUP (short):
  1) Python 3.10 + pip install hyperliquid-python-sdk eth-account
  2) On https://app.hyperliquid-testnet.xyz connect Phantom, faucet some USDC,
     open 1 test trade, then API page -> authorize API wallet -> copy its key.
  3) config.json:  {"agent_key":"0x..","account_address":"0x.."}
  4) Put index.html in the SAME folder as this file (the bot serves it).
  5) export TELEGRAM_BOT_TOKEN=...   (optional: export DASH_TOKEN=...)
  6) python copy_bot.py   ->  open http://localhost:8080/  (or server IP)
"""

import os, json, time, math, datetime, threading, secrets
import urllib.request, urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from eth_account import Account
from hyperliquid.exchange import Exchange
from hyperliquid.utils import constants

# ============================ CONFIG ============================
WHALE            = "0x0c349d9b92fbd172bbb5a17a9db0a673a6a10ad3"
SOURCE_URL       = "https://api.hyperliquid.xyz"        # read whale from MAINNET
EXEC_URL         = constants.TESTNET_API_URL            # execute on TESTNET
# EXEC_URL       = constants.MAINNET_API_URL            # <- switch to GO LIVE

CAPITAL_FRACTION = 0.20
MAX_POSITIONS    = 5
CROSS_LEV_CAP    = 10
MAX_LEV          = 40
TP_ROE           = 0.20
DAILY_LOSS_LIMIT = 0.25
POLL_SECONDS     = 4
COIN_WHITELIST   = None      # None = all coins; or {"BTC","HYPE"}

TG_TOKEN  = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TG_CHAT   = "1253059682"

DASH_HOST  = "0.0.0.0"
DASH_PORT  = 80
DASH_TOKEN = os.environ.get("DASH_TOKEN") or secrets.token_urlsafe(12)
# ===============================================================

STATE = {"running": False, "paused": False, "killed": False, "net": "",
         "equity": 0.0, "day_start_equity": 0.0, "slots_used": 0,
         "max_positions": MAX_POSITIONS, "log": []}
LOCK = threading.Lock()
EX   = {"ex": None}


# ---------------- notifications + log ----------------
def tg(msg):
    with LOCK:
        STATE["log"].append({"t": time.strftime("%H:%M:%S"), "text": msg})
        if len(STATE["log"]) > 200:
            STATE["log"] = STATE["log"][-200:]
    print(msg)
    if not TG_TOKEN:
        return
    try:
        data = urllib.parse.urlencode({"chat_id": TG_CHAT, "text": msg}).encode()
        urllib.request.urlopen("https://api.telegram.org/bot%s/sendMessage" % TG_TOKEN,
                               data=data, timeout=10)
    except Exception as e:
        print("TG error:", e)


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

def get_positions(base, addr):
    out = {}
    for dex in perp_dexes(base):
        body = {"type": "clearinghouseState", "user": addr}
        if dex:
            body["dex"] = dex
        try:
            d = hl_post(base, body)
        except Exception:
            continue
        for ap in d.get("assetPositions", []):
            x = parse_pos(ap, dex)
            if x:
                out[x["key"]] = x
    return out

def get_equity(base, addr):
    eq = 0.0
    for dex in perp_dexes(base):
        body = {"type": "clearinghouseState", "user": addr}
        if dex:
            body["dex"] = dex
        try:
            eq += float(hl_post(base, body).get("marginSummary", {}).get("accountValue") or 0)
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


# ---------------- order actions ----------------
def open_copy(ex, w, equity):
    lev = my_leverage(w["lev"], w["mode"])
    is_cross = (w["mode"] == "cross")
    is_buy = w["szi"] > 0
    coin, mark = w["coin"], (w["mark"] or 1)
    margin = equity * CAPITAL_FRACTION
    sz = round_sz(EXEC_URL, w["bare"], (margin * lev) / mark)
    if sz <= 0:
        tg("⚠️ Übersprungen %s: Größe 0 (zu wenig Kapital?)" % w["bare"]); return
    try:
        ex.update_leverage(lev, coin, is_cross)
        ex.market_open(coin, is_buy, sz)
        time.sleep(1.0)
        mine = get_positions(EXEC_URL, ex.account_address).get(w["key"])
        entry = mine["entry"] if mine else mark
        move = TP_ROE / lev
        tp = round_px(entry * (1 + move) if is_buy else entry * (1 - move))
        ex.order(coin, (not is_buy), sz, tp,
                 {"trigger": {"triggerPx": tp, "isMarket": True, "tpsl": "tp"}}, reduce_only=True)
        tg("✅ KOPIERT %s %s · Größe %.4f @ ~$%.4f · %dx %s · TP +20%% ROE @ $%s"
           % (w["bare"], w["side"], sz, entry, lev, "Cross" if is_cross else "Isolated", tp))
    except Exception as e:
        tg("❌ Fehler beim Öffnen %s: %s" % (w["bare"], e))

def close_copy(ex, w, reason):
    try:
        ex.market_close(w["coin"]); tg("🔻 GESCHLOSSEN %s (%s)" % (w["bare"], reason))
    except Exception as e:
        tg("❌ Fehler beim Schließen %s: %s" % (w["bare"], e))

def flatten_all():
    ex = EX["ex"]
    if not ex:
        return
    tg("🧹 Flatten: schließe alle Positionen…")
    for p in get_positions(EXEC_URL, ex.account_address).values():
        try:
            ex.market_close(p["coin"])
        except Exception as e:
            tg("❌ Flatten %s: %s" % (p["bare"], e))


# ---------------- web server (dashboard + status + controls) ----------------
HERE = os.path.dirname(os.path.abspath(__file__))

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
                return self._send(404, "index.html nicht gefunden (neben copy_bot.py legen)", "text/plain")
            return self._send(200, html.replace("__DASH_TOKEN__", DASH_TOKEN), "text/html; charset=utf-8")
        if path == "/status":
            if not self._auth():
                return self._send(403, json.dumps({"error": "forbidden"}))
            with LOCK:
                st = dict(STATE)
                st["day_pnl"] = round(st["equity"] - st["day_start_equity"], 2)
                st["log"] = STATE["log"][-60:]
            return self._send(200, json.dumps(st))
        return self._send(404, "not found", "text/plain")

    def do_POST(self):
        path = urllib.parse.urlparse(self.path).path
        if not self._auth():
            return self._send(403, json.dumps({"error": "forbidden"}))
        if path == "/pause":
            STATE["paused"] = True;  tg("⏸ Bot pausiert (Dashboard)")
        elif path == "/resume":
            STATE["paused"] = False; tg("▶️ Bot fortgesetzt (Dashboard)")
        elif path == "/flatten":
            threading.Thread(target=flatten_all, daemon=True).start()
        else:
            return self._send(404, "not found", "text/plain")
        return self._send(200, json.dumps({"ok": True}))

def start_server():
    srv = ThreadingHTTPServer((DASH_HOST, DASH_PORT), Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()


# ============================ MAIN LOOP ============================
def main():
    cfg = json.load(open(os.path.join(HERE, "config.json")))
    ex = Exchange(Account.from_key(cfg["agent_key"]), EXEC_URL, account_address=cfg["account_address"])
    EX["ex"] = ex
    main_addr = cfg["account_address"]

    net = "TESTNET" if EXEC_URL == constants.TESTNET_API_URL else "🔴 MAINNET"
    STATE["net"] = net; STATE["running"] = True
    start_server()
    tg("🤖 Copy-Bot läuft auf %s · Dashboard im Browser: http://<server-ip>/" % net)
    print("\n>>> Dashboard im Browser:  http://<server-ip>/   (Token: %s)\n" % DASH_TOKEN)

    prev_whale = set()
    day = datetime.datetime.utcnow().date()
    day_start_eq = get_equity(EXEC_URL, main_addr)
    killed = False

    while True:
        try:
            today = datetime.datetime.utcnow().date()
            if today != day:
                day = today; day_start_eq = get_equity(EXEC_URL, main_addr); killed = False
                tg("🌅 Neuer Tag — Kill-Switch zurückgesetzt.")
            equity = get_equity(EXEC_URL, main_addr)
            if not killed and day_start_eq > 0 and equity <= day_start_eq * (1 - DAILY_LOSS_LIMIT):
                killed = True
                tg("🛑 KILL-SWITCH: −%.0f%% heute. Keine neuen Trades." % (DAILY_LOSS_LIMIT * 100))

            whale = get_positions(SOURCE_URL, WHALE)
            if COIN_WHITELIST is not None:
                whale = {k: v for k, v in whale.items() if v["bare"] in COIN_WHITELIST}
            mine = get_positions(EXEC_URL, main_addr)

            with LOCK:
                STATE.update(equity=equity, day_start_equity=day_start_eq, killed=killed,
                             slots_used=len(mine))

            # 1) whale closed -> close ours
            wk = set(whale.keys())
            for key in list(mine.keys()):
                if key not in wk:
                    close_copy(ex, mine[key], "Whale-Exit")

            # 2) whale opened new -> copy if a slot is free
            for key in (wk - prev_whale):
                if key in mine:
                    continue
                if STATE["paused"]:
                    tg("⏭️ %s übersprungen — pausiert." % whale[key]["bare"]); continue
                if killed:
                    tg("⏭️ %s übersprungen — Kill-Switch." % whale[key]["bare"]); continue
                if len(get_positions(EXEC_URL, main_addr)) >= MAX_POSITIONS:
                    tg("⏭️ %s übersprungen — 5 Slots belegt." % whale[key]["bare"]); continue
                open_copy(ex, whale[key], equity)

            prev_whale = wk
        except Exception as e:
            print("loop error:", e)
        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()
