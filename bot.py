#!/usr/bin/env python3
"""
Hyperliquid Copy-Trading Bot  ·  v3  (PAPER mode + 24/7 service + dashboard)
============================================================================
Reads the tracked whale from MAINNET and copies its trades. It serves the
dashboard + a live status panel, reachable from any device via a free
Cloudflare tunnel (printed at startup and written to link.txt).

Three modes (set below):
  * PAPER_MODE = True  -> simulates copying into a virtual $1000 account
                          (no keys, no funds, no exchange). Best for testing.
  * PAPER_MODE = False + EXEC_URL = TESTNET  -> real test trades (needs config.json)
  * PAPER_MODE = False + EXEC_URL = MAINNET  -> LIVE real money (needs config.json)

Rules (all modes): copy opens & exits · 1/5 equity margin/trade · max 5 positions
· leverage cross->min(his,10x) / isolated->exact(<=40x) · TP +20% ROE ·
no SL · daily -25% kill-switch · all coins.

RUN:
  python bot.py           -> run in this window (stops when window closes)
  python bot.py install   -> install as a 24/7 service (survives reboot / Ctrl+C)
  cat link.txt            -> show the current dashboard link any time
"""

import os, sys, json, time, math, datetime, threading, secrets, subprocess, re
import urllib.request, urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# ============================ CONFIG ============================
WHALE            = "0x0c349d9b92fbd172bbb5a17a9db0a673a6a10ad3"
SOURCE_URL       = "https://api.hyperliquid.xyz"        # read whale from MAINNET

PAPER_MODE       = True          # <- simulate (no money). Set False for real trading.
PAPER_START      = 1000.0        # virtual starting capital for paper mode

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
PAPER = {"cash": PAPER_START, "pos": {}}     # pos: key(tuple) -> dict


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
    # paper mode
    if PAPER_MODE:
        try:    whale = get_positions(SOURCE_URL, WHALE)
        except Exception: whale = {}
        try:    mids = hl_post(SOURCE_URL, {"type": "allMids"})
        except Exception: mids = {}
        msgs = ["🧹 Flatten: schließe alle Papier-Positionen…"]
        with LOCK:
            for key in list(PAPER["pos"].keys()):
                p = PAPER["pos"].pop(key)
                pnl = _pnl(key, p, whale, mids)
                PAPER["cash"] += pnl
                msgs.append("🔻 GESCHLOSSEN %s (manuell) — PnL $%.2f" % (p["bare"], pnl))
        for m in msgs:
            tg(m)
        return
    # real mode
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
                return self._send(404, "index.html nicht gefunden (neben bot.py legen)", "text/plain")
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

def start_tunnel():
    """Download cloudflared and open a free https tunnel so the dashboard
    is reachable from any network (e.g. a restricted PC)."""
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
                tg("🔗 Dashboard-Link (PC): " + url)
                break
    except Exception as e:
        print("Tunnel-Fehler:", e)


def start_server():
    srv = ThreadingHTTPServer((DASH_HOST, DASH_PORT), Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()


# ============================ PAPER LOOP ============================
def run_paper():
    STATE["net"] = "PAPIER"; STATE["running"] = True
    start_server()
    threading.Thread(target=start_tunnel, daemon=True).start()
    tg("🤖 Copy-Bot im PAPIER-Modus (simuliert, kein echtes Geld) · Start $%.0f" % PAPER_START)
    print("\n>>> Dashboard im Browser:  http://<server-ip>/   (Token: %s)\n" % DASH_TOKEN)

    prev_whale = set()
    seeded = False           # only copy NEW opens, never the whale's current book
    day = datetime.datetime.now(datetime.timezone.utc).date()
    day_start_eq = PAPER_START
    killed = False

    while True:
        try:
            today = datetime.datetime.now(datetime.timezone.utc).date()
            whale = get_positions(SOURCE_URL, WHALE)
            if COIN_WHITELIST is not None:
                whale = {k: v for k, v in whale.items() if v["bare"] in COIN_WHITELIST}
            try:    mids = hl_post(SOURCE_URL, {"type": "allMids"})
            except Exception: mids = {}

            if not seeded:
                prev_whale = set(whale.keys())
                seeded = True
                with LOCK:
                    STATE.update(equity=round(PAPER["cash"], 2),
                                 day_start_equity=round(day_start_eq, 2), slots_used=0)
                tg("👀 Beobachte ab jetzt — %d bestehende Whale-Positionen werden NICHT kopiert, nur NEUE." % len(prev_whale))
                time.sleep(POLL_SECONDS); continue

            events = []
            with LOCK:
                if today != day:
                    day = today; killed = False
                    day_start_eq = paper_equity(whale, mids)
                    events.append("🌅 Neuer Tag — Kill-Switch zurückgesetzt.")

                # TP +20% ROE  /  liquidation at -100% ROE
                for key in list(PAPER["pos"].keys()):
                    p = PAPER["pos"][key]
                    pnl = _pnl(key, p, whale, mids)
                    roe = pnl / p["margin"] if p["margin"] else 0
                    if pnl <= -p["margin"]:
                        PAPER["cash"] -= p["margin"]; PAPER["pos"].pop(key)
                        events.append("💥 LIQUIDIERT %s — Margin weg ($%.2f)" % (p["bare"], p["margin"]))
                    elif roe >= TP_ROE:
                        PAPER["cash"] += pnl; PAPER["pos"].pop(key)
                        events.append("🎯 TP +20%% ROE %s — Gewinn $%.2f" % (p["bare"], pnl))

                # whale closed -> close ours
                for key in list(PAPER["pos"].keys()):
                    if key not in whale:
                        p = PAPER["pos"].pop(key)
                        pnl = _pnl(key, p, whale, mids)
                        PAPER["cash"] += pnl
                        events.append("🔻 GESCHLOSSEN %s (Whale-Exit) — PnL $%.2f" % (p["bare"], pnl))

                equity = paper_equity(whale, mids)
                STATE.update(equity=round(equity, 2), day_start_equity=round(day_start_eq, 2),
                             slots_used=len(PAPER["pos"]), killed=killed)

            for e in events:
                tg(e)

            # daily kill-switch
            if not killed and day_start_eq > 0 and equity <= day_start_eq * (1 - DAILY_LOSS_LIMIT):
                killed = True
                tg("🛑 KILL-SWITCH: −%.0f%% heute. Keine neuen Trades." % (DAILY_LOSS_LIMIT * 100))

            # whale opened new -> copy if a slot is free
            for key in (set(whale.keys()) - prev_whale):
                with LOCK:
                    have = key in PAPER["pos"]; nslots = len(PAPER["pos"])
                if have:
                    continue
                if STATE["paused"]:
                    tg("⏭️ %s übersprungen — pausiert." % whale[key]["bare"]); continue
                if killed:
                    tg("⏭️ %s übersprungen — Kill-Switch." % whale[key]["bare"]); continue
                if nslots >= MAX_POSITIONS:
                    tg("⏭️ %s übersprungen — 5 Slots belegt." % whale[key]["bare"]); continue
                w = whale[key]
                entry = w["mark"] or 0
                if entry <= 0:
                    continue
                lev = my_leverage(w["lev"], w["mode"])
                with LOCK:
                    eq = paper_equity(whale, mids)
                    margin = eq * CAPITAL_FRACTION
                    sz = (margin * lev) / entry
                    PAPER["pos"][key] = {"bare": w["bare"], "coin": w["coin"], "dex": w["dex"],
                                         "side": w["side"], "entry": entry, "lev": lev,
                                         "mode": w["mode"], "sz": sz, "margin": margin,
                                         "opened": time.strftime("%H:%M:%S")}
                tg("✅ KOPIERT %s %s · Einsatz $%.2f · %dx %s · TP +20%% ROE"
                   % (w["bare"], w["side"], margin, lev, "Cross" if w["mode"] == "cross" else "Isolated"))

            prev_whale = set(whale.keys())
        except Exception as e:
            print("paper loop error:", e)
        time.sleep(POLL_SECONDS)


# ============================ REAL LOOP ============================
def main():
    if PAPER_MODE:
        run_paper()
        return

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
    tg("🤖 Copy-Bot läuft auf %s · Dashboard im Browser: http://<server-ip>/" % net)
    print("\n>>> Dashboard im Browser:  http://<server-ip>/   (Token: %s)\n" % DASH_TOKEN)

    prev_whale = set()
    seeded = False           # only copy NEW opens, never the whale's current book
    day = datetime.datetime.now(datetime.timezone.utc).date()
    day_start_eq = get_equity(EXEC_URL, main_addr)
    killed = False

    while True:
        try:
            today = datetime.datetime.now(datetime.timezone.utc).date()
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

            if not seeded:
                prev_whale = set(whale.keys())
                seeded = True
                tg("👀 Beobachte ab jetzt — %d bestehende Whale-Positionen werden NICHT kopiert, nur NEUE." % len(prev_whale))
                time.sleep(POLL_SECONDS); continue

            wk = set(whale.keys())
            for key in list(mine.keys()):
                if key not in wk:
                    close_copy(ex, mine[key], "Whale-Exit")

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


# ============================ SERVICE INSTALL ============================
def setup_service():
    py = sys.executable
    script = os.path.abspath(__file__)
    workdir = os.path.dirname(script)
    tg_line = ("Environment=TELEGRAM_BOT_TOKEN=%s\n" % TG_TOKEN) if TG_TOKEN else ""
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
        print("Konnte Service-Datei nicht schreiben (als root ausführen):", e)
        return
    os.system("systemctl daemon-reload")
    os.system("systemctl enable copybot")
    os.system("systemctl restart copybot")
    print("\n==================================================")
    print(">>> DAUERBETRIEB AKTIV. Der Bot läuft jetzt 24/7.")
    print(">>> Auch nach Konsole-zu und nach Server-Neustart.")
    print("==================================================")
    print(">>> Link ansehen:   cat " + os.path.join(workdir, "link.txt"))
    print(">>> Status:         systemctl status copybot")
    print(">>> Live-Logs:      journalctl -u copybot -f")
    print(">>> Stoppen:        systemctl stop copybot")
    print(">>> (warte ~10s, dann 'cat link.txt' fuer den Dashboard-Link)\n")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "install":
        setup_service()
    else:
        main()
