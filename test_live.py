#!/usr/bin/env python3
"""In-code test of the live-trading engine: mocks the Hyperliquid Exchange + API reads
and drives the REAL live_tick() logic through every critical scenario."""
import sys, os, types, importlib.util

BOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bot.py")
spec = importlib.util.spec_from_file_location("bot", BOT)
bot = importlib.util.module_from_spec(spec)
sys.modules["bot"] = bot
spec.loader.exec_module(bot)

import tempfile; SCRATCH = tempfile.gettempdir()
bot.LIVE_STATE_FILE = os.path.join(SCRATCH, "live_state_test.json")
bot.EXEC_URL = "EXEC"
bot.TG["token"] = ""          # no telegram network
bot.round_sz = lambda base, coin, sz: round(sz, 3)

ST = {"whale_pos": {}, "whale_ok": {""}, "ex_pos": {}, "ex_ok": {""}, "equity": 900.0, "fills": [], "entry": 100.0}

def makepos(dex, bare, side, entry, mark, lev=6, mode="cross"):
    szi = 1.0 if side == "LONG" else -1.0
    coin = (dex + ":" + bare) if dex else bare
    return {"key": (dex, bare), "coin": coin, "bare": bare, "dex": dex, "szi": szi,
            "side": side, "entry": entry, "lev": lev, "mode": mode, "mark": mark}

bot.get_positions_ex = lambda base, addr: (dict(ST["whale_pos"]), set(ST["whale_ok"])) if base == bot.SOURCE_URL else (dict(ST["ex_pos"]), set(ST["ex_ok"]))
bot.get_positions = lambda base, addr: dict(ST["ex_pos"])
bot.get_unified_value = lambda base, addr: ST["equity"]
_real_hl = bot.hl_post
bot.hl_post = lambda base, body, timeout=10: ST["fills"] if body.get("type") == "userFills" else {}

class FakeEx:
    def __init__(self, addr): self.account_address = addr; self.calls = []
    def update_leverage(self, lev, coin, is_cross): self.calls.append(("lev", lev, coin, is_cross))
    def market_open(self, coin, is_buy, sz):
        self.calls.append(("open", coin, is_buy, sz))
        ST["ex_pos"][("", coin)] = makepos("", coin, "LONG" if is_buy else "SHORT", ST["entry"], ST["entry"])
    def market_close(self, coin):
        self.calls.append(("close", coin)); ST["ex_pos"].pop(("", coin), None)
    def order(self, *a, **k): self.calls.append(("order",) + a)

FAILS = []
def check(name, cond):
    print(("  PASS " if cond else "  FAIL ") + name)
    if not cond: FAILS.append(name)

def reset(equity=900.0, killswitch=True, max_lev=6):
    if os.path.exists(bot.LIVE_STATE_FILE): os.remove(bot.LIVE_STATE_FILE)
    ST.update(whale_pos={}, whale_ok={""}, ex_pos={}, ex_ok={""}, equity=equity, fills=[], entry=100.0)
    ex = FakeEx("0xMyAcct")
    bot.LIVE.update(enabled=True, ready=True, ex=ex, addr="0xMyAcct", net="MAINNET",
                    killed=False, killswitch=killswitch, max_lev=max_lev,
                    owned={}, closed=[], log=[], equity=0.0, day_start_eq=0.0)
    ctx = {"known": set(), "based": set(), "miss": {}, "openseen": {}, "close_miss": {},
           "announced": False, "was_enabled": False, "day": None}
    return ex, ctx

def ticks(ctx, n=1):
    for _ in range(n): bot.live_tick(ctx)

def logs(): return " | ".join(e["text"] for e in bot.LIVE["log"])

# ---------------------------------------------------------------- 1: open a main-perp copy
print("\n[1] Opens a NEW main-perp whale trade after OPEN_CONFIRM polls")
ex, ctx = reset(equity=1000.0)
ticks(ctx, 1)                                   # baseline + announce
ST["whale_pos"][("", "ZEC")] = makepos("", "ZEC", "LONG", 100.0, 100.0)
ST["entry"] = 100.0
ticks(ctx, 1)                                   # openseen=1 (<2) -> no open
check("not opened after 1 confirm", ("", "ZEC") not in bot.LIVE["owned"])
ticks(ctx, 1)                                   # openseen=2 -> open
check("opened after 2 confirms", ("", "ZEC") in bot.LIVE["owned"])
m = bot.LIVE["owned"].get(("", "ZEC"), {})
check("margin = 20% of equity (200)", abs(m.get("margin", 0) - 200.0) < 0.01)
check("leverage 6x recorded", m.get("lev") == 6)
check("market_open called once", sum(1 for c in ex.calls if c[0] == "open") == 1)
check("TP reduce-only order placed", any(c[0] == "order" for c in ex.calls))
check("OPEN telegram/log present", "✅ OPEN ZEC" in logs())

# ---------------------------------------------------------------- 2a: no false close (1 poll)
print("\n[2a] No false close on a single missing poll")
del ST["whale_pos"][("", "ZEC")]                 # whale no longer relevant; keep our pos
ST["ex_pos"].pop(("", "ZEC"))                    # vanish for ONE tick
ticks(ctx, 1)
check("still owned after 1 absent poll", ("", "ZEC") in bot.LIVE["owned"])
check("no close recorded", len(bot.LIVE["closed"]) == 0)
ST["ex_pos"][("", "ZEC")] = makepos("", "ZEC", "LONG", 100.0, 100.0)  # back
ticks(ctx, 1)
check("recovers (still owned, no close)", ("", "ZEC") in bot.LIVE["owned"] and len(bot.LIVE["closed"]) == 0)

# ---------------------------------------------------------------- 2b: dex read fail != close
print("\n[2b] Dex read failure does not trigger a close")
ST["ex_pos"].pop(("", "ZEC"))                    # absent...
ST["ex_ok"] = set()                              # ...because the dex read FAILED
ticks(ctx, 3)
check("not closed while dex unreadable", ("", "ZEC") in bot.LIVE["owned"] and len(bot.LIVE["closed"]) == 0)
ST["ex_ok"] = {""}                               # restore reads

# ---------------------------------------------------------------- 2c: real TP close + PnL
print("\n[2c] Real TP close after CLOSE_CONFIRM polls reports PnL + %")
import time as _t
ST["fills"] = [{"coin": "ZEC", "time": int(_t.time() * 1000), "closedPnl": "24.0"}]
# ZEC absent, dex OK -> needs CLOSE_CONFIRM consecutive
ticks(ctx, bot.CLOSE_CONFIRM)
check("closed after CLOSE_CONFIRM polls", ("", "ZEC") not in bot.LIVE["owned"])
check("recorded 1 closed trade", len(bot.LIVE["closed"]) == 1)
ct = bot.LIVE["closed"][-1] if bot.LIVE["closed"] else {}
check("closed PnL = +24 from fills", abs(ct.get("pnl", 0) - 24.0) < 0.01)
check("closed ROE = +12% (24/200)", abs(ct.get("roe", 0) - 0.12) < 0.001)
check("CLOSE log has PnL", "🔻 CLOSE" in logs())

# ---------------------------------------------------------------- 3: builder-dex skip
print("\n[3] Builder-dex (stock perp) is skipped cleanly, no error")
ex, ctx = reset()
ST["whale_ok"] = {"", "xyz"}; ST["ex_ok"] = {"", "xyz"}
ticks(ctx, 1)                                    # baseline + announce (empty)
ST["whale_pos"][("xyz", "SPCX")] = makepos("xyz", "SPCX", "LONG", 50.0, 50.0)
ticks(ctx, 3)
check("SPCX never opened", ("xyz", "SPCX") not in bot.LIVE["owned"])
check("no market_open for builder dex", not any(c[0] == "open" for c in ex.calls))
check("skip note shown", "Builder-Dex" in logs())

# ---------------------------------------------------------------- 4: whale exit does NOT close us
print("\n[4] Whale closing the trade does NOT close our copy")
ex, ctx = reset(equity=1000.0)
ticks(ctx, 1)
ST["whale_pos"][("", "SOL")] = makepos("", "SOL", "LONG", 100.0, 100.0)
ticks(ctx, 2)                                    # confirm + open
check("SOL opened", ("", "SOL") in bot.LIVE["owned"])
del ST["whale_pos"][("", "SOL")]                 # whale exits; OUR position stays open
ticks(ctx, bot.CLOSE_CONFIRM + 1)
check("our SOL still open after whale exit", ("", "SOL") in bot.LIVE["owned"])
check("no close recorded on whale exit", len(bot.LIVE["closed"]) == 0)

# ---------------------------------------------------------------- 5: account-wide position cap
print("\n[5] Account-wide cap: 5 manual positions block new copies")
ex, ctx = reset(equity=1000.0)
for i in range(bot.MAX_POSITIONS):
    ST["ex_pos"][("", "M%d" % i)] = makepos("", "M%d" % i, "LONG", 10.0, 10.0)
ticks(ctx, 1)
ST["whale_pos"][("", "AVAX")] = makepos("", "AVAX", "LONG", 100.0, 100.0)
ticks(ctx, 3)
check("new copy blocked at cap", ("", "AVAX") not in bot.LIVE["owned"])
check("slots-full message shown", "Slots belegt" in logs())

# ---------------------------------------------------------------- 6: kill-switch
print("\n[6] Kill-switch halts new trades; toggling it off lifts the halt")
ex, ctx = reset(equity=1000.0)
ticks(ctx, 1)                                    # day_start_eq = 1000
ST["equity"] = 700.0                             # -30% > 25% loss
ST["whale_pos"][("", "DOGE")] = makepos("", "DOGE", "LONG", 100.0, 100.0)
ticks(ctx, 3)
check("kill-switch tripped", bot.LIVE["killed"] is True)
check("no open while killed", ("", "DOGE") not in bot.LIVE["owned"])
bot.LIVE["killswitch"] = False
ticks(ctx, 1)
check("killed lifted when switch off", bot.LIVE["killed"] is False)

# ---------------------------------------------------------------- 7: leverage cap
print("\n[7] Leverage cap limits live leverage")
ex, ctx = reset(equity=1000.0, max_lev=2)
check("live_lev = min(6, cap2) = 2", bot.live_lev() == 2)
ticks(ctx, 1)
ST["whale_pos"][("", "LINK")] = makepos("", "LINK", "LONG", 100.0, 100.0)
ticks(ctx, 2)
check("opened at capped 2x", bot.LIVE["owned"].get(("", "LINK"), {}).get("lev") == 2)
check("update_leverage(2,...) sent", any(c[:2] == ("lev", 2) for c in ex.calls))

# ---------------------------------------------------------------- 8: persistence across restart
print("\n[8] Owned + closed survive a restart (live_save/load_state)")
ex, ctx = reset(equity=1000.0)
ticks(ctx, 1)
ST["whale_pos"][("", "BTC")] = makepos("", "BTC", "LONG", 100.0, 100.0)
ticks(ctx, 2)
check("BTC owned + persisted to disk", ("", "BTC") in bot.LIVE["owned"] and os.path.exists(bot.LIVE_STATE_FILE))
bot.LIVE["owned"] = {}; bot.LIVE["closed"] = []   # simulate process restart wiping memory
bot.live_load_state()
check("owned restored after restart", ("", "BTC") in bot.LIVE["owned"])
m = bot.LIVE["owned"][("", "BTC")]
check("restored meta has margin", m.get("margin") == 200.0)

# ---------------------------------------------------------------- 9: closed-during-downtime recovery
print("\n[9] A position closed while the bot was down is recovered + notified")
# continue from [8]: BTC owned (restored), but it closed on the exchange while down
ST["ex_pos"].pop(("", "BTC"), None)
ST["fills"] = [{"coin": "BTC", "time": int(_t.time() * 1000), "closedPnl": "24.0"}]
ctx2 = {"known": set(), "based": set(), "miss": {}, "openseen": {}, "close_miss": {},
        "announced": False, "was_enabled": False, "day": None}
ticks(ctx2, 1)                                    # baseline/announce
ticks(ctx2, bot.CLOSE_CONFIRM)                    # detect the missed close
check("missed close recovered", ("", "BTC") not in bot.LIVE["owned"])
check("recorded with PnL", any(abs(t.get("pnl", 0) - 24.0) < 0.01 for t in bot.LIVE["closed"]))

# ---------------------------------------------------------------- 10: disabled engine is inert
print("\n[10] Disabled engine opens nothing")
ex, ctx = reset(equity=1000.0)
bot.LIVE["enabled"] = False
ST["whale_pos"][("", "ETH")] = makepos("", "ETH", "LONG", 100.0, 100.0)
ticks(ctx, 5)
check("nothing opened while disabled", len(bot.LIVE["owned"]) == 0)
check("no market_open while disabled", not any(c[0] == "open" for c in ex.calls))

print("\n================ RESULT ================")
if FAILS:
    print("FAILED %d check(s): %s" % (len(FAILS), FAILS)); sys.exit(1)
print("ALL CHECKS PASSED")
