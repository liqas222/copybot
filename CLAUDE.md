# Copy-Trade Command Center — Project Context

> This file is read automatically by Claude Code. It captures everything about
> this project so any new session continues seamlessly. Keep it up to date.

## What this is
A **Hyperliquid whale copy-trading bot** (Python) plus a **single-file live dashboard**
(HTML/CSS/JS). The bot mirrors one whale's trades into a virtual $1000 **paper account**
(simulated — no real money by default) and serves a "command center" dashboard.

## Files
- **`bot.py`** — the bot + dashboard web server (~900 lines, Python standard library only,
  no pip installs needed for paper mode). Polls the whale on Hyperliquid every 3s, mirrors
  opens and exits into the paper account, and serves the dashboard + a JSON `/status` API on
  port 80.
- **`index.html`** — single-file dashboard (vanilla JS, no build step). Served by `bot.py`.
  Reads Hyperliquid's public API client-side for live markets (ticker, Market Watch, popup
  charts) and polls `bot.py`'s `/status` for bot state.
- **Runtime files — NEVER commit these (they live only on the server):**
  - `secrets.json` — Telegram bot token + chat id, take-profit settings, tracked wallets.
  - `paper_state.json` — paper account state (cash, positions, history, closed trades).

## Run / deploy
- GitHub repo: **`liqas222/copybot`** (consider making it **private** — it documents your bot).
- Server: a **DigitalOcean droplet, Ubuntu 24.04**, repo cloned at **`/root/bot`**.
- Runs 24/7 as a **systemd service `copybot`** (`Restart=always`, enabled).
  - `systemctl status copybot` · `systemctl restart copybot` · `systemctl stop copybot`
  - live logs: `journalctl -u copybot -f`
- **Update flow:** edit files → commit & push → on the server `cd /root/bot && git pull` →
  if `bot.py` changed run `systemctl restart copybot` (changes to `index.html` need no
  restart — the bot serves it fresh; just hard-refresh the page).
- Dashboard is reachable at a **fixed Tailscale Funnel URL** (a `…ts.net` address). It is
  persistent (`tailscale funnel --bg 80`) and survives reboots. Keep the exact URL private.

## Copy-trading ruleset (currently `PAPER_MODE = True`)
- Copies **one whale** (`WHALE` in `bot.py`); mirrors **opens and exits**.
- **Sizing:** 1/5 of equity margin per trade, **max 5** concurrent positions.
- **Leverage:** cross → `min(whale, 20x)`; isolated → exact, capped at **40x**.
- **Take-profit per asset class** (crypto / stocks), settable from the dashboard, **locked
  per position at open**. **No stop-loss.**
- **Daily −25% kill-switch** halts new trades for the day.
- New opens/closes are confirmed over **2 polls** each to ignore single API flickers.
- **Closed trades** (PnL + ROE%) are recorded and shown on the dashboard.

## Going live with real money — NOT done yet
Requires: `PAPER_MODE = False`, switch exec URL to Hyperliquid **mainnet**, and an
**agent/API wallet** (trade-only, cannot withdraw) in a `config.json`. The real-order
execution path is untested — **test on testnet first**, use tiny amounts. Aggressive params
(up to 40x, no stop-loss) mean liquidation on a small adverse move. Not financial advice.

## Gotchas / constraints
- The DigitalOcean **browser console mangles special characters** and SSH port 22 is blocked.
  Running Claude Code **directly on the server** avoids this entirely.
- Secrets are entered via the **dashboard's input fields** (paste works there), never typed
  into the console. **Never commit `secrets.json`.**
- Dashboard performance data fills in over time (a fresh bot has little history, so the
  1W/1M/All ranges look similar at first — expected).

## First step in a new Claude Code session
Ask it to read this file plus `bot.py` and `index.html`, or run `/init` to (re)generate
project notes. Then describe the change you want.
