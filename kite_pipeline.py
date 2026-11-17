"""
kite_pipeline.py - Zerodha Kite Connect data automation

Pulls historical OHLC candles for a watchlist of NSE stocks, stores them in
SQLite (no duplicates on re-runs) and exports a CSV per symbol. Every run is
logged to logs/pipeline.log.

Usage
  python kite_pipeline.py --login            # one-time daily login, saves access token
  python kite_pipeline.py                    # fetch data for watchlist in config.json
  python kite_pipeline.py --demo             # run end-to-end with generated data (no Zerodha account needed)
  python kite_pipeline.py --days 30 --interval 15minute
"""
import argparse
import csv
import json
import logging
import math
import os
import random
import sqlite3
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

BASE = Path(__file__).resolve().parent
CONFIG_FILE = BASE / "config.json"
TOKEN_FILE = BASE / ".access_token.json"
DB_FILE = BASE / "market_data.db"
EXPORT_DIR = BASE / "exports"
LOG_DIR = BASE / "logs"

VALID_INTERVALS = {"minute", "3minute", "5minute", "15minute", "30minute", "60minute", "day"}
# Kite limits how many days of data one request can return, per interval
MAX_DAYS_PER_CALL = {"minute": 60, "3minute": 100, "5minute": 100, "15minute": 200,
                     "30minute": 200, "60minute": 400, "day": 2000}

log = logging.getLogger("kite_pipeline")


def setup_logging():
    LOG_DIR.mkdir(exist_ok=True)
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    fh = logging.FileHandler(LOG_DIR / "pipeline.log", encoding="utf-8")
    fh.setFormatter(fmt)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    log.setLevel(logging.INFO)
    log.handlers = [fh, sh]


def load_config():
    if not CONFIG_FILE.exists():
        sys.exit("config.json not found. Copy config.example.json to config.json and fill it in.")
    with open(CONFIG_FILE, encoding="utf-8") as f:
        return json.load(f)


# ---------- Authentication ----------
def get_kite(cfg):
    try:
        from kiteconnect import KiteConnect
    except ImportError:
        sys.exit("Install the SDK first: pip install kiteconnect")
    kite = KiteConnect(api_key=cfg["api_key"])
    if TOKEN_FILE.exists():
        saved = json.loads(TOKEN_FILE.read_text())
        if saved.get("date") == date.today().isoformat():   # Kite tokens expire daily
            kite.set_access_token(saved["access_token"])
            return kite
    sys.exit("No valid access token for today. Run: python kite_pipeline.py --login")


def login(cfg):
    from kiteconnect import KiteConnect
    kite = KiteConnect(api_key=cfg["api_key"])
    print("1. Open this URL and log in:\n  ", kite.login_url())
    print("2. After login you are redirected. Copy the 'request_token' value from that URL.")
    request_token = input("Paste request_token: ").strip()
    session = kite.generate_session(request_token, api_secret=cfg["api_secret"])
    TOKEN_FILE.write_text(json.dumps({"access_token": session["access_token"],
                                      "date": date.today().isoformat()}))
    log.info("Login successful, token saved for %s", date.today())


# ---------- Fetching ----------
def instrument_tokens(kite, symbols, exchange="NSE"):
    """Map trading symbols (e.g. RELIANCE) to Kite instrument tokens."""
    lookup = {i["tradingsymbol"]: i["instrument_token"] for i in kite.instruments(exchange)}
    missing = [s for s in symbols if s not in lookup]
    if missing:
        log.warning("Symbols not found on %s: %s", exchange, ", ".join(missing))
    return {s: lookup[s] for s in symbols if s in lookup}


def with_retry(fn, *args, attempts=3, **kwargs):
    for n in range(1, attempts + 1):
        try:
            return fn(*args, **kwargs)
        except Exception as e:  # network blips, rate limits (3 req/sec on historical API)
            if n == attempts:
                raise
            wait = 2 ** n
            log.warning("Attempt %d failed (%s). Retrying in %ds", n, e, wait)
            time.sleep(wait)


def fetch_history(kite, token, start, end, interval):
    """Fetch candles in chunks so long date ranges stay within Kite's per-call limit."""
    chunk = timedelta(days=MAX_DAYS_PER_CALL[interval])
    candles, cur = [], start
    while cur <= end:
        stop = min(cur + chunk, end)
        candles += with_retry(kite.historical_data, token, cur, stop, interval)
        cur = stop + timedelta(days=1)
        time.sleep(0.35)  # stay under the rate limit
    return candles


def demo_history(symbol, start, end, interval):
    """Generate realistic-looking random-walk candles so the pipeline can be tested offline."""
    rnd = random.Random(symbol)
    price = rnd.uniform(200, 3000)
    rows, d = [], start
    while d <= end:
        if d.weekday() < 5:  # trading days only
            o = price
            c = o * (1 + rnd.gauss(0, 0.015))
            h = max(o, c) * (1 + abs(rnd.gauss(0, 0.006)))
            l = min(o, c) * (1 - abs(rnd.gauss(0, 0.006)))
            rows.append({"date": datetime.combine(d, datetime.min.time()).replace(hour=9, minute=15),
                         "open": round(o, 2), "high": round(h, 2), "low": round(l, 2),
                         "close": round(c, 2), "volume": int(rnd.uniform(2e5, 5e6))})
            price = c
        d += timedelta(days=1)
    return rows


# ---------- Storage ----------
def init_db(conn):
    conn.execute("""CREATE TABLE IF NOT EXISTS candles (
        symbol TEXT NOT NULL, interval TEXT NOT NULL, ts TEXT NOT NULL,
        open REAL, high REAL, low REAL, close REAL, volume INTEGER,
        PRIMARY KEY (symbol, interval, ts))""")


def validate(c):
    """Drop bad candles instead of storing garbage."""
    return (c["low"] <= min(c["open"], c["close"]) and c["high"] >= max(c["open"], c["close"])
            and c["low"] > 0 and c["volume"] >= 0)


def save(conn, symbol, interval, candles):
    good = [c for c in candles if validate(c)]
    bad = len(candles) - len(good)
    if bad:
        log.warning("%s: skipped %d invalid candles", symbol, bad)
    before = conn.total_changes
    conn.executemany(
        "INSERT OR IGNORE INTO candles VALUES (?,?,?,?,?,?,?,?)",
        [(symbol, interval, str(c["date"]), c["open"], c["high"], c["low"], c["close"], c["volume"])
         for c in good])
    conn.commit()
    return conn.total_changes - before


def export_csv(conn, symbol, interval):
    EXPORT_DIR.mkdir(exist_ok=True)
    path = EXPORT_DIR / f"{symbol}_{interval}.csv"
    rows = conn.execute("SELECT ts, open, high, low, close, volume FROM candles "
                        "WHERE symbol=? AND interval=? ORDER BY ts", (symbol, interval)).fetchall()
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["timestamp", "open", "high", "low", "close", "volume"])
        w.writerows(rows)
    return path, len(rows)


def summary(conn, interval):
    """Print a quick per-symbol report: last close, period return, avg volume."""
    print(f"\n{'Symbol':<12}{'Last close':>12}{'Return %':>10}{'Avg volume':>14}")
    for sym, in conn.execute("SELECT DISTINCT symbol FROM candles WHERE interval=? ORDER BY symbol", (interval,)):
        first, last, avgv = conn.execute(
            "SELECT (SELECT close FROM candles WHERE symbol=? AND interval=? ORDER BY ts LIMIT 1),"
            "       (SELECT close FROM candles WHERE symbol=? AND interval=? ORDER BY ts DESC LIMIT 1),"
            "       AVG(volume) FROM candles WHERE symbol=? AND interval=?",
            (sym, interval) * 3).fetchone()
        print(f"{sym:<12}{last:>12.2f}{(last / first - 1) * 100:>10.2f}{avgv:>14,.0f}")


# ---------- Main ----------
def main():
    p = argparse.ArgumentParser(description="Zerodha Kite Connect data pipeline")
    p.add_argument("--login", action="store_true", help="generate today's access token")
    p.add_argument("--demo", action="store_true", help="use generated data, no API calls")
    p.add_argument("--days", type=int, help="how many days of history (overrides config)")
    p.add_argument("--interval", help="candle interval (overrides config)")
    args = p.parse_args()
    setup_logging()

    cfg = {"watchlist": ["RELIANCE", "TCS", "INFY", "HDFCBANK", "ITC"],
           "interval": "day", "days": 90} if args.demo else load_config()
    if args.login:
        return login(cfg)

    interval = args.interval or cfg.get("interval", "day")
    days = args.days or cfg.get("days", 90)
    if interval not in VALID_INTERVALS:
        sys.exit(f"Invalid interval '{interval}'. Use one of: {', '.join(sorted(VALID_INTERVALS))}")
    end = date.today()
    start = end - timedelta(days=days)
    log.info("Run started | mode=%s | %d symbols | %s to %s | %s",
             "DEMO" if args.demo else "LIVE", len(cfg["watchlist"]), start, end, interval)

    if args.demo:
        tokens = {s: None for s in cfg["watchlist"]}
    else:
        kite = get_kite(cfg)
        tokens = instrument_tokens(kite, cfg["watchlist"], cfg.get("exchange", "NSE"))

    conn = sqlite3.connect(DB_FILE)
    init_db(conn)
    failed = []
    for sym, tok in tokens.items():
        try:
            candles = (demo_history(sym, start, end, interval) if args.demo
                       else fetch_history(kite, tok, start, end, interval))
            added = save(conn, sym, interval, candles)
            path, total = export_csv(conn, sym, interval)
            log.info("%s: %d candles fetched, %d new, %d total -> %s",
                     sym, len(candles), added, total, path.name)
        except Exception as e:
            failed.append(sym)
            log.error("%s failed: %s", sym, e)

    summary(conn, interval)
    conn.close()
    log.info("Run finished | %d ok | %d failed %s", len(tokens) - len(failed), len(failed), failed or "")


if __name__ == "__main__":
    main()
