# Kite Data Automation (Zerodha Kite Connect)

A Python pipeline that pulls historical OHLC candles for a watchlist of NSE stocks using the Zerodha Kite Connect API, stores them in SQLite and exports clean CSVs. You can schedule it to run daily.

## Features
- **Daily login handling**: saves the Kite access token and reuses it for the day.
- **Chunked history download**: splits long date ranges to respect Kite's per-request limits.
- **Retry with backoff** for network errors and rate limits.
- **Data validation**: drops impossible candles (for example, high below low) before saving.
- **No duplicates**: re-running only adds new candles (`INSERT OR IGNORE` on a primary key).
- **CSV export** per symbol and a **summary report** (last close, period return, average volume).
- **Logging** to `logs/pipeline.log`.
- **Demo mode**: runs end-to-end with generated data, so you can test it without a Zerodha account.

## Quick start
```bash
pip install -r requirements.txt
python kite_pipeline.py --demo          # try it now, no API key needed
```

## Live use
1. Create an app at https://developers.kite.trade to get your API key and secret.
2. `cp config.example.json config.json` and fill in your key, secret and watchlist.
3. `python kite_pipeline.py --login` (once per day; Kite tokens expire daily).
4. `python kite_pipeline.py` or `python kite_pipeline.py --days 30 --interval 15minute`

## Sample output (demo mode)
```
Symbol        Last close  Return %    Avg volume
HDFCBANK         1516.97     20.10     2,528,068
INFY             1885.98    -16.24     2,667,472
```
*Demo numbers are randomly generated and are not real market prices.*

## Schedule it (Linux/Mac cron, 4 PM on weekdays)
```
0 16 * * 1-5 cd /path/to/kite-data-automation && python kite_pipeline.py
```
On Windows, use Task Scheduler with the same command.

## Tech
Python 3, kiteconnect SDK, SQLite, CSV, logging, argparse.

## Possible extensions
PostgreSQL storage, Telegram alerts on price moves, live quotes through the WebSocket ticker, and a dashboard.
