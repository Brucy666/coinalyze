# historical_export.py
import os, time, json, random, signal
from datetime import datetime, timedelta
from coinalyze_api import (
    get_open_interest_history,
    get_funding_rate_history,
    get_predicted_funding_rate_history,
    get_liquidation_history,
    get_long_short_ratio_history,
    get_ohlcv_history,
)
from data_sink import append_jsonl

SYMBOL     = os.getenv("SYMBOL", "BTCUSDT_PERP.A")
INTERVAL   = os.getenv("INTERVAL", "1min")
START_DATE = os.getenv("START_DATE", "2023-01-01")  # format: YYYY-MM-DD
END_DATE   = os.getenv("END_DATE", datetime.utcnow().strftime("%Y-%m-%d"))
SLEEP_SEC  = int(os.getenv("SLEEP_SECONDS", "1"))

shutdown = False
def _sigterm(*_): 
    global shutdown
    shutdown = True
signal.signal(signal.SIGINT, _sigterm); signal.signal(signal.SIGTERM, _sigterm)

def daterange(start_date, end_date):
    cur = start_date
    while cur <= end_date:
        yield cur
        cur += timedelta(days=1)

def fetch_day(symbol, interval, day_str):
    # convert day_str → timestamps
    dt = datetime.strptime(day_str, "%Y-%m-%d")
    start_ts = int(dt.timestamp())
    end_ts   = int((dt + timedelta(days=1)).timestamp())

    data = {}
    errors = {}

    try:
        data["oi"]   = get_open_interest_history(symbol, interval, start_ts, end_ts)
        data["fr"]   = get_funding_rate_history(symbol, interval, start_ts, end_ts)
        data["pfr"]  = get_predicted_funding_rate_history(symbol, interval, start_ts, end_ts)
        data["liq"]  = get_liquidation_history(symbol, interval, start_ts, end_ts)
        data["ls"]   = get_long_short_ratio_history(symbol, interval, start_ts, end_ts)
        data["ohlcv"]= get_ohlcv_history(symbol, interval, start_ts, end_ts)
    except Exception as e:
        errors["fetch_error"] = str(e)

    return {"symbol": symbol, "interval": interval, "day": day_str,
            "data": data, "errors": errors, "fetched_at": int(time.time())}

def main():
    start_date = datetime.strptime(START_DATE, "%Y-%m-%d")
    end_date   = datetime.strptime(END_DATE, "%Y-%m-%d")

    print(f"=== Historical Export ===")
    print(f"Symbol={SYMBOL} | Interval={INTERVAL} | From {START_DATE} to {END_DATE}")
    print("Press Ctrl+C to stop\n")

    total_days = (end_date - start_date).days + 1
    counter = 0

    for dt in daterange(start_date, end_date):
        if shutdown: break
        day_str = dt.strftime("%Y-%m-%d")
        counter += 1

        pack = fetch_day(SYMBOL, INTERVAL, day_str)
        append_jsonl(SYMBOL, INTERVAL, pack)

        # ✅ Throttled logging
        if counter % 10 == 0 or counter == total_days:
            print(f"[{counter}/{total_days}] {day_str} saved")

        time.sleep(SLEEP_SEC + random.uniform(0, 0.3*SLEEP_SEC))

    print("Export finished.")

if __name__ == "__main__":
    main()
