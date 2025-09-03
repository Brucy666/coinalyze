import os, sys, traceback
from pathlib import Path
from datetime import datetime, timedelta, timezone

from export_helpers import (
    daterange_utc, unix, write_jsonl, load_state, save_state,
    ensure_dir, jitter_sleep_ms, unwrap_history
)

# Reuse your hardened client
from coinalyze_api import (
    get_ohlcv_history, get_open_interest_history, get_funding_rate_history,
    get_predicted_funding_rate_history, get_long_short_ratio_history, get_liquidation_history
)

# --------- ENV ---------
SYMBOLS = [s.strip() for s in os.getenv("SYMBOLS","BTCUSDT_PERP.A").split(",") if s.strip()]
INTERVALS = [s.strip() for s in os.getenv("INTERVALS","1min").split(",") if s.strip()]
START_DATE = os.getenv("START_DATE","2024-01-01")
END_DATE   = os.getenv("END_DATE","")
OUT_ROOT   = Path(os.getenv("OUT_ROOT","/data/lake"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES","6"))
SLEEP_BETWEEN_CALLS_MS = int(os.getenv("SLEEP_BETWEEN_CALLS_MS","250"))

# Which endpoints to export
ENDPOINTS = {
    "ohlcv": get_ohlcv_history,
    "oi":    get_open_interest_history,
    "fr":    get_funding_rate_history,
    "pfr":   get_predicted_funding_rate_history,
    "ls":    get_long_short_ratio_history,
    "liq":   get_liquidation_history
}

def dt_utc(y,m,d,h=0,mi=0,sec=0):
    return datetime(y,m,d,h,mi,sec,tzinfo=timezone.utc)

def export_day(symbol: str, interval: str, day: datetime, state: dict):
    day_str = day.strftime("%Y-%m-%d")
    day_dir = OUT_ROOT / symbol / interval / day_str
    ensure_dir(day_dir)

    # Day window: [00:00, 24:00) UTC
    t0 = unix(day.replace(hour=0,minute=0,second=0,microsecond=0))
    t1 = unix((day + timedelta(days=1)).replace(hour=0,minute=0,second=0,microsecond=0)) - 1

    for key, fn in ENDPOINTS.items():
        out_path = day_dir / f"{key}.jsonl"
        done_key = f"{day_str}:{key}"
        if out_path.exists() or state.get(done_key) == "ok":
            print(f"SKIP {symbol} {interval} {day_str} {key} (exists)")
            continue

        # Pull with retries
        tries = 0
        while True:
            tries += 1
            try:
                print(f"FETCH {symbol} {interval} {day_str} {key} (try {tries})")
                if key == "ohlcv":
                    resp = fn(symbol, interval, t0, t1)         # returns list/dict
                    rows = unwrap_history(resp)
                else:
                    resp = fn(symbol, interval, t0, t1)
                    rows = unwrap_history(resp)
                # Basic integrity: write even if empty (so we don't refetch)
                write_jsonl(out_path, rows)
                state[done_key] = "ok"
                break
            except Exception as e:
                print(f"ERROR {symbol} {interval} {day_str} {key}: {repr(e)}")
                if tries >= MAX_RETRIES:
                    state[done_key] = f"error:{repr(e)}"
                    break
                jitter_sleep_ms(1200)
        save_state(OUT_ROOT / "_state" / f"{symbol}_{interval}.json", state)
        jitter_sleep_ms(SLEEP_BETWEEN_CALLS_MS)

def main():
    ensure_dir(OUT_ROOT)
    for symbol in SYMBOLS:
        for interval in INTERVALS:
            state_path = OUT_ROOT / "_state" / f"{symbol}_{interval}.json"
            state = load_state(state_path)
            for day in daterange_utc(START_DATE, END_DATE if END_DATE else None):
                export_day(symbol, interval, day, state)

if __name__ == "__main__":
    try:
        print("=== AlphaOps • Historical Export ===")
        print("Symbols:", SYMBOLS, "| Intervals:", INTERVALS)
        print("Range:", START_DATE, "→", (END_DATE or "today"))
        main()
        print("DONE.")
    except KeyboardInterrupt:
        print("Interrupted.")
    except Exception:
        traceback.print_exc()
        sys.exit(1)
