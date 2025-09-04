import os, traceback, sys
from pathlib import Path
from datetime import datetime, timedelta, timezone

from export_helpers import (
    daterange_utc, unix, write_jsonl, load_state, save_state,
    ensure_dir, jitter_sleep_ms, unwrap_history
)

from coinalyze_api import (
    get_ohlcv_history, get_open_interest_history, get_funding_rate_history,
    get_predicted_funding_rate_history, get_long_short_ratio_history, get_liquidation_history
)

# ---------- ENV ----------
SYMBOLS   = [s.strip() for s in os.getenv("SYMBOLS","BTCUSDT_PERP.A").split(",") if s.strip()]
INTERVALS = [s.strip() for s in os.getenv("INTERVALS","1min").split(",") if s.strip()]
START_DATE = os.getenv("START_DATE","2024-01-01")
END_DATE   = os.getenv("END_DATE","")
OUT_ROOT   = Path(os.getenv("OUT_ROOT","/data/lake"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES","8"))
GLOBAL_DELAY_MS = int(os.getenv("SLEEP_BETWEEN_CALLS_MS","400"))

# ---- Per-endpoint pacing (override in Railway Variables as needed) ----
EP_DELAY_MS = {
    "ohlcv": int(os.getenv("DELAY_OHLCV_MS","600")),
    "oi":    int(os.getenv("DELAY_OI_MS","700")),
    "fr":    int(os.getenv("DELAY_FR_MS","2500")),
    "pfr":   int(os.getenv("DELAY_PFR_MS","7000")),  # heaviest
    "ls":    int(os.getenv("DELAY_LS_MS","2500")),
    "liq":   int(os.getenv("DELAY_LIQ_MS","1200")),
}

ENDPOINTS = {
    "ohlcv": get_ohlcv_history,
    "oi":    get_open_interest_history,
    "fr":    get_funding_rate_history,
    "pfr":   get_predicted_funding_rate_history,
    "ls":    get_long_short_ratio_history,
    "liq":   get_liquidation_history,
}

def export_day(symbol: str, interval: str, day: datetime, state: dict):
    day_str = day.strftime("%Y-%m-%d")
    day_dir = OUT_ROOT / symbol / interval / day_str
    ensure_dir(day_dir)

    # UTC 00:00 → 23:59:59
    t0 = unix(day.replace(hour=0, minute=0, second=0, microsecond=0))
    t1 = unix((day + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)) - 1

    for key, fn in ENDPOINTS.items():
        out_path = day_dir / f"{key}.jsonl"
        done_key = f"{day_str}:{key}"
        if out_path.exists() or state.get(done_key) == "ok":
            print(f"SKIP {symbol} {interval} {day_str} {key} (exists)")
            continue

        # Endpoint-specific pacing before request
        jitter_sleep_ms(EP_DELAY_MS.get(key, GLOBAL_DELAY_MS))

        tries = 0
        while True:
            tries += 1
            try:
                print(f"FETCH {symbol} {interval} {day_str} {key} (try {tries})")
                resp = fn(symbol, interval, t0, t1)
                rows = unwrap_history(resp)
                write_jsonl(out_path, rows)
                state[done_key] = "ok"
                break
            except Exception as e:
                print(f"ERROR {symbol} {interval} {day_str} {key}: {repr(e)}")
                if tries >= MAX_RETRIES:
                    state[done_key] = f"error:{repr(e)}"
                    break
                # after errors add a longer pause
                jitter_sleep_ms(max(EP_DELAY_MS.get(key, GLOBAL_DELAY_MS), 2000))

        save_state(OUT_ROOT / "_state" / f"{symbol}_{interval}.json", state)
        # small global gap
        jitter_sleep_ms(GLOBAL_DELAY_MS)

def main():
    ensure_dir(OUT_ROOT)
    rng_end = END_DATE if END_DATE else None
    for symbol in SYMBOLS:
        for interval in INTERVALS:
            state_path = OUT_ROOT / "_state" / f"{symbol}_{interval}.json"
            state = load_state(state_path)
            for day in daterange_utc(START_DATE, rng_end):
                export_day(symbol, interval, day, state)

if __name__ == "__main__":
    try:
        print("=== AlphaOps • Coinalyze Historical Export ===")
        print("Symbols:", SYMBOLS, "| Intervals:", INTERVALS)
        print("Range:", START_DATE, "→", (END_DATE or "today"))
        main()
        print("DONE.")
    except KeyboardInterrupt:
        print("Interrupted")
    except Exception:
        traceback.print_exc()
        sys.exit(1)
