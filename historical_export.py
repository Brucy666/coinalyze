import os, json, time, random, signal, traceback
from pathlib import Path
from datetime import datetime, timedelta, timezone
from dateutil import parser as dtparse

from coinalyze_api import (
    get_ohlcv_history,
    get_open_interest_history,
    get_funding_rate_history,
    get_predicted_funding_rate_history,
    get_long_short_ratio_history,
    get_liquidation_history,
)

# ============ ENV ============
SYMBOLS    = [s.strip() for s in os.getenv("SYMBOLS", "BTCUSDT_PERP.A").split(",") if s.strip()]
INTERVALS  = [s.strip() for s in os.getenv("INTERVALS", "1min").split(",") if s.strip()]
START_DATE = os.getenv("START_DATE", "2023-01-01")
END_DATE   = os.getenv("END_DATE", "")  # empty => today UTC
OUT_ROOT   = Path(os.getenv("OUT_ROOT", "/data/lake"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "8"))
GLOBAL_DELAY_MS = int(os.getenv("SLEEP_BETWEEN_CALLS_MS", "400"))

# Per-endpoint pacing (override via Railway variables if needed)
EP_DELAY_MS = {
    "ohlcv": int(os.getenv("DELAY_OHLCV_MS", "600")),
    "oi":    int(os.getenv("DELAY_OI_MS",    "700")),
    "fr":    int(os.getenv("DELAY_FR_MS",    "3000")),
    "pfr":   int(os.getenv("DELAY_PFR_MS",   "7000")),  # <- heaviest
    "ls":    int(os.getenv("DELAY_LS_MS",    "3000")),
    "liq":   int(os.getenv("DELAY_LIQ_MS",   "1200")),
}

ENDPOINTS = {
    "ohlcv": get_ohlcv_history,
    "oi":    get_open_interest_history,
    "fr":    get_funding_rate_history,
    "pfr":   get_predicted_funding_rate_history,
    "ls":    get_long_short_ratio_history,
    "liq":   get_liquidation_history,
}

# ============ UTIL ============
shutdown = False
def _sigterm(*_):
    global shutdown; shutdown = True
signal.signal(signal.SIGINT, _sigterm)
signal.signal(signal.SIGTERM, _sigterm)

def ensure_dir(p: Path): p.mkdir(parents=True, exist_ok=True)

def jitter_sleep_ms(ms: int):
    delay = (ms/1000.0) + random.uniform(0, (ms/1000.0)*0.3)
    if delay > 0:
        time.sleep(delay)

def unix_utc(dt: datetime) -> int:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp())

def parse_date_utc(s: str) -> datetime:
    if not s:
        return datetime.utcnow().replace(tzinfo=timezone.utc)
    d = dtparse.parse(s)
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)
    else:
        d = d.astimezone(timezone.utc)
    # strip sub-second noise
    return d.replace(microsecond=0)

def day_range_utc(start: datetime, end: datetime):
    cur = start.replace(hour=0, minute=0, second=0, microsecond=0)
    end = end.replace(hour=0, minute=0, second=0, microsecond=0)
    one = timedelta(days=1)
    while cur <= end and not shutdown:
        yield cur
        cur += one

def write_jsonl(path: Path, rows):
    ensure_dir(path.parent)
    with open(path, "w", encoding="utf-8") as f:
        if isinstance(rows, list):
            for r in rows:
                f.write(json.dumps(r, separators=(",",":"), ensure_ascii=False) + "\n")
        else:
            f.write(json.dumps(rows, separators=(",",":"), ensure_ascii=False) + "\n")

def load_state(state_path: Path) -> dict:
    if state_path.exists():
        try: return json.loads(state_path.read_text())
        except Exception: return {}
    return {}

def save_state(state_path: Path, state: dict):
    ensure_dir(state_path.parent)
    state_path.write_text(json.dumps(state, indent=2))

def unwrap_history(resp):
    # Accepts: [ {symbol, history:[...]} ]  OR  {history:[...]}  OR  plain list
    if isinstance(resp, list):
        if resp and isinstance(resp[0], dict) and "history" in resp[0]:
            return resp[0].get("history") or []
        return resp
    if isinstance(resp, dict) and "history" in resp:
        return resp.get("history") or []
    return []

# ============ CORE ============
def export_day(symbol: str, interval: str, day_utc: datetime, state: dict, log_every:int=10):
    day_str = day_utc.strftime("%Y-%m-%d")
    day_dir = OUT_ROOT / symbol / interval / day_str
    ensure_dir(day_dir)

    # UTC day window
    t0 = unix_utc(day_utc.replace(hour=0, minute=0, second=0))
    t1 = unix_utc((day_utc + timedelta(days=1)).replace(hour=0, minute=0, second=0)) - 1

    for key, fn in ENDPOINTS.items():
        if shutdown: break
        out_path = day_dir / f"{key}.jsonl"
        done_key = f"{day_str}:{key}"
        if out_path.exists() or state.get(done_key) == "ok":
            # throttle skip logs heavily
            if random.random() < 0.005:
                print(f"SKIP {symbol} {interval} {day_str} {key}")
            continue

        # pace by endpoint
        jitter_sleep_ms(EP_DELAY_MS.get(key, GLOBAL_DELAY_MS))

        tries = 0
        while not shutdown:
            tries += 1
            try:
                if tries == 1:
                    # throttle fetch logs: first try only
                    print(f"FETCH {symbol} {interval} {day_str} {key}")
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
                # longer wait after errors
                jitter_sleep_ms(max(EP_DELAY_MS.get(key, GLOBAL_DELAY_MS), 2000))

        # small global gap & persist state
        jitter_sleep_ms(GLOBAL_DELAY_MS)
        save_state(OUT_ROOT / "_state" / f"{symbol}_{interval}.json", state)

def main():
    start_dt = parse_date_utc(START_DATE)
    end_dt   = parse_date_utc(END_DATE) if END_DATE else datetime.utcnow().replace(tzinfo=timezone.utc)

    print("=== AlphaOps • Coinalyze Historical Export ===")
    print("Symbols:", SYMBOLS, "| Intervals:", INTERVALS)
    print("Range:", start_dt.strftime("%Y-%m-%d"), "→", end_dt.strftime("%Y-%m-%d"))
    print("Pacing(ms):", EP_DELAY_MS, "| Global:", GLOBAL_DELAY_MS)
    print("Max retries:", MAX_RETRIES)
    print("Press Ctrl+C to stop\n")

    for symbol in SYMBOLS:
        for interval in INTERVALS:
            if shutdown: break
            state_path = OUT_ROOT / "_state" / f"{symbol}_{interval}.json"
            state = load_state(state_path)

            total_days = (end_dt.date() - start_dt.date()).days + 1
            processed = 0
            for day_utc in day_range_utc(start_dt, end_dt):
                if shutdown: break
                export_day(symbol, interval, day_utc, state)
                processed += 1
                # progress log every 10 days
                if processed % 10 == 0 or processed == total_days:
                    print(f"[{symbol} {interval}] {processed}/{total_days} days saved (up to {day_utc:%Y-%m-%d})")

    print("DONE.")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Interrupted")
    except Exception:
        traceback.print_exc()
        raise
