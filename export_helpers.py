import os, json, time, math, random
from datetime import datetime, timedelta, timezone
from pathlib import Path

def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)

def unix(dt: datetime) -> int:
    # Treat everything as UTC
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp())

def daterange_utc(start_date: str, end_date: str|None):
    start = datetime.fromisoformat(start_date).replace(tzinfo=timezone.utc)
    end = datetime.utcnow().replace(tzinfo=timezone.utc) if not end_date else \
          datetime.fromisoformat(end_date).replace(tzinfo=timezone.utc)
    cur = start
    one = timedelta(days=1)
    while cur <= end:
        yield cur
        cur += one

def write_jsonl(path: Path, rows):
    ensure_dir(path.parent)
    with open(path, "w", encoding="utf-8") as f:
        if isinstance(rows, list):
            for r in rows:
                f.write(json.dumps(r, separators=(",", ":"), ensure_ascii=False) + "\n")
        else:
            # If API returns dict, store as one line
            f.write(json.dumps(rows, separators=(",", ":"), ensure_ascii=False) + "\n")

def load_state(state_path: Path):
    if state_path.exists():
        try:
            return json.loads(state_path.read_text())
        except Exception:
            return {}
    return {}

def save_state(state_path: Path, state: dict):
    ensure_dir(state_path.parent)
    state_path.write_text(json.dumps(state, indent=2))

def jitter_sleep_ms(ms: int):
    delay = ms/1000.0 + random.uniform(0, ms/1000.0*0.3)
    time.sleep(delay)

def unwrap_history(resp):
    # Accepts: [ {symbol, history:[...]} ]  OR  {history:[...]}  OR  plain list
    if isinstance(resp, list):
        if resp and isinstance(resp[0], dict) and "history" in resp[0]:
            return resp[0].get("history") or []
        return resp
    if isinstance(resp, dict) and "history" in resp:
        return resp.get("history") or []
    return []
