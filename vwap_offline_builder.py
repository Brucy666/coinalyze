import os, json, csv
from pathlib import Path
from datetime import datetime

OUT_ROOT = Path(os.getenv("OUT_ROOT","/data/lake"))
AN_OUT   = Path(os.getenv("AN_OUT","/data/analytics"))

SYMBOL   = os.getenv("SYMBOL","BTCUSDT_PERP.A")
INTERVAL = os.getenv("INTERVAL","1min")  # For daily VWAP we use 1min

def read_jsonl(path: Path):
    rows = []
    if not path.exists(): return rows
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line=line.strip()
            if not line: continue
            try:
                rows.append(json.loads(line))
            except Exception:
                pass
    return rows

def day_dirs(symbol: str, interval: str):
    root = OUT_ROOT / symbol / interval
    if not root.exists(): return []
    return sorted([p.name for p in root.iterdir() if p.is_dir()])

def vwap_of_day(ohlcv_rows):
    # Expect rows with 'open','high','low','close','volume' and maybe 'v','bv'
    # Tolerate keys: o/h/l/c/v OR open/high/low/close/volume
    cum_pv, cum_v = 0.0, 0.0
    for r in ohlcv_rows:
        p = r.get("close", r.get("c"))
        v = r.get("volume", r.get("v"))
        if p is None or v is None: continue
        p = float(p); v = float(v)
        cum_pv += p * v
        cum_v  += v
    return (cum_pv / cum_v) if cum_v > 0 else None

def touched_today(level: float, ohlcv_rows) -> bool:
    for r in ohlcv_rows:
        hi = float(r.get("high", r.get("h")))
        lo = float(r.get("low",  r.get("l")))
        if lo <= level <= hi:
            return True
    return False

def first_touch_reaction(level: float, ohlcv_rows, lookahead=120):
    # returns max excursion in bps away from level after the first touch within lookahead bars
    first_idx = None
    for i, r in enumerate(ohlcv_rows):
        hi = float(r.get("high", r.get("h")))
        lo = float(r.get("low",  r.get("l")))
        if lo <= level <= hi:
            first_idx = i
            break
    if first_idx is None:
        return None, None
    mx, mn = -1e9, 1e9
    end = min(len(ohlcv_rows), first_idx + lookahead)
    for j in range(first_idx, end):
        c = float(ohlcv_rows[j].get("close", ohlcv_rows[j].get("c")))
        mx = max(mx, c)
        mn = min(mn, c)
    up_bps   = (mx/level - 1.0) * 10000.0
    down_bps = (1.0 - mn/level) * 10000.0
    return up_bps, down_bps

def main():
    days = day_dirs(SYMBOL, INTERVAL)
    if not days:
        print("No data found for", SYMBOL, INTERVAL)
        return

    # Compute end-of-day VWAP per day
    daily_levels = []
    for d in days:
        rows = read_jsonl(OUT_ROOT / SYMBOL / INTERVAL / d / "ohlcv.jsonl")
        level = vwap_of_day(rows)
        if level is not None:
            daily_levels.append((d, level))

    # Build naked ledger by checking next-day touches
    out_dir = AN_OUT / SYMBOL
    out_dir.mkdir(parents=True, exist_ok=True)
    out_csv = out_dir / "daily_naked_vwap_ledger.csv"

    with open(out_csv, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["day","eod_vwap","touched_next_day","first_touch_day","up_bps","down_bps"])
        for i,(d,level) in enumerate(daily_levels):
            touched = "N"
            ft_day, up_bps, down_bps = "", "", ""
            # Check next day for touch
            if i+1 < len(daily_levels):
                next_day = daily_levels[i+1][0]
                nxt_rows = read_jsonl(OUT_ROOT / SYMBOL / INTERVAL / next_day / "ohlcv.jsonl")
                if touched_today(level, nxt_rows):
                    touched = "Y"
                    ft_day = next_day
                    up_bps, down_bps = first_touch_reaction(level, nxt_rows, lookahead=240)
                    up_bps   = "" if up_bps   is None else round(up_bps, 1)
                    down_bps = "" if down_bps is None else round(down_bps, 1)
            w.writerow([d, round(level,2), touched, ft_day, up_bps, down_bps])

    print("Wrote", out_csv)

if __name__ == "__main__":
    main()
