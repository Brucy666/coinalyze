import os, json, time, gzip, shutil
from pathlib import Path

DATA_DIR     = os.getenv("DATA_DIR", "/data/coinalyze")
USE_JSONL    = os.getenv("ENABLE_JSONL", "true").lower() == "true"
GZIP_JSONL   = os.getenv("GZIP_JSONL", "true").lower() == "true"
RETAIN_DAYS  = int(os.getenv("RETAIN_DAYS", "7"))

def _ts():
    return int(time.time())

def _day_dir(symbol, interval, ts=None):
    t = ts or _ts()
    day = time.strftime("%Y%m%d", time.gmtime(t))
    p = Path(DATA_DIR) / symbol / interval / day
    p.mkdir(parents=True, exist_ok=True)
    return p

def write_snapshot(symbol, interval, pack):
    """Write a timestamped JSON snapshot file."""
    pdir = _day_dir(symbol, interval, pack.get("fetched_at"))
    fname = f"{pack['fetched_at']}.json"
    fpath = pdir / fname
    with open(fpath, "w") as f:
        json.dump(pack, f, separators=(",", ":"), ensure_ascii=False)
    return str(fpath)

def append_jsonl(symbol, interval, pack):
    """Append one line to rolling JSONL (optionally gzipped)."""
    pdir = _day_dir(symbol, interval, pack.get("fetched_at"))
    base = pdir / "stream.jsonl"
    path = str(base)
    if GZIP_JSONL:
        # Write to temp then append gz
        tmp = str(base) + ".tmp"
        with open(tmp, "a") as f:
            f.write(json.dumps(pack, separators=(",", ":"), ensure_ascii=False) + "\n")
        with open(tmp, "rb") as fin, gzip.open(str(base) + ".gz", "ab") as fout:
            shutil.copyfileobj(fin, fout)
        os.remove(tmp)
        return str(base) + ".gz"
    else:
        with open(base, "a") as f:
            f.write(json.dumps(pack, separators=(",", ":"), ensure_ascii=False) + "\n")
        return path

def retention_cleanup():
    """Delete day folders older than RETAIN_DAYS."""
    root = Path(DATA_DIR)
    if not root.exists():
        return
    cutoff = _ts() - RETAIN_DAYS * 86400
    for symbol_dir in root.iterdir():
        if not symbol_dir.is_dir(): 
            continue
        for interval_dir in symbol_dir.iterdir():
            if not interval_dir.is_dir():
                continue
            for day_dir in interval_dir.iterdir():
                try:
                    t = time.mktime(time.strptime(day_dir.name, "%Y%m%d"))
                    if t < cutoff:
                        shutil.rmtree(day_dir, ignore_errors=True)
                except Exception:
                    continue
