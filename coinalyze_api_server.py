# coinalyze_api_server.py
# Minimal FastAPI that reads the latest snapshot JSONs written by coinalyze and returns a compact metrics object.

import os, glob, json, time
from pathlib import Path
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from typing import Optional

app = FastAPI(title="Coinalyze Local API")

DATA_DIR = os.getenv("DATA_DIR", "/data/coinalyze")  # should match your collector's DATA_DIR
DEFAULT_INTERVALS = [s.strip() for s in os.getenv("INTERVALS", "1min,5min,15min,1hour").split(",") if s.strip()]

def _latest_file_for(symbol: str, interval: str):
    p = Path(DATA_DIR) / symbol / interval
    if not p.exists():
        return None
    # files inside are per-day subdirs: /symbol/interval/YYYYMMDD/*.json  OR jsonl files
    day_dirs = sorted([d for d in p.iterdir() if d.is_dir()], reverse=True)
    # fallback: files directly in p
    candidates = []
    for d in day_dirs:
        candidates.extend(sorted(d.glob("*.json"), key=lambda x: x.stat().st_mtime, reverse=True))
        candidates.extend(sorted(d.glob("*.jsonl"), key=lambda x: x.stat().st_mtime, reverse=True))
        if candidates:
            return candidates[0]
    # fallback to any json under p
    candidates = sorted(p.glob("*.json"), key=lambda x: x.stat().st_mtime, reverse=True)
    return candidates[0] if candidates else None

def _read_json_file(path: Path):
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception:
        # if jsonl: read last non-empty line as JSON
        try:
            with open(path, "rb") as f:
                lines = f.read().splitlines()
                for ln in reversed(lines):
                    ln = ln.strip()
                    if not ln:
                        continue
                    return json.loads(ln.decode("utf-8"))
        except Exception:
            return None

def _normalize(snapshot: dict, symbol: str):
    # Map common keys from your files to expected keys
    out = {}
    out["symbol"] = symbol
    out["ts"] = snapshot.get("ts") or snapshot.get("timestamp") or int(time.time())
    out["oi_delta"] = snapshot.get("oi") or snapshot.get("oi_delta") or snapshot.get("open_interest_delta") or 0
    out["cvd_divergence"] = snapshot.get("cvd_div") or snapshot.get("cvd_divergence") or snapshot.get("divergence") or "none"
    out["net_long_short"] = snapshot.get("nl_ns") or snapshot.get("net_long_short") or snapshot.get("long_short_ratio") or 1.0
    out["funding"] = snapshot.get("funding") or snapshot.get("funding_rate") or 0.0
    # optional minimal OHLCV
    out["ohlcv"] = {}
    for k in ("o","h","l","c","v","open","high","low","close","volume"):
        if k in snapshot:
            out["ohlcv"] = {
                "o": snapshot.get("o") or snapshot.get("open"),
                "h": snapshot.get("h") or snapshot.get("high"),
                "l": snapshot.get("l") or snapshot.get("low"),
                "c": snapshot.get("c") or snapshot.get("close"),
                "v": snapshot.get("v") or snapshot.get("volume")
            }
            break
    # For multi-interval returns, the caller can request 'aggregate' below
    out["_raw"] = snapshot
    return out

@app.get("/v1/metrics")
def metrics(symbol: str = Query(..., description="Symbol e.g. BTCUSDT"), interval: Optional[str] = Query(None)):
    sym = symbol.upper()
    intervals = [interval] if interval else DEFAULT_INTERVALS
    result = {}
    found = False
    for inter in intervals:
        file_path = _latest_file_for(sym, inter)
        if not file_path:
            continue
        snap = _read_json_file(file_path)
        if not snap:
            continue
        normalized = _normalize(snap, sym)
        result[inter] = normalized
        found = True
    if not found:
        raise HTTPException(status_code=404, detail=f"No data for {sym} in {DATA_DIR}")
    # Compose a compact single snapshot merging preferred intervals (1m -> 5m -> 15m -> 1h)
    compact = {
        "symbol": sym,
        "ts": int(time.time()),
        "intervals": result,
        # convenience top-level keys for engine:
        "oi_delta": result.get("1min", result.get("5min", {})).get("oi_delta", 0),
        "cvd_divergence": result.get("1min", result.get("5min", {})).get("cvd_divergence", "none"),
        "net_long_short": result.get("1min", result.get("5min", {})).get("net_long_short", 1.0),
        "funding": result.get("1min", result.get("5min", {})).get("funding", 0.0),
        "ohlcv": {k:v for k,v in (result.get("1min") or {}).get("ohlcv", {}).items()}
    }
    return JSONResponse(compact)
