# coinalyze_api_server.py
# Read-only FastAPI to serve CoinAnalyzer snapshots
# Works with flat JSON (TF/OI/FR/CVD/Ls) written by your collector.

import os, time, glob, json
from pathlib import Path
from typing import Any, Dict, Optional, Tuple, List

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

# ---------------- ENV ----------------
DATA_DIR = os.getenv("DATA_DIR", "/data/coinalyze")
FILE_GLOB = os.getenv("FILE_GLOB", "*.json")
SCAN_LIMIT = int(os.getenv("SCAN_LIMIT", "500"))
CACHE_TTL = int(os.getenv("CACHE_TTL_SEC", "5"))
DEFAULT_SYMBOL = os.getenv("DEFAULT_SYMBOL", "BTCUSDT").upper()

# ---------------- APP ----------------
app = FastAPI(title="CoinAnalyzer API", version="1.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["GET"], allow_headers=["*"])

_cache: Dict[str, Tuple[float, Dict[str, Any]]] = {}

def _cache_get(k: str) -> Optional[Dict[str, Any]]:
    hit = _cache.get(k)
    if not hit: return None
    exp, payload = hit
    if time.time() > exp:
        _cache.pop(k, None)
        return None
    return payload

def _cache_set(k: str, payload: Dict[str, Any], ttl: int = CACHE_TTL):
    _cache[k] = (time.time() + ttl, payload)

# ---------------- HELPERS ----------------
def _read_json(path: Path) -> Optional[Dict[str, Any]]:
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception:
        return None

def _scan_latest(data_dir: str, pattern: str, limit: int) -> List[Path]:
    pat = str(Path(data_dir) / pattern)
    files = sorted(glob.glob(pat), key=os.path.getmtime, reverse=True)
    return [Path(p) for p in files[:max(1, limit)]]

def _norm_interval(raw: Any) -> str:
    if not raw: return "unknown"
    s = str(raw).lower()
    if s in ("1min","1m"): return "1m"
    if s in ("3min","3m"): return "3m"
    if s in ("5min","5m"): return "5m"
    if s in ("15min","15m"): return "15m"
    if s in ("30min","30m"): return "30m"
    if s in ("1h","1hour","60m"): return "1h"
    return s

def _extract_core(j: Dict[str, Any]) -> Dict[str, Any]:
    # Map flat keys (TF/OI/FR/CVD/Ls)
    interval = _norm_interval(j.get("TF") or j.get("tf") or "")
    ts = int(j.get("ts") or j.get("timestamp") or time.time())
    oi = float(j.get("OI") or 0.0)
    fund = float(j.get("FR") or 0.0)
    cvd_val = float(j.get("CVD") or 0.0)
    ls_ratio = float(j.get("Ls") or 1.0)

    cvd_div = "bullish" if cvd_val > 0 else ("bearish" if cvd_val < 0 else "none")

    return {
        "symbol": DEFAULT_SYMBOL,
        "interval": interval,
        "ts": ts,
        "oi_delta": oi,
        "cvd_divergence": cvd_div,
        "net_long_short": ls_ratio,
        "funding": fund,
        "ohlcv": None,
        "_raw": j
    }

def _pick_latest() -> Dict[str, Any]:
    for p in _scan_latest(DATA_DIR, FILE_GLOB, SCAN_LIMIT):
        j = _read_json(p)
        if not j: continue
        return _extract_core(j)
    raise HTTPException(status_code=404, detail=f"No data found in {DATA_DIR}")

# ---------------- ROUTES ----------------
@app.get("/healthz")
def healthz():
    return {"status": "ok" if Path(DATA_DIR).exists() else "missing_data_dir", "dir": DATA_DIR}

@app.get("/v1/metrics")
def metrics(symbol: str = Query(..., description="Symbol, but will return DEFAULT_SYMBOL set in env")):
    key = "latest"
    hit = _cache_get(key)
    if hit: return JSONResponse(hit)

    core = _pick_latest()
    _cache_set(key, core)
    return JSONResponse(core)

@app.get("/v1/metrics/debug")
def metrics_debug():
    try:
        core = _pick_latest()
        return JSONResponse(core)
    except HTTPException as e:
        return JSONResponse({"detail": e.detail, "dir": DATA_DIR}, status_code=e.status_code)
