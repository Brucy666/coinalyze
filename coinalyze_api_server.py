# coinalyze_api_server.py
# Read-only FastAPI for CoinAnalyzer log-style files (TF/OI/FR/LS/CVD ...).
# Parses the newest .json text file and exposes a compact JSON schema.

import os, time, glob
from pathlib import Path
from typing import Dict, Any, Optional, Tuple, List

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

# ---------------- ENV ----------------
DATA_DIR        = os.getenv("DATA_DIR", "/data/coinalyze")
FILE_GLOB       = os.getenv("FILE_GLOB", "*.json")
SCAN_LIMIT      = int(os.getenv("SCAN_LIMIT", "500"))
CACHE_TTL       = int(os.getenv("CACHE_TTL_SEC", "5"))
DEFAULT_SYMBOL  = os.getenv("DEFAULT_SYMBOL", "BTCUSDT").upper()

# ---------------- APP ----------------
app = FastAPI(title="CoinAnalyzer API (flat logs)", version="1.0")
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
def _scan_latest(data_dir: str, pattern: str, limit: int) -> List[Path]:
    pat = str(Path(data_dir) / pattern)
    files = sorted(glob.glob(pat), key=os.path.getmtime, reverse=True)
    return [Path(p) for p in files[:max(1, limit)]]

def _parse_flat_line(txt: str) -> Dict[str, Any]:
    """
    Parse a line like:
    [14:44:32] TF:1min OI:93306.038 FR:0.01 Candles:360 LIQ:127 LS:0 CVD:1100.92
    """
    tokens = txt.replace("[","").replace("]","").split()
    out: Dict[str, Any] = {}
    for token in tokens:
        if ":" not in token: 
            continue
        k,v = token.split(":",1)
        k = k.strip().upper()
        v = v.strip()
        try:
            out[k] = float(v)
        except:
            out[k] = v
    return out

def _extract_core(raw: Dict[str, Any]) -> Dict[str, Any]:
    interval = str(raw.get("TF","unknown")).lower()
    if interval.endswith("min") and not interval.endswith("m"):
        interval = interval.replace("min","m")
    if interval == "1hour" or interval == "60":
        interval = "1h"

    oi   = float(raw.get("OI",0.0))
    fr   = float(raw.get("FR",0.0))
    cvd  = float(raw.get("CVD",0.0))
    ls   = float(raw.get("LS",1.0))

    cvd_div = "bullish" if cvd>0 else ("bearish" if cvd<0 else "none")

    return {
        "symbol": DEFAULT_SYMBOL,
        "interval": interval,
        "ts": int(time.time()),
        "oi_delta": oi,
        "cvd_divergence": cvd_div,
        "net_long_short": ls,
        "funding": fr,
        "ohlcv": None,
        "_raw": raw
    }

def _pick_latest() -> Dict[str, Any]:
    for p in _scan_latest(DATA_DIR, FILE_GLOB, SCAN_LIMIT):
        try:
            with open(p,"r") as f:
                txt = f.read().strip()
            if not txt:
                continue
            raw = _parse_flat_line(txt)
            if raw:
                return _extract_core(raw)
        except Exception:
            continue
    raise HTTPException(status_code=404, detail=f"No data found in {DATA_DIR}")

# ---------------- ROUTES ----------------
@app.get("/healthz")
def healthz():
    return {"status": "ok" if Path(DATA_DIR).exists() else "missing_data_dir", "dir": DATA_DIR}

@app.get("/v1/metrics")
def metrics():
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
