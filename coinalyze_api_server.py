# -*- coding: utf-8 -*-
"""
CoinAnalyzer API (read-only, one-symbol)
---------------------------------------
Serves the latest snapshot written by your collector as timestamp-named JSON files.
Works with your flat metric format (TF/OI/FR/CVD/Ls). Designed to run alongside the
collector without changing it.

Routes:
  GET /healthz
  GET /v1/metrics           -> compact JSON for DEFAULT_SYMBOL
  GET /v1/metrics/debug     -> same as /v1/metrics with a bit more context

Env:
  DATA_DIR=/data/coinalyze
  FILE_GLOB=*.json
  SCAN_LIMIT=500
  CACHE_TTL_SEC=5
  DEFAULT_SYMBOL=BTCUSDT
  DEFAULT_INTERVAL=1m
"""

import os, time, glob, json
from pathlib import Path
from typing import Any, Dict, Optional, Tuple, List

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

# -------------------- ENV --------------------
DATA_DIR        = os.getenv("DATA_DIR", "/data/coinalyze")
FILE_GLOB       = os.getenv("FILE_GLOB", "*.json")
SCAN_LIMIT      = int(os.getenv("SCAN_LIMIT", "500"))
CACHE_TTL       = int(os.getenv("CACHE_TTL_SEC", "5"))
DEFAULT_SYMBOL  = os.getenv("DEFAULT_SYMBOL", "BTCUSDT").upper()
DEFAULT_INTERVAL= os.getenv("DEFAULT_INTERVAL", "1m").lower()

# -------------------- APP --------------------
app = FastAPI(title="CoinAnalyzer API (read-only, one-symbol)", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["GET"], allow_headers=["*"])

_cache: Dict[str, Tuple[float, Dict[str, Any]]] = {}

def _cache_get(k: str) -> Optional[Dict[str, Any]]:
    hit = _cache.get(k)
    if not hit:
        return None
    exp, payload = hit
    if time.time() > exp:
        _cache.pop(k, None)
        return None
    return payload

def _cache_set(k: str, payload: Dict[str, Any], ttl: int = CACHE_TTL):
    _cache[k] = (time.time() + ttl, payload)

# -------------------- IO helpers --------------------
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

# -------------------- parsing --------------------
def _norm_interval(raw: Any) -> str:
    if not raw:
        return DEFAULT_INTERVAL
    s = str(raw).lower().strip()
    if s in ("1", "1m", "1min", "one_minute"):
        return "1m"
    if s in ("3", "3m", "3min"):
        return "3m"
    if s in ("5", "5m", "5min"):
        return "5m"
    if s in ("12", "12m", "12min"):
        return "12m"
    if s in ("15", "15m", "15min"):
        return "15m"
    if s in ("30", "30m", "30min"):
        return "30m"
    if s in ("60", "60m", "1h", "1hour", "hour"):
        return "1h"
    if s.endswith("min"):
        return s.replace("min", "m")
    return s

def _extract_core(j: Dict[str, Any]) -> Dict[str, Any]:
    """
    Map flat keys TF/OI/FR/CVD/Ls to the compact schema the sniper engine expects.
    If a field is missing, fall back to safe defaults.
    """
    interval = _norm_interval(j.get("TF") or j.get("tf") or DEFAULT_INTERVAL)
    ts       = int(j.get("ts") or j.get("timestamp") or time.time())
    oi       = float(j.get("OI") or 0.0)
    funding  = float(j.get("FR") or 0.0)
    cvd_val  = float(j.get("CVD") or 0.0)
    ls_ratio = float(j.get("Ls") or 1.0)

    # derive divergence from CVD sign if explicit field not present
    cvd_div = "bullish" if cvd_val > 0 else ("bearish" if cvd_val < 0 else "none")

    return {
        "symbol": DEFAULT_SYMBOL,
        "interval": interval,
        "ts": ts,
        "oi_delta": oi,
        "cvd_divergence": cvd_div,
        "net_long_short": ls_ratio,
        "funding": funding,
        "ohlcv": None  # not present in these files
    }

def _pick_latest() -> Dict[str, Any]:
    for p in _scan_latest(DATA_DIR, FILE_GLOB, SCAN_LIMIT):
        j = _read_json(p)
        if not isinstance(j, dict):
            continue
        return _extract_core(j)
    raise HTTPException(status_code=404, detail=f"No data found in {DATA_DIR}")

# -------------------- routes --------------------
@app.get("/healthz")
def healthz():
    ok = Path(DATA_DIR).exists()
    return {"status": "ok" if ok else "missing_data_dir", "dir": DATA_DIR, "glob": FILE_GLOB}

@app.get("/v1/metrics")
def metrics():
    """
    One-symbol endpoint. Always returns DEFAULT_SYMBOL from env.
    """
    key = "latest"
    hit = _cache_get(key)
    if hit:
        return JSONResponse(hit)

    core = _pick_latest()
    _cache_set(key, core)
    return JSONResponse(core)

@app.get("/v1/metrics/debug")
def metrics_debug():
    """
    Same as /v1/metrics, without cache and with extra context on errors.
    """
    try:
        core = _pick_latest()
        core["_dir"] = DATA_DIR
        core["_glob"] = FILE_GLOB
        return JSONResponse(core)
    except HTTPException as e:
        return JSONResponse({"detail": e.detail, "dir": DATA_DIR, "glob": FILE_GLOB}, status_code=e.status_code)
