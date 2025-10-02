# -*- coding: utf-8 -*-
"""
CoinAnalyzer API (flat logs, recursive scan)
- Serves the latest snapshot from timestamp-named files written by your collector.
- Parses either valid JSON or flat 'TF/OI/FR/LS/CVD ...' text.
- Recursively scans DATA_DIR using FILE_GLOB (e.g. **/*.json)
- One-symbol service (DEFAULT_SYMBOL).
Routes:
  GET /healthz
  GET /v1/files?n=20
  GET /v1/metrics
  GET /v1/metrics/debug
Env:
  DATA_DIR=/data
  FILE_GLOB=**/*.json
  SCAN_LIMIT=1000
  CACHE_TTL_SEC=5
  DEFAULT_SYMBOL=BTCUSDT
  DEFAULT_INTERVAL=1m
"""

import os, time, glob, json
from pathlib import Path
from typing import Dict, Any, Optional, Tuple, List

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

# ---------------- ENV ----------------
DATA_DIR        = os.getenv("DATA_DIR", "/data")
FILE_GLOB       = os.getenv("FILE_GLOB", "**/*.json")  # recursive by default
SCAN_LIMIT      = int(os.getenv("SCAN_LIMIT", "1000"))
CACHE_TTL       = int(os.getenv("CACHE_TTL_SEC", "5"))
DEFAULT_SYMBOL  = os.getenv("DEFAULT_SYMBOL", "BTCUSDT").upper()
DEFAULT_INTERVAL= os.getenv("DEFAULT_INTERVAL", "1m").lower()

# ---------------- APP ----------------
app = FastAPI(title="CoinAnalyzer API (flat logs, recursive)", version="1.1.0")
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
def _rscan_latest(data_dir: str, pattern: str, limit: int) -> List[Path]:
    root = Path(data_dir)
    files = sorted(root.rglob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)
    return files[:max(1, limit)]

def _norm_interval(raw: Any) -> str:
    if not raw: return DEFAULT_INTERVAL
    s = str(raw).lower().strip()
    if s in ("1","1m","1min","one_minute"): return "1m"
    if s in ("3","3m","3min"): return "3m"
    if s in ("5","5m","5min"): return "5m"
    if s in ("12","12m","12min"): return "12m"
    if s in ("15","15m","15min"): return "15m"
    if s in ("30","30m","30min"): return "30m"
    if s in ("60","60m","1h","1hour","hour"): return "1h"
    if s.endswith("min"): return s.replace("min","m")
    return s

def _parse_flat(txt: str) -> Dict[str, Any]:
    """
    Parse lines like:
    [14:44:32] TF:1min OI:93306.038 FR:0.01 Candles:360 LIQ:127 LS:0 CVD:1100.92 ...
    """
    out: Dict[str, Any] = {}
    s = txt.replace("["," ").replace("]"," ")
    # tokenize by whitespace; accept KEY:VAL or KEY=VAL
    for tok in s.split():
        if ":" in tok:
            k,v = tok.split(":",1)
        elif "=" in tok:
            k,v = tok.split("=",1)
        else:
            continue
        k = k.strip().upper(); v = v.strip().strip(",")
        try:
            out[k] = float(v)
        except:
            out[k] = v
    return out

def _read_file_obj(path: Path) -> Optional[Dict[str, Any]]:
    """Try JSON first; if that fails, parse flat text."""
    try:
        with open(path,"r") as f:
            txt = f.read().strip()
            if not txt:
                return None
            if txt.startswith("{") or txt.startswith("["):
                return json.loads(txt)
            return _parse_flat(txt)
    except Exception:
        return None

def _extract_core(obj: Dict[str, Any]) -> Dict[str, Any]:
    # Accept either structured JSON or parsed flat tokens
    interval = _norm_interval(obj.get("interval") or obj.get("tf") or obj.get("TF") or DEFAULT_INTERVAL)
    ts       = int(obj.get("ts") or obj.get("timestamp") or time.time())
    oi       = float(obj.get("oi_delta") or obj.get("oi") or obj.get("OI") or 0.0)
    funding  = float(obj.get("funding") or obj.get("funding_rate") or obj.get("FR") or 0.0)
    ls_ratio = float(obj.get("net_long_short") or obj.get("nl_ns") or obj.get("LS") or 1.0)
    # derive divergence from raw CVD sign if not present
    cvd_div  = obj.get("cvd_divergence") or obj.get("cvd_div") or None
    if not cvd_div:
        cvd_val = float(obj.get("cvd") or obj.get("CVD") or 0.0)
        cvd_div = "bullish" if cvd_val > 0 else ("bearish" if cvd_val < 0 else "none")
    else:
        cvd_div = str(cvd_div).lower()

    return {
        "symbol": DEFAULT_SYMBOL,
        "interval": interval,
        "ts": ts,
        "oi_delta": oi,
        "cvd_divergence": cvd_div,
        "net_long_short": ls_ratio,
        "funding": funding,
        "ohlcv": None
    }

def _pick_latest() -> Dict[str, Any]:
    files = _rscan_latest(DATA_DIR, FILE_GLOB, SCAN_LIMIT)
    for p in files:
        obj = _read_file_obj(p)
        if isinstance(obj, dict) and obj:
            return _extract_core(obj)
    raise HTTPException(status_code=404, detail=f"No data found in {DATA_DIR}")

# ---------------- ROUTES ----------------
@app.get("/healthz")
def healthz():
    return {"status": "ok" if Path(DATA_DIR).exists() else "missing_data_dir", "dir": DATA_DIR, "glob": FILE_GLOB}

@app.get("/v1/files")
def list_files(n: int = 20):
    files = [str(p) for p in _rscan_latest(DATA_DIR, FILE_GLOB, n)]
    return {"dir": DATA_DIR, "glob": FILE_GLOB, "count": len(files), "files": files}

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
        return JSONResponse({"detail": e.detail, "dir": DATA_DIR, "glob": FILE_GLOB}, status_code=e.status_code)
