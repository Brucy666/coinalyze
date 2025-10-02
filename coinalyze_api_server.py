# -*- coding: utf-8 -*-
"""
CoinAnalyzer API (flat logs, recursive scan)
- Recursively scans DATA_DIR (e.g., /data/BTCUSDT_PERP_A/1min/20251002/*.json)
- Parses either valid JSON or flat 'TF/OI/FR/LS/CVD ...' lines
- Derives symbol/interval from path segments when missing
- One-symbol service (DEFAULT_SYMBOL) for safety

Routes:
  GET /healthz
  GET /v1/files?n=20
  GET /v1/metrics
  GET /v1/metrics/debug
"""

import os, time, json
from pathlib import Path
from typing import Dict, Any, Optional, Tuple, List

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

# ---------------- ENV ----------------
DATA_DIR         = os.getenv("DATA_DIR", "/data")
FILE_GLOB        = os.getenv("FILE_GLOB", "**/*.json")  # recursive
SCAN_LIMIT       = int(os.getenv("SCAN_LIMIT", "1000"))
CACHE_TTL        = int(os.getenv("CACHE_TTL_SEC", "5"))
DEFAULT_SYMBOL   = os.getenv("DEFAULT_SYMBOL", "BTCUSDT").upper()
DEFAULT_INTERVAL = os.getenv("DEFAULT_INTERVAL", "1m").lower()

# ---------------- APP ----------------
app = FastAPI(title="CoinAnalyzer API (flat logs, recursive)", version="1.2.0")
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

# ---------------- SCAN / READ ----------------
def _rscan_latest(data_dir: str, pattern: str, limit: int) -> List[Path]:
    root = Path(data_dir)
    files = sorted(root.rglob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)
    return files[:max(1, limit)]

def _read_text(path: Path) -> Optional[str]:
    try:
        return path.read_text(encoding="utf-8", errors="ignore").strip()
    except Exception:
        return None

# ---------------- PARSING ----------------
def _norm_symbol_from_path(path: Path) -> Optional[str]:
    parts = [p.upper() for p in path.parts]
    for seg in reversed(parts):
        if "PERP" in seg or seg.endswith("USDT") or seg.endswith("USD"):
            s = seg.replace("-PERP", "_PERP").replace("PERPETUAL", "PERP")
            if "_PERP" in s: s = s.split("_PERP")[0]
            s = "".join(ch for ch in s if ch.isalnum())
            return s
    return None

def _norm_interval_from_path(path: Path) -> Optional[str]:
    parts = [p.lower() for p in path.parts]
    for seg in reversed(parts):
        if seg in ("1m","3m","5m","12m","15m","30m","1h","1hour","60m","1min","3min","5min","12min","15min","30min"):
            if seg in ("1hour","60m"): return "1h"
            if seg.endswith("min"):   return seg.replace("min","m")
            return seg
    return None

def _norm_interval(raw: Any) -> str:
    if not raw: return DEFAULT_INTERVAL
    s = str(raw).lower().strip()
    m = {
        "1":"1m","1m":"1m","1min":"1m",
        "3":"3m","3m":"3m","3min":"3m",
        "5":"5m","5m":"5m","5min":"5m",
        "12":"12m","12m":"12m","12min":"12m",
        "15":"15m","15m":"15m","15min":"15m",
        "30":"30m","30m":"30m","30min":"30m",
        "60":"1h","60m":"1h","1h":"1h","1hour":"1h","hour":"1h"
    }
    if s.endswith("min"): s = s.replace("min","m")
    return m.get(s, s)

def _parse_flat(txt: str) -> Dict[str, Any]:
    """
    Parse lines like:
    [14:44:32] TF:1min OI:93306.038 FR:0.01 Candles:360 LIQ:127 LS:0 CVD:1100.92 ...
    Accepts KEY:VAL and KEY=VAL tokens.
    """
    out: Dict[str, Any] = {}
    s = txt.replace("["," ").replace("]"," ").replace(","," ")
    for tok in s.split():
        if ":" in tok:
            k, v = tok.split(":", 1)
        elif "=" in tok:
            k, v = tok.split("=", 1)
        else:
            continue
        k = k.strip().upper(); v = v.strip()
        try:
            out[k] = float(v)
        except:
            out[k] = v
    return out

def _parse_any(path: Path) -> Optional[Dict[str, Any]]:
    txt = _read_text(path)
    if not txt:
        return None
    # If JSON object/array
    if txt[:1] in ("{","["):
        try:
            return json.loads(txt)
        except Exception:
            pass
    # fallback: flat tokens
    return _parse_flat(txt)

# ---------------- EXTRACT ----------------
def _extract_core(obj: Dict[str, Any], path: Path) -> Dict[str, Any]:
    # interval: prefer content, else from path
    interval = _norm_interval(obj.get("interval") or obj.get("tf") or obj.get("TF") or _norm_interval_from_path(path) or DEFAULT_INTERVAL)
    # metrics (allow both lower and UPPER)
    oi  = float(obj.get("oi_delta") or obj.get("oi") or obj.get("OI") or 0.0)
    fr  = float(obj.get("funding")  or obj.get("funding_rate") or obj.get("FR") or 0.0)
    ls  = float(obj.get("net_long_short") or obj.get("nl_ns") or obj.get("LS") or 1.0)
    # divergence: explicit or derive from raw CVD sign
    cvd_div = obj.get("cvd_divergence") or obj.get("cvd_div")
    if not cvd_div:
        cvd_val = float(obj.get("cvd") or obj.get("CVD") or 0.0)
        cvd_div = "bullish" if cvd_val > 0 else ("bearish" if cvd_val < 0 else "none")
    else:
        cvd_div = str(cvd_div).lower()
    # symbol: prefer from path; fallback env
    sym = _norm_symbol_from_path(path) or DEFAULT_SYMBOL

    return {
        "symbol": sym,
        "interval": interval,
        "ts": int(time.time()),
        "oi_delta": oi,
        "cvd_divergence": cvd_div,
        "net_long_short": ls,
        "funding": fr,
        "ohlcv": None,
        "_file": str(path)
    }

def _pick_latest() -> Dict[str, Any]:
    for p in _rscan_latest(DATA_DIR, FILE_GLOB, SCAN_LIMIT):
        obj = _parse_any(p)
        if isinstance(obj, dict) and obj:
            return _extract_core(obj, p)
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
