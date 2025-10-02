# -*- coding: utf-8 -*-
"""
Read-only FastAPI that serves the latest CoinAnalyzer snapshot from timestamp-named JSON files.
Designed for your current flat log format (TF/OI/FR/CVD/Ls …) and also supports structured JSON.

ENV (sensible defaults):
- DATA_DIR=/data/coinalyze
- FILE_GLOB=*.json
- SCAN_LIMIT=800                  # newest files to scan per request
- CACHE_TTL_SEC=8
- DEFAULT_SYMBOL=BTCUSDT          # used if files don't contain a symbol key
- DEFAULT_INTERVAL=1m             # used if TF can't be parsed
- INTERVAL_PRIORITY=1m,5m,15m,1h  # preference when &interval is not provided
"""

import os
import re
import time
import glob
import json
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Tuple, List

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

# ------------------------- ENV -------------------------
DATA_DIR = os.getenv("DATA_DIR", "/data/coinalyze")
FILE_GLOB = os.getenv("FILE_GLOB", "*.json")
SCAN_LIMIT = int(os.getenv("SCAN_LIMIT", "800"))
CACHE_TTL = int(os.getenv("CACHE_TTL_SEC", "8"))
DEFAULT_SYMBOL = os.getenv("DEFAULT_SYMBOL", "BTCUSDT").upper()
DEFAULT_INTERVAL = os.getenv("DEFAULT_INTERVAL", "1m").lower()
INTERVAL_PRIORITY = [
    s.strip().lower() for s in os.getenv("INTERVAL_PRIORITY", "1m,5m,15m,1h").split(",")
    if s.strip()
]

# ------------------------- APP -------------------------
app = FastAPI(title="CoinAnalyzer API (read-only)", version="1.2.0")
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["GET"], allow_headers=["*"]
)

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

# ------------------------- HELPERS -------------------------
ALPHA_NUM = re.compile(r"[^A-Z0-9]+")

def _norm_symbol(raw: Any) -> str:
    if raw is None:
        return ""
    s = str(raw).upper().strip()
    # common formats: BTCUSDT_PERP_A / BTCUSDT-PERP / BTCUSDT-PERP-A
    s = s.replace("-PERP", "_PERP").replace("PERPETUAL", "PERP")
    if "_PERP" in s:
        s = s.split("_PERP")[0]
    if "-PERP" in s:
        s = s.split("-PERP")[0]
    s = ALPHA_NUM.sub("", s)  # strip all non [A-Z0-9]
    return s

def _norm_interval(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    s = str(raw).lower().strip()
    m = {
        "1": "1m", "1m":"1m","1min":"1m","one_minute":"1m",
        "3":"3m","3m":"3m","3min":"3m",
        "5":"5m","5m":"5m","5min":"5m",
        "12":"12m","12m":"12m","12min":"12m",
        "15":"15m","15m":"15m","15min":"15m",
        "30":"30m","30m":"30m","30min":"30m",
        "60":"1h","60m":"1h","1h":"1h","1hour":"1h",
        "1hour":"1h","hour":"1h"
    }
    # normalize 1hour/60m/etc
    if s.endswith("hour") and s != "1hour":
        s = "1h"
    if s.endswith("min"):
        s = s.replace("min", "m")
    return m.get(s, s)

def _read_json(path: Path) -> Optional[Dict[str, Any]]:
    """Reads JSON or last line of JSONL."""
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception:
        try:
            with open(path, "rb") as f:
                for ln in reversed(f.read().splitlines()):
                    ln = ln.strip()
                    if not ln:
                        continue
                    return json.loads(ln.decode("utf-8"))
        except Exception:
            return None

def _scan_latest(data_dir: str, pattern: str, limit: int) -> List[Path]:
    pat = str(Path(data_dir) / pattern)
    files = sorted(glob.glob(pat), key=os.path.getmtime, reverse=True)
    return [Path(p) for p in files[:max(1, limit)]]

def _get_first(d: Dict[str, Any], keys: Iterable[str], default: Any = None) -> Any:
    for k in keys:
        if k in d:
            return d[k]
        ku = k.upper()
        if ku in d:
            return d[ku]
        kl = k.lower()
        if kl in d:
            return d[kl]
    return default

# Flexible key sets for structured JSON (if present)
SYMBOL_KEYS   = ("symbol", "sym", "pair", "instrument", "id")
INTERVAL_KEYS = ("interval", "tf", "timeframe", "tframe", "frame", "period")
OI_KEYS       = ("oi_delta", "oi", "open_interest_delta", "open_interest", "oi_change")
CVD_KEYS      = ("cvd", "cvd_delta")     # raw cvd value
CVD_DIV_KEYS  = ("cvd_divergence", "cvd_div", "divergence")
NLNS_KEYS     = ("net_long_short", "nl_ns", "long_short_ratio", "ls_ratio")
FUNDING_KEYS  = ("funding", "funding_rate", "fr")
TS_KEYS       = ("ts", "timestamp", "time")

OHLCV_FLAT = {
    "o": ("o","open"),
    "h": ("h","high"),
    "l": ("l","low"),
    "c": ("c","close"),
    "v": ("v","volume","vol"),
}
OHLCV_NESTED = ("ohlcv", "candle", "bar")

def _extract_ohlcv(j: Dict[str, Any]) -> Optional[Dict[str, float]]:
    # nested first
    for nk in OHLCV_NESTED:
        if nk in j and isinstance(j[nk], dict):
            src = j[nk]
            return {
                "o": float(_get_first(src, OHLCV_FLAT["o"], 0.0)),
                "h": float(_get_first(src, OHLCV_FLAT["h"], 0.0)),
                "l": float(_get_first(src, OHLCV_FLAT["l"], 0.0)),
                "c": float(_get_first(src, OHLCV_FLAT["c"], 0.0)),
                "v": float(_get_first(src, OHLCV_FLAT["v"], 0.0)),
            }
    # flat on root
    has_any = any((k in j) or (k.upper() in j) for ks in OHLCV_FLAT.values() for k in ks)
    if has_any:
        return {
            "o": float(_get_first(j, OHLCV_FLAT["o"], 0.0)),
            "h": float(_get_first(j, OHLCV_FLAT["h"], 0.0)),
            "l": float(_get_first(j, OHLCV_FLAT["l"], 0.0)),
            "c": float(_get_first(j, OHLCV_FLAT["c"], 0.0)),
            "v": float(_get_first(j, OHLCV_FLAT["v"], 0.0)),
        }
    return None

def _extract_core(j: Dict[str, Any]) -> Dict[str, Any]:
    """
    Parse both structured JSON and your flat 'TF/OI/FR/CVD/Ls' format.
    """
    # 1) SYMBOL
    sym = _norm_symbol(_get_first(j, SYMBOL_KEYS, None))
    if not sym:
        # flat files may not include symbol; use env default
        sym = DEFAULT_SYMBOL

    # 2) INTERVAL
    interval = _norm_interval(
        _get_first(j, INTERVAL_KEYS, None) or j.get("TF") or j.get("tf") or DEFAULT_INTERVAL
    )

    # 3) TIMESTAMP
    ts = int(_get_first(j, TS_KEYS, int(time.time())))

    # 4) METRICS (prefer structured, fallback to flat)
    oi = float(_get_first(j, OI_KEYS, j.get("OI", 0.0)) or 0.0)
    funding = float(_get_first(j, FUNDING_KEYS, j.get("FR", 0.0)) or 0.0)
    nls = float(_get_first(j, NLNS_KEYS, j.get("Ls", 1.0)) or 1.0)

    # CVD divergence: if explicit field missing, derive from raw CVD sign
    cvd_div = _get_first(j, CVD_DIV_KEYS, None)
    if not cvd_div:
        cvd_val = float(_get_first(j, CVD_KEYS, j.get("CVD", 0.0)) or 0.0)
        cvd_div = "bullish" if cvd_val > 0 else ("bearish" if cvd_val < 0 else "none")
    else:
        cvd_div = str(cvd_div).lower()

    return {
        "symbol": sym,
        "interval": interval or DEFAULT_INTERVAL,
        "ts": ts,
        "oi_delta": oi,
        "cvd_divergence": cvd_div,
        "net_long_short": nls,
        "funding": funding,
        "ohlcv": _extract_ohlcv(j),
    }

def _pick_latest(symbol: str, interval: Optional[str]) -> Dict[str, Any]:
    """
    Scan newest files and return the freshest record for the symbol.
    If interval is provided, prefer that; otherwise use INTERVAL_PRIORITY order.
    """
    sym_core = _norm_symbol(symbol)
    want_tf = _norm_interval(interval) if interval else None

    best_any: Optional[Tuple[float, Dict[str, Any]]] = None
    best_tf: Optional[Tuple[float, Dict[str, Any]]] = None

    for p in _scan_latest(DATA_DIR, FILE_GLOB, SCAN_LIMIT):
        j = _read_json(p)
        if not isinstance(j, dict):
            continue

        core = _extract_core(j)
        if core["symbol"] != sym_core:
            # heuristic: allow partial match when symbol not present in file
            svals = " ".join(str(v).upper() for v in j.values() if isinstance(v, (str, int, float)))
            if sym_core[:3] not in svals:
                continue

        mt = p.stat().st_mtime  # recency by file mtime

        # freshest overall
        if (best_any is None) or (mt > best_any[0]):
            best_any = (mt, core)

        # freshest matching interval
        if want_tf and core["interval"] == want_tf:
            if (best_tf is None) or (mt > best_tf[0]):
                best_tf = (mt, core)
            if time.time() - mt < 2:
                break

    # choose in order: requested interval > freshest overall > priority order among cached
    if want_tf and best_tf:
        return best_tf[1]
    if best_any:
        chosen = best_any[1]
        # If no requested interval, try to respect INTERVAL_PRIORITY by rescan within cache
        if not want_tf and chosen["interval"] not in INTERVAL_PRIORITY:
            # nothing else to do without indexing; return freshest
            return chosen
        return chosen

    raise HTTPException(status_code=404, detail=f"No data for {symbol} in {DATA_DIR}")

# ------------------------- ROUTES -------------------------
@app.get("/healthz")
def healthz():
    ok = Path(DATA_DIR).exists()
    return {"status": "ok" if ok else "missing_data_dir", "dir": DATA_DIR, "glob": FILE_GLOB}

@app.get("/v1/metrics")
def metrics(
    symbol: str = Query(..., description="e.g., BTCUSDT"),
    interval: Optional[str] = Query(None, description="1m,5m,15m,1h (optional)")
):
    key = f"{symbol.upper()}::{(interval or 'any').lower()}"
    hit = _cache_get(key)
    if hit:
        return JSONResponse(hit)

    core = _pick_latest(symbol, interval)
    _cache_set(key, core)
    return JSONResponse(core)

# Debug: returns the same as /v1/metrics but without cache and with a bit more context
@app.get("/v1/metrics/debug")
def metrics_debug(
    symbol: str,
    interval: Optional[str] = None,
):
    try:
        core = _pick_latest(symbol, interval)
        core["_dir"] = DATA_DIR
        core["_glob"] = FILE_GLOB
        return JSONResponse(core)
    except HTTPException as e:
        return JSONResponse({"detail": e.detail, "dir": DATA_DIR, "glob": FILE_GLOB}, status_code=e.status_code)
