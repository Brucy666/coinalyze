# coinalyze_api_server.py
# Read-only FastAPI wrapper that serves the latest CoinAnalyzer JSON snapshot(s)
# from a local directory. Safe with your running collector.
#
# GET /v1/metrics?symbol=BTCUSDT[&interval=1m]
# -> {
#      "symbol":"BTCUSDT","ts":169..., "interval":"1m",
#      "oi_delta":..., "cvd_divergence":"bullish|bearish|none",
#      "net_long_short":1.07, "funding":0.0031,
#      "ohlcv":{"o":...,"h":...,"l":...,"c":...,"v":...}
#    }

import os, time, glob, json
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Tuple

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

# ---------------- Env knobs (non-destructive defaults) ----------------
DATA_DIR      = os.getenv("DATA_DIR", "/data/coinalyze")   # where your collector writes *.json
FILE_GLOB     = os.getenv("FILE_GLOB", "*.json")           # pattern to scan
SCAN_LIMIT    = int(os.getenv("SCAN_LIMIT", "500"))        # max newest files to scan per request
CACHE_TTL_SEC = int(os.getenv("CACHE_TTL_SEC", "10"))      # small read cache to reduce IO

# If your JSON uses different keys, adjust here:
SYMBOL_KEYS      = ("symbol", "sym")
INTERVAL_KEYS    = ("interval", "tf", "timeframe")         # we accept "1m", "5m", "15m", "1hour" etc.
OI_KEYS          = ("oi_delta", "oi", "open_interest_delta")
CVD_DIV_KEYS     = ("cvd_divergence", "cvd_div", "divergence")
NLNS_KEYS        = ("net_long_short", "nl_ns", "long_short_ratio")
FUNDING_KEYS     = ("funding", "funding_rate")
TS_KEYS          = ("ts", "timestamp", "time")

# OHLCV keys (flat or nested)
OHLCV_FLAT = {"o":("o","open"), "h":("h","high"), "l":("l","low"), "c":("c","close"), "v":("v","volume")}
OHLCV_NESTED_KEYS = ("ohlcv", "candle", "bar")  # if your JSON nests them

# ---------------- App setup ----------------
app = FastAPI(title="CoinAnalyzer API (read-only)", version="1.0.0")

# Allow cross-service calls (sniper engine)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=False,
    allow_methods=["GET"], allow_headers=["*"],
)

# ---------------- tiny cache ----------------
_cache: Dict[str, Tuple[float, Dict[str, Any]]] = {}  # key -> (expiry_ts, payload)

def _cache_get(key: str) -> Optional[Dict[str, Any]]:
    hit = _cache.get(key)
    if not hit:
        return None
    expiry, payload = hit
    if time.time() > expiry:
        _cache.pop(key, None)
        return None
    return payload

def _cache_set(key: str, payload: Dict[str, Any], ttl: int = CACHE_TTL_SEC):
    _cache[key] = (time.time() + ttl, payload)

# ---------------- helpers ----------------
def _norm_interval(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    s = str(raw).lower().strip()
    aliases = {
        "1": "1m", "1min": "1m", "1m": "1m", "one_minute": "1m",
        "3": "3m", "3min": "3m", "3m": "3m",
        "5": "5m", "5min": "5m", "5m": "5m",
        "12": "12m", "12min": "12m", "12m": "12m",
        "15": "15m", "15min": "15m", "15m": "15m",
        "30": "30m", "30min": "30m", "30m": "30m",
        "1h": "1h", "1hour": "1h", "60m": "1h"
    }
    return aliases.get(s, raw)

def _get_first(d: Dict[str, Any], keys: Iterable[str], default: Any=None) -> Any:
    for k in keys:
        if k in d:
            return d[k]
    return default

def _extract_ohlcv(j: Dict[str, Any]) -> Optional[Dict[str, float]]:
    # nested first
    for nk in OHLCV_NESTED_KEYS:
        if nk in j and isinstance(j[nk], dict):
            src = j[nk]
            return {
                "o": float(_get_first(src, OHLCV_FLAT["o"], 0.0)),
                "h": float(_get_first(src, OHLCV_FLAT["h"], 0.0)),
                "l": float(_get_first(src, OHLCV_FLAT["l"], 0.0)),
                "c": float(_get_first(src, OHLCV_FLAT["c"], 0.0)),
                "v": float(_get_first(src, OHLCV_FLAT["v"], 0.0)),
            }
    # flat keys on root
    has_any = any(k in j for ks in OHLCV_FLAT.values() for k in ks)
    if has_any:
        return {
            "o": float(_get_first(j, OHLCV_FLAT["o"], 0.0)),
            "h": float(_get_first(j, OHLCV_FLAT["h"], 0.0)),
            "l": float(_get_first(j, OHLCV_FLAT["l"], 0.0)),
            "c": float(_get_first(j, OHLCV_FLAT["c"], 0.0)),
            "v": float(_get_first(j, OHLCV_FLAT["v"], 0.0)),
        }
    return None

def _read_json(path: Path) -> Optional[Dict[str, Any]]:
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception:
        # fallback jsonl: return last non-empty line
        try:
            with open(path, "rb") as f:
                for ln in reversed(f.read().splitlines()):
                    ln = ln.strip()
                    if not ln:
                        continue
                    return json.loads(ln.decode("utf-8"))
        except Exception:
            return None

def _scan_latest_files(data_dir: str, file_glob: str, limit: int) -> Iterable[Path]:
    pat = str(Path(data_dir) / file_glob)
    files = sorted(glob.glob(pat), key=os.path.getmtime, reverse=True)
    for p in files[:max(1, limit)]:
        yield Path(p)

def _extract_core(j: Dict[str, Any]) -> Dict[str, Any]:
    sym = str(_get_first(j, SYMBOL_KEYS, "") or "").upper()
    raw_interval = _get_first(j, INTERVAL_KEYS, None)
    interval = _norm_interval(raw_interval)
    ts = _get_first(j, TS_KEYS, int(time.time()))

    # normalize metrics
    oi  = float(_get_first(j, OI_KEYS, 0.0) or 0.0)
    div = str(_get_first(j, CVD_DIV_KEYS, "none") or "none").lower()
    nls = float(_get_first(j, NLNS_KEYS, 1.0) or 1.0)
    fr  = float(_get_first(j, FUNDING_KEYS, 0.0) or 0.0)

    return {
        "symbol": sym,
        "ts": int(ts),
        "interval": interval or raw_interval or "unknown",
        "oi_delta": oi,
        "cvd_divergence": div,
        "net_long_short": nls,
        "funding": fr,
        "ohlcv": _extract_ohlcv(j)
    }

def _pick_latest_by_symbol(symbol: str, target_interval: Optional[str]) -> Dict[str, Any]:
    """
    Scan newest files and return the freshest record for the symbol,
    preferring the requested interval (if given), otherwise the newest.
    """
    symbol = symbol.upper()
    target_interval = _norm_interval(target_interval) if target_interval else None

    best_any: Optional[Tuple[float, Dict[str, Any]]] = None
    best_interval: Optional[Tuple[float, Dict[str, Any]]] = None

    for path in _scan_latest_files(DATA_DIR, FILE_GLOB, SCAN_LIMIT):
        j = _read_json(path)
        if not isinstance(j, dict):
            continue

        core = _extract_core(j)
        if core["symbol"] != symbol:
            continue

        # compute file mtime as recency
        recency = path.stat().st_mtime

        # newest overall
        if (best_any is None) or (recency > best_any[0]):
            best_any = (recency, core)

        # newest matching interval
        if target_interval and _norm_interval(core.get("interval")) == target_interval:
            if (best_interval is None) or (recency > best_interval[0]):
                best_interval = (recency, core)

        # quick exit if we already found a very fresh interval (micro-opt)
        if best_interval and time.time() - best_interval[0] < 3:
            break

    if target_interval and best_interval:
        return best_interval[1]
    if best_any:
        return best_any[1]

    raise HTTPException(status_code=404, detail=f"No data for {symbol} in {DATA_DIR}")

# ---------------- Routes ----------------
@app.get("/healthz")
def health():
    # basic liveness & directory check
    ok = Path(DATA_DIR).exists()
    return {"status": "ok" if ok else "missing_data_dir", "data_dir": DATA_DIR}

@app.get("/v1/metrics")
def metrics(
    symbol: str = Query(..., description="Symbol e.g. BTCUSDT"),
    interval: Optional[str] = Query(None, description="Optional interval: 1m,3m,5m,12m,15m,30m,1h"),
):
    key = f"{symbol.upper()}::{interval or 'any'}"
    hit = _cache_get(key)
    if hit:
        return JSONResponse(hit)

    core = _pick_latest_by_symbol(symbol, interval)
    _cache_set(key, core)
    return JSONResponse(core)
