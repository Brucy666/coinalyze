# coinalyze_api_server.py
# Simplified parser for flat-line CoinAnalyzer logs

import os, time, re, glob, logging
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

# ---------- ENV ----------
DATA_DIR = os.getenv("DATA_DIR", "/data")
FILE_GLOB = os.getenv("FILE_GLOB", "**/*.json")
SCAN_LIMIT = int(os.getenv("SCAN_LIMIT", "1000"))
CACHE_TTL_SEC = int(os.getenv("CACHE_TTL_SEC", "5"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [coinalyze_api] %(message)s")
log = logging.getLogger("coinalyze_api")

# ---------- FASTAPI ----------
app = FastAPI(title="CoinAnalyzer FlatLog API", version="1.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["GET"], allow_headers=["*"])

_cache: Dict[str, Any] = {}

# ---------- HELPERS ----------
def _cache_get(k: str):
    if k in _cache:
        exp, payload = _cache[k]
        if time.time() < exp:
            return payload
        else:
            _cache.pop(k, None)
    return None

def _cache_set(k: str, payload: Any):
    _cache[k] = (time.time() + CACHE_TTL_SEC, payload)

def _rscan_latest(base: str, pattern: str, limit: int) -> List[Path]:
    basep = Path(base)
    if not basep.exists():
        log.warning("DATA_DIR missing: %s", base)
        return []
    files = glob.glob(str(basep / pattern), recursive=True)
    files.sort(key=lambda p: Path(p).stat().st_mtime, reverse=True)
    return [Path(f) for f in files[:max(1, limit)]]

def _infer_symbol(path: Path) -> str:
    for seg in path.parts:
        if "BTC" in seg.upper():
            return seg.split("_")[0].upper()
    return "UNKNOWN"

# Regex to match flat-line logs
LINE_RE = re.compile(
    r"TF[:=]?(?P<tf>\S+).*?"
    r"OI[:=]?(?P<oi>[-\d\.]+).*?"
    r"FR[:=]?(?P<fr>[-\d\.]+).*?"
    r"LIQ[:=]?(?P<liq>[-\d\.]+).*?"
    r"LS[:=]?(?P<ls>[-\d\.]+).*?"
    r"CVD[:=]?(?P<cvd>[-\d\.]+)",
    re.IGNORECASE
)

def _parse_flat_file(path: Path) -> Optional[Dict[str, Any]]:
    try:
        text = path.read_text(encoding="utf-8", errors="ignore").strip()
    except Exception as e:
        log.warning("Cannot read %s: %s", path, e)
        return None

    if not text:
        return None

    m = LINE_RE.search(text)
    if not m:
        return None

    g = m.groupdict()
    tf = g.get("tf", "")
    oi = float(g.get("oi", 0))
    fr = float(g.get("fr", 0))
    liq = float(g.get("liq", 0))
    ls = float(g.get("ls", 0))
    cvd = float(g.get("cvd", 0))

    # Basic derived fields
    cvd_div = "bullish" if ls > liq * 1.05 else ("bearish" if liq > ls * 1.05 else "none")

    return {
        "symbol": _infer_symbol(path),
        "interval": tf,
        "oi": oi,
        "funding_rate": fr,
        "liq_long": liq,
        "liq_short": ls,
        "cvd": cvd,
        "cvd_divergence": cvd_div,
        "_file": str(path),
        "ts": int(path.stat().st_mtime),
    }

# ---------- CORE ----------
def _get_latest_for_symbol(symbol: str, tf: str) -> Dict[str, Any]:
    symbol = symbol.upper()
    tf = tf.lower()
    for p in _rscan_latest(DATA_DIR, FILE_GLOB, SCAN_LIMIT):
        if symbol in str(p).upper() and tf in str(p).lower():
            core = _parse_flat_file(p)
            if core:
                return core
    raise HTTPException(status_code=404, detail=f"No data found for {symbol} {tf}")

def _get_all_tfs(symbol: str) -> Dict[str, Any]:
    tfs = ("1m", "5m", "15m", "1h")
    out = {}
    for tf in tfs:
        try:
            out[tf] = _get_latest_for_symbol(symbol, tf)
        except HTTPException:
            continue
    if not out:
        raise HTTPException(status_code=404, detail=f"No data found for {symbol}")
    return out

# ---------- ROUTES ----------
@app.get("/healthz")
def healthz():
    return {"status": "ok", "dir": DATA_DIR, "glob": FILE_GLOB}

@app.get("/v1/files")
def list_files(n: int = Query(25, ge=1, le=1000)):
    files = [str(p) for p in _rscan_latest(DATA_DIR, FILE_GLOB, n)]
    return {"dir": DATA_DIR, "glob": FILE_GLOB, "count": len(files), "files": files}

@app.get("/v1/metrics/{symbol}")
def metrics_symbol(symbol: str):
    key = f"metrics:{symbol}"
    hit = _cache_get(key)
    if hit: return hit
    data = _get_all_tfs(symbol)
    payload = {"ok": True, "latest": data}
    _cache_set(key, payload)
    return payload

@app.get("/v1/metrics/debug")
def metrics_debug(symbol: Optional[str] = None, tf: Optional[str] = None):
    try:
        if symbol and tf:
            return {"ok": True, "picked": _get_latest_for_symbol(symbol, tf)}
        elif symbol:
            return {"ok": True, "picked": _get_all_tfs(symbol)}
        else:
            return {"ok": True, "files": [str(p) for p in _rscan_latest(DATA_DIR, FILE_GLOB, 20)]}
    except HTTPException as e:
        return JSONResponse({"detail": e.detail}, status_code=e.status_code)

@app.on_event("startup")
def startup_info():
    log.info("API up — DATA_DIR=%s | GLOB=%s", DATA_DIR, FILE_GLOB)
    log.info("Routes: /healthz, /v1/files, /v1/metrics/{symbol}, /v1/metrics/debug")
