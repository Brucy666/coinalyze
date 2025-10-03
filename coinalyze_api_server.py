# coinalyze_api_server.py
"""
CoinAnalyzer read-only HTTP service (FastAPI)

Features:
- Recursively scans DATA_DIR for JSON / text files matching a pattern
- Parses JSON or flat-line files (both supported)
- Picks latest *valid* file per timeframe (skips files that don't have essential tokens)
- Exposes /v1/metrics, /v1/metrics/all, /v1/metrics/debug, /v1/files, /healthz
- Caches responses briefly to reduce disk IO
"""

import os
import time
import glob
import json
import logging
from pathlib import Path
from typing import Dict, Any, Optional, Tuple, List

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

# ---------------- ENV ----------------
DATA_DIR = os.getenv("DATA_DIR", "/data")  # changed default to /data (volume mount)
FILE_GLOB = os.getenv("FILE_GLOB", "**/*.json")  # recursive by default
SCAN_LIMIT = int(os.getenv("SCAN_LIMIT", "500"))
CACHE_TTL = int(os.getenv("CACHE_TTL_SEC", "5"))
DEFAULT_SYMBOL = os.getenv("DEFAULT_SYMBOL", "BTCUSDT").upper()
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

# ---------------- LOGGING ----------------
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
log = logging.getLogger("coinalyze_api")

# ---------------- APP ----------------
app = FastAPI(title="CoinAnalyzer API (robust)", version="1.1")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["GET"], allow_headers=["*"])

# simple TTL cache: key -> (expire_ts, payload)
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


# ---------------- Helpers: scanning & parsing ----------------
def _scan_latest(data_dir: str, pattern: str, limit: int = SCAN_LIMIT) -> List[Path]:
    """
    Glob recursively and return up to `limit` files sorted by mtime desc.
    Pattern supports recursive glob like '**/*.json'
    """
    pat = str(Path(data_dir) / pattern)
    # glob.glob with recursive=True for '**' patterns
    files = glob.glob(pat, recursive=True)
    files = sorted(files, key=lambda p: Path(p).stat().st_mtime if Path(p).exists() else 0, reverse=True)
    paths = [Path(p) for p in files[:max(1, limit)]]
    log.debug("scanned %d files (limit=%d) using pattern %s", len(paths), limit, pattern)
    return paths


def _parse_flat_line(txt: str) -> Dict[str, Any]:
    """
    Parse a line like:
    [14:44:32] TF:1min OI:93306.038 FR:0.01 Candles:360 LIQ:127 LS:0 CVD:1100.92

    Return mapping of UPPER keys to numeric/string values.
    """
    out: Dict[str, Any] = {}
    if not txt:
        return out
    # remove common wrapping and split tokens
    txt = txt.strip()
    # sometimes file might contain multiple lines; just parse the first non-empty line
    for line in txt.splitlines():
        if line.strip():
            txt = line.strip()
            break

    # remove brackets timestamps
    txt = txt.replace("[", "").replace("]", "")
    tokens = txt.split()
    for token in tokens:
        if ":" not in token:
            continue
        k, v = token.split(":", 1)
        k = k.strip().upper()
        v = v.strip()
        # attempt numeric conversion
        try:
            # integers first
            if v.isdigit():
                out[k] = int(v)
            else:
                out[k] = float(v)
        except Exception:
            out[k] = v
    return out


def _parse_any_file(path: Path) -> Optional[Dict[str, Any]]:
    """
    Try to parse file as JSON first; if not JSON try to parse as flat-line text.
    Returns parsed mapping or None if parsing failed / empty.
    """
    try:
        with open(path, "r", encoding="utf-8") as f:
            txt = f.read().strip()
        if not txt:
            return None
        # try JSON
        try:
            obj = json.loads(txt)
            if isinstance(obj, dict):
                # normalize keys to upper for token parsing convenience
                return {k.upper(): v for k, v in obj.items()}
            # if JSON isn't a dict, fall back to flat parse of first line
        except Exception:
            pass
        # fallback to flat parser
        parsed = _parse_flat_line(txt)
        return parsed if parsed else None
    except Exception as e:
        log.debug("failed to parse file %s: %s", path, e)
        return None


# normalize TF tokens: input like "1min", "1m", "1", "60", "1hour" -> "1m", "5m", "15m", "1h"
def _normalize_interval(tf_raw: str) -> str:
    if tf_raw is None:
        return "unknown"
    s = str(tf_raw).lower().strip()
    # common normalizations
    s = s.replace("min", "m").replace("mins", "m")
    s = s.replace("hour", "h").replace("hrs", "h")
    # handle cases like '1mim' or '1min'
    s = s.replace("1mim", "1m").replace("60", "1h")
    # make canonical: 1m, 5m, 15m, 1h
    if s in ("1m", "1", "60s"):
        return "1m"
    if s in ("5m",):
        return "5m"
    if s in ("15m",):
        return "15m"
    if s in ("1h", "60"):
        return "1h"
    return s


def _extract_core(parsed: Dict[str, Any], src_path: Optional[Path] = None) -> Dict[str, Any]:
    """
    Build the compact response schema from parsed tokens.
    Expects parsed keys upper-cased when appropriate.
    """
    # support both TB: TF or TF token or 'interval' etc.
    interval = parsed.get("TF") or parsed.get("TF:") or parsed.get("INTERVAL") or parsed.get("T")
    interval = _normalize_interval(interval)

    def _get_float(keys, default=0.0):
        for k in keys:
            if k in parsed:
                try:
                    return float(parsed[k])
                except Exception:
                    try:
                        return float(str(parsed[k]).replace(",", ""))
                    except Exception:
                        return default
        return default

    oi = _get_float(["OI", "OI_DELTA", "OI_DELTA:"])
    fr = _get_float(["FR", "FUNDING", "FUND"])
    cvd = _get_float(["CVD", "CVD_DELTA", "CVD:"])
    ls = _get_float(["LS", "NL_NS", "NL/NS", "NET_LONG_SHORT"])

    cvd_div = "bullish" if cvd > 0 else ("bearish" if cvd < 0 else "none")

    core = {
        "symbol": parsed.get("SYMBOL", parsed.get("symbol", DEFAULT_SYMBOL)).upper() if parsed.get("SYMBOL", None) else DEFAULT_SYMBOL,
        "interval": interval,
        "ts": int(time.time()),
        "oi_delta": oi,
        "cvd_divergence": cvd_div,
        "net_long_short": ls,
        "funding": fr,
        "ohlcv": None,
        "_raw": parsed,
    }
    if src_path:
        core["_file"] = str(src_path)
    return core


# ---------------- File selection logic ----------------
def _pick_latest_for_tf(tf: str, data_dir: str = DATA_DIR, pattern: str = FILE_GLOB, scan_limit: int = SCAN_LIMIT) -> Dict[str, Any]:
    """
    Find the most recent file under the interval folder that contains valid fields.
    Mapping between normalized tf and expected folder name is supported.
    This function backtracks through recent files until it finds a file with OI/FR/LS.
    """
    # map normalized requests to common folder names in your storage (adjust as needed)
    folder_map = {
        "1m": "1min",
        "5m": "5min",
        "15m": "15min",
        "1h": "1hour",
    }
    tf_norm = _normalize_interval(tf)
    folder = folder_map.get(tf_norm, tf_norm)

    # pattern to search inside DATA_DIR
    # we try [**/<folder>/**/*.json] because your files are organised like /data/<SYMBOL>/<tf>/<timestamp>.json
    search_pattern = f"**/{folder}/**/*.json"
    log.debug("pick_latest_for_tf: searching for tf=%s using pattern=%s", tf_norm, search_pattern)

    for p in _scan_latest(data_dir, search_pattern, scan_limit):
        parsed = _parse_any_file(p)
        if not parsed:
            continue
        # quick validity check: must contain at least one of OI / FR / LS or have 'CVD' and 'OI' etc.
        has_oi = any(k in parsed for k in ("OI", "OI_DELTA", "OI_DELTA:"))
        has_fr = any(k in parsed for k in ("FR", "FUND", "FUNDING"))
        has_ls = any(k in parsed for k in ("LS", "NL_NS", "NET_LONG_SHORT", "NL/NS"))

        # If file lacks the richer tokens, skip it — we want "complete" metric rows for CA bias
        if not (has_oi or has_fr or has_ls):
            # keep looking further back
            log.debug("skipping incomplete file %s (no OI/FR/LS)", p)
            continue

        # At least one critical metric present -> accept this file
        core = _extract_core(parsed, p)
        core["_picked_from"] = str(p)
        return core

    # if nothing found, raise 404
    raise HTTPException(status_code=404, detail=f"No valid data found for timeframe '{tf}' in {data_dir}")


def _pick_latest_any(data_dir: str = DATA_DIR, pattern: str = FILE_GLOB, scan_limit: int = SCAN_LIMIT) -> Dict[str, Any]:
    """
    Fallback pick latest file regardless of timeframe by scanning the top-most files.
    This returns a single core dict or raises.
    """
    for p in _scan_latest(data_dir, pattern, scan_limit):
        parsed = _parse_any_file(p)
        if parsed:
            core = _extract_core(parsed, p)
            core["_picked_from"] = str(p)
            return core
    raise HTTPException(status_code=404, detail=f"No data found in {data_dir}")


# ---------------- Routes ----------------
@app.get("/healthz")
def healthz():
    ok = Path(DATA_DIR).exists()
    return {"status": "ok" if ok else "missing_data_dir", "dir": DATA_DIR, "glob": FILE_GLOB}


@app.get("/v1/files")
def list_files(limit: int = Query(50, ge=1, le=1000)):
    """
    Return a short listing of recent files (for debugging).
    """
    key = f"files:{limit}"
    hit = _cache_get(key)
    if hit:
        return JSONResponse(hit)
    files = [str(p) for p in _scan_latest(DATA_DIR, FILE_GLOB, limit)]
    payload = {"dir": DATA_DIR, "glob": FILE_GLOB, "count": len(files), "files": files}
    _cache_set(key, payload)
    return JSONResponse(payload)


@app.get("/v1/metrics")
def metrics(tf: Optional[str] = Query(None, description="Optional timeframe: e.g. 1m,5m,15m,1h")):
    """
    Return the latest best datapoint. If tf provided, try to pick latest valid for that timeframe.
    """
    key = f"latest:{tf or 'any'}"
    hit = _cache_get(key)
    if hit:
        return JSONResponse(hit)
    try:
        if tf:
            core = _pick_latest_for_tf(tf, DATA_DIR, FILE_GLOB, SCAN_LIMIT)
        else:
            core = _pick_latest_any(DATA_DIR, FILE_GLOB, SCAN_LIMIT)
        _cache_set(key, core)
        return JSONResponse(core)
    except HTTPException as e:
        raise e


@app.get("/v1/metrics/all")
def metrics_all():
    """
    Return latest valid entries for a set of TFs (1m,5m,15m,1h)
    """
    key = "metrics:all"
    hit = _cache_get(key)
    if hit:
        return JSONResponse(hit)

    tfs = ["1m", "5m", "15m", "1h"]
    out = {"symbol": DEFAULT_SYMBOL, "latest": {}, "missing": []}
    for tf in tfs:
        try:
            core = _pick_latest_for_tf(tf, DATA_DIR, FILE_GLOB, SCAN_LIMIT)
            out["latest"][tf] = core
        except HTTPException as e:
            out["missing"].append(tf)
            log.debug("no data for tf %s: %s", tf, e.detail)
            continue

    _cache_set(key, out)
    return JSONResponse(out)


@app.get("/v1/metrics/debug")
def metrics_debug(tf: Optional[str] = Query(None)):
    """
    Debug endpoint that returns parsing and file selection info, for troubleshooting.
    """
    try:
        if tf:
            # show the file we picked and the raw parsed content (not cached)
            core = _pick_latest_for_tf(tf, DATA_DIR, FILE_GLOB, SCAN_LIMIT)
            return JSONResponse({"ok": True, "picked": core})
        else:
            core = _pick_latest_any(DATA_DIR, FILE_GLOB, SCAN_LIMIT)
            return JSONResponse({"ok": True, "picked": core})
    except HTTPException as e:
        return JSONResponse({"detail": e.detail, "dir": DATA_DIR}, status_code=e.status_code)


# ---------------- Startup note ----------------
log.info("CoinAnalyzer API server starting up. DATA_DIR=%s FILE_GLOB=%s SCAN_LIMIT=%d", DATA_DIR, FILE_GLOB, SCAN_LIMIT)
