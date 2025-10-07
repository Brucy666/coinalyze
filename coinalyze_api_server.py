# coinalyze_api_server.py
# Read-only FastAPI for CoinAnalyzer snapshots
# - Recursively scans DATA_DIR for *.json
# - Infers symbol from path when JSON lacks it (e.g., BTCUSDT_PERP_A -> BTCUSDT)
# - Extracts nested metrics (OPEN_INTEREST.value, FUNDING_RATE.fr_value,
#   latest NET_LONG_SHORT, LIQUIDATIONS sums; optional CVD from TRADES/HISTORY)
# - Backtracks newest→older per timeframe until a file with metrics is found
# - Endpoints: /healthz, /v1/files, /v1/metrics/all, /v1/metrics/{symbol}, /v1/metrics/debug
#
# ENV (defaults):
#   DATA_DIR=/data
#   FILE_GLOB=**/*.json
#   SCAN_LIMIT=1000
#   CACHE_TTL_SEC=5
#   LOGLEVEL=INFO

import os
import time
import json
import glob
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Iterable

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

# ---------------- ENV ----------------
DATA_DIR      = os.getenv("DATA_DIR", "/data")
FILE_GLOB     = os.getenv("FILE_GLOB", "**/*.json")   # recursive
SCAN_LIMIT    = int(os.getenv("SCAN_LIMIT", "1000"))
CACHE_TTL_SEC = int(os.getenv("CACHE_TTL_SEC", "5"))
LOGLEVEL      = os.getenv("LOGLEVEL", "INFO").upper()

logging.basicConfig(
    level=getattr(logging, LOGLEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s [coinalyze_api] %(message)s"
)
log = logging.getLogger("coinalyze_api")

# ---------------- APP ----------------
app = FastAPI(title="CoinAnalyzer API", version="1.8")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["GET"], allow_headers=["*"])

_cache: Dict[str, Tuple[float, Dict[str, Any]]] = {}

def _cache_get(k: str) -> Optional[Dict[str, Any]]:
    ent = _cache.get(k)
    if not ent:
        return None
    exp, payload = ent
    if time.time() > exp:
        _cache.pop(k, None)
        return None
    return payload

def _cache_set(k: str, payload: Dict[str, Any], ttl: int = CACHE_TTL_SEC):
    _cache[k] = (time.time() + ttl, payload)

# ---------------- Utils ----------------
def _rscan_latest(base: str, pattern: str, limit: int) -> List[Path]:
    basep = Path(base)
    if not basep.exists():
        log.warning("DATA_DIR does not exist: %s", base)
        return []
    paths = glob.glob(str(basep / pattern), recursive=True)
    paths.sort(key=lambda p: Path(p).stat().st_mtime if Path(p).exists() else 0, reverse=True)
    return [Path(p) for p in paths[:max(1, limit)]]

def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        try:
            return float(str(v).replace(",", ""))
        except Exception:
            return default

def _normalize_interval(tf_raw: Any) -> str:
    s = (str(tf_raw) if tf_raw is not None else "").lower()
    s = s.replace("mins", "m").replace("min", "m").replace("hour", "h")
    mapping = {"1m":"1m", "1":"1m", "60s":"1m",
               "5m":"5m",
               "15m":"15m",
               "1h":"1h", "60":"1h", "60m":"1h"}
    return mapping.get(s, s)

def _load_json(path: Path) -> Optional[Dict[str, Any]]:
    try:
        with path.open("r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception as e:
        log.debug("Failed to load %s: %s", path, e)
        return None

def _infer_raw_symbol_from_path(path: Path) -> str:
    """Infer symbol from a path segment like BTCUSDT_PERP_A or BTCUSDT."""
    for seg in path.parts:
        s = seg.upper()
        if s.endswith("_PERP_A") or s.endswith("_PERP") or s.endswith("_SPOT"):
            return s
        if len(s) >= 6 and s.isalnum() and s.endswith("USDT"):
            return s
    return ""

# ---------------- Extraction ----------------
def _unwrap_body(parsed: Dict[str, Any]) -> Dict[str, Any]:
    """Return the dict that actually contains metrics: top-level or under DATA/SNAPSHOT/PAYLOAD."""
    if not isinstance(parsed, dict):
        return {}
    for key in ("DATA", "SNAPSHOT", "PAYLOAD"):
        if isinstance(parsed.get(key), dict):
            return parsed[key]
    return parsed

def _extract_core(parsed: Dict[str, Any], path: Optional[Path]) -> Dict[str, Any]:
    """Extract nested metrics; tolerant of token-light snapshots. Keeps both raw and normalized symbols."""
    raw_symbol = (parsed.get("SYMBOL") or parsed.get("symbol") or parsed.get("symbol_name") or "").upper()
    if (not raw_symbol or raw_symbol == "") and path:
        raw_symbol = _infer_raw_symbol_from_path(path)

    symbol = raw_symbol
    if symbol.endswith("_PERP_A"):
        symbol = symbol.replace("_PERP_A", "")

    interval = _normalize_interval(parsed.get("INTERVAL") or parsed.get("interval") or parsed.get("tf") or "")
    ts = parsed.get("ts") or parsed.get("timestamp") or parsed.get("time")
    try:
        ts = int(ts) if ts is not None else int(time.time())
    except Exception:
        ts = int(time.time())

    body = _unwrap_body(parsed)

    # OPEN_INTEREST
    oi_block = body.get("OPEN_INTEREST") or body.get("open_interest") or {}
    oi_val = _safe_float(oi_block.get("value"), 0.0) if isinstance(oi_block, dict) else 0.0

    # FUNDING_RATE
    fr_block = body.get("FUNDING_RATE") or body.get("funding_rate") or {}
    fr_val = 0.0
    if isinstance(fr_block, dict):
        fr_val = _safe_float(fr_block.get("fr_value") or fr_block.get("value"), 0.0)

    # NET_LONG_SHORT (list of [ts, value] or dict with value)
    nls_val = 1.0
    nls_block = body.get("NET_LONG_SHORT") or body.get("net_long_short")
    if isinstance(nls_block, list) and nls_block:
        last = nls_block[-1]
        if isinstance(last, (list, tuple)) and len(last) >= 2:
            nls_val = _safe_float(last[1], 1.0)
        elif isinstance(last, dict):
            nls_val = _safe_float(last.get("value") or last.get("v"), 1.0)
    elif isinstance(nls_block, (int, float, str)):
        nls_val = _safe_float(nls_block, 1.0)

    # LIQUIDATIONS: sum long(buy)/short(sell)
    liqs = body.get("LIQUIDATIONS") or body.get("liquidations") or []
    liq_long = 0.0
    liq_short = 0.0
    if isinstance(liqs, list):
        for it in liqs:
            if not isinstance(it, dict):
                continue
            liq_long += _safe_float(it.get("b") or it.get("buy") or it.get("long"), 0.0)
            liq_short += _safe_float(it.get("s") or it.get("sell") or it.get("short"), 0.0)

    # Optional CVD from trades/history
    cvd_delta = None
    trades = body.get("TRADES") or body.get("trades")
    if isinstance(trades, list) and trades:
        try:
            cvd_delta = sum(_safe_float(t.get("delta"), 0.0) for t in trades if isinstance(t, dict))
        except Exception:
            cvd_delta = None

    # Divergence heuristic from liqs
    cvd_div = "none"
    if liq_long or liq_short:
        if liq_short > liq_long * 1.05:
            cvd_div = "bullish"
        elif liq_long > liq_short * 1.05:
            cvd_div = "bearish"

    return {
        "symbol": symbol,                 # normalized e.g. BTCUSDT
        "raw_symbol": raw_symbol,         # original e.g. BTCUSDT_PERP_A
        "interval": interval,
        "ts": int(ts),
        "oi_delta": oi_val,
        "funding": fr_val,
        "net_long_short": nls_val,
        "liq_long": float(liq_long),
        "liq_short": float(liq_short),
        "cvd_delta": _safe_float(cvd_delta, 0.0) if cvd_delta is not None else None,
        "cvd_divergence": cvd_div,
        "_file": str(path) if path else None,
    }

def _has_metrics(parsed: Dict[str, Any]) -> bool:
    """Return True if metrics exist at top or under DATA/SNAPSHOT/PAYLOAD."""
    if not isinstance(parsed, dict):
        return False
    body = _unwrap_body(parsed)
    return any(k in body for k in ("OPEN_INTEREST", "FUNDING_RATE", "NET_LONG_SHORT", "LIQUIDATIONS"))

# ---------------- Backtracker ----------------
def _backtrack_latest_valid(tf: str, symbol_aliases: List[str]) -> Dict[str, Any]:
    """
    Backtrack newest → older for the given timeframe and symbol aliases.

    - Accepts BOTH raw and normalized symbols (e.g., BTCUSDT_PERP_A and BTCUSDT).
    - Matches by *path* (folder names) as well as by JSON 'SYMBOL' field.
    - Tolerates different TF folder spellings (1m/1min/60s, 5m/5min, 15m/15min, 1h/1hour/60m).
    - Returns the first file that contains usable metrics (OI/FR/NLS/LIQS).
    """
    tf_norm = _normalize_interval(tf)
    tf_aliases_map = {
        "1m":  ["1m", "1min", "60s"],
        "5m":  ["5m", "5min"],
        "15m": ["15m", "15min"],
        "1h":  ["1h", "1hour", "60m"],
    }
    tf_aliases = tf_aliases_map.get(tf_norm, [tf_norm])
    alias_upper = [a.upper() for a in symbol_aliases]

    # Collect candidate files by PATH (fast and robust)
    candidates: List[Path] = []
    for p in _rscan_latest(DATA_DIR, FILE_GLOB, SCAN_LIMIT * 2):
        upath = str(p).upper()
        if any(a in upath for a in alias_upper) and any(tfa in upath for tfa in tf_aliases):
            candidates.append(Path(p))

    if not candidates:
        raise HTTPException(status_code=404, detail=f"No files matched {symbol_aliases} {tf}")

    # Try candidates newest → older until we find valid metrics
    for p in candidates:
        parsed = _load_json(p)
        if not parsed:
            continue
        core = _extract_core(parsed, p)

        # Accept if JSON symbol match OR path contains alias (path fallback)
        if (core["raw_symbol"] not in symbol_aliases
            and core["symbol"] not in symbol_aliases
            and not any(a in str(p).upper() for a in alias_upper)):
            continue

        if _has_metrics(parsed):
            log.info("[backtrack] valid %s %s -> %s", symbol_aliases, tf, p)
            return core

    raise HTTPException(status_code=404, detail=f"No valid data for {symbol_aliases} {tf}")

def _pick_all_intervals_for_aliases(symbol_aliases: List[str], tfs: Iterable[str]=("1m", "5m", "15m", "1h")) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for tf in tfs:
        try:
            out[tf] = _backtrack_latest_valid(tf, symbol_aliases)
        except HTTPException:
            continue
    if not out:
        raise HTTPException(status_code=404, detail=f"No data found for {symbol_aliases}")
    return out

# ---------------- Endpoints ----------------
@app.get("/healthz")
def healthz():
    return {"status": "ok" if Path(DATA_DIR).exists() else "missing_data_dir",
            "dir": DATA_DIR, "glob": FILE_GLOB}

@app.get("/v1/files")
def list_files(n: int = Query(50, ge=1, le=2000)):
    key = f"files:{n}"
    hit = _cache_get(key)
    if hit:
        return hit
    files = [str(p) for p in _rscan_latest(DATA_DIR, FILE_GLOB, n)]
    payload = {"dir": DATA_DIR, "glob": FILE_GLOB, "count": len(files), "files": files}
    _cache_set(key, payload)
    return payload

@app.get("/v1/metrics/all")
def metrics_all():
    """Return latest cores for all symbols and intervals (both raw+normalized keys)."""
    key = "metrics:all"
    hit = _cache_get(key)
    if hit:
        return hit

    sym_map: Dict[str, Dict[str, Any]] = {}
    scanned = 0

    for p in _rscan_latest(DATA_DIR, FILE_GLOB, SCAN_LIMIT):
        parsed = _load_json(p)
        if not parsed or not _has_metrics(parsed):
            continue
        core = _extract_core(parsed, p)
        tf  = core.get("interval")
        if tf not in {"1m", "5m", "15m", "1h"}:
            continue

        raw = core["raw_symbol"] or ""
        norm = core["symbol"] or raw

        for key_sym in {raw, norm}:
            if not key_sym:
                continue
            sym_map.setdefault(key_sym, {})
            sym_map[key_sym].setdefault(tf, core)

        scanned += 1
        if scanned >= SCAN_LIMIT:
            break

    payload = {"ok": True, "latest": sym_map}
    _cache_set(key, payload)
    return payload

@app.get("/v1/metrics/{symbol}")
def metrics_symbol(symbol: str):
    """Return latest cores for a single symbol (normalized or raw)."""
    symbol = symbol.upper()
    aliases = [symbol, f"{symbol}_PERP_A"]
    key = f"metrics:{aliases}"
    hit = _cache_get(key)
    if hit:
        return hit
    data = _pick_all_intervals_for_aliases(aliases)
    payload = {"ok": True, "latest": data}
    _cache_set(key, payload)
    return payload

@app.get("/v1/metrics/debug")
def metrics_debug(symbol: Optional[str] = None, tf: Optional[str] = None):
    """Debug picker (no cache). Accepts normalized or raw symbols."""
    try:
        if symbol and tf:
            aliases = [symbol.upper(), f"{symbol.upper()}_PERP_A"]
            core = _backtrack_latest_valid(tf, aliases)
            return {"ok": True, "picked": core}
        elif symbol:
            aliases = [symbol.upper(), f"{symbol.upper()}_PERP_A"]
            return {"ok": True, "picked": _pick_all_intervals_for_aliases(aliases)}
        else:
            files = [str(p) for p in _rscan_latest(DATA_DIR, FILE_GLOB, 25)]
            return {"ok": True, "files": files}
    except HTTPException as e:
        return JSONResponse({"detail": e.detail}, status_code=e.status_code)

# ------------- Print route list at startup (so you see it in Railway logs) -------------
@app.on_event("startup")
def _print_routes():
    route_paths = sorted({getattr(r, 'path', '') for r in app.router.routes})
    log.info("API up. DATA_DIR=%s FILE_GLOB=%s SCAN_LIMIT=%d", DATA_DIR, FILE_GLOB, SCAN_LIMIT)
    log.info("Routes: %s", ", ".join(route_paths))
