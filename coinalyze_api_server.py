# coinalyze_api_server.py
# Read-only FastAPI for CoinAnalyzer snapshots (nested extractor + symbol aliasing + route list)
import os, time, json, glob, logging
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
app = FastAPI(title="CoinAnalyzer API", version="1.4")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["GET"], allow_headers=["*"])

_cache: Dict[str, Tuple[float, Dict[str, Any]]] = {}

def _cache_get(k: str) -> Optional[Dict[str, Any]]:
    ent = _cache.get(k)
    if not ent: return None
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
    try: return float(v)
    except Exception:
        try: return float(str(v).replace(",", ""))
        except Exception: return default

def _normalize_interval(tf_raw: Any) -> str:
    s = (str(tf_raw) if tf_raw is not None else "").lower()
    s = s.replace("mins", "m").replace("min", "m").replace("hour", "h")
    mapping = {"1m":"1m","1":"1m","60s":"1m","5m":"5m","15m":"15m","1h":"1h","60":"1h","60m":"1h"}
    return mapping.get(s, s)

def _load_json(path: Path) -> Optional[Dict[str, Any]]:
    try:
        with path.open("r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception as e:
        log.debug("Failed to load %s: %s", path, e)
        return None

# ---------------- Extraction ----------------
def _extract_core(parsed: Dict[str, Any], path: Optional[Path]) -> Dict[str, Any]:
    """Extract nested metrics; tolerate token-light snapshots. Keeps both raw and normalized symbols."""
    raw_symbol = (parsed.get("SYMBOL") or parsed.get("symbol") or parsed.get("symbol_name") or "").upper()
    symbol = raw_symbol
    if symbol.endswith("_PERP_A"):
        symbol = symbol.replace("_PERP_A", "")

    interval = _normalize_interval(parsed.get("INTERVAL") or parsed.get("interval") or parsed.get("tf") or "")
    ts = parsed.get("ts") or parsed.get("timestamp") or parsed.get("time")
    try: ts = int(ts) if ts is not None else int(time.time())
    except Exception: ts = int(time.time())

    # OPEN_INTEREST
    oi_block = parsed.get("OPEN_INTEREST") or parsed.get("open_interest") or {}
    oi_val = oi_block.get("value") if isinstance(oi_block, dict) else 0.0

    # FUNDING_RATE
    fr_block = parsed.get("FUNDING_RATE") or parsed.get("funding_rate") or {}
    fr_val = 0.0
    if isinstance(fr_block, dict):
        fr_val = _safe_float(fr_block.get("fr_value") or fr_block.get("value"), 0.0)

    # NET_LONG_SHORT (use last item if list of pairs)
    nls_val = 1.0
    nls_block = parsed.get("NET_LONG_SHORT") or parsed.get("net_long_short")
    if isinstance(nls_block, list) and nls_block:
        last = nls_block[-1]
        if isinstance(last, (list, tuple)) and len(last) >= 2:
            nls_val = _safe_float(last[1], 1.0)
        elif isinstance(last, dict):
            nls_val = _safe_float(last.get("value") or last.get("v"), 1.0)
    elif isinstance(nls_block, (int, float, str)):
        nls_val = _safe_float(nls_block, 1.0)

    # LIQUIDATIONS: sum long(buy)/short(sell)
    liqs = parsed.get("LIQUIDATIONS") or parsed.get("liquidations") or []
    liq_long = 0.0; liq_short = 0.0
    if isinstance(liqs, list):
        for it in liqs:
            if not isinstance(it, dict): continue
            liq_long += _safe_float(it.get("b") or it.get("buy") or it.get("long"), 0.0)
            liq_short+= _safe_float(it.get("s") or it.get("sell") or it.get("short"), 0.0)

    # Optional CVD from trades/history
    cvd_delta = None
    trades = parsed.get("TRADES") or parsed.get("trades")
    if isinstance(trades, list) and trades:
        last = trades[-1]
        if isinstance(last, dict):
            cvd_delta = last.get("cvd") or last.get("CVD")
        if cvd_delta is None:
            try:
                cvd_delta = sum(_safe_float(it.get("delta"), 0.0) for it in trades[-200:] if isinstance(it, dict))
            except Exception:
                cvd_delta = None

    # Divergence heuristic from liqs
    cvd_div = "none"
    if liq_long or liq_short:
        if liq_short > liq_long * 1.05: cvd_div = "bullish"
        elif liq_long > liq_short * 1.05: cvd_div = "bearish"

    # Price (optional)
    last_price = None
    hist = parsed.get("HISTORY") or parsed.get("history")
    if isinstance(hist, list) and hist:
        last = hist[-1]
        if isinstance(last, dict): last_price = _safe_float(last.get("c") or last.get("close"), None)

    return {
        "symbol": symbol,           # normalized e.g. BTCUSDT
        "raw_symbol": raw_symbol,   # original e.g. BTCUSDT_PERP_A
        "interval": interval,
        "ts": ts,
        "oi_delta": _safe_float(oi_val, 0.0),
        "funding": _safe_float(fr_val, 0.0),
        "net_long_short": _safe_float(nls_val, 1.0),
        "liq_long": float(liq_long),
        "liq_short": float(liq_short),
        "cvd_delta": _safe_float(cvd_delta, 0.0) if cvd_delta is not None else None,
        "cvd_divergence": cvd_div,
        "last_price": last_price,
        "_file": str(path) if path else None,
    }

def _has_metrics(parsed: Dict[str, Any]) -> bool:
    """File is 'complete' if it has any of OI / FR / NLS / LIQUIDATIONS."""
    if not isinstance(parsed, dict): return False
    if isinstance(parsed.get("OPEN_INTEREST"), dict): return True
    if isinstance(parsed.get("FUNDING_RATE"), dict): return True
    if isinstance(parsed.get("NET_LONG_SHORT"), list): return True
    if isinstance(parsed.get("LIQUIDATIONS"), list): return True
    return False

def _backtrack_latest_valid(tf: str, symbol_aliases: List[str]) -> Dict[str, Any]:
    """Backtrack newest→older under TF folder until we find a file for any alias that has metrics."""
    folder_map = {"1m":"1min","5m":"5min","15m":"15min","1h":"1hour"}
    folder = folder_map.get(_normalize_interval(tf), tf)
    pattern = f"**/{folder}/**/*.json"
    for p in _rscan_latest(DATA_DIR, pattern, SCAN_LIMIT):
        parsed = _load_json(p)
        if not parsed: continue
        core = _extract_core(parsed, p)
        if core["raw_symbol"] not in symbol_aliases and core["symbol"] not in symbol_aliases:
            continue
        if _has_metrics(parsed):
            return core
    raise HTTPException(status_code=404, detail=f"No valid data for {symbol_aliases} {tf}")

def _pick_all_intervals_for_aliases(symbol_aliases: List[str], tfs: Iterable[str]=("1m","5m","15m","1h")) -> Dict[str, Any]:
    out = {}
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
    if hit: return hit
    files = [str(p) for p in _rscan_latest(DATA_DIR, FILE_GLOB, n)]
    payload = {"dir": DATA_DIR, "glob": FILE_GLOB, "count": len(files), "files": files}
    _cache_set(key, payload)
    return payload

@app.get("/v1/metrics/all")
def metrics_all():
    """Return latest cores for all symbols and intervals (both raw+normalized keys)."""
    key = "metrics:all"
    hit = _cache_get(key)
    if hit: return hit

    sym_map: Dict[str, Dict[str, Any]] = {}
    scanned = 0

    for p in _rscan_latest(DATA_DIR, FILE_GLOB, SCAN_LIMIT):
        parsed = _load_json(p)
        if not parsed: continue
        if not _has_metrics(parsed): continue
        core = _extract_core(parsed, p)
        tf  = core.get("interval")
        if tf not in {"1m","5m","15m","1h"}: continue

        raw = core["raw_symbol"] or ""
        norm = core["symbol"] or raw

        for key_sym in {raw, norm}:
            if not key_sym: continue
            if key_sym not in sym_map:
                sym_map[key_sym] = {}
            if tf not in sym_map[key_sym]:
                sym_map[key_sym][tf] = core

        scanned += 1
        if scanned >= SCAN_LIMIT: break

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
    if hit: return hit
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
