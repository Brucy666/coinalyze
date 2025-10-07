# coinalyze_api_server.py
# Read-only FastAPI for CoinAnalyzer snapshots (flat-line + nested support)

import os, time, json, glob, re, logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Iterable

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

# ----------- ENV -----------
DATA_DIR      = os.getenv("DATA_DIR", "/data")
FILE_GLOB     = os.getenv("FILE_GLOB", "**/*.json")   # recursive
SCAN_LIMIT    = int(os.getenv("SCAN_LIMIT", "1000"))
CACHE_TTL_SEC = int(os.getenv("CACHE_TTL_SEC", "5"))
LOGLEVEL      = os.getenv("LOGLEVEL", "INFO").upper()

logging.basicConfig(level=getattr(logging, LOGLEVEL, logging.INFO),
                    format="%(asctime)s %(levelname)s [coinalyze_api] %(message)s")
log = logging.getLogger("coinalyze_api")

# ----------- APP -----------
app = FastAPI(title="CoinAnalyzer API", version="2.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["GET"], allow_headers=["*"])

_cache: Dict[str, Tuple[float, Dict[str, Any]]] = {}

def _cache_get(k: str) -> Optional[Dict[str, Any]]:
    ent = _cache.get(k); 
    if not ent: return None
    exp, payload = ent
    if time.time() > exp:
        _cache.pop(k, None)
        return None
    return payload

def _cache_set(k: str, payload: Dict[str, Any], ttl: int = CACHE_TTL_SEC):
    _cache[k] = (time.time() + ttl, payload)

# ----------- FS Utils -----------
def _rscan_latest(base: str, pattern: str, limit: int) -> List[Path]:
    basep = Path(base)
    if not basep.exists():
        log.warning("DATA_DIR does not exist: %s", base); 
        return []
    paths = glob.glob(str(basep / pattern), recursive=True)
    paths.sort(key=lambda p: Path(p).stat().st_mtime if Path(p).exists() else 0, reverse=True)
    return [Path(p) for p in paths[:max(1, limit)]]

def _load_text(path: Path) -> Optional[str]:
    try:
        return path.read_text(encoding="utf-8", errors="ignore")
    except Exception as e:
        log.debug("Failed to read %s: %s", path, e); 
        return None

def _load_json(path: Path) -> Optional[Dict[str, Any]]:
    try:
        return json.loads(_load_text(path) or "{}")
    except Exception:
        return None

# ----------- parsing helpers -----------
def _safe_float(v: Any, d: float = 0.0) -> float:
    try: return float(v)
    except Exception:
        try: return float(str(v).replace(",", ""))
        except Exception: return d

def _normalize_interval(tf_raw: Any) -> str:
    s = (str(tf_raw) if tf_raw is not None else "").lower()
    s = s.replace("mins", "m").replace("min", "m").replace("hour", "h")
    mapping = {"1m":"1m","1":"1m","60s":"1m","5m":"5m","15m":"15m","1h":"1h","60":"1h","60m":"1h"}
    return mapping.get(s, s)

def _infer_raw_symbol_from_path(path: Path) -> str:
    for seg in path.parts:
        s = seg.upper()
        if s.endswith("_PERP_A") or s.endswith("_PERP") or s.endswith("_SPOT"):
            return s
        if len(s) >= 6 and s.isalnum() and s.endswith("USDT"):
            return s
    return ""

# ----------- core extraction -----------
_FLAT_RE = re.compile(
    r"TF\s*:\s*(?P<tf>[^ ]+).*(?:OI\s*:\s*(?P<oi>[-\d\.]+))?.*"
    r"(?:FR\s*:\s*(?P<fr>[-\d\.]+))?.*"
    r"(?:LIQ\s*:\s*(?P<liq>[-\d\.]+))?.*"
    r"(?:LS\s*:\s*(?P<ls>[-\d\.]+))?.*"
    r"(?:CVD\s*:\s*(?P<cvd>[-\d\.]+))?",
    re.IGNORECASE
)

def _unwrap_body(parsed: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(parsed, dict): return {}
    for key in ("DATA","SNAPSHOT","snapshot","PAYLOAD","payload"):
        if isinstance(parsed.get(key), dict):
            return parsed[key]
    return parsed

def _extract_flat_line(txt: str) -> Optional[Dict[str, Any]]:
    """Parse flat one-line summary like: TF:5min OI:94321.872 FR:0.006194 LIQ:63 LS:69 CVD:-8527.5"""
    if not txt: return None
    # use the first non-empty line
    line = next((ln.strip() for ln in txt.splitlines() if ln.strip()), "")
    if not line: return None
    m = _FLAT_RE.search(line)
    if not m: return None
    g = m.groupdict()
    tf = _normalize_interval(g.get("tf",""))
    return {
        "interval": tf,
        "oi_delta": _safe_float(g.get("oi")),
        "funding": _safe_float(g.get("fr")),
        # interpret LIQ as long-liquidation notional, LS as short-liquidation notional (adjust if reversed)
        "liq_long": _safe_float(g.get("liq")),
        "liq_short": _safe_float(g.get("ls")),
        "cvd_delta": _safe_float(g.get("cvd"), 0.0)
    }

def _extract_core_from_parsed(parsed: Dict[str, Any], path: Optional[Path]) -> Dict[str, Any]:
    raw_symbol = (parsed.get("SYMBOL") or parsed.get("symbol") or parsed.get("symbol_name") or "").upper()
    if (not raw_symbol or raw_symbol == "") and path:
        raw_symbol = _infer_raw_symbol_from_path(path)
    symbol = raw_symbol.replace("_PERP_A","") if raw_symbol.endswith("_PERP_A") else raw_symbol

    interval = _normalize_interval(parsed.get("INTERVAL") or parsed.get("interval") or parsed.get("tf") or "")
    ts = parsed.get("ts") or parsed.get("timestamp") or parsed.get("time") or time.time()
    ts = int(_safe_float(ts, time.time()))

    body = _unwrap_body(parsed)
    oi_block = body.get("OPEN_INTEREST") or body.get("open_interest") or {}
    fr_block = body.get("FUNDING_RATE") or body.get("funding_rate") or {}
    nls_block = body.get("NET_LONG_SHORT") or body.get("net_long_short")
    liqs = body.get("LIQUIDATIONS") or body.get("liquidations") or []

    oi_val = _safe_float(oi_block.get("value")) if isinstance(oi_block, dict) else 0.0
    fr_val = _safe_float(fr_block.get("fr_value") or fr_block.get("value")) if isinstance(fr_block, dict) else 0.0

    nls_val = 1.0
    if isinstance(nls_block, list) and nls_block:
        last = nls_block[-1]
        if isinstance(last, (list, tuple)) and len(last) >= 2:
            nls_val = _safe_float(last[1], 1.0)
        elif isinstance(last, dict):
            nls_val = _safe_float(last.get("value") or last.get("v"), 1.0)
    elif isinstance(nls_block, (int, float, str)):
        nls_val = _safe_float(nls_block, 1.0)

    liq_long = sum(_safe_float(x.get("b") or x.get("buy") or 0) for x in liqs if isinstance(x, dict))
    liq_short = sum(_safe_float(x.get("s") or x.get("sell") or 0) for x in liqs if isinstance(x, dict))

    # Optional CVD
    cvd = None
    trades = body.get("TRADES") or body.get("trades")
    if isinstance(trades, list) and trades:
        try:
            cvd = sum(_safe_float(t.get("delta"), 0.0) for t in trades if isinstance(t, dict))
        except Exception:
            cvd = None

    # Divergence heuristic
    cvd_div = "none"
    if liq_long or liq_short:
        if liq_short > liq_long * 1.05: cvd_div = "bullish"
        elif liq_long > liq_short * 1.05: cvd_div = "bearish"

    return {
        "symbol": symbol,
        "raw_symbol": raw_symbol,
        "interval": interval,
        "ts": ts,
        "oi_delta": oi_val,
        "funding": fr_val,
        "net_long_short": nls_val,
        "liq_long": float(liq_long),
        "liq_short": float(liq_short),
        "cvd_delta": _safe_float(cvd, 0.0) if cvd is not None else None,
        "cvd_divergence": cvd_div,
        "_file": str(path) if path else None,
    }

def _extract_core(path: Path) -> Optional[Dict[str, Any]]:
    """
    Try JSON first; if that doesn't yield metrics, parse as flat-line summary.
    """
    txt = _load_text(path)
    if not txt: 
        return None

    # 1) Try JSON + nested
    try:
        parsed = json.loads(txt)
        if isinstance(parsed, dict):
            core = _extract_core_from_parsed(parsed, path)
            if _has_metrics_parsed(parsed) or any(core.get(k) for k in ("oi_delta","funding","net_long_short","liq_long","liq_short","cvd_delta")):
                return core
    except Exception:
        pass

    # 2) Try flat-line
    flat = _extract_flat_line(txt)
    if flat:
        raw_symbol = _infer_raw_symbol_from_path(path)
        symbol = raw_symbol.replace("_PERP_A","") if raw_symbol.endswith("_PERP_A") else raw_symbol
        flat.update({
            "symbol": symbol,
            "raw_symbol": raw_symbol,
            "ts": int(Path(path).stat().st_mtime),
            "_file": str(path),
        })
        # quick cvd_div from liqs
        liq_long = flat.get("liq_long") or 0.0
        liq_short= flat.get("liq_short") or 0.0
        cvd_div = "none"
        if liq_long or liq_short:
            if liq_short > liq_long * 1.05: cvd_div = "bullish"
            elif liq_long > liq_short * 1.05: cvd_div = "bearish"
        flat["cvd_divergence"] = cvd_div
        return flat

    return None

def _has_metrics_parsed(parsed: Dict[str, Any]) -> bool:
    body = _unwrap_body(parsed)
    return any(k in body for k in ("OPEN_INTEREST","FUNDING_RATE","NET_LONG_SHORT","LIQUIDATIONS"))

def _has_metrics_any(path: Path) -> bool:
    txt = _load_text(path) or ""
    # fast path: if flat line has "TF:" it's likely usable
    if "TF:" in txt or "tf:" in txt:
        return True
    try:
        parsed = json.loads(txt)
        return _has_metrics_parsed(parsed)
    except Exception:
        return False

# ----------- Backtracker -----------
def _backtrack_latest_valid(tf: str, symbol_aliases: List[str]) -> Dict[str, Any]:
    tf_norm = _normalize_interval(tf)
    tf_aliases_map = {"1m":["1m","1min","60s"], "5m":["5m","5min"], "15m":["15m","15min"], "1h":["1h","1hour","60m"]}
    tf_aliases = tf_aliases_map.get(tf_norm, [tf_norm])
    alias_upper = [a.upper() for a in symbol_aliases]

    # Collect by PATH
    candidates: List[Path] = []
    for p in _rscan_latest(DATA_DIR, FILE_GLOB, SCAN_LIMIT * 2):
        upath = str(p).upper()
        if any(a in upath for a in alias_upper) and any(tfa in upath for tfa in tf_aliases):
            candidates.append(Path(p))
    if not candidates:
        raise HTTPException(status_code=404, detail=f"No files matched {symbol_aliases} {tf}")

    for p in candidates:
        if not _has_metrics_any(p):
            continue
        core = _extract_core(p)
        if not core:
            continue
        # accept if either JSON symbol match or path contains alias
        if (core["raw_symbol"] not in symbol_aliases
            and core["symbol"] not in symbol_aliases
            and not any(a in str(p).upper() for a in alias_upper)):
            continue
        log.info("[backtrack] valid %s %s -> %s", symbol_aliases, tf, p)
        return core

    raise HTTPException(status_code=404, detail=f"No valid data for {symbol_aliases} {tf}")

def _pick_all_intervals_for_aliases(symbol_aliases: List[str], tfs: Iterable[str]=("1m","5m","15m","1h")) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for tf in tfs:
        try:
            out[tf] = _backtrack_latest_valid(tf, symbol_aliases)
        except HTTPException:
            continue
    if not out:
        raise HTTPException(status_code=404, detail=f"No data found for {symbol_aliases}")
    return out

# ----------- API Endpoints -----------
@app.get("/healthz")
def healthz():
    return {"status": "ok" if Path(DATA_DIR).exists() else "missing_data_dir", "dir": DATA_DIR, "glob": FILE_GLOB}

@app.get("/v1/files")
def list_files(n: int = Query(50, ge=1, le=2000)):
    key = f"files:{n}"
    hit = _cache_get(key)
    if hit: return hit
    files = [str(p) for p in _rscan_latest(DATA_DIR, FILE_GLOB, n)]
    payload = {"dir": DATA_DIR, "glob": FILE_GLOB, "count": len(files), "files": files}
    _cache_set(key, payload)
    return payload

@app.get("/v1/metrics/{symbol}")
def metrics_symbol(symbol: str):
    symbol = symbol.upper()
    aliases = [symbol, f"{symbol}_PERP_A"]
    key = f"metrics:{aliases}"
    hit = _cache_get(key)
    if hit: return hit
    data = _pick_all_intervals_for_aliases(aliases)
    payload = {"ok": True, "latest": data}
    _cache_set(key, payload)
    return payload

@app.get("/v1/metrics/all")
def metrics_all():
    key = "metrics:all"
    hit = _cache_get(key)
    if hit: return hit

    sym_map: Dict[str, Dict[str, Any]] = {}
    scanned = 0
    for p in _rscan_latest(DATA_DIR, FILE_GLOB, SCAN_LIMIT):
        if not _has_metrics_any(p): 
            continue
        core = _extract_core(p)
        if not core: 
            continue
        tf = core.get("interval")
        if tf not in {"1m","5m","15m","1h"}: 
            continue

        raw = core["raw_symbol"] or ""
        norm = core["symbol"] or raw

        for key_sym in {raw, norm}:
            if not key_sym: continue
            sym_map.setdefault(key_sym, {})
            sym_map[key_sym].setdefault(tf, core)

        scanned += 1
        if scanned >= SCAN_LIMIT: break

    payload = {"ok": True, "latest": sym_map}
    _cache_set(key, payload)
    return payload

@app.get("/v1/metrics/debug")
def metrics_debug(symbol: Optional[str] = None, tf: Optional[str] = None):
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

# ----------- Startup route list -----------
@app.on_event("startup")
def _print_routes():
    route_paths = sorted({getattr(r, 'path', '') for r in app.router.routes})
    log.info("API up. DATA_DIR=%s FILE_GLOB=%s SCAN_LIMIT=%d", DATA_DIR, FILE_GLOB, SCAN_LIMIT)
    log.info("Routes: %s", ", ".join(route_paths))
