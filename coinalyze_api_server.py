# coinalyze_api_server.py
"""
CoinAnalyzer API server (rewritten extractor)

Features:
- Scans JSON snapshot files under DATA_DIR using FILE_GLOB.
- Extracts nested values: OPEN_INTEREST, FUNDING_RATE, NET_LONG_SHORT, LIQUIDATIONS, TRADES/HISTORY.
- Returns compact "core" metrics via FastAPI endpoints:
    GET /v1/metrics/all
    GET /v1/metrics/{symbol}
"""

import os
import time
import json
import glob
import logging
from pathlib import Path
from typing import Any, Dict, Generator, Iterable, List, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

# ----------------------------
# Config (environment friendly)
# ----------------------------
DATA_DIR = os.environ.get("DATA_DIR", "/data")
FILE_GLOB = os.environ.get("FILE_GLOB", "**/*.json")
SCAN_LIMIT = int(os.environ.get("SCAN_LIMIT", "1000"))  # how many files to scan at most when searching latest

# Logging
logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))
log = logging.getLogger("coinalyze_api")

# FastAPI
app = FastAPI(title="CoinAnalyzer API", version="0.1")

# ----------------------------
# Utility functions
# ----------------------------


def _rscan_latest(base_dir: str, pattern: str, limit: int = 1000) -> Generator[Path, None, None]:
    """
    Yield file Paths matching pattern sorted by mtime (descending / newest first).
    pattern is a glob pattern relative to base_dir (eg. '**/*.json').
    """
    p = Path(base_dir)
    if not p.exists():
        log.warning("DATA_DIR does not exist: %s", base_dir)
        return

    # Use glob to produce list, then sort by mtime
    full_pattern = str(p / pattern)
    files = glob.glob(full_pattern, recursive=True)
    files_sorted = sorted(files, key=lambda x: Path(x).stat().st_mtime, reverse=True)
    for i, f in enumerate(files_sorted):
        if i >= limit:
            break
        yield Path(f)


def _safe_load_json(path: Path) -> Optional[Dict[str, Any]]:
    try:
        with path.open("r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception as e:
        log.exception("Failed to load json %s: %s", path, e)
        return None


def _coalesce_float(v: Any, default: float = 0.0) -> float:
    try:
        if v is None:
            return float(default)
        return float(v)
    except Exception:
        return default


# ----------------------------
# Extraction logic (core)
# ----------------------------


def _normalize_interval(interval_raw: str) -> str:
    """
    Normalise interval token. Accept some variants.
    """
    if not interval_raw:
        return ""
    t = str(interval_raw).lower()
    # handle common forms
    mapping = {
        "1min": "1m",
        "1m": "1m",
        "5min": "5m",
        "5m": "5m",
        "15min": "15m",
        "15m": "15m",
        "1hour": "1h",
        "1h": "1h",
    }
    for k, v in mapping.items():
        if t.startswith(k):
            return v
    return t


def _extract_core(parsed: Dict[str, Any], src_path: Optional[Path] = None) -> Dict[str, Any]:
    """
    Extract a compact core dictionary from a raw snapshot parsed JSON.
    Pull nested data (OPEN_INTEREST, FUNDING_RATE, LIQUIDATIONS, NET_LONG_SHORT, HISTORY, TRADES).
    """
    # get top-level fields safely
    symbol = parsed.get("SYMBOL") or parsed.get("symbol") or parsed.get("symbol_name")
    interval_raw = parsed.get("INTERVAL") or parsed.get("interval") or parsed.get("tf") or ""
    interval = _normalize_interval(interval_raw)
    ts = parsed.get("ts") or parsed.get("TS") or parsed.get("timestamp") or parsed.get("time")
    try:
        ts = int(ts) if ts is not None else int(time.time())
    except Exception:
        ts = int(time.time())

    # OPEN_INTEREST
    oi_val = None
    oi = parsed.get("OPEN_INTEREST") or parsed.get("open_interest") or {}
    if isinstance(oi, dict):
        # possible shapes: {"value": 12345.6, ...} or nested lists
        oi_val = oi.get("value") or oi.get("oi") or oi.get("oi_value")
    if oi_val is None:
        # attempt to find `open_interest` inside arrays
        try:
            # some snapshots contain lists of objects in the "open_interest" key
            if isinstance(oi, list) and len(oi) and isinstance(oi[0], dict):
                oi_val = oi[0].get("value") or oi[0].get("oi")
        except Exception:
            oi_val = None
    oi_delta = _coalesce_float(oi_val, 0.0)

    # FUNDING_RATE
    fr_val = None
    fr = parsed.get("FUNDING_RATE") or parsed.get("funding_rate") or {}
    if isinstance(fr, dict):
        fr_val = fr.get("fr_value") or fr.get("value") or fr.get("funding")
    fr_val = _coalesce_float(fr_val, 0.0)

    # NET_LONG_SHORT
    nls_val = None
    nls = parsed.get("NET_LONG_SHORT") or parsed.get("net_long_short")
    if nls is None:
        # sometimes stored as list of [ [ts, value], ... ]
        nl = parsed.get("NET_LONG_SHORT_HISTORY") or parsed.get("net_long_short_history")
        if isinstance(nl, list) and nl:
            last = nl[-1]
            # either pair [ts, val] or dict
            if isinstance(last, (list, tuple)) and len(last) >= 2:
                nls_val = last[1]
            elif isinstance(last, dict):
                nls_val = last.get("value") or last.get("v")
    else:
        nls_val = nls
    nls_val = _coalesce_float(nls_val, 1.0)

    # LIQUIDATIONS
    liq_long_total = 0.0
    liq_short_total = 0.0
    liqs = parsed.get("LIQUIDATIONS") or parsed.get("liquidations") or []
    # liqs often looks like: [{"t":ts,"b":123.4,"s":45.6}, ...] where b=buy (long), s=sell (short) OR vice versa
    if isinstance(liqs, list):
        for item in liqs:
            if not isinstance(item, dict):
                continue
            # common keys: "b", "s" or "buy", "sell", "long", "short"
            b = item.get("b") or item.get("buy") or item.get("long") or 0.0
            s = item.get("s") or item.get("sell") or item.get("short") or 0.0
            try:
                liq_long_total += float(b or 0.0)
                liq_short_total += float(s or 0.0)
            except Exception:
                continue

    # Try to extract CVD/trades deltas from trades or history (if present)
    cvd_delta = None
    cvd_history = parsed.get("TRADES") or parsed.get("trades") or parsed.get("HISTORY") or parsed.get("history")
    if isinstance(cvd_history, list) and cvd_history:
        # Search last few entries for 'cvd' or 'delta' keys and sum or take last.
        # This is heuristic: prefer 'cvd' in last element; fallback to summing 'delta' entries.
        last = cvd_history[-1]
        if isinstance(last, dict):
            cvd_delta = last.get("cvd") or last.get("CVD") or last.get("cumulative_delta")
        # fallback: sum 'delta' from a small selection
        if cvd_delta is None:
            try:
                s = 0.0
                for it in cvd_history[-200:]:
                    if isinstance(it, dict) and "delta" in it:
                        s += float(it.get("delta", 0.0) or 0.0)
                cvd_delta = s
            except Exception:
                cvd_delta = None

    # Determine simple cvd_divergence (heuristic)
    cvd_div = "none"
    if (liq_long_total or liq_short_total):
        if liq_short_total > liq_long_total * 1.05:
            cvd_div = "bullish"  # more short liquidations -> push price up
        elif liq_long_total > liq_short_total * 1.05:
            cvd_div = "bearish"

    # Also capture basic OHLC last value if available
    ohlcv_snapshot = parsed.get("HISTORY") or parsed.get("history") or parsed.get("ohlc") or parsed.get("OHLC")
    last_price = None
    if isinstance(ohlcv_snapshot, list) and ohlcv_snapshot:
        last = ohlcv_snapshot[-1]
        if isinstance(last, dict):
            last_price = last.get("c") or last.get("close")
    # Build core dict
    core = {
        "symbol": symbol,
        "interval": interval,
        "ts": int(ts),
        "oi_delta": _coalesce_float(oi_delta, 0.0),
        "funding": _coalesce_float(fr_val, 0.0),
        "net_long_short": _coalesce_float(nls_val, 1.0),
        "liq_long": float(liq_long_total),
        "liq_short": float(liq_short_total),
        "cvd_delta": _coalesce_float(cvd_delta, 0.0) if cvd_delta is not None else None,
        "cvd_divergence": cvd_div,
        "last_price": _coalesce_float(last_price, None) if last_price is not None else None,
        "_file": str(src_path) if src_path is not None else None,
    }
    return core


# ----------------------------
# High-level scan helpers
# ----------------------------


def pick_latest_for_intervals(symbol_filter: Optional[str] = None, intervals: Optional[Iterable[str]] = None) -> Dict[str, Dict[str, Any]]:
    """
    Return latest core for each interval for given symbol_filter.
    Returns mapping interval -> core dict
    """
    result: Dict[str, Dict[str, Any]] = {}
    intervals = set(intervals or ["1m", "5m", "15m", "1h"])
    # Scan newest files first
    for path in _rscan_latest(DATA_DIR, FILE_GLOB, limit=SCAN_LIMIT):
        parsed = _safe_load_json(path)
        if not parsed:
            continue
        core = _extract_core(parsed, path)
        if not core.get("symbol"):
            continue
        if symbol_filter and str(core["symbol"]).lower() != str(symbol_filter).lower():
            continue
        interval = core.get("interval") or ""
        if interval in intervals and interval not in result:
            result[interval] = core
            # if found all intervals, break early
            if set(result.keys()) >= intervals:
                break
    return result


# ----------------------------
# API models (optional)
# ----------------------------


class MetricsAllResponse(BaseModel):
    ok: bool
    latest: Dict[str, Dict[str, Any]]


# ----------------------------
# Endpoints
# ----------------------------


@app.get("/v1/metrics/all", response_model=MetricsAllResponse)
def metrics_all():
    """
    Return latest cores for all symbols / intervals found (limited).
    Structure:
      { "ok": True, "latest": { "BTCUSDT": { "1m": {...}, "5m": {...} }, "ETHUSDT": {...} } }
    Implementation: scan files newest first and pick the first hit per symbol/interval.
    """
    # We'll build map: symbol -> interval -> core
    latest_map: Dict[str, Dict[str, Dict[str, Any]]] = {}
    max_symbols = 200
    scanned = 0

    for path in _rscan_latest(DATA_DIR, FILE_GLOB, limit=SCAN_LIMIT):
        parsed = _safe_load_json(path)
        if not parsed:
            continue
        core = _extract_core(parsed, path)
        symbol = core.get("symbol")
        interval = core.get("interval")
        if not symbol or not interval:
            continue
        if symbol not in latest_map:
            latest_map[symbol] = {}
        if interval not in latest_map[symbol]:
            latest_map[symbol][interval] = core
        # avoid scanning forever; stop if we've collected enough symbols
        scanned += 1
        if len(latest_map) >= max_symbols:
            break
        # small safety break to keep response prompt
        if scanned >= SCAN_LIMIT:
            break

    return {"ok": True, "latest": latest_map}


@app.get("/v1/metrics/{symbol}", response_model=MetricsAllResponse)
def metrics_for_symbol(symbol: str):
    """
    Return the latest cores for requested symbol across the main intervals.
    """
    intervals = ["1m", "5m", "15m", "1h"]
    picked = pick_latest_for_intervals(symbol_filter=symbol, intervals=intervals)
    if not picked:
        raise HTTPException(status_code=404, detail=f"No data found for {symbol}")
    return {"ok": True, "latest": picked}


@app.get("/")
def root():
    return {"status": "ok", "dir": DATA_DIR, "glob": FILE_GLOB}


# ----------------------------
# Run with uvicorn if executed directly
# ----------------------------
if __name__ == "__main__":
    import uvicorn

    log.info("Starting CoinAnalyzer API server. DATA_DIR=%s FILE_GLOB=%s SCAN_LIMIT=%s", DATA_DIR, FILE_GLOB, SCAN_LIMIT)
    uvicorn.run("coinalyze_api_server:app", host="0.0.0.0", port=8080, log_level="info", reload=False)
