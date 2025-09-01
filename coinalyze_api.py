import os
import itertools
import requests
from typing import Any, Dict, Optional
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------------- Env / Keys ----------------
# Use a single API_KEY or a comma-separated pool in API_KEYS
_API_KEYS = [k.strip() for k in os.getenv("API_KEYS", "").split(",") if k.strip()]
if not _API_KEYS:
    single = os.getenv("API_KEY", "").strip()
    if not single:
        raise RuntimeError("Missing API_KEY or API_KEYS env var.")
    _API_KEYS = [single]

_key_cycle = itertools.cycle(_API_KEYS)

BASE = os.getenv("COINALYZE_BASE", "https://api.coinalyze.net/v1").rstrip("/")
DEFAULT_TIMEOUT = float(os.getenv("COINALYZE_TIMEOUT", "15"))

# ---------------- Session / Retries ----------------
session = requests.Session()
retries = Retry(
    total=5,
    backoff_factor=0.8,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"],
    raise_on_status=False,
)
session.mount("https://", HTTPAdapter(max_retries=retries))

def _headers() -> Dict[str, str]:
    key = next(_key_cycle)
    return {
        "Authorization": f"Bearer {key}",
        "Accept": "application/json",
        "User-Agent": "alphaops-coinalyze/1.1",
    }

# ---------------- Core GET ----------------
def _get(path: str, params: Optional[Dict[str, Any]] = None, timeout: Optional[float] = None) -> Any:
    url = f"{BASE}{path if path.startswith('/') else '/' + path}"
    r = session.get(url, headers=_headers(), params=params or {}, timeout=timeout or DEFAULT_TIMEOUT)
    r.raise_for_status()
    return r.json()

def _get_failover(paths: list, params: Optional[Dict[str, Any]] = None, timeout: Optional[float] = None) -> Any:
    last_exc = None
    for p in paths:
        try:
            return _get(p, params=params, timeout=timeout)
        except requests.HTTPError as e:
            # try next only for 404; raise immediately for 401 (auth) to avoid burning keys
            if e.response is not None and e.response.status_code == 404:
                last_exc = e
                continue
            raise
    if last_exc:
        raise last_exc
    raise RuntimeError("No paths provided to _get_failover")

# ---------------- Discovery ----------------
def get_exchanges():      return _get("/exchanges")
def get_future_markets(): return _get("/future-markets")
def get_spot_markets():   return _get("/spot-markets")

# ---------------- Snapshots (require symbols) ----------------
def get_open_interest(symbols: str, convert_to_usd: bool = False):
    return _get("/open-interest", {
        "symbols": symbols,
        "convert_to_usd": str(convert_to_usd).lower()
    })

def get_funding_rate(symbols: str):
    return _get("/funding-rate", {"symbols": symbols})

def get_predicted_funding_rate(symbols: str):
    return _get("/predicted-funding-rate", {"symbols": symbols})

# ---------------- Histories (symbols, interval, from, to) ----------------
def get_open_interest_history(symbols: str, interval: str, start_ts: int, end_ts: int, convert_to_usd: bool = False):
    return _get("/open-interest-history", {
        "symbols": symbols, "interval": interval, "from": start_ts, "to": end_ts,
        "convert_to_usd": str(convert_to_usd).lower()
    })

def get_funding_rate_history(symbols: str, interval: str, start_ts: int, end_ts: int):
    return _get("/funding-rate-history", {
        "symbols": symbols, "interval": interval, "from": start_ts, "to": end_ts
    })

def get_predicted_funding_rate_history(symbols: str, interval: str, start_ts: int, end_ts: int):
    return _get("/predicted-funding-rate-history", {
        "symbols": symbols, "interval": interval, "from": start_ts, "to": end_ts
    })

def get_liquidation_history(symbols: str, interval: str, start_ts: int, end_ts: int, convert_to_usd: bool = False):
    return _get("/liquidation-history", {
        "symbols": symbols, "interval": interval, "from": start_ts, "to": end_ts,
        "convert_to_usd": str(convert_to_usd).lower()
    })

def get_long_short_ratio_history(symbols: str, interval: str, start_ts: int, end_ts: int):
    return _get("/long-short-ratio-history", {
        "symbols": symbols, "interval": interval, "from": start_ts, "to": end_ts
    })

def get_ohlcv_history(symbols: str, interval: str, start_ts: int, end_ts: int):
    """
    Tolerant OHLCV:
      Primary:   /get-ohlcv-history   (newer; includes 'bv' (buy volume) in many deployments)
      Fallback:  /ohlcv-history       (legacy)
    """
    params = {"symbols": symbols, "interval": interval, "from": start_ts, "to": end_ts}
    return _get_failover(["/get-ohlcv-history", "/ohlcv-history"], params=params)

# ---------------- Optional: buy/sell history hook (for future) ----------------
def get_buy_sell_history(symbols: str, interval: str, start_ts: int, end_ts: int):
    """
    Placeholder: if Coinalyze exposes a dedicated taker volume endpoint later.
    Expected: list/dict with buy_volume & sell_volume per bar.
    """
    raise NotImplementedError("Buy/Sell history endpoint not published by the API.")
