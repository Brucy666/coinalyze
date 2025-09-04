import os, time, itertools, requests
from typing import Any, Dict, Optional
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---- Keys & config ----
_API_KEYS = [k.strip() for k in os.getenv("API_KEYS","").split(",") if k.strip()]
if not _API_KEYS:
    single = os.getenv("API_KEY","").strip()
    if not single:
        raise RuntimeError("Missing API_KEY or API_KEYS env var.")
    _API_KEYS = [single]
_key_cycle = itertools.cycle(_API_KEYS)

BASE = os.getenv("COINALYZE_BASE","https://api.coinalyze.net/v1").rstrip("/")
DEFAULT_TIMEOUT = float(os.getenv("COINALYZE_TIMEOUT","20"))

# ---- Session with basic retries for DNS/connection ----
session = requests.Session()
session.mount(
    "https://",
    HTTPAdapter(
        max_retries=Retry(
            total=3, backoff_factor=0.6,
            status_forcelist=[502, 503, 504],
            allowed_methods=["GET"],
            raise_on_status=False,
        )
    ),
)

def _headers() -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {next(_key_cycle)}",
        "Accept": "application/json",
        "User-Agent": "alphaops-coinalyze/1.2",
    }

def _get(path: str, params: Optional[Dict[str, Any]] = None, timeout: Optional[float] = None) -> Any:
    """GET with resilient 429 handling. Falls back to 60s sleep if Retry-After header is malformed."""
    url = f"{BASE}{path if path.startswith('/') else '/' + path}"
    tries, max_tries = 0, 8
    backoff = 5  # for 5xx
    while True:
        tries += 1
        r = session.get(url, headers=_headers(), params=params or {}, timeout=timeout or DEFAULT_TIMEOUT)

        # Success
        if 200 <= r.status_code < 300:
            return r.json()

        # Coinalyze rate limiting
        if r.status_code == 429:
            ra = r.headers.get("Retry-After","").strip()
            try:
                wait = int(float(ra)) if ra else 60
            except Exception:
                wait = 60
            wait = max(wait, 30)  # floor
            print(f"[429] {path} -> sleep {wait}s (try {tries}/{max_tries})")
            time.sleep(wait)
            continue

        # Transient server error
        if 500 <= r.status_code < 600 and tries < max_tries:
            print(f"[{r.status_code}] {path} -> backoff {backoff}s (try {tries}/{max_tries})")
            time.sleep(backoff)
            backoff = min(int(backoff * 1.8), 120)
            continue

        # Hard errors or exhausted
        r.raise_for_status()

# ---- Discovery ----
def get_exchanges():      return _get("/exchanges")
def get_future_markets(): return _get("/future-markets")
def get_spot_markets():   return _get("/spot-markets")

# ---- Snapshots (require symbols) ----
def get_open_interest(symbols: str, convert_to_usd: bool=False):
    return _get("/open-interest", {"symbols": symbols, "convert_to_usd": str(convert_to_usd).lower()})

def get_funding_rate(symbols: str):
    return _get("/funding-rate", {"symbols": symbols})

def get_predicted_funding_rate(symbols: str):
    return _get("/predicted-funding-rate", {"symbols": symbols})

# ---- Histories (symbols, interval, from, to) ----
def get_open_interest_history(symbols: str, interval: str, start_ts: int, end_ts: int, convert_to_usd: bool=False):
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

def get_long_short_ratio_history(symbols: str, interval: str, start_ts: int, end_ts: int):
    return _get("/long-short-ratio-history", {
        "symbols": symbols, "interval": interval, "from": start_ts, "to": end_ts
    })

def get_liquidation_history(symbols: str, interval: str, start_ts: int, end_ts: int, convert_to_usd: bool=False):
    return _get("/liquidation-history", {
        "symbols": symbols, "interval": interval, "from": start_ts, "to": end_ts,
        "convert_to_usd": str(convert_to_usd).lower()
    })

def get_ohlcv_history(symbols: str, interval: str, start_ts: int, end_ts: int):
    # Prefer new path when present (includes bv), fallback to legacy
    try:
        return _get("/get-ohlcv-history", {"symbols": symbols, "interval": interval, "from": start_ts, "to": end_ts})
    except requests.HTTPError as e:
        if e.response is not None and e.response.status_code == 404:
            return _get("/ohlcv-history", {"symbols": symbols, "interval": interval, "from": start_ts, "to": end_ts})
        raise
