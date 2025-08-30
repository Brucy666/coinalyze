import os, time, requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

API_KEY = os.getenv("API_KEY")
if not API_KEY:
    raise RuntimeError("Missing API_KEY env var.")

HEADERS = {
    "Authorization": f"Bearer {API_KEY}",
    "Accept": "application/json",
    "User-Agent": "alphaops-coinalyze/1.0"
}
BASE = "https://api.coinalyze.net/v1"
session = requests.Session()
retries = Retry(total=5, backoff_factor=0.8,
                status_forcelist=[429,500,502,503,504],
                allowed_methods=["GET"])
session.mount("https://", HTTPAdapter(max_retries=retries))

def _get(path, params=None, timeout=15):
    url = f"{BASE}{path}"
    r = session.get(url, headers=HEADERS, params=params, timeout=timeout)
    r.raise_for_status()
    return r.json()

# --- discovery ---
def get_exchanges():           return _get("/exchanges")
def get_future_markets():      return _get("/future-markets")
def get_spot_markets():        return _get("/spot-markets")

# --- current snapshots (require symbols) ---
def get_open_interest(symbols, convert_to_usd=False):
    return _get("/open-interest", {"symbols": symbols, "convert_to_usd": str(convert_to_usd).lower()})

def get_funding_rate(symbols):
    return _get("/funding-rate", {"symbols": symbols})

def get_predicted_funding_rate(symbols):
    return _get("/predicted-funding-rate", {"symbols": symbols})

# --- histories (require symbols, interval, from, to) ---
def get_open_interest_history(symbols, interval, start_ts, end_ts, convert_to_usd=False):
    return _get("/open-interest-history", {
        "symbols": symbols, "interval": interval, "from": start_ts, "to": end_ts,
        "convert_to_usd": str(convert_to_usd).lower()
    })

def get_funding_rate_history(symbols, interval, start_ts, end_ts):
    return _get("/funding-rate-history", {
        "symbols": symbols, "interval": interval, "from": start_ts, "to": end_ts
    })

def get_predicted_funding_rate_history(symbols, interval, start_ts, end_ts):
    return _get("/predicted-funding-rate-history", {
        "symbols": symbols, "interval": interval, "from": start_ts, "to": end_ts
    })

def get_liquidation_history(symbols, interval, start_ts, end_ts, convert_to_usd=False):
    return _get("/liquidation-history", {
        "symbols": symbols, "interval": interval, "from": start_ts, "to": end_ts,
        "convert_to_usd": str(convert_to_usd).lower()
    })

def get_long_short_ratio_history(symbols, interval, start_ts, end_ts):
    return _get("/long-short-ratio-history", {
        "symbols": symbols, "interval": interval, "from": start_ts, "to": end_ts
    })

def get_ohlcv_history(symbols, interval, start_ts, end_ts):
    return _get("/ohlcv-history", {
        "symbols": symbols, "interval": interval, "from": start_ts, "to": end_ts
    })

# --- optional: placeholder for buy/sell history if exposed (for true CVD) ---
def get_buy_sell_history(symbols, interval, start_ts, end_ts):
    """
    Placeholder: implement once Coinalyze reveals endpoint name for buy/sell (taker) history.
    Expected to return [{ts, buy_volume, sell_volume}, ...] for CVD computation.
    """
    raise NotImplementedError("Buy/Sell history endpoint not documented in provided screenshots.")
