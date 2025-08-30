import os, time, requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

API_KEY = os.getenv("API_KEY")

HEADERS = {
    "Authorization": f"Bearer {API_KEY}",
    "Accept": "application/json",
    "User-Agent": "alphaops-coinalyze/1.0"
}

BASE = "https://api.coinalyze.net"

session = requests.Session()
retries = Retry(total=5, backoff_factor=0.8,
                status_forcelist=[429,500,502,503,504],
                allowed_methods=["GET"])
session.mount("https://", HTTPAdapter(max_retries=retries))

def fetch(path, params=None):
    url = f"{BASE}{path}"
    r = session.get(url, headers=HEADERS, params=params, timeout=15)
    r.raise_for_status()
    return r.json()

# --- Public Wrappers ---

def get_exchanges():
    return fetch("/exchanges")

def get_future_markets():
    return fetch("/future-markets")

def get_spot_markets():
    return fetch("/spot-markets")

def get_open_interest(symbols, convert_to_usd=False):
    return fetch("/open-interest", {"symbols": symbols, "convert_to_usd": str(convert_to_usd).lower()})

def get_funding_rate(symbols):
    return fetch("/funding-rate", {"symbols": symbols})

def get_predicted_funding_rate(symbols):
    return fetch("/predicted-funding-rate", {"symbols": symbols})

def get_open_interest_history(symbols, interval, start, end, convert_to_usd=False):
    return fetch("/open-interest-history", {
        "symbols": symbols,
        "interval": interval,
        "from": start,
        "to": end,
        "convert_to_usd": str(convert_to_usd).lower()
    })

def get_funding_rate_history(symbols, interval, start, end):
    return fetch("/funding-rate-history", {
        "symbols": symbols,
        "interval": interval,
        "from": start,
        "to": end
    })

def get_predicted_funding_rate_history(symbols, interval, start, end):
    return fetch("/predicted-funding-rate-history", {
        "symbols": symbols,
        "interval": interval,
        "from": start,
        "to": end
    })

def get_liquidation_history(symbols, interval, start, end, convert_to_usd=False):
    return fetch("/liquidation-history", {
        "symbols": symbols,
        "interval": interval,
        "from": start,
        "to": end,
        "convert_to_usd": str(convert_to_usd).lower()
    })

def get_long_short_ratio_history(symbols, interval, start, end):
    return fetch("/long-short-ratio-history", {
        "symbols": symbols,
        "interval": interval,
        "from": start,
        "to": end
    })

def get_ohlcv_history(symbols, interval, start, end):
    return fetch("/ohlcv-history", {
        "symbols": symbols,
        "interval": interval,
        "from": start,
        "to": end
    })

# --- Demo ---
if __name__ == "__main__":
    print("✅ Exchanges:", len(get_exchanges()))
    print("✅ Futures:", len(get_future_markets()))
    print("✅ Spots:", len(get_spot_markets()))

    # Example: BTCUSDT perpetual open interest
    symbols = "BTCUSDTPERP.BINANCE"
    now = int(time.time())
    past = now - 3600*24  # last 24h

    print("Open Interest (current):", get_open_interest(symbols))
    print("Funding Rate (current):", get_funding_rate(symbols))
    print("OI History (24h):", get_open_interest_history(symbols, "1hour", past, now)[:2])
