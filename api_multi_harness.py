import os, time, requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

API_KEY = os.getenv("API_KEY")
HEADERS = {
    "Authorization": f"Bearer {API_KEY}",
    "Accept": "application/json",
    "User-Agent": "alphaops-polymerize/1.0"
}

session = requests.Session()
retries = Retry(total=5, backoff_factor=0.8,
                status_forcelist=[429,500,502,503,504],
                allowed_methods=["GET"])
session.mount("https://", HTTPAdapter(max_retries=retries))

ENDPOINTS = {
    "exchanges": "https://api.coinalyze.net/v1/exchanges",
    "markets": "https://api.coinalyze.net/v1/markets",
    "futures_markets": "https://api.coinalyze.net/v1/futures/markets",
    "symbols": "https://api.coinalyze.net/v1/symbols"
}

def fetch(name, url, params=None):
    r = session.get(url, headers=HEADERS, params=params, timeout=15)
    r.raise_for_status()
    return name, r.json()

if __name__ == "__main__":
    t0 = time.time()
    results = {}
    for name, url in ENDPOINTS.items():
        try:
            n, data = fetch(name, url)
            results[n] = data
            print(f"✅ {n}: {len(data)} items")
            print("Sample:", data[:3])
        except Exception as e:
            print(f"❌ {name} failed:", repr(e))
    print("\nCompleted in", round(time.time()-t0,2), "s")
