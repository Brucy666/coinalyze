import os, requests, time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = "https://api.coinalyze.net/v1/exchanges"
API_KEY  = os.getenv("API_KEY")

session = requests.Session()
retries = Retry(total=5, backoff_factor=0.8,
                status_forcelist=[429, 500, 502, 503, 504],
                allowed_methods=["GET"])
session.mount("https://", HTTPAdapter(max_retries=retries))

HEADERS = {
    "Authorization": f"Bearer {API_KEY}",
    "Accept": "application/json",
    "User-Agent": "alphaops-polymerize/1.0"
}

def fetch(params=None):
    r = session.get(BASE_URL, headers=HEADERS, params=params, timeout=15)
    r.raise_for_status()
    return r.json()

if __name__ == "__main__":
    t0 = time.time()
    data = fetch()
    print("OK:", len(data), "exchanges")
    print("Sample:", data[:5])
    print("Fetched in", round(time.time()-t0, 2), "s")
