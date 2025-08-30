import os, time, json, requests

API_KEY   = os.getenv("API_KEY")
SYMBOL    = os.getenv("SYMBOL", "BTCUSDT_PERP.A")
INTERVAL  = os.getenv("INTERVAL", "5min")
HOURS     = int(os.getenv("FROM_HOURS", "24"))
BASE      = "https://api.coinalyze.net/v1"

assert API_KEY, "Missing API_KEY"
now  = int(time.time())
past = now - HOURS*3600

HDR = {"Authorization": f"Bearer {API_KEY}", "Accept":"application/json", "User-Agent":"alphaops-cvd-probe/1.0"}

# Candidate paths & param styles we’ve seen Coinalyze use
CANDIDATE_PATHS = [
    "/buy-sell-history",
    "/taker-volume-history",
    "/aggressor-volume-history",
    "/volume-delta-history",
    "/delta-history",
    "/cvd-history",
    "/trades-history",         # just in case
    "/footprint-history",      # just in case
]

PARAM_CANDIDATES = [
    {"symbols": SYMBOL, "interval": INTERVAL, "from": past, "to": now},
    {"symbol": SYMBOL,  "interval": INTERVAL, "from": past, "to": now},          # alt name
    {"symbols": SYMBOL, "granularity": INTERVAL, "from": past, "to": now},       # alt param
]

def good(body: str):
    t = body.lower()
    keys = ["buy_volume","sell_volume","taker_buy","taker_sell","bid_volume","ask_volume","buy","sell"]
    return any(k in t for k in keys)

def try_one(path, params):
    url = f"{BASE}{path}"
    try:
        r = requests.get(url, headers=HDR, params=params, timeout=20)
        print(f"\n== {r.request.method} {url}  → {r.status_code}")
        if r.status_code == 200:
            text = r.text
            print("BODY_PREVIEW:", text[:400].replace("\n"," ") + ("..." if len(text)>400 else ""))
            if good(text):
                print("✅ MATCH: looks like buy/sell content present.")
                # Try decode & print keys if possible
                try:
                    data = r.json()
                    sample = data[0] if isinstance(data, list) and data else data
                    if isinstance(sample, dict):
                        print("JSON_KEYS:", list(sample.keys()))
                except Exception:
                    pass
                return True, path, params
            else:
                print("200 but no obvious buy/sell fields.")
        else:
            print("Non-200:", r.text[:200].replace("\n"," "))
    except requests.RequestException as e:
        print("REQ_ERR:", repr(e))
    return False, None, None

def main():
    print("=== Coinalyze CVD Probe ===")
    print("Symbol:", SYMBOL, "| Interval:", INTERVAL, "| Hours:", HOURS)
    for path in CANDIDATE_PATHS:
        for params in PARAM_CANDIDATES:
            ok, p, pr = try_one(path, params)
            if ok:
                print("\nWINNER_ENDPOINT:", p)
                print("WINNER_PARAMS:", pr)
                print("\nACTION: Set env CVD_ENDPOINT to", p, "and keep using params style shown above.")
                return
    print("\nNo CVD endpoint found with our guesses. Paste these logs, and we’ll widen the search list.")

if __name__ == "__main__":
    main()
