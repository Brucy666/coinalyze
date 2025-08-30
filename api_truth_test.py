import os, time, socket, ssl, requests

BASE_HOST = os.getenv("BASE_HOST", "api.coinalyze.net").strip().replace("https://","").replace("http://","").split("/")[0]
API_KEY   = os.getenv("API_KEY", "")
TIMEOUT   = float(os.getenv("TIMEOUT", "15"))

# Candidate endpoint paths to try (lightweight first)
CANDIDATE_PATHS = [
    "/v1/ping",
    "/v1/status",
    "/v1/markets",
    "/v1/exchanges",
    "/v1/symbols",
    "/v1/futures/markets",
    "/v1/spot/markets",
    "/markets",
    "/status",
    "/ping",
]

UA = "alphaops-polymerize/1.0"

def dns_lookup(host):
    try:
        return socket.getaddrinfo(host, 443, proto=socket.IPPROTO_TCP)
    except Exception as e:
        return f"DNS error: {e}"

def tls_probe(host):
    ctx = ssl.create_default_context()
    try:
        with socket.create_connection((host, 443), timeout=5) as sock:
            with ctx.wrap_socket(sock, server_hostname=host) as ssock:
                return {"tls_version": ssock.version(), "cipher": ssock.cipher(), "peer": ssock.getpeername()}
    except Exception as e:
        return f"TLS error: {e}"

def try_call(url, headers, label):
    print(f"\n--- {label} → {url} ---")
    try:
        r = requests.get(url, headers=headers, timeout=TIMEOUT)
        print("HTTP_STATUS:", r.status_code)
        if r.status_code == 200:
            preview = r.text[:400].replace("\n"," ")
            print("OK_BODY_PREVIEW:", preview + ("..." if len(r.text)>400 else ""))
            return 200, r.text
        else:
            print("BODY_PREVIEW:", r.text[:200].replace("\n"," "))
            return r.status_code, r.text
    except requests.exceptions.RequestException as e:
        print("REQUEST_EXCEPTION:", repr(e))
        return -1, None

if __name__ == "__main__":
    host = BASE_HOST
    print("=== Polymerize API Multi-Probe ===")
    print("HOST:", host)
    print("DNS:", dns_lookup(host))
    print("TLS:", tls_probe(host))
    print("Time:", time.strftime("%Y-%m-%d %H:%M:%S %Z"))
    print("------------------------------")

    base_url = f"https://{host}"
    bearer_headers = {"Authorization": f"Bearer {API_KEY}", "Accept":"application/json", "User-Agent": UA}
    xkey_headers   = {"X-API-KEY": API_KEY, "Accept":"application/json", "User-Agent": UA}

    winner = None

    for path in CANDIDATE_PATHS:
        url = base_url + path

        # Try Bearer
        code, _ = try_call(url, bearer_headers, "Authorization: Bearer")
        if code == 401: print("→ 401 = key/auth header mismatch or activation delay.")
        if code == 403: print("→ 403 = geo/ASN block (but DNS/TLS ok).")
        if code == 200:
            winner = ("Authorization: Bearer", url); break
        if code == 404:
            # Try same path with X-API-KEY before moving on
            code2, _ = try_call(url, xkey_headers, "X-API-KEY")
            if code2 == 200:
                winner = ("X-API-KEY", url); break
        else:
            # Non-404 → try X-API-KEY anyway
            code2, _ = try_call(url, xkey_headers, "X-API-KEY")
            if code2 == 200:
                winner = ("X-API-KEY", url); break

    print("\n==============================")
    if winner:
        print(f"WINNER: {winner[0]} @ {winner[1]}")
        print("ACTION: Lock this header+path in the production harness.")
    else:
        print("No 200s found. Likely: wrong paths or auth. Paste these logs here and we'll refine.")
