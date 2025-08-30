import os, json, time, socket, ssl
import requests

BASE_URL = os.getenv("BASE_URL", "https://api.example.com/v1/ping")
API_KEY  = os.getenv("API_KEY", "")
TIMEOUT  = float(os.getenv("TIMEOUT", "15"))

HEADERS = {
    "Authorization": f"Bearer {API_KEY}" if API_KEY else "",
    "Accept": "application/json",
    "User-Agent": "alphaops-polymerize/1.0"
}

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
                return {
                    "tls_version": ssock.version(),
                    "cipher": ssock.cipher(),
                    "peer": ssock.getpeername()
                }
    except Exception as e:
        return f"TLS error: {e}"

def main():
    host = BASE_URL.split("/")[2]
    print("=== Polymerize API Truth Test ===")
    print("BASE_URL:", BASE_URL)
    print("HOST:", host)
    print("DNS:", dns_lookup(host))
    print("TLS:", tls_probe(host))
    print("Time:", time.strftime("%Y-%m-%d %H:%M:%S %Z"))
    print("------------------------------")

    try:
        r = requests.get(BASE_URL, headers=HEADERS, timeout=TIMEOUT)
        print("HTTP_STATUS:", r.status_code)
        print("RESPONSE_HEADERS:", dict(r.headers))
        text = r.text
        print("BODY_PREVIEW:", text[:400].replace("\n"," ") + ("..." if len(text)>400 else ""))
        if r.status_code == 200:
            try:
                data = r.json()
                print("JSON_KEYS:", list(data.keys())[:20])
            except Exception:
                print("NOTE: Non-JSON body or JSON parse failed.")
        else:
            print("ERROR_HINT:",
                  "401=auth/key | 403=geo/firewall | 429=rate | 5xx=provider")
    except requests.exceptions.RequestException as e:
        print("REQUEST_EXCEPTION:", repr(e))

if __name__ == "__main__":
    main()
