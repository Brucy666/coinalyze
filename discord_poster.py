import os, json, requests

WEBHOOK_URL = os.getenv("WEBHOOK_URL", "").strip()

def post_summary(text, embed=None):
    """Post a compact message to Discord webhook if configured."""
    if not WEBHOOK_URL:
        return False
    payload = {"content": text}
    if embed:
        payload["embeds"] = [embed]
    r = requests.post(WEBHOOK_URL, json=payload, timeout=10)
    r.raise_for_status()
    return True

def build_embed(symbol, interval, pack):
    oi = (pack.get("snapshots",{}).get("open_interest") or [{}])[0]
    fr = (pack.get("snapshots",{}).get("funding_rate") or [{}])[0]
    fields = []
    if oi: fields.append({"name":"Open Interest", "value": str(oi.get("value","?")), "inline": True})
    if fr: fields.append({"name":"Funding", "value": str(fr.get("value","?")), "inline": True})
    fields.append({"name":"Candles", "value": str(len(pack.get("history",{}).get("ohlcv",[]))), "inline": True})
    fields.append({"name":"LIQ", "value": str(len(pack.get("history",{}).get("liquidations",[]))), "inline": True})
    return {
        "title": f"Coinalyze • {symbol} • {interval}",
        "description": "Live snapshot",
        "fields": fields
    }
