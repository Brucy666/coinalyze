import os, json, requests
from typing import Dict, Any, List

# Allow a comma-separated list of webhook URLs
_WEBHOOKS = [w.strip() for w in os.getenv("WEBHOOK_URL", "").split(",") if w.strip()]

def post_summary(text: str, embed: Dict[str, Any] = None) -> bool:
    """
    Post a compact message (and optional embed) to all configured Discord webhooks.
    Returns True if at least one succeeded.
    """
    if not _WEBHOOKS:
        return False

    payload: Dict[str, Any] = {"content": text}
    if embed:
        payload["embeds"] = [embed]

    success = False
    for url in _WEBHOOKS:
        try:
            r = requests.post(url, json=payload, timeout=10)
            r.raise_for_status()
            success = True
        except Exception as e:
            print(f"Discord post error ({url}):", repr(e))
    return success

def build_embed(symbol: str, interval: str, pack: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build a Discord embed from a Coinalyze data pack.
    Shows OI, Funding, Candles, Liquidations, LS ratio, and CVD if present.
    """
    snaps = pack.get("snapshots", {})
    hist  = pack.get("history", {})

    oi_val = snaps.get("oi_value")
    fr_val = snaps.get("fr_value")

    fields: List[Dict[str, Any]] = []
    if oi_val is not None:
        fields.append({"name": "Open Interest", "value": str(oi_val), "inline": True})
    if fr_val is not None:
        fields.append({"name": "Funding", "value": str(fr_val), "inline": True})

    fields.append({"name": "Candles", "value": str(len(hist.get("ohlcv", []))), "inline": True})
    fields.append({"name": "LIQ", "value": str(len(hist.get("liquidations", []))), "inline": True})
    fields.append({"name": "LS",  "value": str(len(hist.get("long_short_ratio", []))), "inline": True})

    cvd = hist.get("cvd", [])
    if cvd:
        cvd_last = cvd[-1].get("cvd", "?")
        fields.append({"name": "CVD(last)", "value": str(cvd_last), "inline": True})

    embed: Dict[str, Any] = {
        "title": f"Coinalyze • {symbol} • {interval}",
        "description": "Live snapshot",
        "color": 0x2ECC71 if fr_val and float(fr_val) > 0 else 0xE74C3C,  # green if positive funding
        "fields": fields,
    }
    return embed
