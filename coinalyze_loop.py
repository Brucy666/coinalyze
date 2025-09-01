import os, time, json, signal, random
from coinalyze_api import (
    get_exchanges, get_future_markets,
    get_open_interest, get_funding_rate,
    get_open_interest_history, get_funding_rate_history,
    get_predicted_funding_rate_history, get_liquidation_history,
    get_long_short_ratio_history, get_ohlcv_history
)
from data_sink import write_snapshot, append_jsonl, retention_cleanup
from discord_poster import post_summary, build_embed

# ---------------- Env / Config ----------------
SYMBOL       = os.getenv("SYMBOL", "").strip()
EXCHANGE     = os.getenv("EXCHANGE", "BINANCE").upper()
BASE_ASSET   = os.getenv("BASE_ASSET", "BTC").upper()
INTERVALS    = [s.strip() for s in os.getenv("INTERVALS", "1min,5min,15min,1hour").split(",") if s.strip()]
ROTATE_TF    = os.getenv("ROTATE_INTERVALS", "true").lower() == "true"
WINDOW_HR    = int(os.getenv("WINDOW_HOURS", "6"))
SLEEP_SEC    = int(os.getenv("SLEEP_SECONDS", "60"))
PRINT_JSON   = os.getenv("PRINT_JSON", "false").lower() == "true"

def sleep_with_jitter(sec: int):  # small jitter to avoid thundering herd
    time.sleep(sec + random.uniform(0, 0.3*sec))

shutdown = False
def _sigterm(*_):
    global shutdown; shutdown = True
signal.signal(signal.SIGINT, _sigterm)
signal.signal(signal.SIGTERM, _sigterm)

# ---------------- Helpers ----------------
def norm(s): return (s or "").upper()
def now_ts(): return int(time.time())

def unwrap_history(resp):
    """
    Accepts: [ {symbol, history:[...]} ]  OR  {history:[...]}  OR  plain list
    Returns the history list (possibly empty).
    """
    if isinstance(resp, list):
        if resp and isinstance(resp[0], dict) and "history" in resp[0]:
            return resp[0].get("history") or []
        return resp
    if isinstance(resp, dict) and "history" in resp:
        return resp.get("history") or []
    return []

def unwrap_snapshot_value(resp, key="value"):
    """
    Snapshot endpoints usually return a list with a single dict containing 'value'.
    """
    if isinstance(resp, list) and resp:
        item = resp[0]
        if isinstance(item, dict):
            return item.get(key)
    if isinstance(resp, dict):
        return resp.get(key)
    return None

def compute_cvd_from_ohlcv(ohlcv_bars):
    """
    Coinalyze OHLCV history may include:
      - 'v'  (total volume)
      - 'bv' (buy volume)
    Then sell volume = v - bv, delta = bv - (v - bv) = 2*bv - v, and CVD is cumulative delta.
    Returns a list of {'ts', 'buy', 'sell', 'delta', 'cvd'}.
    If 'bv' missing, returns [] (we won't guess).
    """
    out = []
    cvd = 0.0
    for b in ohlcv_bars:
        ts = b.get("timestamp") or b.get("ts") or b.get("time") or 0
        v  = b.get("v")  or b.get("volume")
        bv = b.get("bv") or b.get("buy_volume")
        if v is None or bv is None:
            # Cannot compute true CVD without explicit buy volume
            return []
        try:
            v  = float(v)
            bv = float(bv)
        except Exception:
            return []
        sv    = max(v - bv, 0.0)
        delta = bv - sv  # = 2*bv - v
        cvd  += delta
        out.append({"ts": ts, "buy": bv, "sell": sv, "delta": delta, "cvd": cvd})
    return out

def auto_pick_symbol():
    fut = get_future_markets()
    strict = [m for m in fut if m.get("is_perpetual", False)
              and norm(m.get("base_asset")) == BASE_ASSET
              and "USDT" in norm(m.get("quote_asset"))
              and EXCHANGE in norm(m.get("exchange"))]
    if strict:
        strict.sort(key=lambda m: (norm(m.get("quote_asset"))!="USDT", m.get("symbol","")))
        return strict[0]["symbol"]
    any_btc = [m for m in fut if m.get("is_perpetual", False) and (norm(m.get("base_asset"))==BASE_ASSET)]
    if any_btc:
        any_btc.sort(key=lambda m: (norm(m.get("quote_asset"))!="USDT", m.get("exchange",""), m.get("symbol","")))
        return any_btc[0]["symbol"]
    raise RuntimeError(f"No perp market found for {BASE_ASSET} (exchange hint='{EXCHANGE}')")

# ---------------- Fetch (per interval) ----------------
def fetch_block_for_interval(symbol: str, interval: str):
    t1 = now_ts(); t0 = t1 - WINDOW_HR*3600

    # Snapshots
    oi_snap = get_open_interest(symbol)
    fr_snap = get_funding_rate(symbol)
    oi_now  = unwrap_snapshot_value(oi_snap, "value")
    fr_now  = unwrap_snapshot_value(fr_snap, "value")

    # Histories (unwrap all)
    oi_hist  = unwrap_history(get_open_interest_history(symbol, interval, t0, t1))
    fr_hist  = unwrap_history(get_funding_rate_history(symbol, interval, t0, t1))
    pfr_hist = unwrap_history(get_predicted_funding_rate_history(symbol, interval, t0, t1))
    liq_hist = unwrap_history(get_liquidation_history(symbol, interval, t0, t1))
    ls_hist  = unwrap_history(get_long_short_ratio_history(symbol, interval, t0, t1))
    ohlcv    = unwrap_history(get_ohlcv_history(symbol, interval, t0, t1))

    # CVD from OHLCV (uses 'bv' and 'v' if available)
    cvd_series = compute_cvd_from_ohlcv(ohlcv)

    pack = {
        "source": "coinalyze",
        "symbol": symbol,
        "interval": interval,
        "window_hours": WINDOW_HR,
        "snapshots": {"open_interest": oi_snap, "funding_rate": fr_snap, "oi_value": oi_now, "fr_value": fr_now},
        "history": {
            "open_interest": oi_hist, "funding_rate": fr_hist,
            "predicted_funding_rate": pfr_hist, "liquidations": liq_hist,
            "long_short_ratio": ls_hist, "ohlcv": ohlcv, "cvd": cvd_series
        },
        "fetched_at": t1
    }
    return pack

# ---------------- Main Loop ----------------
def main_loop():
    symbol = SYMBOL or auto_pick_symbol()
    print("=== AlphaOps • Coinalyze Live ===")
    print(f"Symbol: {symbol} | TFs: {INTERVALS} | Window(h): {WINDOW_HR}")
    print("Ctrl+C to stop.\n")

    idx, backoff, cycle = 0, SLEEP_SEC, 0
    while not shutdown:
        t0 = time.time()
        try:
            interval = INTERVALS[idx % len(INTERVALS)] if ROTATE_TF else INTERVALS[0]
            pack = fetch_block_for_interval(symbol, interval)

            # persist
            snapshot_path = write_snapshot(symbol, interval, pack)
            _ = append_jsonl(symbol, interval, pack)

            # terminal summary
            oi_val = pack["snapshots"].get("oi_value")
            fr_val = pack["snapshots"].get("fr_value")
            ohlcv  = pack["history"]["ohlcv"]
            liq    = pack["history"]["liquidations"]
            ls     = pack["history"]["long_short_ratio"]
            cvd    = pack["history"]["cvd"]
            cvd_last = cvd[-1]["cvd"] if cvd else "NA"

            print(f"[{time.strftime('%H:%M:%S')}] TF:{interval} "
                  f"OI:{oi_val} FR:{fr_val} "
                  f"Candles:{len(ohlcv)} LIQ:{len(liq)} LS:{len(ls)} "
                  f"CVD:{cvd_last} "
                  f"Saved:{os.path.basename(snapshot_path)} "
                  f"Dur:{round(time.time()-t0,2)}s")

            # optional JSON print
            if PRINT_JSON:
                s = json.dumps(pack, separators=(",", ":"), ensure_ascii=False)
                print(s[:800] + ("..." if len(s) > 800 else ""))

            # Discord (if WEBHOOK_URL configured in poster module)
            try:
                post_summary(f"Coinalyze • {symbol} • {interval}", build_embed(symbol, interval, pack))
            except Exception as e:
                print("Discord post error:", repr(e))

            # periodic retention
            cycle += 1
            if cycle % 60 == 0:
                retention_cleanup()

            backoff = SLEEP_SEC
            idx += 1
        except Exception as e:
            print(f"[{time.strftime('%H:%M:%S')}] ERROR: {repr(e)} | backoff:{backoff}s")
            sleep_with_jitter(backoff)
            backoff = min(backoff * 2, 600)
            continue

        sleep_with_jitter(SLEEP_SEC)

# ---------------- Boot ----------------
if __name__ == "__main__":
    try:
        ex = get_exchanges()
        fut = get_future_markets()
        print(f"Discovery: exchanges={len(ex)} futures_markets={len(fut)}")
    except Exception as e:
        print("Discovery error:", repr(e))
    main_loop()
