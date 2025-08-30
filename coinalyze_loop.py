import os, time, json, signal, sys, random
from coinalyze_api import (
    get_exchanges, get_future_markets, get_spot_markets,
    get_open_interest, get_funding_rate,
    get_open_interest_history, get_funding_rate_history,
    get_predicted_funding_rate_history, get_liquidation_history,
    get_long_short_ratio_history, get_ohlcv_history
)
from data_sink import write_snapshot, append_jsonl, retention_cleanup
from discord_poster import post_summary, build_embed

SYMBOL      = os.getenv("SYMBOL", "").strip()
EXCHANGE    = os.getenv("EXCHANGE", "BINANCE").upper()
BASE_ASSET  = os.getenv("BASE_ASSET", "BTC").upper()
INTERVAL    = os.getenv("INTERVAL", "5min")
WINDOW_HR   = int(os.getenv("WINDOW_HOURS", "6"))
SLEEP_SEC   = int(os.getenv("SLEEP_SECONDS", "60"))
PRINT_JSON  = os.getenv("PRINT_JSON", "false").lower() == "true"

def sleep_with_jitter(sec): time.sleep(sec + random.uniform(0, 0.3*sec))
shutdown = False
def _sigterm(*_): 
    global shutdown; shutdown = True
signal.signal(signal.SIGINT, _sigterm); signal.signal(signal.SIGTERM, _sigterm)

def auto_pick_symbol():
    fut = get_future_markets()
    def norm(s): return (s or "").upper()
    strict = [m for m in fut if m.get("is_perpetual", False)
              and norm(m.get("base_asset")) == BASE_ASSET
              and "USDT" in norm(m.get("quote_asset"))
              and EXCHANGE in norm(m.get("exchange"))]
    if strict:
        strict.sort(key=lambda m: (norm(m.get("quote_asset"))!="USDT", m.get("symbol","")))
        return strict[0]["symbol"]
    any_btc = [m for m in fut if m.get("is_perpetual", False) and (m.get("base_asset","").upper()==BASE_ASSET)]
    if any_btc:
        any_btc.sort(key=lambda m: (m.get("quote_asset","").upper()!="USDT", m.get("exchange",""), m.get("symbol","")))
        return any_btc[0]["symbol"]
    raise RuntimeError(f"No perp market found for {BASE_ASSET} (exchange hint='{EXCHANGE}')")

def now_ts(): return int(time.time())

def fetch_block(symbol):
    t1 = now_ts(); t0 = t1 - WINDOW_HR*3600
    oi  = get_open_interest(symbol)
    fr  = get_funding_rate(symbol)
    oi_hist  = get_open_interest_history(symbol, INTERVAL, t0, t1)
    fr_hist  = get_funding_rate_history(symbol, INTERVAL, t0, t1)
    pfr_hist = get_predicted_funding_rate_history(symbol, INTERVAL, t0, t1)
    liq_hist = get_liquidation_history(symbol, INTERVAL, t0, t1)
    ls_hist  = get_long_short_ratio_history(symbol, INTERVAL, t0, t1)
    ohlcv    = get_ohlcv_history(symbol, INTERVAL, t0, t1)
    return {
        "symbol": symbol,
        "interval": INTERVAL,
        "window_hours": WINDOW_HR,
        "snapshots": {"open_interest": oi, "funding_rate": fr},
        "history": {
            "open_interest": oi_hist, "funding_rate": fr_hist,
            "predicted_funding_rate": pfr_hist, "liquidations": liq_hist,
            "long_short_ratio": ls_hist, "ohlcv": ohlcv
        },
        "fetched_at": t1
    }

def main_loop():
    symbol = SYMBOL or auto_pick_symbol()
    print(f"=== AlphaOps • Coinalyze Live ===")
    print(f"Symbol: {symbol} | Interval: {INTERVAL} | Window(h): {WINDOW_HR}")
    print("Ctrl+C to stop.\n")

    backoff = SLEEP_SEC
    cycle = 0
    while not shutdown:
        t0 = time.time()
        try:
            pack = fetch_block(symbol)

            # persist
            snapshot_path = write_snapshot(symbol, INTERVAL, pack)
            stream_path   = append_jsonl(symbol, INTERVAL, pack)

            # terminal summary
            oi_now = (pack["snapshots"]["open_interest"] or [{}])[0]
            fr_now = (pack["snapshots"]["funding_rate"] or [{}])[0]
            print(f"[{time.strftime('%H:%M:%S')}] "
                  f"OI:{oi_now.get('value','?')} FR:{fr_now.get('value','?')} "
                  f"Candles:{len(pack['history']['ohlcv'])} "
                  f"LIQ:{len(pack['history']['liquidations'])} "
                  f"LS:{len(pack['history']['long_short_ratio'])} "
                  f"Saved:{snapshot_path.split('/')[-1]}  Dur:{round(time.time()-t0,2)}s")

            # optional JSON print
            if PRINT_JSON:
                s = json.dumps(pack, separators=(",", ":"), ensure_ascii=False)
                print(s[:800] + ("..." if len(s) > 800 else ""))

            # Discord (if WEBHOOK_URL set)
            try:
                post_summary(f"Coinalyze • {symbol} • {INTERVAL}", build_embed(symbol, INTERVAL, pack))
            except Exception as e:
                print("Discord post error:", repr(e))

            # periodic retention
            cycle += 1
            if cycle % 60 == 0:
                retention_cleanup()

            backoff = SLEEP_SEC
        except Exception as e:
            print(f"[{time.strftime('%H:%M:%S')}] ERROR: {repr(e)} | backoff:{backoff}s")
            time.sleep(backoff)
            backoff = min(backoff * 2, 600)
            continue

        time.sleep(SLEEP_SEC)

if __name__ == "__main__":
    try:
        ex = get_exchanges()
        fut = get_future_markets()
        print(f"Discovery: exchanges={len(ex)} futures_markets={len(fut)}")
    except Exception as e:
        print("Discovery error:", repr(e))
    main_loop()
