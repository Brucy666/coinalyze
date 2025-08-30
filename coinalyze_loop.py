import os, time, json, signal, sys, random
from coinalyze_api import (
    get_exchanges, get_future_markets, get_spot_markets,
    get_open_interest, get_funding_rate,
    get_open_interest_history, get_funding_rate_history,
    get_predicted_funding_rate_history, get_liquidation_history,
    get_long_short_ratio_history, get_ohlcv_history
)

# -------- Config via ENV --------
# If SYMBOL is unset, we auto-pick BTC perp on BINANCE.
SYMBOL      = os.getenv("SYMBOL", "").strip()
EXCHANGE    = os.getenv("EXCHANGE", "BINANCE").upper()
BASE_ASSET  = os.getenv("BASE_ASSET", "BTC").upper()
INTERVAL    = os.getenv("INTERVAL", "5min")   # 1min,5min,15min,30min,1hour,2hour,4hour,6hour,12hour,daily
WINDOW_HR   = int(os.getenv("WINDOW_HOURS", "6"))
SLEEP_SEC   = int(os.getenv("SLEEP_SECONDS", "60"))
PRINT_JSON  = os.getenv("PRINT_JSON", "false").lower() == "true"

# backoff on 429
def sleep_with_jitter(sec):
    time.sleep(sec + random.uniform(0, 0.3*sec))

shutdown = False
def _sigterm(*_):
    global shutdown
    shutdown = True
signal.signal(signal.SIGINT, _sigterm)
signal.signal(signal.SIGTERM, _sigterm)

def auto_pick_symbol():
    """Pick <BASE> perp on EXCHANGE (e.g., BTCUSDTPERP.BINANCE)."""
    fut = get_future_markets()
    # Each item has: symbol, exchange, base_asset, is_perpetual, has_ohlcv_data, has_buy_sell_data, ...
    cands = [m for m in fut
             if m.get("exchange","").upper() == EXCHANGE
             and m.get("is_perpetual", False)
             and m.get("base_asset","").upper() == BASE_ASSET]
    if not cands:
        raise RuntimeError(f"No perp market found for {BASE_ASSET} on {EXCHANGE}")
    # prefer USDT-quoted if multiple
    cands.sort(key=lambda m: (m.get("quote_asset","")!="USDT", m.get("symbol","")))
    return cands[0]["symbol"]

def now_ts(): return int(time.time())

def fetch_block(symbol):
    t1 = now_ts()
    t0 = t1 - WINDOW_HR*3600

    # Current snapshots (cheap)
    oi = get_open_interest(symbol)
    fr = get_funding_rate(symbol)

    # Histories (bounded window)
    oi_hist   = get_open_interest_history(symbol, INTERVAL, t0, t1)
    fr_hist   = get_funding_rate_history(symbol, INTERVAL, t0, t1)
    pfr_hist  = get_predicted_funding_rate_history(symbol, INTERVAL, t0, t1)
    liq_hist  = get_liquidation_history(symbol, INTERVAL, t0, t1)
    ls_hist   = get_long_short_ratio_history(symbol, INTERVAL, t0, t1)
    ohlcv     = get_ohlcv_history(symbol, INTERVAL, t0, t1)

    return {
        "symbol": symbol,
        "interval": INTERVAL,
        "window_hours": WINDOW_HR,
        "snapshots": {
            "open_interest": oi,
            "funding_rate": fr
        },
        "history": {
            "open_interest": oi_hist,
            "funding_rate": fr_hist,
            "predicted_funding_rate": pfr_hist,
            "liquidations": liq_hist,
            "long_short_ratio": ls_hist,
            "ohlcv": ohlcv
        },
        "fetched_at": t1
    }

def main_loop():
    symbol = SYMBOL or auto_pick_symbol()
    print(f"=== AlphaOps • Coinalyze Live ===")
    print(f"Symbol: {symbol} | Exchange: {EXCHANGE} | Interval: {INTERVAL} | Window(h): {WINDOW_HR}")
    print("Ctrl+C to stop.\n")

    backoff = SLEEP_SEC
    while not shutdown:
        t0 = time.time()
        try:
            pack = fetch_block(symbol)
            # lightweight terminal summary
            oi_now = pack["snapshots"]["open_interest"][0] if pack["snapshots"]["open_interest"] else {}
            fr_now = pack["snapshots"]["funding_rate"][0]   if pack["snapshots"]["funding_rate"] else {}
            ohlc_last = pack["history"]["ohlcv"][-1] if pack["history"]["ohlcv"] else {}

            print(f"[{time.strftime('%H:%M:%S')}] "
                  f"OI:{oi_now.get('value','?')}  FR:{fr_now.get('value','?')}  "
                  f"Candles:{len(pack['history']['ohlcv'])}  "
                  f"LIQ:{len(pack['history']['liquidations'])}  "
                  f"LS:{len(pack['history']['long_short_ratio'])}  "
                  f"Dur:{round(time.time()-t0,2)}s")

            if PRINT_JSON:
                print(json.dumps(pack)[:800] + ("..." if len(json.dumps(pack))>800 else ""))

            # reset backoff on success
            backoff = SLEEP_SEC

        except Exception as e:
            # 429 / 5xx will land here due to raise_for_status; backoff progressively
            print(f"[{time.strftime('%H:%M:%S')}] ERROR: {repr(e)}  | backing off: {backoff}s")
            sleep_with_jitter(backoff)
            backoff = min(backoff*2, 600)  # cap at 10 min
            continue

        # normal sleep
        sleep_with_jitter(SLEEP_SEC)

    print("Shutdown received. Exiting cleanly.")

if __name__ == "__main__":
    # Touch discovery once (confirms reachability)
    try:
        ex = get_exchanges()
        fut = get_future_markets()
        print(f"Discovery: exchanges={len(ex)} futures_markets={len(fut)}")
    except Exception as e:
        print("Discovery error:", repr(e))
    main_loop()
