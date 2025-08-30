import os, time, json
from dateutil.relativedelta import relativedelta
from coinalyze_api import (
    get_exchanges, get_future_markets, get_open_interest, get_funding_rate,
    get_open_interest_history, get_funding_rate_history, get_predicted_funding_rate_history,
    get_liquidation_history, get_long_short_ratio_history, get_ohlcv_history
)

# --- config via env (safe defaults) ---
SYMBOL = os.getenv("SYMBOL", "BTCUSDTPERP.BINANCE")   # futures perp on Binance
INTERVAL = os.getenv("INTERVAL", "5min")              # enums: 1min,5min,15min,30min,1hour,2hour,4hour,6hour,12hour,daily
WINDOW_HOURS = int(os.getenv("WINDOW_HOURS", "24"))

def unix_now(): return int(time.time())

def main():
    t0 = time.time()
    now = unix_now()
    start = now - WINDOW_HOURS * 3600

    print("=== AlphaOps • Coinalyze Runner ===")
    print("Symbol:", SYMBOL, "| Interval:", INTERVAL, "| Window(h):", WINDOW_HOURS)

    # Discovery (once per boot is fine)
    ex = get_exchanges()
    fut = get_future_markets()
    print("Exchanges:", len(ex))
    print("Futures markets:", len(fut))

    # Current snapshots
    print("\n-- Current Snapshots --")
    print("Open Interest:", json.dumps(get_open_interest(SYMBOL), separators=(',',':'))[:200], "...")
    print("Funding Rate :", json.dumps(get_funding_rate(SYMBOL), separators=(',',':'))[:200], "...")

    # Histories
    print("\n-- Histories --")
    print("OI History     :", str(get_open_interest_history(SYMBOL, INTERVAL, start, now)[:3])[:200], "...")
    print("FR History     :", str(get_funding_rate_history(SYMBOL, INTERVAL, start, now)[:3])[:200], "...")
    print("Pred FR History:", str(get_predicted_funding_rate_history(SYMBOL, INTERVAL, start, now)[:3])[:200], "...")
    print("Liq History    :", str(get_liquidation_history(SYMBOL, INTERVAL, start, now)[:3])[:200], "...")
    print("LSR History    :", str(get_long_short_ratio_history(SYMBOL, INTERVAL, start, now)[:3])[:200], "...")
    print("OHLCV History  :", str(get_ohlcv_history(SYMBOL, INTERVAL, start, now)[:3])[:200], "...")

    # TODO: when buy/sell endpoint is known, compute true CVD:
    #   CVD_t = Σ (buy_vol - sell_vol) up to t
    # For now, a proxy "Delta Pack" can be created downstream using:
    #   OIΔ per bar, LIQ net, L/S ratio Δ, price Δ — for Sniper scoring.

    print("\nCompleted in", round(time.time()-t0, 2), "s")

if __name__ == "__main__":
    main()
