import os, json, re
from coinalyze_api import get_future_markets

TARGET_BASE = os.getenv("BASE_ASSET", "BTC").upper()
EX_HINT     = os.getenv("EXCHANGE_HINT", "BINANCE").upper()  # try BINANCE, BYBIT, OKX etc.

futs = get_future_markets()
print(f"Total futures markets: {len(futs)}")
# Show a couple raw rows for context
print("Sample row:", futs[0])

def norm(s): return (s or "").upper()

cands = []
for m in futs:
    if not m.get("is_perpetual", False):
        continue
    if norm(m.get("base_asset")) != TARGET_BASE:
        continue
    ex = norm(m.get("exchange"))
    if EX_HINT in ex:  # loose contains
        cands.append(m)

print(f"\nCandidates for base={TARGET_BASE} with exchange containing '{EX_HINT}': {len(cands)}")
for m in cands[:30]:
    print({
        "symbol": m.get("symbol"),
        "exchange": m.get("exchange"),
        "symbol_on_exchange": m.get("symbol_on_exchange"),
        "quote": m.get("quote_asset"),
        "has_ohlcv": m.get("has_ohlcv_data"),
        "has_buy_sell": m.get("has_buy_sell_data"),
    })

if not cands:
    # fallback: list ANY exchange for BTC perps so you can pick one
    alt = [m for m in futs if m.get("is_perpetual", False) and norm(m.get("base_asset")) == TARGET_BASE]
    print(f"\nNo match for '{EX_HINT}'. Here are some BTC perps on other exchanges ({len(alt)} found):")
    for m in alt[:30]:
        print({"symbol": m.get("symbol"), "exchange": m.get("exchange"), "quote": m.get("quote_asset")})
