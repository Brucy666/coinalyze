import os, json, argparse, gzip
from pathlib import Path

DATA_DIR = os.getenv("DATA_DIR", "/data/coinalyze")

def export(symbol, interval, date, out_file):
    """
    symbol: e.g., BTCUSDT_PERP.A
    interval: e.g., 5min
    date: e.g., 20250901
    out_file: path to output .jsonl
    """
    p = Path(DATA_DIR) / symbol / interval / date
    if not p.exists():
        raise FileNotFoundError(f"No data folder: {p}")

    with open(out_file, "w") as out:
        for f in sorted(p.glob("*.json")):
            try:
                with open(f) as fh:
                    pack = json.load(fh)
                out.write(json.dumps(pack, separators=(",", ":"), ensure_ascii=False) + "\n")
            except Exception as e:
                print(f"Error reading {f}: {e}")

    print(f"Exported {out_file} with {sum(1 for _ in open(out_file))} lines")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", required=True, help="e.g., BTCUSDT_PERP.A")
    ap.add_argument("--interval", required=True, help="e.g., 5min")
    ap.add_argument("--date", required=True, help="e.g., 20250901")
    ap.add_argument("--out", required=True, help="output file (e.g., btc_5min.jsonl)")
    args = ap.parse_args()
    export(args.symbol, args.interval, args.date, args.out)
