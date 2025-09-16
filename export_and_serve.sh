#!/bin/bash
set -euo pipefail

# ---------- CONFIG ----------
# If you set EXPORT_MONTH=YYYY-MM as an env var in Railway, we’ll use that.
# Otherwise we auto-pick the newest month available in the lake.
SYM="${SYM:-BTCUSDT_PERP.A}"
INT="${INT:-1min}"
LAKE_ROOT="${LAKE_ROOT:-/data/lake}"
EXPORT_ROOT="${EXPORT_ROOT:-/data/exports}"
MONTH="${EXPORT_MONTH:-}"

mkdir -p "$EXPORT_ROOT"

# Symlink for tools that expect /data/coinalyze
[ -e /data/coinalyze ] || ln -s "$LAKE_ROOT" /data/coinalyze 2>/dev/null || true

SRC_DIR="$LAKE_ROOT/$SYM/$INT"

# Auto-detect latest month if not provided
if [ -z "$MONTH" ]; then
  # list YYYY-MM-* day folders, extract YYYY-MM, sort, take newest
  MONTH=$(ls -1 "$SRC_DIR" 2>/dev/null | awk -F- 'NF>=3{print $1"-"$2}' | sort -u | sort | tail -n1)
fi

echo "[serve] SYM=$SYM INT=$INT MONTH=$MONTH"
if [ -z "$MONTH" ] || [ ! -d "$SRC_DIR" ]; then
  echo "[serve] WARN: No data at $SRC_DIR — serving current exports only."
else
  MONTH_GLOB="$SRC_DIR/$MONTH-*"
  if compgen -G "$MONTH_GLOB/ohlcv.jsonl" > /dev/null; then
    # Build a consolidated OHLCV file if not there yet
    OHL_OUT="$EXPORT_ROOT/${SYM}_${INT}_${MONTH}_ohlcv.jsonl"
    if [ ! -s "$OHL_OUT" ]; then
      echo "[serve] Merging OHLCV for $MONTH → $OHL_OUT"
      cat $MONTH_GLOB/ohlcv.jsonl > "$OHL_OUT"
      echo "[serve] Wrote $OHL_OUT"
    fi

    # Build a full endpoints tarball if not there yet
    TGZ_OUT="$EXPORT_ROOT/${SYM}_${INT}_${MONTH}_ALL.tgz"
    if [ ! -s "$TGZ_OUT" ]; then
      echo "[serve] Packing ALL endpoints for $MONTH → $TGZ_OUT"
      tar -czf "$TGZ_OUT" -C "$SRC_DIR" $(ls -1 "$SRC_DIR" | grep "^$MONTH-") 2>/dev/null || true
      echo "[serve] Wrote $TGZ_OUT"
    fi
  else
    echo "[serve] WARN: No day folders for $MONTH in $SRC_DIR — skipping pack."
  fi
fi

# Generate a simple index with links (nice to click)
INDEX="$EXPORT_ROOT/index.html"
{
  echo "<html><body><h3>Exports ($SYM $INT)</h3><ul>"
  for f in $(ls -1 "$EXPORT_ROOT" | sort); do
    echo "<li><a href=\"$f\">$f</a></li>"
  done
  echo "</ul><p>Serving /data/exports on port 8000</p></body></html>"
} > "$INDEX"

cd "$EXPORT_ROOT"
echo "[serve] Serving /data/exports on 0.0.0.0:8000"
exec python3 -m http.server 8000
