#!/bin/bash
# Orchestrator: build + stitch + import for every remaining contract.
# Smallest-first so the user gets fast progress feedback.
#
# For each symbol:
#   1. Stage 1 build (SCID -> per-expiry CSVs) via scid_to_orderflow.py
#   2. Rollover engine (per-expiry CSVs -> manifest.json)
#   3. Continuous stitcher (manifest + per-expiry -> continuous CSVs)
#   4. Import to market.db with source='scid-seed'
#
# || continue on each step so a single failure doesn't kill the whole queue.

SEED_OUT="C:/Users/Admin/AppData/Local/Temp/seed_build_all"
SCID_DIR="C:/SierraChart/Data"
SCRIPT_DIR="d:/Algot-Trading/Utilities-tools/scid-converter"
CENTRAL_DIR="/e/Data/central"

mkdir -p "$SEED_OUT"

# Smallest -> largest by total 2015+ SCID size on disk. (Order is heuristic;
# actual per-symbol runtime depends on full-history SCID file sizes, which
# vary 100x between contracts.) Override via SYMBOLS env var for a
# narrower run, e.g. SYMBOLS="ZN" ./seed_all_orchestrator.sh
#
# 10Y is intentionally EXCLUDED from the default basket. Sierra's
# `10Y*.scid` files store the 10-year Treasury YIELD (decimal percentages
# like 1.4920%), not the futures contract PRICE. Our TICK_SIZE_MAP['10Y']
# = 0.015625 assumes price-space (1/64 ticks). Re-ingesting under those
# assumptions produces footprints that don't align with bar OHLC — the
# downstream price-space invariant breaks. Decide between (a) treating
# 10Y as a yield-space series with its own tick model, or (b) dropping
# it in favor of ZN (the actual futures contract, which is now fixed and
# in the basket). Until that decision is made, do not re-add 10Y here.
SYMBOLS="${SYMBOLS:-MES MNQ M2K MCL YM MYM RTY GC CL MGC ZN NQ ES}"

# Target DB for the import step. Defaults to the production market.db.
# Override via DB_PATH env var for scratch validation, e.g.
# DB_PATH="e:/Data/central/scratch.db" ./seed_all_orchestrator.sh
DB_PATH_ARG=""
if [ -n "$DB_PATH" ]; then
  DB_PATH_ARG="--db $DB_PATH"
fi

ts() { date +"%Y-%m-%d %H:%M:%S"; }

for sym in $SYMBOLS; do
  echo
  echo "########## $(ts)  START $sym ##########"

  out_dir="$SEED_OUT/$sym"
  per_expiry="$out_dir/per_expiry"
  continuous="$out_dir/continuous"
  mkdir -p "$continuous"

  echo "[$sym] $(ts)  Stage 1: build per-expiry..."
  # --from-year 15 pulls full Sierra-side history. Standards (ES/NQ/YM/RTY/
  # GC/CL/ZN) have continuous tick data back to 2015; micros (MES/MNQ/MYM/
  # M2K/MCL) only existed from their respective launch dates (2019-2021);
  # 10Y T-Note tick history starts ~2022 on disk. Override via FROM_YEAR=NN
  # env var if you want a shorter run (e.g. FROM_YEAR=22 ./seed_all_orchestrator.sh).
  PYTHONIOENCODING=utf-8 python "$SCRIPT_DIR/scid_to_orderflow.py" \
      "$sym" "$SCID_DIR" batch --from-year "${FROM_YEAR:-15}" \
      --output "$SEED_OUT" 2>&1 | tail -20
  if [ ${PIPESTATUS[0]} -ne 0 ]; then
    echo "[$sym] BUILD FAILED — skipping"
    continue
  fi

  echo "[$sym] $(ts)  Stage 2a: rollover..."
  PYTHONIOENCODING=utf-8 python "$SCRIPT_DIR/rollover_engine.py" "$sym" \
      --csv-root "$per_expiry" \
      --output "$continuous/manifest.json" 2>&1 | tail -10
  if [ ${PIPESTATUS[0]} -ne 0 ]; then
    echo "[$sym] ROLLOVER FAILED — skipping"
    continue
  fi

  echo "[$sym] $(ts)  Stage 2b: stitch..."
  PYTHONIOENCODING=utf-8 python "$SCRIPT_DIR/continuous_stitcher.py" "$sym" \
      --manifest "$continuous/manifest.json" \
      --csv-root "$per_expiry" \
      --output "$continuous" 2>&1 | tail -10
  if [ ${PIPESTATUS[0]} -ne 0 ]; then
    echo "[$sym] STITCH FAILED — skipping"
    continue
  fi

  # The Sierra SCID files for the 10-year T-Note are labeled "10Y" but NT8's
  # display symbol for the same instrument is "10yr". Rename `10y_*.csv` to
  # `10yr_*.csv` so the importer's auto-detected root matches the NT8 vocab.
  # `_validate_root` in central_data/db.py auto-prefixes digit-leading roots
  # with `m`, so both this seed import and live NT8 POSTs resolve to the same
  # `m10yr_*` tables.
  if [ "$sym" = "10Y" ]; then
    echo "[$sym] $(ts)  Renaming continuous CSVs to 10yr_* (NT8 vocab alignment)"
    for f in "$continuous"/10y_*.csv; do
      [ -e "$f" ] || continue
      mv "$f" "${f/10y_/10yr_}"
    done
  fi

  echo "[$sym] $(ts)  Import to ${DB_PATH:-market.db}..."
  (cd "$CENTRAL_DIR" && PYTHONIOENCODING=utf-8 python -m central_data.scripts.import_dump_folder \
       "$continuous" --source scid-seed $DB_PATH_ARG) 2>&1 | tail -10
  if [ ${PIPESTATUS[0]} -ne 0 ]; then
    echo "[$sym] IMPORT FAILED — skipping"
    continue
  fi

  echo "########## $(ts)  DONE $sym ##########"
done

echo
echo "$(ts)  ALL CONTRACTS PROCESSED"

# Cleanup phase: drop the per-contract intermediate CSVs once the DB has the
# canonical data. Per-expiry + continuous CSVs are cached intermediates only;
# market.db is the source of truth from here on. Skip cleanup if the env var
# SEED_KEEP_INTERMEDIATES is set (useful when debugging a failure).
if [ -n "$SEED_KEEP_INTERMEDIATES" ]; then
  echo "$(ts)  Skipping cleanup (SEED_KEEP_INTERMEDIATES set); intermediates remain at $SEED_OUT"
else
  echo
  echo "$(ts)  Cleanup phase: removing intermediate CSVs at $SEED_OUT..."
  size_before=$(du -sh "$SEED_OUT" 2>/dev/null | cut -f1)
  rm -rf "$SEED_OUT"
  echo "$(ts)  Cleanup done. Reclaimed: $size_before"
fi
