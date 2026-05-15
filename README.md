# scid-converter

Tools for turning Sierra Chart SCID tick files into clean, validated orderflow
data. Two converter paths plus a rollover engine, a continuous stitcher, and
a master orchestrator. The orderflow pipeline is complete and field-tested
across 14 futures contracts.

## What's in this repo

| File | Role |
|---|---|
| [`scid_to_orderflow.py`](scid_to_orderflow.py) | **Stage 1 build pipeline.** SCID → per-expiry CSVs in a 4-file orderflow schema (bars, footprints, big-trades, tick-counts). Preserves Sierra's `bid_vol`/`ask_vol` ground truth — no Lee-Ready inference. Three aggressor policies (`HybridPolicy` default/locked, `SplitPolicy`, `MajorityPolicy`). |
| [`rollover_engine.py`](rollover_engine.py) | **Stage 2a.** Volume-based front-month detection across per-expiry CSVs → `manifest.json` (date ranges + chosen expiry per range). |
| [`continuous_stitcher.py`](continuous_stitcher.py) | **Stage 2b.** Manifest + per-expiry CSVs → continuous CSVs at root level (e.g. `mes_1m.csv`). **No back-adjustment by design** — raw prices, price-jumps at roll boundaries are expected and correct for at-price footprint mapping. |
| [`seed_all_orchestrator.sh`](seed_all_orchestrator.sh) | **End-to-end driver.** Build → rollover → stitch for every contract in `SYMBOLS=…`, smallest-first. Final cleanup phase by default (`SEED_KEEP_INTERMEDIATES=1` to keep CSVs for debugging). |
| [`scid_to_nt8_converter.py`](scid_to_nt8_converter.py) | **Legacy path.** SCID → NT8 `.txt` files for manual `Tools → Import → Historical Data` in NinjaTrader 8. Datetime midnight-rounding bug fixed in this codebase (the orderflow pipeline surfaced it). Per-ticker output subfolders, `--from-year` filter. Useful for ad-hoc NT8 import work. |
| [`tests/`](tests/) | pytest suite for the active pipeline components. |

## How the pieces fit together

```
SCID files (<SCID_DIR>/*.scid)
    │
    │  scid_to_orderflow.py SYMBOL <SCID_DIR> batch --from-year 25
    ▼
Per-expiry CSVs (<OUTPUT_DIR>/{ROOT}/per_expiry/)
    │   {root}_{expiry}_1m.csv, _1m_footprint.csv, _1m_big_trades.csv, _1m_tick_counts.csv
    │
    │  rollover_engine.py + continuous_stitcher.py
    ▼
Continuous CSVs (<OUTPUT_DIR>/{ROOT}/continuous/)
        {root}_1m.csv, _1m_footprint.csv, _1m_big_trades.csv, _1m_tick_counts.csv
```

`<SCID_DIR>` is wherever Sierra Chart writes its `.scid` files.
`<OUTPUT_DIR>` is whatever you pass to `--output`.

## Supported contracts (calibrated)

`scid_to_orderflow.py` carries two per-symbol maps so the build is correctly
parameterized for each contract class:

| Class | Roots | Tick size | Big-trade threshold |
|---|---|---|---|
| Micros (index) | `MES MNQ MYM M2K MCL` | per-symbol (0.25 / 1.0 / 0.10 / 0.01) | **25** |
| Standards (index) | `ES NQ YM RTY CL` | per-symbol | **10** |
| Metals (micro + standard) | `MGC GC` | 0.10 | 25 / 10 |
| Treasuries | `ZN 10Y` | 0.015625 | **50** |

Both `--tick-size` and `--big-trade-size` are CLI-overridable per run.

## Stage 1 — `scid_to_orderflow.py`

Builds per-expiry orderflow CSVs from raw SCID. Five top-level subcommands:

```bash
# Dev / spot-check
python scid_to_orderflow.py smoke-test <SCID_DIR>/MESM26-CME.scid
python scid_to_orderflow.py verify-headers <OUTPUT_DIR> ROOT EXPIRY
python scid_to_orderflow.py build-bars <SCID_PATH> <OUTPUT_DIR> ROOT EXPIRY

# Converter-style (the everyday CLI)
python scid_to_orderflow.py SYMBOL <SCID_DIR> {single|batch|all} \
    [--policy {hybrid,split,majority}]  # default: hybrid (locked)
    [--tick-size FLOAT]                 # default per TICK_SIZE_MAP
    [--big-trade-size N]                # default per contract class
    [--output PATH]                     # writes <OUTPUT>/{ROOT}/per_expiry/...
    [--from-year YY]                    # e.g. 25 = 2025+

# Examples
python scid_to_orderflow.py MES <SCID_DIR> single MESM26-CME.scid
python scid_to_orderflow.py MNQ <SCID_DIR> batch --from-year 25
python scid_to_orderflow.py MGC <SCID_DIR> all   --from-year 15
```

**Per-symbol builds are serial by design.** A multi-process audit found that
launching 4 simultaneous `batch` runs against the same SCID directory caused
one of them (MNQ) to silently truncate output. Avoid `python … &` chaining;
use a shell `for` loop or the orchestrator script.

### Aggressor policies

| Policy | Bar buy/sell | Footprint | Why use it |
|---|---|---|---|
| `hybrid` (default, locked) | `buy_vol = ask_vol`, `sell_vol = bid_vol` (Sierra ground truth) | one row at `snap(last)` += `(ask_vol, bid_vol)` | Ground truth from SCID. No inference. |
| `split` | same as hybrid | row at `snap(ask)` += `(ask_vol, 0)` AND row at `snap(bid)` += `(0, bid_vol)` | Reserved for future NT8 round-trip comparisons. |
| `majority` | All volume to the side with the larger contribution (mimics Lee-Ready "all-to-majority") | one row at `snap(last)` += all on majority side | Comparison against legacy NT8/Lee-Ready inference if needed. |

### Output schema

| File | Header |
|---|---|
| `{root}_{expiry}_1m.csv` | `timestamp,open,high,low,close,volume,buy_vol,sell_vol,delta` |
| `{root}_{expiry}_1m_footprint.csv` | `timestamp,price,buy_vol,sell_vol` |
| `{root}_{expiry}_1m_big_trades.csv` | `bar_timestamp,print_timestamp,price,size,side` |
| `{root}_{expiry}_1m_tick_counts.csv` | `bar_timestamp,price,n_trades,n_buy,n_sell` |

Timestamps are **naive UTC** ISO-8601 (`YYYY-MM-DDTHH:MM:SS` for bars,
`YYYY-MM-DDTHH:MM:SS.fffffffN` for `print_timestamp`). Prices use F4
(4-decimal).

## Stages 2a + 2b — rollover and stitching

```bash
python rollover_engine.py MES \
    --csv-root <OUTPUT_DIR>/MES/per_expiry \
    --output   <OUTPUT_DIR>/MES/continuous/manifest.json

python continuous_stitcher.py MES \
    --manifest <OUTPUT_DIR>/MES/continuous/manifest.json \
    --csv-root <OUTPUT_DIR>/MES/per_expiry \
    --output   <OUTPUT_DIR>/MES/continuous
```

The rollover engine picks the front-month per calendar date by highest daily
volume, with a `min_volume_ratio` guard (default 1% of the day's max) that
filters out dribble-volume expiries. Manifest entry shape:

```json
{"date_range_start": "2025-03-18",
 "date_range_end":   "2025-06-15",
 "source_expiry":    "m25",
 "roll_method":      "volume"}
```

The stitcher concatenates per-expiry slices according to the manifest into
4 continuous CSVs per root, **without back-adjusting prices**. The price-jump
at each roll boundary equals the spread between the two contracts on that
date — this is correct for orderflow because the footprint is price-keyed; a
back-adjusted view, if needed, is a downstream price-only transform.

## Orchestrator — `seed_all_orchestrator.sh`

End-to-end driver that loops build → rollover → stitch for every contract in
`SYMBOLS=…`, smallest-first. Edit the path variables at the top of the script
to match your environment before running.

```bash
bash seed_all_orchestrator.sh
```

Smallest-first order so the first few completions land quickly. Skip the
cleanup phase if you need to keep the intermediate CSVs around:

```bash
SEED_KEEP_INTERMEDIATES=1 bash seed_all_orchestrator.sh
```

## Legacy path — `scid_to_nt8_converter.py`

Produces NT8 tick `.txt` files for manual import via NT8's `Tools → Import →
Historical Data`. Useful for ad-hoc NT8 chart work; the orderflow path is
faster and more accurate for production-style ingestion.

```bash
python scid_to_nt8_converter.py MES <SCID_DIR> batch --from-year 25 \
    --output <OUTPUT_DIR>
```

Output lands at `<OUTPUT_DIR>/MES/MES 03-25.txt`, etc. The per-ticker
subdirectory is intentional — it isolates each contract's output so re-runs
don't share a folder.

**Datetime midnight-rounding bug fix:** the legacy `deserialize_datetime` used
a float-roundtrip that decoded `23:59:59.999998` as `00:00:00` of the SAME
day, producing duplicate-timestamp ticks at every UTC midnight. Fixed in this
codebase via direct `SIERRA_EPOCH + timedelta(microseconds=dt_raw)` arithmetic.
External callers of `deserialize_datetime` get the fix transparently; the
converter's hot path no longer touches floats at all.

## Edge cases worth knowing

- **Parallel build truncation.** Four simultaneous `scid_to_orderflow.py
  batch` processes against the same SCID directory caused MNQ output to
  silently truncate by ~50%. Root cause never proven (likely AV scan or
  Sierra's internal write-lock under contention). Per-symbol serial is the
  safe pattern; cross-symbol parallelism is not currently exposed.
- **`preserve_subtrades=True`** is the default in `iter_scid_records`. Each
  SCID record emits its own `TickEvent` at its actual price (multi-leg trades
  marked by Sierra's `FIRST_SUB_TRADE` / `LAST_SUB_TRADE` sentinels). The
  legacy `False` mode collapses sub-trades into a single tick at the last
  sub's price — kept only for byte-equivalence with the NT8 `.txt` path.

## Tests

```bash
python -m pytest tests/ -v
```

Covers the SCID iterator, accumulator, aggressor policies, rollover engine,
and stitcher.
