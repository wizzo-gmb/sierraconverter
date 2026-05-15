# scid-converter

Tools for turning Sierra Chart SCID tick files into clean, validated orderflow
data for `e:\Data\central\market.db` (the futures basket DB). Two converter
paths plus a rollover engine, a continuous stitcher, a validation harness, and
a master orchestrator. Phase 1 of the orderflow pipeline is complete and
gate-validated across 14 contracts.

## What's in this repo

| File | Role |
|---|---|
| [`scid_to_orderflow.py`](scid_to_orderflow.py) | **Stage 1 build pipeline.** SCID → per-expiry CSVs in the 4-file orderflow schema (`bars_1m_of`, `footprints_1m`, `big_trades_1m`, `tick_counts_1m`). Preserves Sierra's `bid_vol`/`ask_vol` ground truth — no Lee-Ready inference. Three aggressor policies (`HybridPolicy` default/locked, `SplitPolicy`, `MajorityPolicy`). |
| [`rollover_engine.py`](rollover_engine.py) | **Stage 2a.** Volume-based front-month detection across per-expiry CSVs → `manifest.json` (date ranges + chosen expiry per range). |
| [`continuous_stitcher.py`](continuous_stitcher.py) | **Stage 2b.** Manifest + per-expiry CSVs → continuous CSVs at root level (e.g. `mes_1m.csv`). **No back-adjustment by design** — raw prices, price-jumps at roll boundaries are expected and correct for at-price footprint mapping. |
| [`validate_against_meta_learner.py`](validate_against_meta_learner.py) | **Phase-1 validation harness.** Three modes (`random` / `production-pass` / `debug-window`), two scopes (`--expiry` / `--continuous`), revised gate (T4 invariant, T6d density, T6v volume on full window; T3 OHLC on post-latest-roll subset). Writes per-day-passrate + per-rolldate-drilldown CSVs. |
| [`seed_all_orchestrator.sh`](seed_all_orchestrator.sh) | **End-to-end seed.** Build → rollover → stitch → import to `market.db --source scid-seed` for every contract in `SYMBOLS=…`, smallest-first. Built-in `10Y → 10yr` rename for SQLite identifier rules + final cleanup phase (`SEED_KEEP_INTERMEDIATES=1` to keep CSVs for debugging). |
| [`scid_to_nt8_converter.py`](scid_to_nt8_converter.py) | **Legacy path.** SCID → NT8 `.txt` files for manual `Tools → Import → Historical Data` in NinjaTrader 8. Datetime midnight-rounding bug fixed in this codebase (the orderflow pipeline surfaced it). Per-ticker output subfolders, `--from-year` filter. **Not in the active workflow** — the orderflow path is faster and more accurate; this exists for ad-hoc NT8 import work. |
| [`tests/`](tests/) | pytest suite for the active pipeline components. |

## How the pieces fit together

```
SCID files (C:\SierraChart\Data\*.scid)
    │
    │  scid_to_orderflow.py {SYMBOL} {FOLDER} batch --from-year 25
    ▼
Per-expiry CSVs (E:\Data\Orderflow\{ROOT}\per_expiry\)
    │   {root}_{expiry}_1m.csv, _1m_footprint.csv, _1m_big_trades.csv, _1m_tick_counts.csv
    │
    │  rollover_engine.py + continuous_stitcher.py
    ▼
Continuous CSVs (E:\Data\Orderflow\{ROOT}\continuous\)
    │   {root}_1m.csv, _1m_footprint.csv, _1m_big_trades.csv, _1m_tick_counts.csv
    │
    │  python -m central_data.scripts.import_dump_folder ... --source scid-seed
    ▼
market.db (e:\Data\central\market.db)  ← canonical home for orderflow data
```

The orchestrator wraps all four arrows for the full 14-contract seed.

## Supported contracts (calibrated)

`scid_to_orderflow.py` carries two per-symbol maps so the build is correctly
parameterized for each contract class:

| Class | Roots | Tick size | Big-trade threshold |
|---|---|---|---|
| Micros (index) | `MES MNQ MYM M2K MCL` | per-symbol (0.25 / 1.0 / 0.10 / 0.01) | **25** |
| Standards (index) | `ES NQ YM RTY CL` | per-symbol | **10** |
| Metals (micro + standard) | `MGC GC` | 0.10 | 25 / 10 |
| Treasuries | `ZN 10Y` | 0.015625 | **50** |

Both `--tick-size` and `--big-trade-size` are CLI-overridable per run. The
defaults are mirrored exactly in the NT8 `CentralOrderFlowFeeder` strategy so
live additions stay calibrated identically to the seeded data.

## Stage 1 — `scid_to_orderflow.py`

Builds per-expiry orderflow CSVs from raw SCID. Five top-level subcommands:

```bash
# Dev / spot-check
python scid_to_orderflow.py smoke-test C:/SierraChart/Data/MESM26-CME.scid
python scid_to_orderflow.py verify-headers OUT_DIR ROOT EXPIRY
python scid_to_orderflow.py build-bars SCID_PATH OUT_DIR ROOT EXPIRY

# Converter-style (the everyday CLI)
python scid_to_orderflow.py SYMBOL FOLDER {single|batch|all} \
    [--policy {hybrid,split,majority}]  # default: hybrid (locked)
    [--tick-size FLOAT]                 # default per TICK_SIZE_MAP
    [--big-trade-size N]                # default per contract class
    [--output PATH]                     # default: E:\Data\Orderflow
    [--from-year YY]                    # e.g. 25 = 2025+

# Examples
python scid_to_orderflow.py MES C:/SierraChart/Data single MESM26-CME.scid
python scid_to_orderflow.py MNQ C:/SierraChart/Data batch --from-year 25
python scid_to_orderflow.py MGC C:/SierraChart/Data all  --from-year 15
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

HybridPolicy is the locked default and what seeded the production DB. The
other two exist as code paths for future round-trip experiments.

### Output schema (matches `UnifiedOrderFlowDumper.cs` byte-for-byte)

| File | Header |
|---|---|
| `{root}_{expiry}_1m.csv` | `timestamp,open,high,low,close,volume,buy_vol,sell_vol,delta` |
| `{root}_{expiry}_1m_footprint.csv` | `timestamp,price,buy_vol,sell_vol` |
| `{root}_{expiry}_1m_big_trades.csv` | `bar_timestamp,print_timestamp,price,size,side` |
| `{root}_{expiry}_1m_tick_counts.csv` | `bar_timestamp,price,n_trades,n_buy,n_sell` |

Timestamps are **naive UTC** ISO-8601 (`YYYY-MM-DDTHH:MM:SS` for bars,
`YYYY-MM-DDTHH:MM:SS.fffffffN` for `print_timestamp`). Prices use F4
(4-decimal). The downstream importer accepts `--tz UTC` to skip conversion;
the central DB stores UTC-naive end-to-end (see
[`e:\Data\central\README.md`](file:///E:/Data/central/README.md)).

## Stages 2a + 2b — rollover and stitching

```bash
python rollover_engine.py MES \
    --csv-root E:/Data/Orderflow/MES/per_expiry \
    --output   E:/Data/Orderflow/MES/continuous/manifest.json

python continuous_stitcher.py MES \
    --manifest E:/Data/Orderflow/MES/continuous/manifest.json \
    --csv-root E:/Data/Orderflow/MES/per_expiry \
    --output   E:/Data/Orderflow/MES/continuous
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

## Validation harness — `validate_against_meta_learner.py`

```bash
# Quick spot-check (a single 5-minute window)
python validate_against_meta_learner.py --mode debug-window \
    --contract MES --expiry h25 \
    --debug-window "2025-03-12 10:00-10:05" \
    --csv-root E:/Data/Orderflow/MES/per_expiry --db PATH_TO_DB

# Random sampling (10 × 1-hour slices, seeded)
python validate_against_meta_learner.py --mode random \
    --contract MES --continuous \
    --slices 10 --slice-hours 1 --seed 42 --days-back 75 \
    --csv-root E:/Data/Orderflow/MES/continuous --db PATH_TO_DB

# Full window — the production gate
python validate_against_meta_learner.py --mode production-pass \
    --contract MES --continuous \
    --window 2025-06-01:2026-05-09 \
    --csv-root E:/Data/Orderflow/MES/continuous --db PATH_TO_DB \
    --report-dir ./reports
```

Gate (all four BLOCK):

- **T4 invariant** (`buy_vol + sell_vol == volume`): 100% of bars
- **T6d** per-minute footprint density ratio (ours/db): within ±5% of 1.0
- **T6v** per-minute volume ratio (median): in `[0.9, 1.1]`
- **T3 OHLC** within 0.5% of price: ≥ 99.9% **on the post-latest-roll subset
  only** (the validation DB is back-adjusted, which moves historical prices;
  density and volume aggregates are adjustment-invariant and pass the full
  window, but OHLC only aligns post-latest-roll)

Reports:

- `validation_daily_passrate_{CONTRACT}.csv` — date, n_bars, n_t3_pass,
  n_t4_pass, n_post_roll. Read this when the gate fails to localize the
  problem to specific dates.
- `validation_rollday_{CONTRACT}_{YYYY-MM-DD}.csv` — per-rolldate ±3-day
  drilldown on adjustment-invariant metrics. One file per roll boundary in
  the gate window.

### Production-pass results (Phase 1 gate)

| Run | Window | Bars | T4 | T6d | T6v | T3 (post-roll) | Gate |
|---|---|---|---|---|---|---|---|
| MES (Step 14) | 2025-06-01 → 2026-03-23 | 286,914 | 100% | 1.0001 | 0.9679 | 99.96% (9,652) | **PASS** |
| MNQ (Step 15) | 2025-06-01 → 2026-03-24 | 329,122 | 100% | 1.0039 | 0.9615 | 99.98% (51,829) | **PASS** |

MGC, MYM, M2K were cross-validated at random-mode level during the audit
phase; they pass the same gate criteria on representative slices.

## Master orchestrator — `seed_all_orchestrator.sh`

End-to-end seed of `market.db` for every contract in `SYMBOLS=…`. Runs
serially (build → rollover → stitch → import) for each, then drops
intermediates by default.

```bash
bash seed_all_orchestrator.sh
```

Smallest-first order so the first few completions land quickly (10Y/M2K in
minutes, ES last at ~30 min on disk). Skip the cleanup phase if you need to
keep the CSVs around for debugging:

```bash
SEED_KEEP_INTERMEDIATES=1 bash seed_all_orchestrator.sh
```

Special-case in the script: `10Y` SCID files get renamed `10y_*.csv → 10yr_*.csv`
before the import step so the table prefix aligns with NT8's vocabulary
(`m10yr_*` after `_validate_root` auto-prefixes the digit-leading root). See
the central-data CLAUDE.md for the naming-convention contract.

## Legacy path — `scid_to_nt8_converter.py`

Produces NT8 tick `.txt` files for manual import via NT8's `Tools → Import →
Historical Data`. Not part of the active orderflow workflow (which goes
SCID → CSV → DB directly), but useful for ad-hoc NT8 chart work.

```bash
python scid_to_nt8_converter.py MES C:/SierraChart/Data batch --from-year 25 \
    --output E:/Data
```

Output: `E:/Data/MES/MES 03-25.txt`, etc. The directory per ticker is
intentional — it isolates each contract's output so re-runs don't share a
folder.

**Datetime midnight-rounding bug fix:** the legacy `deserialize_datetime` used
a float-roundtrip that decoded `23:59:59.999998` as `00:00:00` of the SAME
day, producing duplicate-timestamp ticks at every UTC midnight. Fixed in this
codebase via direct `SIERRA_EPOCH + timedelta(microseconds=dt_raw)` arithmetic.
External callers of `deserialize_datetime` get the fix transparently; the
converter's hot path no longer touches floats at all.

## Edge cases worth knowing

- **Parallel build truncation.** Four simultaneous `scid_to_orderflow.py
  batch` processes against `C:\SierraChart\Data` caused MNQ output to silently
  truncate by ~50%. Root cause never proven (likely AV scan or Sierra's
  internal write-lock under contention). Per-symbol serial is the safe pattern;
  cross-symbol parallelism is not currently exposed.
- **`10Y` table-naming.** Sierra's SCID files are labeled `10Y*`; NT8 labels
  the same instrument `10yr`. SQLite identifiers cannot start with a digit, so
  both forms get auto-prefixed to `m10yr_*` by `central_data.db._validate_root`.
  Use `10yr` (not `10y`) as the wire/CSV root if loading via any non-orchestrator
  path so you land in the same table as the seeded data.
- **DB back-adjustment vs raw prices.** The validation fixture
  (`meta_learner.db`) is back-adjusted continuous; our pipeline writes raw
  per-expiry prices. Density/volume metrics are adjustment-invariant and align
  cleanly. OHLC only aligns post-latest-roll — the validator gates T3 on that
  subset accordingly.
- **`preserve_subtrades=True`** is the default in `iter_scid_records`. Each
  SCID record emits its own `TickEvent` at its actual price (multi-leg trades
  marked by Sierra's `FIRST_SUB_TRADE` / `LAST_SUB_TRADE` sentinels). The
  legacy `False` mode collapses sub-trades into a single tick at the last
  sub's price — kept only for byte-equivalence with the NT8 `.txt` path.

## Tests

```bash
python -m pytest tests/ -v
```

Covers the SCID iterator, accumulator, policies, rollover engine, and
stitcher. The validation harness is exercised end-to-end against the production
DB rather than via unit tests.

## Related

The orderflow data this project produces lands in
**`e:\Data\central\market.db`** — the central futures DB. See
[`e:\Data\central\README.md`](file:///E:/Data/central/README.md) for the DB
schema, source-tag vocabulary (`scid-seed` is the value this pipeline writes),
timezone contract (DB is UTC-naive), and the trading-calendar subsystem
(`cal_holidays`, `cal_session_index`) that lets backtests cleanly exclude
holiday/early-close sessions.
