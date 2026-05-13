"""Validate scid_to_orderflow.py output against the meta_learner DB.

Phase 1 validation harness. See plan:
C:\\Users\\Admin\\.claude\\plans\\the-data-loading-is-memoized-knuth.md

Modes (selected via --mode):
    debug-window    Step 4: print 5-minute side-by-side bar tables.
    random          Step 5/10: N random 1-hour slices + per-bar tests.
    production-pass Step 13/14: every bar in --window, batched.

Scopes (selected by passing one of these flags):
    --expiry XYY    Compare a single per-expiry CSV against DB rows in that
                    expiry's tenure. Only valid when the expiry is wholly
                    within DB coverage.
    --continuous    Compare the rollover-stitched continuous CSV against the
                    DB. The DB stores a continuous series, so this is the
                    natural comparison once Stage 2 exists.

Step 4 scope: --mode debug-window only. Random and production-pass added in
Steps 5 and 13.
"""

import argparse
import csv
import json
import os
import random
import sqlite3
import sys
from datetime import datetime, timedelta

import pandas as pd

# Schema oddity: meta_learner DB labels micros under their standard ticker.
# Mapping micro -> DB ticker.
MICRO_TO_DB_TICKER = {
    'MES': 'ES',
    'MNQ': 'NQ',
    'MYM': 'YM',
    'MGC': 'GC',
    'M2K': 'RTY',
}


def db_ticker_for(contract: str) -> str:
    """Map a SCID contract symbol to the meta_learner DB ticker label."""
    contract = contract.upper()
    return MICRO_TO_DB_TICKER.get(contract, contract)


def _csv_path(csv_root: str, contract: str, expiry: str | None,
                continuous: bool, suffix: str) -> str:
    root = contract.lower()
    if continuous:
        return os.path.join(csv_root, f"{root}{suffix}.csv")
    if expiry is None:
        raise ValueError("--expiry is required when not using --continuous")
    return os.path.join(csv_root, f"{root}_{expiry.lower()}{suffix}.csv")


def load_csv_bars(csv_root: str, contract: str, expiry: str | None,
                   continuous: bool) -> pd.DataFrame:
    """Load our pipeline's bars CSV for the requested scope."""
    path = _csv_path(csv_root, contract, expiry, continuous, "_1m")
    if not os.path.exists(path):
        raise FileNotFoundError(f"Bars CSV not found: {path}")
    return pd.read_csv(path, parse_dates=['timestamp'])


def load_csv_footprint(csv_root: str, contract: str, expiry: str | None,
                        continuous: bool) -> pd.DataFrame:
    """Load our pipeline's footprint CSV."""
    path = _csv_path(csv_root, contract, expiry, continuous, "_1m_footprint")
    if not os.path.exists(path):
        raise FileNotFoundError(f"Footprint CSV not found: {path}")
    return pd.read_csv(path, parse_dates=['timestamp'])


def load_csv_big_trades(csv_root: str, contract: str, expiry: str | None,
                         continuous: bool) -> pd.DataFrame:
    """Load our pipeline's big_trades CSV."""
    path = _csv_path(csv_root, contract, expiry, continuous, "_1m_big_trades")
    if not os.path.exists(path):
        raise FileNotFoundError(f"Big trades CSV not found: {path}")
    return pd.read_csv(path, parse_dates=['bar_timestamp', 'print_timestamp'])


def _et_window_for(start: datetime, end: datetime) -> tuple[str, str]:
    """Translate UTC window -> ET-local naive (DB storage TZ) ISO 8601 strings."""
    s = (pd.Timestamp(start).tz_localize('UTC')
         .tz_convert('America/New_York').tz_localize(None))
    e = (pd.Timestamp(end).tz_localize('UTC')
         .tz_convert('America/New_York').tz_localize(None))
    return s.strftime('%Y-%m-%dT%H:%M:%S'), e.strftime('%Y-%m-%dT%H:%M:%S')


def _et_naive_to_utc_naive(ts_col):
    """Convert a Series of ET-naive datetimes to UTC-naive."""
    return (ts_col
            .dt.tz_localize('America/New_York', ambiguous='NaT', nonexistent='NaT')
            .dt.tz_convert('UTC')
            .dt.tz_localize(None))


def load_db_footprint(db_path: str, contract: str,
                       start: datetime, end: datetime) -> pd.DataFrame:
    """Load DB footprint rows for matching ticker and UTC window. DB stores ET.

    Note: the `footprints_1m_of` table has MIXED timestamp formats — about 98%
    use space-separator ('YYYY-MM-DD HH:MM:SS') and 2% use T-separator
    ('YYYY-MM-DDTHH:MM:SS'). Probably a legacy import inconsistency. We
    normalize at query time via `replace(timestamp, ' ', 'T')` so comparisons
    work across both. This kills index use on this column but the table is
    only 3.2M rows so it's tolerable.
    """
    ticker = db_ticker_for(contract)
    s_et, e_et = _et_window_for(start, end)
    conn = sqlite3.connect(db_path)
    try:
        df = pd.read_sql_query(
            "SELECT timestamp, price, buy_vol, sell_vol FROM footprints_1m_of "
            "WHERE ticker = ? "
            "  AND replace(timestamp, ' ', 'T') >= ? "
            "  AND replace(timestamp, ' ', 'T') < ? "
            "ORDER BY timestamp, price",
            conn, params=[ticker, s_et, e_et], parse_dates=['timestamp'],
        )
    finally:
        conn.close()
    if not df.empty:
        df['timestamp'] = _et_naive_to_utc_naive(df['timestamp'])
    return df


def load_db_big_trades(db_path: str, contract: str,
                        start: datetime, end: datetime) -> pd.DataFrame:
    """Load DB big_trade rows for matching ticker and UTC window. DB stores ET."""
    ticker = db_ticker_for(contract)
    s_et, e_et = _et_window_for(start, end)
    conn = sqlite3.connect(db_path)
    try:
        df = pd.read_sql_query(
            "SELECT bar_timestamp, print_timestamp, price, size, side "
            "FROM big_trades_1m_of "
            "WHERE ticker = ? AND print_timestamp >= ? AND print_timestamp < ? "
            "ORDER BY print_timestamp",
            conn, params=[ticker, s_et, e_et],
            parse_dates=['bar_timestamp', 'print_timestamp'],
        )
    finally:
        conn.close()
    if not df.empty:
        df['bar_timestamp'] = _et_naive_to_utc_naive(df['bar_timestamp'])
        df['print_timestamp'] = _et_naive_to_utc_naive(df['print_timestamp'])
    return df


def load_db_bars(db_path: str, contract: str,
                  start: datetime, end: datetime) -> pd.DataFrame:
    """Load DB rows for the matching ticker and timestamp window.

    `start` and `end` are in our pipeline's timezone (UTC). DB timestamps
    are stored as naive ET (Eastern Time, DST-aware). We convert the
    requested window from UTC to ET for the SQL query, then localize the
    DB results back to UTC so the rest of the validator sees consistent
    timestamps.

    `start` is inclusive, `end` is exclusive.
    """
    ticker = db_ticker_for(contract)
    # Translate UTC window -> ET (DB's storage TZ).
    start_et = (
        pd.Timestamp(start).tz_localize('UTC').tz_convert('America/New_York')
        .tz_localize(None)
    )
    end_et = (
        pd.Timestamp(end).tz_localize('UTC').tz_convert('America/New_York')
        .tz_localize(None)
    )
    conn = sqlite3.connect(db_path)
    try:
        q = (
            "SELECT timestamp, open, high, low, close, volume, buy_vol, "
            "sell_vol, delta FROM bars_1m_of "
            "WHERE ticker = ? AND timestamp >= ? AND timestamp < ? "
            "ORDER BY timestamp"
        )
        df = pd.read_sql_query(
            q, conn,
            params=[ticker, start_et.strftime('%Y-%m-%dT%H:%M:%S'),
                    end_et.strftime('%Y-%m-%dT%H:%M:%S')],
            parse_dates=['timestamp'],
        )
    finally:
        conn.close()
    # Translate DB timestamps (ET, naive) -> UTC (naive) for downstream merge.
    if not df.empty:
        df['timestamp'] = (
            df['timestamp']
            .dt.tz_localize('America/New_York', ambiguous='NaT', nonexistent='NaT')
            .dt.tz_convert('UTC')
            .dt.tz_localize(None)
        )
    return df


def parse_debug_window(spec: str) -> tuple[datetime, datetime]:
    """Parse "YYYY-MM-DD HH:MM-HH:MM" into (start, end) datetimes.

    The end is exclusive (10:00-10:05 means [10:00, 10:05) which yields the
    five 1-minute bars at 10:00, 10:01, 10:02, 10:03, 10:04).
    """
    try:
        date_part, time_part = spec.split(' ', 1)
        start_hm, end_hm = time_part.split('-')
        start = datetime.strptime(f"{date_part}T{start_hm}", '%Y-%m-%dT%H:%M')
        end = datetime.strptime(f"{date_part}T{end_hm}", '%Y-%m-%dT%H:%M')
    except (ValueError, IndexError) as e:
        raise ValueError(
            f"Invalid --debug-window {spec!r}; expected 'YYYY-MM-DD HH:MM-HH:MM'"
        ) from e
    if end <= start:
        raise ValueError(f"--debug-window end must be after start: {spec!r}")
    return start, end


def fmt_row(row) -> str:
    """Format a single bar row for the side-by-side printout."""
    ts_val = row['timestamp']
    ts = ts_val.strftime('%H:%M') if hasattr(ts_val, 'strftime') else str(ts_val)
    return (
        f"{ts} O={row['open']:>8.2f} H={row['high']:>8.2f} "
        f"L={row['low']:>8.2f} C={row['close']:>8.2f} "
        f"V={int(row['volume']):>5d} "
        f"buy={int(row['buy_vol']):>4d} sell={int(row['sell_vol']):>4d} "
        f"d={int(row['delta']):>+5d}"
    )


def print_side_by_side(ours: pd.DataFrame, db: pd.DataFrame,
                        contract: str, scope_label: str,
                        start: datetime, end: datetime) -> None:
    """Print our bars and DB bars side-by-side over the debug window."""
    print(f"=== Debug-window comparison ===")
    print(f"  contract: {contract}  (DB ticker: {db_ticker_for(contract)})")
    print(f"  scope:    {scope_label}")
    print(f"  window:   {start.isoformat()} to {end.isoformat()}")
    print(f"  ours:     {len(ours)} bars  |  db: {len(db)} bars")
    print()
    print(f"{'OURS':<78} | {'DB':<78}")
    print(f"{'-' * 78} | {'-' * 78}")

    n = max(len(ours), len(db))
    for i in range(n):
        left = fmt_row(ours.iloc[i]) if i < len(ours) else "(missing)"
        right = fmt_row(db.iloc[i]) if i < len(db) else "(missing)"
        print(f"{left:<78} | {right:<78}")


def csv_date_range(csv_root: str, contract: str, expiry: str | None,
                    continuous: bool) -> tuple[datetime, datetime]:
    """Return (min, max) timestamp from our pipeline's bars CSV."""
    df = load_csv_bars(csv_root, contract, expiry, continuous)
    if df.empty:
        raise ValueError("CSV bars file is empty")
    return (df['timestamp'].min().to_pydatetime(),
            df['timestamp'].max().to_pydatetime())


def db_date_range(db_path: str, contract: str) -> tuple[datetime, datetime]:
    """Return (min, max) timestamp for the matching DB ticker."""
    ticker = db_ticker_for(contract)
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT MIN(timestamp), MAX(timestamp) FROM bars_1m_of WHERE ticker = ?",
            [ticker],
        )
        lo_s, hi_s = cur.fetchone()
        if lo_s is None or hi_s is None:
            raise ValueError(f"DB has no bars for ticker={ticker}")
    finally:
        conn.close()
    lo = datetime.fromisoformat(lo_s)
    hi = datetime.fromisoformat(hi_s)
    return lo, hi


def generate_random_slices(start: datetime, end: datetime, n_slices: int,
                            slice_hours: int, seed: int) -> list[tuple[datetime, datetime]]:
    """Generate N random slice windows within [start, end - slice_hours].

    Slices are aligned to minute boundaries (random within the window).
    Returns sorted list of (slice_start, slice_end) datetime pairs.
    """
    rng = random.Random(seed)
    delta_seconds = int((end - start).total_seconds()) - slice_hours * 3600
    if delta_seconds <= 0:
        raise ValueError(
            f"Eligible window [{start}, {end}] is shorter than slice "
            f"({slice_hours}h)"
        )
    slices = []
    for _ in range(n_slices):
        offset_min = rng.randint(0, delta_seconds // 60)
        s = start + timedelta(minutes=offset_min)
        s = s.replace(second=0, microsecond=0)
        e = s + timedelta(hours=slice_hours)
        slices.append((s, e))
    slices.sort()
    return slices


# Per-bar test tolerances (Path A: relaxed to accommodate back-adjustment).
# Fixed-point tolerance is unfair across instruments — MES at 7400 vs MNQ at
# 22000 have similar percentage adjustment but very different point gaps.
# Scale tolerance to ~0.5% of bar price (covers typical back-adjustment
# residual on recent data while staying tight enough to catch real bugs).
PRICE_TOLERANCE_PCT = 0.005  # 0.5% of price
VOLUME_RATIO_RANGE = (0.1, 10.0)  # report-only; not BLOCK


def run_per_bar_tests(ours: pd.DataFrame, db: pd.DataFrame, slice_label: str,
                       ours_fp: pd.DataFrame | None = None,
                       db_fp: pd.DataFrame | None = None,
                       ours_bt: pd.DataFrame | None = None,
                       db_bt: pd.DataFrame | None = None) -> dict:
    """Run the per-bar/per-(bar,price) tests for a single slice.

    Tests:
        T1 - bar count match (advisory; DB partial coverage common)
        T2 - per-bar volume ratio (report-only)
        T3 - per-bar OHLC within tolerance (BLOCK)
        T4 - buy_vol+sell_vol == volume invariant on our side (BLOCK)
        T5 - per-bar delta correlation ours vs DB (WARN)
        T6 - per-(bar,price) footprint sum match + buy/sell agreement (WARN)
        T7 - big_trades count + match rate (advisory)
    """
    res = {
        'slice': slice_label,
        'ours_bars': len(ours),
        'db_bars': len(db),
    }

    # Test 1: bar count match
    res['t1_bar_count_match'] = (len(ours) == len(db))

    # Merge on timestamp for per-bar comparisons.
    merged = pd.merge(
        ours, db, on='timestamp', suffixes=('_ours', '_db'), how='inner'
    )
    res['merged_bars'] = len(merged)

    if len(merged) == 0:
        # Nothing to compare. Both sides empty (weekend/holiday) is acceptable.
        # Skip per-bar tests; they don't apply to empty windows.
        res.update({
            't2_volume_ratios': [],
            't3_ohlc_within_tol': True,   # vacuously true
            't4_invariant_holds': True,   # vacuously true
            't5_delta_corr': None,
            'max_price_diff': None,
            'empty_slice': True,
        })
        return res
    res['empty_slice'] = False

    # Test 2: volume ratio per bar (report-only)
    safe_db = merged['volume_db'].replace(0, pd.NA)
    res['t2_volume_ratios'] = (merged['volume_ours'] / safe_db).dropna().tolist()

    # Test 3: per-bar OHLC within PRICE_TOLERANCE_PCT of bar price.
    # Use db close as reference price (could equally use ours; should match).
    avg_price = float(merged['close_db'].mean())
    tol = avg_price * PRICE_TOLERANCE_PCT
    diffs = pd.concat([
        (merged['open_ours'] - merged['open_db']).abs(),
        (merged['high_ours'] - merged['high_db']).abs(),
        (merged['low_ours'] - merged['low_db']).abs(),
        (merged['close_ours'] - merged['close_db']).abs(),
    ])
    res['max_price_diff'] = float(diffs.max())
    res['price_tolerance'] = tol
    res['t3_ohlc_within_tol'] = bool(diffs.max() <= tol)

    # Test 4: invariant on OUR side (always strict)
    invariant = (merged['buy_vol_ours'] + merged['sell_vol_ours'] ==
                 merged['volume_ours'])
    res['t4_invariant_holds'] = bool(invariant.all())

    # Test 5: delta correlation
    if merged['delta_ours'].std() > 0 and merged['delta_db'].std() > 0:
        res['t5_delta_corr'] = float(
            merged['delta_ours'].corr(merged['delta_db'])
        )
    else:
        res['t5_delta_corr'] = None

    # Test 6: footprint per-(bar, price) agreement (advisory) + density/volume
    # adjustment-invariant metrics (BLOCK).
    if ours_fp is not None and db_fp is not None and not ours_fp.empty and not db_fp.empty:
        # T6c (advisory): per-cell merge — back-adjusted DB prices don't align,
        # so most cells fail to merge as artifact, not data gap.
        fp_merge = pd.merge(
            ours_fp, db_fp, on=['timestamp', 'price'],
            suffixes=('_ours', '_db'), how='outer', indicator=True
        )
        n_total = len(fp_merge)
        n_both = int((fp_merge['_merge'] == 'both').sum())
        n_only_ours = int((fp_merge['_merge'] == 'left_only').sum())
        n_only_db = int((fp_merge['_merge'] == 'right_only').sum())

        both = fp_merge[fp_merge['_merge'] == 'both'].copy()
        if len(both) > 0:
            both['sum_ours'] = both['buy_vol_ours'].fillna(0) + both['sell_vol_ours'].fillna(0)
            both['sum_db'] = both['buy_vol_db'].fillna(0) + both['sell_vol_db'].fillna(0)
            t6c_sum_match = int((both['sum_ours'] == both['sum_db']).sum())
        else:
            t6c_sum_match = 0

        # T6d (BLOCK): per-minute density. Count rows per minute on each side,
        # compare aggregate cells/min ratio. Adjustment-invariant.
        ours_per_min = ours_fp.groupby('timestamp').size()
        db_per_min = db_fp.groupby('timestamp').size()
        ours_total_cells = int(ours_per_min.sum())
        db_total_cells = int(db_per_min.sum())
        ours_minutes = int(ours_per_min.shape[0])
        db_minutes = int(db_per_min.shape[0])
        ours_cells_per_min = ours_total_cells / max(ours_minutes, 1)
        db_cells_per_min = db_total_cells / max(db_minutes, 1)
        density_ratio = ours_cells_per_min / max(db_cells_per_min, 1e-9)

        # T6v (BLOCK): per-minute volume sum ratio. Sum buy+sell per minute,
        # compute ratio per shared minute, take median. Adjustment-invariant
        # because we're aggregating across price levels per minute.
        ours_vol_per_min = (ours_fp.assign(_v=ours_fp['buy_vol'] + ours_fp['sell_vol'])
                            .groupby('timestamp')['_v'].sum())
        db_vol_per_min = (db_fp.assign(_v=db_fp['buy_vol'] + db_fp['sell_vol'])
                          .groupby('timestamp')['_v'].sum())
        common_min = ours_vol_per_min.index.intersection(db_vol_per_min.index)
        if len(common_min) > 0:
            vol_ratios = (ours_vol_per_min.loc[common_min] /
                          db_vol_per_min.loc[common_min].replace(0, pd.NA)).dropna()
            volume_ratio_median = float(vol_ratios.median()) if len(vol_ratios) else None
        else:
            volume_ratio_median = None

        res.update({
            'fp_total_cells': n_total,
            'fp_only_ours': n_only_ours,
            'fp_only_db': n_only_db,
            'fp_both': n_both,
            't6c_sum_match': t6c_sum_match,
            'fp_ours_cells_per_min': ours_cells_per_min,
            'fp_db_cells_per_min': db_cells_per_min,
            't6d_density_ratio': density_ratio,
            't6v_volume_ratio_median': volume_ratio_median,
        })

    # Test 7: big_trades count and rough match.
    if ours_bt is not None and db_bt is not None:
        res['bt_ours'] = len(ours_bt)
        res['bt_db'] = len(db_bt)
        # Match by exact (print_timestamp, price, size, side).
        if len(ours_bt) > 0 and len(db_bt) > 0:
            bt_merged = pd.merge(
                ours_bt[['print_timestamp', 'price', 'size', 'side']],
                db_bt[['print_timestamp', 'price', 'size', 'side']],
                on=['print_timestamp', 'price', 'size', 'side'], how='inner',
            )
            res['bt_exact_match'] = len(bt_merged)
        else:
            res['bt_exact_match'] = 0

    return res


def cmd_random(args: argparse.Namespace) -> int:
    csv_lo, csv_hi = csv_date_range(
        args.csv_root, args.contract, args.expiry, args.continuous
    )
    db_lo, db_hi = db_date_range(args.db, args.contract)
    elig_lo = max(csv_lo, db_lo)
    elig_hi = min(csv_hi, db_hi)

    if args.days_back is not None:
        # Constrain to last N days of the intersection.
        constrained_lo = elig_hi - timedelta(days=args.days_back)
        if constrained_lo > elig_lo:
            elig_lo = constrained_lo

    print(f"=== Random-mode validation ===")
    print(f"  contract:    {args.contract}  (DB ticker: {db_ticker_for(args.contract)})")
    print(f"  scope:       {'continuous' if args.continuous else f'per-expiry {args.expiry}'}")
    print(f"  csv range:   {csv_lo}  ..  {csv_hi}")
    print(f"  db range:    {db_lo}  ..  {db_hi}")
    print(f"  intersection: {elig_lo}  ..  {elig_hi}")
    print(f"  slices:      {args.slices} x {args.slice_hours}h, seed={args.seed}")

    if elig_hi <= elig_lo:
        print(f"\nERROR: SCID and DB date ranges do not overlap.")
        return 1

    slices = generate_random_slices(elig_lo, elig_hi, args.slices,
                                     args.slice_hours, args.seed)

    ours_bars_full = load_csv_bars(
        args.csv_root, args.contract, args.expiry, args.continuous
    )
    # Optional: footprint + big_trades CSVs for T6/T7. Skip gracefully if absent.
    try:
        ours_fp_full = load_csv_footprint(
            args.csv_root, args.contract, args.expiry, args.continuous
        )
    except FileNotFoundError:
        ours_fp_full = None
    try:
        ours_bt_full = load_csv_big_trades(
            args.csv_root, args.contract, args.expiry, args.continuous
        )
    except FileNotFoundError:
        ours_bt_full = None

    all_results = []
    for s, e in slices:
        ours_slice = ours_bars_full[
            (ours_bars_full['timestamp'] >= s) & (ours_bars_full['timestamp'] < e)
        ].reset_index(drop=True)
        db_slice = load_db_bars(args.db, args.contract, s, e)
        ours_fp_slice = None
        db_fp_slice = None
        if ours_fp_full is not None:
            ours_fp_slice = ours_fp_full[
                (ours_fp_full['timestamp'] >= s) & (ours_fp_full['timestamp'] < e)
            ].reset_index(drop=True)
            db_fp_slice = load_db_footprint(args.db, args.contract, s, e)
        ours_bt_slice = None
        db_bt_slice = None
        if ours_bt_full is not None:
            ours_bt_slice = ours_bt_full[
                (ours_bt_full['print_timestamp'] >= s) &
                (ours_bt_full['print_timestamp'] < e)
            ].reset_index(drop=True)
            db_bt_slice = load_db_big_trades(args.db, args.contract, s, e)
        label = f"{s.isoformat()}..{e.isoformat()}"
        r = run_per_bar_tests(
            ours_slice, db_slice, label,
            ours_fp=ours_fp_slice, db_fp=db_fp_slice,
            ours_bt=ours_bt_slice, db_bt=db_bt_slice,
        )
        all_results.append(r)

    print(f"\n=== Per-slice results ===")
    print(f"{'slice start':<19} {'ours':>5} {'db':>5} {'merged':>6} "
          f"{'T1':>3} {'T3':>3} {'T4':>3} {'maxΔp':>7} {'T5corr':>7} {'volR_med':>9}")
    for r in all_results:
        slice_start = r['slice'].split('..')[0]
        slice_start = slice_start.replace('T', ' ')[:16]
        vol_ratios = r['t2_volume_ratios']
        vol_med = (sorted(vol_ratios)[len(vol_ratios) // 2]
                   if vol_ratios else float('nan'))
        max_dp = r.get('max_price_diff')
        t5 = r.get('t5_delta_corr')
        print(
            f"{slice_start:<19} "
            f"{r['ours_bars']:>5} {r['db_bars']:>5} {r['merged_bars']:>6} "
            f"{'OK' if r['t1_bar_count_match'] else 'F':>3} "
            f"{'OK' if r['t3_ohlc_within_tol'] else 'F':>3} "
            f"{'OK' if r['t4_invariant_holds'] else 'F':>3} "
            f"{(max_dp if max_dp is not None else float('nan')):>7.2f} "
            f"{(t5 if t5 is not None else float('nan')):>7.3f} "
            f"{vol_med:>9.2f}"
        )

    # Aggregate.
    total = len(all_results)
    t1_pass = sum(1 for r in all_results if r['t1_bar_count_match'])
    t3_pass = sum(1 for r in all_results if r['t3_ohlc_within_tol'])
    t4_pass = sum(1 for r in all_results if r['t4_invariant_holds'])
    all_corr = [r['t5_delta_corr'] for r in all_results
                if r['t5_delta_corr'] is not None]
    overall_corr = (sum(all_corr) / len(all_corr)) if all_corr else None
    all_ratios = [v for r in all_results for v in r['t2_volume_ratios']]
    median_ratio = (sorted(all_ratios)[len(all_ratios) // 2]
                    if all_ratios else None)

    print(f"\n=== Aggregate ===")
    print(f"  Test 1 (bar count match):   {t1_pass}/{total}")
    print(f"  Test 3 (OHLC ≤ {PRICE_TOLERANCE_PCT * 100:.2f}% of price): {t3_pass}/{total}")
    print(f"  Test 4 (invariant on ours): {t4_pass}/{total}")
    print(f"  Test 5 (delta corr avg):    "
          f"{overall_corr:.3f}" if overall_corr is not None else "  Test 5: n/a")

    # T2 volume distribution (percentiles, not just median).
    if all_ratios:
        rs = sorted(all_ratios)
        n = len(rs)
        def pct(p): return rs[min(n - 1, int(n * p))]
        print(f"  Test 2 (volume ratio):      "
              f"p5={pct(0.05):.2f} p25={pct(0.25):.2f} p50={pct(0.50):.2f} "
              f"p75={pct(0.75):.2f} p95={pct(0.95):.2f}  (n={n})")
    else:
        print("  Test 2 (volume ratio): n/a")

    # T6 footprint — adjustment-invariant gate metrics + advisory cell-level.
    fp_total = sum(r.get('fp_total_cells', 0) for r in all_results)
    fp_both = sum(r.get('fp_both', 0) for r in all_results)
    fp_only_ours = sum(r.get('fp_only_ours', 0) for r in all_results)
    fp_only_db = sum(r.get('fp_only_db', 0) for r in all_results)
    fp_sum_match = sum(r.get('t6c_sum_match', 0) for r in all_results)

    # T6d aggregate density: weight per-slice cells/min by minute count.
    t6d_ratios = [r.get('t6d_density_ratio') for r in all_results
                  if r.get('t6d_density_ratio') is not None]
    t6d_avg = (sum(t6d_ratios) / len(t6d_ratios)) if t6d_ratios else None

    # T6v aggregate volume ratio: median across slices' per-slice medians.
    t6v_per_slice = [r.get('t6v_volume_ratio_median') for r in all_results
                     if r.get('t6v_volume_ratio_median') is not None]
    t6v_overall = (sorted(t6v_per_slice)[len(t6v_per_slice) // 2]
                   if t6v_per_slice else None)

    # Density gate: |ratio - 1| <= 0.05 (within 5%)
    t6d_pass = (t6d_avg is not None and abs(t6d_avg - 1.0) <= 0.05)
    # Volume gate: median in [0.9, 1.1]
    t6v_pass = (t6v_overall is not None and 0.9 <= t6v_overall <= 1.1)

    if fp_total > 0:
        print(f"  Test 6 footprint:")
        print(f"    [BLOCK] T6d density ratio: "
              f"{t6d_avg:.3f}" if t6d_avg is not None else "n/a"
              f"  ({'PASS' if t6d_pass else 'FAIL'} — within 5% of 1.0)")
        print(f"    [BLOCK] T6v volume ratio:  "
              f"{t6v_overall:.3f}" if t6v_overall is not None else "n/a"
              f"  ({'PASS' if t6v_pass else 'FAIL'} — within [0.9, 1.1])")
        print(f"    [ADVISORY] cell-level merge:  total={fp_total:,}  "
              f"(both={fp_both:,}, only ours={fp_only_ours:,}, only db={fp_only_db:,})")
        if fp_both > 0:
            print(f"    [ADVISORY] T6c sum match:  "
                  f"{fp_sum_match:,}/{fp_both:,} ({100*fp_sum_match/fp_both:.2f}%)  "
                  f"[per-(bar,price) — back-adjusted DB makes most cells fail to merge]")
    else:
        print("  Test 6: n/a (no footprint data)")

    # T7 (big_trades) aggregate — ADVISORY (DB threshold differs).
    bt_ours = sum(r.get('bt_ours', 0) for r in all_results)
    bt_db = sum(r.get('bt_db', 0) for r in all_results)
    bt_match = sum(r.get('bt_exact_match', 0) for r in all_results)
    if bt_ours or bt_db:
        print(f"  [ADVISORY] T7 big_trades:   ours={bt_ours:,}  db={bt_db:,}  "
              f"exact match={bt_match:,}  "
              f"({100*bt_match/max(bt_ours,1):.1f}% of ours)")

    # Revised gate: BLOCK tests must all pass; T1 is advisory.
    # BLOCK tests:
    #   T3 OHLC (post-roll-window scope, but applied to all sampled slices here
    #            since user is responsible for choosing --days-back constraint)
    #   T4 invariant (always strict)
    #   T6d density ratio within 5% of 1.0 (only if footprint data present)
    #   T6v volume ratio in [0.9, 1.1] (only if footprint data present)
    block_failures = []
    if t3_pass != total:
        block_failures.append(f"T3 {t3_pass}/{total}")
    if t4_pass != total:
        block_failures.append(f"T4 {t4_pass}/{total}")
    if fp_total > 0:
        if not t6d_pass:
            block_failures.append(
                f"T6d density={t6d_avg:.3f}" if t6d_avg is not None else "T6d n/a"
            )
        if not t6v_pass:
            block_failures.append(
                f"T6v volume={t6v_overall:.3f}" if t6v_overall is not None else "T6v n/a"
            )

    gate_pass = len(block_failures) == 0
    t1_advisory = "" if t1_pass == total else f" (T1 advisory: {t1_pass}/{total} — partial DB coverage)"
    fail_detail = (" — failed: " + ", ".join(block_failures)) if block_failures else ""
    print(f"\n  GATE: {'PASS' if gate_pass else 'FAIL'}{fail_detail}{t1_advisory}")
    return 0 if gate_pass else 1


# ---------------------------------------------------------------------------
# Production-pass mode (Step 13)
# ---------------------------------------------------------------------------

# Gate thresholds (per the plan §"Per-bar tests (revised post-audit)").
PRODUCTION_BAR_PASS_RATE = 0.999     # 99.9% of bars must pass T3/T4
PRODUCTION_DENSITY_TOL = 0.05        # |T6d - 1.0| <= 5%
PRODUCTION_VOLUME_RANGE = (0.9, 1.1)  # T6v median in this band


def parse_window(spec: str) -> tuple[datetime, datetime]:
    """Parse 'YYYY-MM-DD:YYYY-MM-DD' into (start, end_exclusive) datetimes.

    Both dates are interpreted as midnight UTC. The end is inclusive of the
    full final day, so we add 1 day to make it exclusive for downstream use.
    """
    parts = spec.split(':')
    if len(parts) != 2:
        raise ValueError(f"--window must be 'YYYY-MM-DD:YYYY-MM-DD', got {spec!r}")
    s = datetime.strptime(parts[0], '%Y-%m-%d')
    e = datetime.strptime(parts[1], '%Y-%m-%d') + timedelta(days=1)
    if e <= s:
        raise ValueError(f"--window end must be >= start, got {spec!r}")
    return s, e


def detect_latest_roll(manifest_path: str) -> datetime | None:
    """Return the date_range_start of the LAST manifest entry, as a datetime
    at UTC midnight. This is the boundary after which DB back-adjustment is
    ~0 (no further roll spreads have been baked in)."""
    if not os.path.exists(manifest_path):
        return None
    with open(manifest_path, 'r', encoding='utf-8') as f:
        manifest = json.load(f)
    if not manifest:
        return None
    return datetime.strptime(manifest[-1]['date_range_start'], '%Y-%m-%d')


def _month_windows(start: datetime, end: datetime):
    """Yield (month_start, month_end_exclusive) UTC tuples covering [start, end)."""
    cur = datetime(start.year, start.month, 1)
    while cur < end:
        if cur.month == 12:
            nxt = datetime(cur.year + 1, 1, 1)
        else:
            nxt = datetime(cur.year, cur.month + 1, 1)
        chunk_lo = max(cur, start)
        chunk_hi = min(nxt, end)
        if chunk_lo < chunk_hi:
            yield chunk_lo, chunk_hi
        cur = nxt


def _density_volume_for_window(ours_fp: pd.DataFrame,
                                 db_fp: pd.DataFrame) -> dict:
    """Compute T6d (cells/min ratio) + T6v (volume/min ratio median) for a
    pre-sliced ours_fp + db_fp. Returns {} if either side is empty."""
    if ours_fp is None or db_fp is None or ours_fp.empty or db_fp.empty:
        return {}
    ours_per_min = ours_fp.groupby('timestamp').size()
    db_per_min = db_fp.groupby('timestamp').size()
    if ours_per_min.empty or db_per_min.empty:
        return {}
    ours_cells_per_min = float(ours_per_min.sum() / ours_per_min.shape[0])
    db_cells_per_min = float(db_per_min.sum() / db_per_min.shape[0])
    density_ratio = ours_cells_per_min / max(db_cells_per_min, 1e-9)

    ours_v = (ours_fp.assign(_v=ours_fp['buy_vol'] + ours_fp['sell_vol'])
              .groupby('timestamp')['_v'].sum())
    db_v = (db_fp.assign(_v=db_fp['buy_vol'] + db_fp['sell_vol'])
            .groupby('timestamp')['_v'].sum())
    common = ours_v.index.intersection(db_v.index)
    if len(common) == 0:
        return {
            'ours_cells_per_min': ours_cells_per_min,
            'db_cells_per_min': db_cells_per_min,
            'density_ratio': density_ratio,
            'volume_ratio_median': None,
            'common_minutes': 0,
        }
    ratios = (ours_v.loc[common] / db_v.loc[common].replace(0, pd.NA)).dropna()
    return {
        'ours_cells_per_min': ours_cells_per_min,
        'db_cells_per_min': db_cells_per_min,
        'density_ratio': density_ratio,
        'volume_ratio_median': float(ratios.median()) if len(ratios) else None,
        'common_minutes': int(len(common)),
    }


def cmd_production_pass(args: argparse.Namespace) -> int:
    """Step 13 — exhaustive comparison over the full --window.

    Streams by month: load ours bars + footprint once (small enough to fit),
    slice per month, query DB per month, run the per-bar tests, accumulate
    into per-day buckets. Produces:
        - validation_daily_passrate_<contract>.csv  (bar-level T3/T4 daily)
        - validation_rollday_<contract>_<rolldate>.csv  (per-roll ±3 days,
          adjustment-invariant only)
    Gate:
        - T4 invariant: 100% of bars across full window
        - T6d density: |ratio - 1| <= 5% over full window
        - T6v volume:  median in [0.9, 1.1] over full window
        - T3 OHLC: >= 99.9% of bars in post-latest-roll subset
    """
    if not args.continuous:
        print("Error: --mode production-pass requires --continuous "
              "(per-expiry mode is for spot checks only).")
        return 1

    csv_lo, csv_hi = csv_date_range(
        args.csv_root, args.contract, args.expiry, args.continuous
    )
    db_lo, db_hi = db_date_range(args.db, args.contract)

    if args.window:
        win_lo, win_hi = parse_window(args.window)
    else:
        # Default: 2025-01-01 to today (UTC midnight tomorrow exclusive).
        today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        win_lo = datetime(2025, 1, 1)
        win_hi = today + timedelta(days=1)

    elig_lo = max(csv_lo, db_lo, win_lo)
    elig_hi = min(csv_hi, db_hi, win_hi)
    if elig_hi <= elig_lo:
        print(f"ERROR: requested --window has no overlap with CSV+DB coverage.")
        print(f"  --window:    {win_lo}  ..  {win_hi}")
        print(f"  csv:         {csv_lo}  ..  {csv_hi}")
        print(f"  db:          {db_lo}  ..  {db_hi}")
        return 1

    # Manifest-driven post-roll detection (continuous mode only).
    manifest_path = args.manifest or os.path.join(args.csv_root, 'manifest.json')
    latest_roll = detect_latest_roll(manifest_path)
    if latest_roll is None:
        print(f"WARNING: no manifest at {manifest_path}; T3 OHLC gate will "
              f"apply over the full window (may show high failure rate due to "
              f"DB back-adjustment on historical bars).")
        latest_roll = elig_lo  # T3 over full window

    # Roll boundaries for drilldown — every entry's start (skip the first).
    roll_dates: list[datetime] = []
    if os.path.exists(manifest_path):
        with open(manifest_path, 'r', encoding='utf-8') as f:
            mf = json.load(f)
        for entry in mf[1:]:
            d = datetime.strptime(entry['date_range_start'], '%Y-%m-%d')
            if elig_lo <= d <= elig_hi:
                roll_dates.append(d)

    print(f"=== Production-pass validation ===")
    print(f"  contract:        {args.contract}  (DB ticker: {db_ticker_for(args.contract)})")
    print(f"  --window:        {win_lo.date()}  ..  {win_hi.date()}")
    print(f"  eligible:        {elig_lo.date()}  ..  {elig_hi.date()}")
    print(f"  latest roll:     {latest_roll.date()}  (T3 OHLC gates after this)")
    print(f"  manifest rolls:  {len(roll_dates)} drilldown candidate(s)")
    print(f"  report dir:      {args.report_dir}")
    print(f"  bar pass-rate:   >= {PRODUCTION_BAR_PASS_RATE:.3%}")

    os.makedirs(args.report_dir, exist_ok=True)

    # Load full ours data once. Bars are small (~MB); footprint can be hundreds
    # of MB but on this machine that's fine. Big_trades only loaded if needed.
    print(f"\nLoading ours CSVs...")
    ours_bars = load_csv_bars(args.csv_root, args.contract, None, True)
    try:
        ours_fp = load_csv_footprint(args.csv_root, args.contract, None, True)
    except FileNotFoundError:
        ours_fp = None
        print(f"  (no continuous footprint CSV — T6 will be skipped)")
    print(f"  bars: {len(ours_bars):,}  footprint: "
          f"{0 if ours_fp is None else len(ours_fp):,}")

    # Accumulators.
    n_bars_total = 0
    n_bars_post_roll = 0
    n_t3_pass_post_roll = 0
    n_t4_pass_total = 0  # T4 evaluated on every merged bar
    n_t4_eval_total = 0
    daily_rows: list[dict] = []

    # For full-window density / volume.
    fp_cells_ours_total = 0
    fp_cells_db_total = 0
    fp_minutes_ours_total = 0
    fp_minutes_db_total = 0
    monthly_volume_medians: list[float] = []  # one per month with ≥1 common minute

    # Per-month loop.
    months = list(_month_windows(elig_lo, elig_hi))
    for i, (m_lo, m_hi) in enumerate(months, 1):
        ours_m = ours_bars[
            (ours_bars['timestamp'] >= m_lo) & (ours_bars['timestamp'] < m_hi)
        ]
        db_m = load_db_bars(args.db, args.contract, m_lo, m_hi)
        merged = pd.merge(ours_m, db_m, on='timestamp',
                          suffixes=('_ours', '_db'), how='inner')

        if ours_fp is not None:
            ours_fp_m = ours_fp[
                (ours_fp['timestamp'] >= m_lo) & (ours_fp['timestamp'] < m_hi)
            ]
            db_fp_m = load_db_footprint(args.db, args.contract, m_lo, m_hi)
        else:
            ours_fp_m = pd.DataFrame()
            db_fp_m = pd.DataFrame()

        n_merged = len(merged)
        n_bars_total += n_merged

        if n_merged > 0:
            # T4 invariant on ours (full-window gate, evaluated per bar).
            t4_series = (merged['buy_vol_ours'] + merged['sell_vol_ours']
                         == merged['volume_ours'])
            n_t4_eval_total += int(len(t4_series))
            n_t4_pass_total += int(t4_series.sum())

            # T3 OHLC — only count toward gate when bar is in post-roll subset.
            avg_price = float(merged['close_db'].mean())
            tol = avg_price * PRICE_TOLERANCE_PCT
            diffs = pd.concat([
                (merged['open_ours'] - merged['open_db']).abs(),
                (merged['high_ours'] - merged['high_db']).abs(),
                (merged['low_ours'] - merged['low_db']).abs(),
                (merged['close_ours'] - merged['close_db']).abs(),
            ], axis=1)
            max_diff = diffs.max(axis=1)
            t3_series = max_diff <= tol

            post_roll_mask = merged['timestamp'] >= latest_roll
            n_post_in_month = int(post_roll_mask.sum())
            n_bars_post_roll += n_post_in_month
            if n_post_in_month > 0:
                n_t3_pass_post_roll += int((t3_series & post_roll_mask).sum())

            # Daily aggregation.
            daily = pd.DataFrame({
                'date': merged['timestamp'].dt.date,
                't3_pass': t3_series,
                't4_pass': t4_series,
                'post_roll': post_roll_mask,
            })
            grp = daily.groupby('date').agg(
                n=('t4_pass', 'size'),
                n_t3=('t3_pass', 'sum'),
                n_t4=('t4_pass', 'sum'),
                n_post=('post_roll', 'sum'),
            )
            for d, row in grp.iterrows():
                daily_rows.append({
                    'date': d.isoformat() if hasattr(d, 'isoformat') else str(d),
                    'n_bars': int(row['n']),
                    'n_t3_pass': int(row['n_t3']),
                    'n_t4_pass': int(row['n_t4']),
                    'n_post_roll': int(row['n_post']),
                })

        # Footprint aggregates for the month.
        if not ours_fp_m.empty and not db_fp_m.empty:
            o_per_min = ours_fp_m.groupby('timestamp').size()
            d_per_min = db_fp_m.groupby('timestamp').size()
            fp_cells_ours_total += int(o_per_min.sum())
            fp_cells_db_total += int(d_per_min.sum())
            fp_minutes_ours_total += int(o_per_min.shape[0])
            fp_minutes_db_total += int(d_per_min.shape[0])
            stats = _density_volume_for_window(ours_fp_m, db_fp_m)
            vrm = stats.get('volume_ratio_median')
            if vrm is not None:
                monthly_volume_medians.append(vrm)

        print(f"  [{i}/{len(months)}] {m_lo.date()}..{m_hi.date()}  "
              f"merged={n_merged:>6,}  fp_min(ours/db)="
              f"{0 if ours_fp_m.empty else int(ours_fp_m.groupby('timestamp').ngroups):>5,}/"
              f"{0 if db_fp_m.empty else int(db_fp_m.groupby('timestamp').ngroups):>5,}")

        # Drop month dataframes — let GC reclaim.
        del ours_m, db_m, merged
        if ours_fp is not None:
            del ours_fp_m, db_fp_m

    # --- Per-rolldate drilldowns (adjustment-invariant only).
    rollday_paths: list[str] = []
    if ours_fp is not None and roll_dates:
        for rd in roll_dates:
            r_lo = rd - timedelta(days=3)
            r_hi = rd + timedelta(days=4)  # +3 inclusive
            ours_fp_r = ours_fp[
                (ours_fp['timestamp'] >= r_lo) & (ours_fp['timestamp'] < r_hi)
            ]
            db_fp_r = load_db_footprint(args.db, args.contract, r_lo, r_hi)
            stats = _density_volume_for_window(ours_fp_r, db_fp_r)
            if not stats:
                continue
            path = os.path.join(
                args.report_dir,
                f"validation_rollday_{args.contract}_{rd.date()}.csv",
            )
            with open(path, 'w', encoding='utf-8', newline='') as f:
                w = csv.writer(f)
                w.writerow(['metric', 'value'])
                w.writerow(['roll_date', rd.date().isoformat()])
                w.writerow(['window_start', r_lo.date().isoformat()])
                w.writerow(['window_end_exclusive', r_hi.date().isoformat()])
                for k, v in stats.items():
                    w.writerow([k, v])
            rollday_paths.append(path)

    # --- Daily passrate CSV.
    daily_csv = os.path.join(
        args.report_dir, f"validation_daily_passrate_{args.contract}.csv"
    )
    with open(daily_csv, 'w', encoding='utf-8', newline='') as f:
        w = csv.writer(f)
        w.writerow(['date', 'n_bars', 'n_t3_pass', 'n_t4_pass', 'n_post_roll'])
        for row in daily_rows:
            w.writerow([row['date'], row['n_bars'], row['n_t3_pass'],
                        row['n_t4_pass'], row['n_post_roll']])

    # --- Aggregate gate.
    full_density_ratio = None
    if fp_minutes_ours_total > 0 and fp_minutes_db_total > 0:
        ours_cpm = fp_cells_ours_total / fp_minutes_ours_total
        db_cpm = fp_cells_db_total / fp_minutes_db_total
        full_density_ratio = ours_cpm / max(db_cpm, 1e-9)
    full_volume_median = (
        sorted(monthly_volume_medians)[len(monthly_volume_medians) // 2]
        if monthly_volume_medians else None
    )

    t4_pass_rate = (n_t4_pass_total / n_t4_eval_total) if n_t4_eval_total else 1.0
    t3_pass_rate = (n_t3_pass_post_roll / n_bars_post_roll) if n_bars_post_roll else 1.0
    t6d_pass = (full_density_ratio is not None
                and abs(full_density_ratio - 1.0) <= PRODUCTION_DENSITY_TOL)
    t6v_pass = (full_volume_median is not None
                and PRODUCTION_VOLUME_RANGE[0] <= full_volume_median
                <= PRODUCTION_VOLUME_RANGE[1])

    print(f"\n=== Aggregate ({n_bars_total:,} merged bars total) ===")
    print(f"  Adjustment-invariant tests (full window):")
    print(f"    [BLOCK] T4 invariant: {n_t4_pass_total:,}/{n_t4_eval_total:,} "
          f"({t4_pass_rate:.6%})")
    if full_density_ratio is not None:
        print(f"    [BLOCK] T6d density ratio: {full_density_ratio:.4f}  "
              f"({'PASS' if t6d_pass else 'FAIL'} — within {PRODUCTION_DENSITY_TOL:.0%})")
    else:
        print(f"    [BLOCK] T6d density ratio: n/a (no footprint data)")
    if full_volume_median is not None:
        print(f"    [BLOCK] T6v volume ratio:  {full_volume_median:.4f}  "
              f"({'PASS' if t6v_pass else 'FAIL'} — within "
              f"[{PRODUCTION_VOLUME_RANGE[0]}, {PRODUCTION_VOLUME_RANGE[1]}])")
    else:
        print(f"    [BLOCK] T6v volume ratio:  n/a")

    print(f"  Adjustment-sensitive tests (post-latest-roll subset, "
          f"{n_bars_post_roll:,} bars):")
    print(f"    [BLOCK] T3 OHLC: {n_t3_pass_post_roll:,}/{n_bars_post_roll:,} "
          f"({t3_pass_rate:.6%}; needs >= {PRODUCTION_BAR_PASS_RATE:.3%})")

    print(f"\n  Daily passrate CSV: {daily_csv}")
    if rollday_paths:
        print(f"  Per-rolldate drilldowns ({len(rollday_paths)}):")
        for p in rollday_paths:
            print(f"    {p}")

    # Apply gate.
    block_failures = []
    if t4_pass_rate < 1.0:
        block_failures.append(
            f"T4 invariant: {n_t4_pass_total}/{n_t4_eval_total} "
            f"({t4_pass_rate:.4%}) — must be 100%"
        )
    if full_density_ratio is not None and not t6d_pass:
        block_failures.append(
            f"T6d density={full_density_ratio:.3f} "
            f"(allowed |diff| <= {PRODUCTION_DENSITY_TOL:.0%})"
        )
    if full_volume_median is not None and not t6v_pass:
        block_failures.append(
            f"T6v volume_median={full_volume_median:.3f} "
            f"(allowed {PRODUCTION_VOLUME_RANGE})"
        )
    if n_bars_post_roll > 0 and t3_pass_rate < PRODUCTION_BAR_PASS_RATE:
        block_failures.append(
            f"T3 OHLC: {n_t3_pass_post_roll}/{n_bars_post_roll} "
            f"({t3_pass_rate:.4%}) < {PRODUCTION_BAR_PASS_RATE:.3%}"
        )

    gate_pass = len(block_failures) == 0
    if block_failures:
        print(f"\n  GATE: FAIL — " + "; ".join(block_failures))
    else:
        print(f"\n  GATE: PASS")
    return 0 if gate_pass else 1


def cmd_debug_window(args: argparse.Namespace) -> int:
    start, end = parse_debug_window(args.debug_window)

    ours = load_csv_bars(
        csv_root=args.csv_root,
        contract=args.contract,
        expiry=args.expiry,
        continuous=args.continuous,
    )
    # Filter to window.
    mask = (ours['timestamp'] >= start) & (ours['timestamp'] < end)
    ours = ours.loc[mask].reset_index(drop=True)

    db = load_db_bars(args.db, args.contract, start, end)

    scope_label = "continuous" if args.continuous else f"per-expiry {args.expiry}"
    print_side_by_side(ours, db, args.contract, scope_label, start, end)

    if len(ours) == 0 and len(db) == 0:
        print("\nNote: both sides empty for this window. Pick a window where")
        print("      the expiry is front-month and DB coverage exists.")
        return 1
    return 0


def main(argv: list) -> int:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument('--mode', choices=['debug-window', 'random', 'production-pass'],
                   required=True)
    p.add_argument('--contract', required=True,
                   help='SCID contract symbol (MES, MNQ, etc.)')
    p.add_argument('--csv-root', required=True,
                   help='Directory containing bars CSV(s) to validate')
    p.add_argument('--db', required=True,
                   help='Path to meta_learner.db')

    scope = p.add_mutually_exclusive_group(required=True)
    scope.add_argument('--expiry', help='Per-expiry mode: 2-3 char expiry like "h25"')
    scope.add_argument('--continuous', action='store_true',
                       help='Continuous mode: validate rollover-stitched CSV')

    p.add_argument('--debug-window',
                   help='For --mode debug-window: "YYYY-MM-DD HH:MM-HH:MM"')
    p.add_argument('--slices', type=int, default=10,
                   help='Random mode: number of slices (default 10)')
    p.add_argument('--slice-hours', type=int, default=1,
                   help='Random mode: slice length in hours (default 1)')
    p.add_argument('--seed', type=int, default=42,
                   help='Random mode: RNG seed for reproducibility')
    p.add_argument('--days-back', type=int, default=None,
                   help='Random mode: limit sampling to last N days of '
                        'intersection (use to focus on recent data where '
                        'DB back-adjustment is minimal)')

    p.add_argument('--window',
                   help="Production-pass: 'YYYY-MM-DD:YYYY-MM-DD' "
                        "(default 2025-01-01:today)")
    p.add_argument('--manifest',
                   help='Production-pass: path to manifest.json '
                        "(default <csv-root>/manifest.json)")
    p.add_argument('--report-dir', default='.',
                   help='Production-pass: directory for daily-passrate + '
                        "rollday CSVs (default cwd)")

    args = p.parse_args(argv)

    if args.mode == 'debug-window':
        if not args.debug_window:
            p.error("--mode debug-window requires --debug-window 'YYYY-MM-DD HH:MM-HH:MM'")
        return cmd_debug_window(args)

    if args.mode == 'random':
        return cmd_random(args)

    if args.mode == 'production-pass':
        return cmd_production_pass(args)

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
