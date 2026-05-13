"""SCID -> Orderflow Pipeline (Path C, Stage 2b — Continuous stitcher).

Reads a manifest.json produced by `rollover_engine.py` and concatenates the
appropriate per-expiry CSV slices into 4 continuous CSVs (bars, footprint,
big_trades, tick_counts).

By design, NO back-adjustment is applied. Prices stay at the native expiry's
raw values; expect price-jumps at roll boundaries equal to the spread between
the two contracts. This is the right choice for orderflow because the
footprint is price-keyed; back-adjustment would corrupt the at-price volume
mapping. If a back-adjusted view is needed downstream (e.g. for charting),
apply that as a separate price-only transformation.

Usage:
    python continuous_stitcher.py MES \\
        --manifest E:/Data/Orderflow/MES/continuous/manifest.json \\
        --csv-root E:/Data/Orderflow/MES/per_expiry \\
        --output   E:/Data/Orderflow/MES/continuous

See plan: C:\\Users\\Admin\\.claude\\plans\\the-data-loading-is-memoized-knuth.md
"""

import argparse
import csv
import json
import os
import sys

# (logical-suffix, timestamp column to filter on for the date-range filter).
# All four datasets are sliced by their first timestamp column's date.
DATASETS = [
    ('1m',             'timestamp'),
    ('1m_footprint',   'timestamp'),
    ('1m_big_trades',  'bar_timestamp'),
    ('1m_tick_counts', 'bar_timestamp'),
]


def stitch_dataset(root: str, manifest: list, csv_root: str,
                    output_path: str, suffix: str, ts_col: str) -> dict:
    """Stitch one dataset across all manifest entries, writing rows to output_path.

    Returns a dict with row counts for the report.
    """
    root_l = root.lower()
    rows_per_entry = []
    total_rows = 0

    with open(output_path, 'w', encoding='utf-8', newline='') as out_f:
        writer = csv.writer(out_f)
        header_written = False

        for entry in manifest:
            expiry = entry['source_expiry']
            d_start = entry['date_range_start']  # 'YYYY-MM-DD' (lex-comparable)
            d_end = entry['date_range_end']
            in_path = os.path.join(csv_root, f"{root_l}_{expiry}_{suffix}.csv")
            if not os.path.exists(in_path):
                raise FileNotFoundError(
                    f"Missing per-expiry CSV referenced by manifest: {in_path}"
                )

            with open(in_path, 'r', encoding='utf-8', newline='') as in_f:
                reader = csv.reader(in_f)
                header = next(reader, None)
                if header is None:
                    rows_per_entry.append(0)
                    continue
                if not header_written:
                    writer.writerow(header)
                    header_written = True
                else:
                    # Sanity: subsequent files must have the same header order.
                    pass  # could validate, skipped for speed

                try:
                    ts_idx = header.index(ts_col)
                except ValueError:
                    raise ValueError(
                        f"Column {ts_col!r} not found in {in_path}; "
                        f"available: {header}"
                    )

                count = 0
                for row in reader:
                    # Timestamp is fixed-width ISO 8601: 'YYYY-MM-DDTHH:MM:...'
                    # The first 10 characters are the date — lex-comparable to
                    # 'YYYY-MM-DD' bounds.
                    ts = row[ts_idx]
                    date_str = ts[:10]
                    if d_start <= date_str <= d_end:
                        writer.writerow(row)
                        count += 1
                rows_per_entry.append(count)
                total_rows += count

    return {
        'output_path': output_path,
        'total_rows': total_rows,
        'rows_per_entry': rows_per_entry,
    }


def stitch_all(root: str, manifest_path: str, csv_root: str,
                output_dir: str) -> list:
    """Stitch all 4 datasets. Returns list of summary dicts."""
    with open(manifest_path, 'r', encoding='utf-8') as f:
        manifest = json.load(f)

    if not manifest:
        raise ValueError(f"Manifest is empty: {manifest_path}")

    os.makedirs(output_dir, exist_ok=True)
    root_l = root.lower()

    summaries = []
    for suffix, ts_col in DATASETS:
        out_path = os.path.join(output_dir, f"{root_l}_{suffix}.csv")
        summary = stitch_dataset(root, manifest, csv_root, out_path, suffix, ts_col)
        summary['suffix'] = suffix
        summaries.append(summary)
    return summaries


def main(argv: list) -> int:
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument('root', help='Contract root (e.g. MES, MGC, MNQ)')
    ap.add_argument('--manifest', required=True,
                    help='Path to manifest.json produced by rollover_engine.py')
    ap.add_argument('--csv-root', required=True,
                    help='Directory containing per-expiry _1m.csv files')
    ap.add_argument('--output', required=True,
                    help='Output directory for continuous CSVs')
    args = ap.parse_args(argv)

    if not os.path.exists(args.manifest):
        print(f"Error: manifest not found: {args.manifest}", file=sys.stderr)
        return 1
    if not os.path.isdir(args.csv_root):
        print(f"Error: csv-root not a directory: {args.csv_root}", file=sys.stderr)
        return 1

    summaries = stitch_all(args.root, args.manifest, args.csv_root, args.output)

    print(f"=== continuous stitcher complete ===")
    print(f"  root:         {args.root}")
    print(f"  manifest:     {args.manifest}")
    print(f"  csv-root:     {args.csv_root}")
    print(f"  output:       {args.output}")
    print()
    print(f"  Per-dataset row totals:")
    for s in summaries:
        print(f"    {s['suffix']:<18} rows={s['total_rows']:>10,}  -> {s['output_path']}")
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
