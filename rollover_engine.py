"""SCID -> Orderflow Pipeline (Path C, Stage 2a — Rollover engine).

Computes a volume-based front-month manifest for a given root (e.g. MES) by
reading the per-expiry bars CSVs Stage 1 produced. For each calendar date,
picks the expiry with the highest daily volume (with a volume-ratio sanity
threshold), then groups consecutive dates sharing the same front-month into
manifest entries.

Manifest entry shape:
    {"date_range_start": "2025-03-12",
     "date_range_end":   "2025-06-08",
     "source_expiry":    "h25",
     "roll_method":      "volume" | "calendar_fallback"}

Usage:
    python rollover_engine.py MES \\
        --csv-root E:/Data/Orderflow/MES/per_expiry \\
        --output   E:/Data/Orderflow/MES/continuous/manifest.json

See plan: C:\\Users\\Admin\\.claude\\plans\\the-data-loading-is-memoized-knuth.md
"""

import argparse
import json
import os
import re
import sys
from pathlib import Path

import pandas as pd

# Futures month-code mapping (CME convention).
MONTH_CODE_TO_NUM = {
    'F': 1,  'G': 2,  'H': 3,  'J': 4,  'K': 5,  'M': 6,
    'N': 7,  'Q': 8,  'U': 9,  'V': 10, 'X': 11, 'Z': 12,
}

# Per-expiry filename pattern: {root}_{expiry}_1m.csv where expiry = month-code + 2-digit year.
_PER_EXPIRY_RE = re.compile(
    r'^(?P<root>[a-z0-9]+)_(?P<code>[fghjkmnquvxz])(?P<yy>\d{2})_1m\.csv$',
    re.IGNORECASE,
)


def parse_expiry(expiry: str) -> tuple[int, int]:
    """Parse 'h25' -> (month_num=3, year=2025).

    Two-digit years are interpreted in the 21st century. Raises ValueError on
    malformed input.
    """
    if len(expiry) != 3:
        raise ValueError(f"expiry must be 3 chars (month-code + 2-digit year); got {expiry!r}")
    code = expiry[0].upper()
    if code not in MONTH_CODE_TO_NUM:
        raise ValueError(f"unknown month code {code!r} in expiry {expiry!r}")
    try:
        yy = int(expiry[1:])
    except ValueError as e:
        raise ValueError(f"bad year in expiry {expiry!r}") from e
    return MONTH_CODE_TO_NUM[code], 2000 + yy


def discover_expiries(csv_root: str, root: str) -> dict:
    """Return {expiry_str: Path} for all per-expiry bars CSVs in csv_root for `root`.

    Filename convention: {root}_{XYY}_1m.csv (e.g. mes_h25_1m.csv).
    """
    found = {}
    if not os.path.isdir(csv_root):
        raise FileNotFoundError(f"csv_root not found: {csv_root}")
    for entry in os.listdir(csv_root):
        m = _PER_EXPIRY_RE.match(entry)
        if not m:
            continue
        if m.group('root').lower() != root.lower():
            continue
        expiry = (m.group('code') + m.group('yy')).lower()
        found[expiry] = Path(csv_root) / entry
    return found


def daily_volumes(bars_csv: Path) -> pd.DataFrame:
    """Read a per-expiry bars CSV and return a DataFrame with columns ['date', 'volume'].

    Header-only CSVs (contracts that existed in Sierra's directory but had
    no ticks — e.g. pre-2019 micros, pre-2017 micro gold, pre-2021 micro
    crude) yield an empty DataFrame whose `timestamp` column is object
    dtype because pandas can't infer datetime type without any rows.
    Calling `.dt` on an object Series raises; bail early with an empty
    result so the empty expiry contributes nothing to the manifest.
    """
    df = pd.read_csv(bars_csv, usecols=['timestamp', 'volume'], parse_dates=['timestamp'])
    if df.empty:
        return pd.DataFrame({'date': [], 'volume': []})
    df['date'] = df['timestamp'].dt.date
    return df.groupby('date', as_index=False)['volume'].sum()


def _expiry_sort_key(expiry: str) -> tuple[int, int]:
    """Sort key for an expiry: (year, month). Earlier expiries sort first."""
    month, year = parse_expiry(expiry)
    return (year, month)


def compute_manifest(root: str, csv_root: str,
                      min_volume_ratio: float = 0.01) -> list:
    """Compute the rollover manifest.

    For each calendar date in any expiry's coverage, pick the expiry with the
    highest daily volume — but only among candidates whose volume is at least
    `min_volume_ratio` of the day's max (filters out dribble-volume contracts
    that should never be front-month). On ties, prefer the earlier expiry.

    Returns a list of dicts (sorted by date_range_start).
    """
    expiries = discover_expiries(csv_root, root)
    if not expiries:
        raise ValueError(f"No per-expiry CSVs found for root {root!r} in {csv_root}")

    # Build {date: {expiry: daily_volume}}
    daily_by_expiry = {}
    for expiry, csv_path in expiries.items():
        # Validate the expiry before reading (cheap fail).
        parse_expiry(expiry)
        dv = daily_volumes(csv_path)
        for _, row in dv.iterrows():
            d = row['date']
            v = int(row['volume'])
            daily_by_expiry.setdefault(d, {})[expiry] = v

    # Pick front-month per date.
    sorted_dates = sorted(daily_by_expiry.keys())
    front_per_date = {}
    method_per_date = {}
    for d in sorted_dates:
        candidates = daily_by_expiry[d]  # {expiry: volume}
        max_vol = max(candidates.values())
        threshold = max(1, int(min_volume_ratio * max_vol))

        # Drop expiries below the volume threshold.
        valid = {e: v for e, v in candidates.items() if v >= threshold}
        method = "volume"
        if not valid:
            # Fallback: take all candidates (no expiry passed the threshold; rare).
            valid = candidates
            method = "calendar_fallback"

        # Tie-break: highest volume; on tie prefer earlier expiry (lower year/month).
        best = max(valid.items(), key=lambda kv: (kv[1], -_expiry_sort_key(kv[0])[0],
                                                    -_expiry_sort_key(kv[0])[1]))
        front_per_date[d] = best[0]
        method_per_date[d] = method

    # Group consecutive dates with the same front-month into entries.
    manifest = []
    current_expiry = None
    current_start = None
    current_end = None
    current_method = None
    for d in sorted_dates:
        e = front_per_date[d]
        m = method_per_date[d]
        if e != current_expiry or m != current_method:
            if current_expiry is not None:
                manifest.append({
                    'date_range_start': current_start.isoformat(),
                    'date_range_end':   current_end.isoformat(),
                    'source_expiry':    current_expiry,
                    'roll_method':      current_method,
                })
            current_expiry = e
            current_start = d
            current_method = m
        current_end = d
    if current_expiry is not None:
        manifest.append({
            'date_range_start': current_start.isoformat(),
            'date_range_end':   current_end.isoformat(),
            'source_expiry':    current_expiry,
            'roll_method':      current_method,
        })
    return manifest


def main(argv: list) -> int:
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument('root', help='Contract root (e.g., MES, MGC, MNQ)')
    ap.add_argument('--csv-root', required=True,
                    help='Directory containing per-expiry _1m.csv files')
    ap.add_argument('--output', required=True,
                    help='Output manifest.json path')
    ap.add_argument('--min-volume-ratio', type=float, default=0.01,
                    help='Min daily-volume ratio (vs day max) to qualify as front-month '
                         'candidate (default 0.01 = 1%%)')
    args = ap.parse_args(argv)

    manifest = compute_manifest(args.root, args.csv_root, args.min_volume_ratio)

    out_dir = os.path.dirname(os.path.abspath(args.output))
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    with open(args.output, 'w', encoding='utf-8') as f:
        json.dump(manifest, f, indent=2)

    print(f"=== rollover manifest written ===")
    print(f"  root:          {args.root}")
    print(f"  csv-root:      {args.csv_root}")
    print(f"  output:        {args.output}")
    print(f"  date range:    {manifest[0]['date_range_start']} .. {manifest[-1]['date_range_end']}")
    print(f"  entries:       {len(manifest)}")
    n_fallback = sum(1 for e in manifest if e['roll_method'] == 'calendar_fallback')
    if n_fallback:
        print(f"  fallback rows: {n_fallback} (calendar_fallback — no candidate met the volume threshold)")
    print(f"\nFirst {min(20, len(manifest))} entries:")
    for entry in manifest[:20]:
        marker = "" if entry['roll_method'] == 'volume' else "  [fallback]"
        print(f"  {entry['date_range_start']} .. {entry['date_range_end']}: "
              f"{entry['source_expiry']}{marker}")
    if len(manifest) > 20:
        print(f"  ... and {len(manifest) - 20} more")
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
