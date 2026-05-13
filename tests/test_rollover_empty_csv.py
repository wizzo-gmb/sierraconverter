"""Tests for rollover_engine handling of empty per-expiry CSVs.

Pre-2019 micros (MES/MNQ/MYM/M2K), pre-2017 MGC, and pre-2021 MCL all
have header-only CSV files because the underlying SCID files are empty
placeholders (the contracts didn't exist yet). The orchestrator's
stage-1 build still emits header-only CSVs for those expiries; the
rollover engine must tolerate them without crashing.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))

from rollover_engine import compute_manifest, daily_volumes  # noqa: E402


BARS_HEADER = "timestamp,open,high,low,close,volume,buy_vol,sell_vol,delta"


def _write_csv(path: Path, rows: list[str]) -> None:
    path.write_text(BARS_HEADER + "\n" + "\n".join(rows) + ("\n" if rows else ""))


def test_daily_volumes_empty_csv(tmp_path: Path):
    """An empty per-expiry CSV must yield an empty (date, volume) frame
    rather than raising AttributeError from pandas' .dt accessor."""
    empty_csv = tmp_path / "m2k_h15_1m.csv"
    _write_csv(empty_csv, [])
    df = daily_volumes(empty_csv)
    assert df.empty
    assert list(df.columns) == ["date", "volume"]


def test_daily_volumes_non_empty_csv(tmp_path: Path):
    """Sanity-check: real data still aggregates correctly."""
    csv = tmp_path / "m2k_h20_1m.csv"
    _write_csv(csv, [
        "2020-01-15T14:30:00,1500.0,1500.5,1499.5,1500.2,100,50,50,0",
        "2020-01-15T14:31:00,1500.2,1501.0,1500.0,1500.8,200,120,80,40",
        "2020-01-16T14:30:00,1501.0,1502.0,1500.5,1501.5,150,75,75,0",
    ])
    df = daily_volumes(csv)
    by_date = dict(zip(df["date"].astype(str), df["volume"].astype(int)))
    assert by_date == {"2020-01-15": 300, "2020-01-16": 150}


def test_compute_manifest_with_mix_of_empty_and_real_expiries(tmp_path: Path):
    """Mix empty header-only files (simulating pre-launch micros) with
    real-data files. The manifest must include only the real expiries
    and not crash on the empties.
    """
    # Two empty expiries that should be silently ignored.
    _write_csv(tmp_path / "m2k_h18_1m.csv", [])
    _write_csv(tmp_path / "m2k_h19_1m.csv", [])
    # One real expiry — should become the front-month for its date range.
    _write_csv(tmp_path / "m2k_h20_1m.csv", [
        "2020-01-15T14:30:00,1500.0,1500.5,1499.5,1500.2,100,50,50,0",
        "2020-01-16T14:30:00,1501.0,1502.0,1500.5,1501.5,150,75,75,0",
    ])
    manifest = compute_manifest("m2k", str(tmp_path))
    assert len(manifest) == 1
    entry = manifest[0]
    assert entry["source_expiry"] == "h20"
    assert entry["date_range_start"] == "2020-01-15"
    assert entry["date_range_end"] == "2020-01-16"
