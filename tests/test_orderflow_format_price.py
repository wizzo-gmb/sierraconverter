"""Tests for _format_price() in scid_to_orderflow.

The invariant this guards: footprint and big_trades CSV prices use 4
decimals; bar OHLC prices must format to the same price-space so a
footprint's price is never outside [bar.low, bar.high] purely due to
rounding. Before the fix, _format_price hard-coded 2 decimals — fine for
0.01-and-coarser ticks but broken for ZN/10Y at 0.015625, where bar OHLC
rounded 4.5625 → 4.56 while footprints stayed at 4.5625.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))

from scid_to_orderflow import _format_price, _format_price_f4  # noqa: E402


def test_default_two_decimals_for_cent_tick():
    """Cent-or-coarser ticks (CL/MCL=0.01, equities=0.25, etc.) keep 2 decimals."""
    assert _format_price(4521.25, tick_size=0.25) == "4521.25"
    assert _format_price(72.18, tick_size=0.01) == "72.18"
    assert _format_price(2350.10, tick_size=0.10) == "2350.10"


def test_four_decimals_for_sub_cent_tick():
    """Sub-cent ticks (ZN/10Y at 1/64 = 0.015625) get 4 decimals so the
    footprint/OHLC price-space invariant holds."""
    assert _format_price(4.5625, tick_size=0.015625) == "4.5625"
    assert _format_price(109.9531, tick_size=0.015625) == "109.9531"


def test_zn_10y_invariant_footprint_in_bar_range():
    """End-to-end invariant: with the fixed format, a footprint price that
    lives exactly on a 1/64 tick boundary will fall within the [low, high]
    range of a bar whose extrema are at the same tick."""
    tick = 0.015625
    int_ticks = 292  # 292 * 0.015625 = 4.5625
    price = int_ticks * tick

    # Simulate a bar where high == low == footprint price (the failure
    # case before the fix).
    bar_high_str = _format_price(price, tick_size=tick)
    bar_low_str = _format_price(price, tick_size=tick)
    fp_price_str = _format_price_f4(price)

    # All three strings parse to the same float; the footprint stays
    # within the bar's range.
    assert float(fp_price_str) <= float(bar_high_str)
    assert float(fp_price_str) >= float(bar_low_str)
    # Exact equality at the same price level.
    assert bar_high_str == fp_price_str == "4.5625"


def test_default_argument_still_two_decimals():
    """Calling _format_price without tick_size keeps legacy 2-decimal
    behavior, so any caller that hasn't been updated yet still produces
    valid output (just lossy for treasuries)."""
    assert _format_price(4521.25) == "4521.25"
    assert _format_price(4.5625) == "4.56"  # legacy lossy path
