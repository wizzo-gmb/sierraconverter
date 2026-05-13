"""SCID -> Orderflow Pipeline (Path C, Stage 1).

Reads Sierra Chart SCID tick files directly and emits per-expiry orderflow
CSVs matching UnifiedOrderFlowDumper.cs's output schema, preserving Sierra's
actual bid_vol/ask_vol aggression data instead of inferring it via Lee-Ready.

Stage 1 is per-expiry only. Stage 2 (rollover stitching) lives in
rollover_engine.py + continuous_stitcher.py.

See plan: C:\\Users\\Admin\\.claude\\plans\\the-data-loading-is-memoized-knuth.md
"""

import csv
import glob
import os
import struct
import sys
import time as _time
from datetime import date, datetime, time, timedelta
from typing import Iterator, NamedTuple

# SCID file format constants. Source: scid_to_nt8_converter.py:47-48
SIZE_HEADER = 0x38   # 56 bytes
SIZE_RECORD = 0x28   # 40 bytes

# Sentinel values marking unbundled multi-leg trade boundaries.
# Source: scid_to_nt8_converter.py:71-73
FIRST_SUB_TRADE = -1.99900095e+37
LAST_SUB_TRADE = -1.99900197e+37


# Sierra Chart epoch — SCID dt_raw is int64 microseconds since this UTC moment.
SIERRA_EPOCH = datetime(1899, 12, 30)


def scid_dt_raw_to_datetime(dt_raw: int):
    """Convert SCID raw int64 microsecond timestamp to naive datetime (UTC).

    Replaces scid_to_nt8_converter.deserialize_datetime, which had a
    rounding-overflow bug at midnight boundaries (a tick at 23:59:59.999998
    would decode as 00:00:00 of the SAME day, causing duplicate bars at
    midnight in the bars CSV). This implementation uses timedelta directly
    and is exact to microsecond precision.

    Returns None on decode failure (negative or absurdly large dt_raw).
    """
    try:
        return SIERRA_EPOCH + timedelta(microseconds=dt_raw)
    except (OverflowError, ValueError):
        return None


def deserialize_datetime(excel_date_time):
    """Legacy interface kept for tests / compatibility. Internally now uses
    the corrected microsecond-direct path. Accepts a float Excel date-time
    (days since 1899-12-30) and returns a naive datetime truncated to second
    precision (matching the legacy contract)."""
    try:
        microseconds = round(excel_date_time * 86400 * 1_000_000)
        dt = SIERRA_EPOCH + timedelta(microseconds=microseconds)
        return dt.replace(microsecond=0)  # legacy returned second precision
    except (OverflowError, ValueError):
        return None


class TickEvent(NamedTuple):
    """A single resolved tick event from a SCID file.

    For tick data, Sierra Chart uses: high=ask, low=bid, close=last.
    bid_vol and ask_vol are Sierra's ground-truth aggressor data:
        bid_vol = volume traded at the bid (sell-side aggression)
        ask_vol = volume traded at the ask (buy-side aggression)
    """
    ts: datetime           # naive UTC, microsecond precision
    last: float            # last trade price
    bid: float             # bid at trade time (after cleanup)
    ask: float             # ask at trade time (after cleanup)
    bid_vol: int           # volume traded at bid
    ask_vol: int           # volume traded at ask


def iter_scid_records(path: str, preserve_subtrades: bool = True
                       ) -> Iterator[TickEvent]:
    """Generator: yields TickEvent for each resolved tick in a SCID file.

    `preserve_subtrades` (default True): each SCID record emits its own
        TickEvent. Multi-leg trades marked with FIRST_SUB_TRADE / LAST_SUB_TRADE
        sentinels are emitted as individual sub-trade events at their actual
        prices, preserving full price-level detail for footprint reconstruction.
        This matches what NT8 sees from a native broker feed, where each
        sub-trade arrives as its own tick.

    `preserve_subtrades=False`: legacy NT8-import behavior — the unbundled-
        trade markers cause volumes to be summed into a single TickEvent at
        the LAST sub-trade's price (timestamped at the FIRST sub's time).
        Use only for byte-equivalence with `scid_to_nt8_converter.py`.

    Skips invalid records (datetime decode failure or close <= 0). Applies
    NT8-equivalent price cleanup (bid/ask defaults, extreme-spread defense,
    bid <= last <= ask). Constant memory regardless of file size.

    Source for legacy aggregation logic: scid_to_nt8_converter.py:67-141.
    """
    with open(path, 'rb') as f:
        f.seek(SIZE_HEADER)

        # Legacy aggregation state (only used when preserve_subtrades=False).
        in_unbundled = False
        u_dt_raw = 0
        u_last = 0.0
        u_bid = 0.0
        u_ask = 0.0
        u_bid_vol = 0
        u_ask_vol = 0

        while True:
            data = f.read(SIZE_RECORD)
            if len(data) < SIZE_RECORD:
                break

            dt_raw, o, h, l, c, _vol, _trades, bid_vol, ask_vol = struct.unpack(
                '<q4f4I', data
            )

            dt = scid_dt_raw_to_datetime(dt_raw)
            if dt is None or c <= 0:
                continue

            is_first_sub = abs(o - FIRST_SUB_TRADE) < 1e30
            is_last_sub = abs(o - LAST_SUB_TRADE) < 1e30

            if not preserve_subtrades:
                # Legacy collapse: aggregate FIRST..LAST into one event.
                if is_first_sub:
                    in_unbundled = True
                    u_dt_raw = dt_raw
                    u_last = c
                    u_bid = l
                    u_ask = h
                    u_bid_vol = int(bid_vol)
                    u_ask_vol = int(ask_vol)
                    continue
                elif in_unbundled:
                    u_bid_vol += int(bid_vol)
                    u_ask_vol += int(ask_vol)
                    u_last = c
                    u_bid = l
                    u_ask = h
                    if is_last_sub:
                        dt_raw = u_dt_raw
                        c = u_last
                        l = u_bid
                        h = u_ask
                        bid_vol = u_bid_vol
                        ask_vol = u_ask_vol
                        in_unbundled = False
                        dt = scid_dt_raw_to_datetime(dt_raw)
                        if dt is None:
                            continue
                    else:
                        continue
            # preserve_subtrades=True: every record is emitted at its own
            # price. The FIRST/LAST sub markers carry valid sub-trade data in
            # c/l/h/bid_vol/ask_vol; the sentinel only lives in the open field
            # which we don't use. Middle sub-trades are regular records.

            # NT8-equivalent price cleanup (matches scid_to_nt8_converter.py:152-175).
            last_price = float(c)
            bid_price = float(l)
            ask_price = float(h)

            if bid_price <= 0 or ask_price <= 0:
                bid_price = last_price
                ask_price = last_price
            elif (ask_price - bid_price) > last_price * 0.2:
                bid_price = last_price
                ask_price = last_price

            if bid_price > last_price:
                bid_price = last_price
            if ask_price < last_price:
                ask_price = last_price
            if bid_price > ask_price:
                bid_price = ask_price = last_price

            yield TickEvent(
                ts=dt,
                last=last_price,
                bid=bid_price,
                ask=ask_price,
                bid_vol=int(bid_vol),
                ask_vol=int(ask_vol),
            )


# CSV headers — must match UnifiedOrderFlowDumper.cs lines 491-494 byte-for-byte.
BARS_HEADER = "timestamp,open,high,low,close,volume,buy_vol,sell_vol,delta"
FOOTPRINT_HEADER = "timestamp,price,buy_vol,sell_vol"
BIG_TRADES_HEADER = "bar_timestamp,print_timestamp,price,size,side"
TICK_COUNTS_HEADER = "bar_timestamp,price,n_trades,n_buy,n_sell"


class CsvWriters:
    """Per-expiry CSV writer for the 4 orderflow datasets.

    Context-managed: opens 4 file handles in __enter__, writes headers
    immediately, closes all in __exit__ (even on exception).

    Output line endings are CRLF to match C# StringBuilder.AppendLine on
    Windows (the source of the meta_learner.db reference data). Encoding is
    UTF-8 without BOM, matching C# File.WriteAllText default.

    Filenames follow the per-expiry convention: {root}_{expiry}_*.csv
    (e.g. mes_h25_1m.csv). Stage 2 (continuous_stitcher.py) produces the
    bare {root}_*.csv variants by concatenating per-expiry outputs.
    """

    def __init__(self, output_dir: str, root: str, expiry: str):
        self.output_dir = output_dir
        self.root = root.lower()
        self.expiry = expiry.lower()
        self._files = {}      # name -> file handle
        self._writers = {}    # name -> csv.writer

    def __enter__(self):
        os.makedirs(self.output_dir, exist_ok=True)
        prefix = f"{self.root}_{self.expiry}_1m"
        spec = [
            ('bars',        f"{prefix}.csv",            BARS_HEADER),
            ('footprint',   f"{prefix}_footprint.csv",  FOOTPRINT_HEADER),
            ('big_trades',  f"{prefix}_big_trades.csv", BIG_TRADES_HEADER),
            ('tick_counts', f"{prefix}_tick_counts.csv", TICK_COUNTS_HEADER),
        ]
        for key, filename, header in spec:
            path = os.path.join(self.output_dir, filename)
            f = open(path, 'w', encoding='utf-8', newline='')
            w = csv.writer(f)  # csv default lineterminator='\r\n' matches C# AppendLine
            # Pre-split the header on commas so csv.writer emits an identical
            # comma-separated row (no quoting since headers are bare ascii).
            w.writerow(header.split(','))
            self._files[key] = f
            self._writers[key] = w
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        for f in self._files.values():
            try:
                f.close()
            except Exception:
                pass
        self._files.clear()
        self._writers.clear()
        return False  # don't suppress exceptions

    # Row writers (used in later steps; not yet exercised in Step 2).
    def write_bar(self, row: list):
        self._writers['bars'].writerow(row)

    def write_footprint_row(self, row: list):
        self._writers['footprint'].writerow(row)

    def write_big_trade(self, row: list):
        self._writers['big_trades'].writerow(row)

    def write_tick_count(self, row: list):
        self._writers['tick_counts'].writerow(row)


# Tick size per contract symbol. Used to snap prices to integer tick keys
# so float-equality bugs are impossible in the footprint dict.
TICK_SIZE_MAP = {
    'MES': 0.25, 'ES': 0.25,
    'MNQ': 0.25, 'NQ': 0.25,
    'MYM': 1.0,  'YM': 1.0,
    'MGC': 0.10, 'GC': 0.10,
    'M2K': 0.10, 'RTY': 0.10,
    'CL':  0.01, 'MCL': 0.01,
    'ZN':  0.015625, '10Y': 0.015625,
}


class BarUpdate(NamedTuple):
    """Per-record updates to bar-level totals."""
    buy_vol_delta: int
    sell_vol_delta: int


class FootprintUpdate(NamedTuple):
    """Per-record updates to one or more (price, buy, sell) cells.

    Each row is (int_ticks, buy_vol_delta, sell_vol_delta). Most policies emit
    a single row per record; SplitPolicy may emit two (one per side).
    """
    rows: list  # list[tuple[int, int, int]]


class TickCountUpdate(NamedTuple):
    """Per-record updates to per-(bar, price) trade counters.

    Each row is (int_ticks, n_trades_delta, n_buy_delta, n_sell_delta).
    """
    rows: list  # list[tuple[int, int, int, int]]


class HybridPolicy:
    """Aggressor classification policy: Option C (Hybrid).

    Bar level:    buy_vol += ask_vol, sell_vol += bid_vol (Sierra ground truth)
    Footprint:    one row at snap(last) += (ask_vol, bid_vol)
    Tick counts:  one row at snap(last); n_trades += 1; n_buy += 1 if
                  ask_vol > 0; n_sell += 1 if bid_vol > 0.

    Big trades (Step 7) are emitted by the accumulator from the same record
    using a uniform rule across policies: one row if (ask_vol + bid_vol) >=
    threshold, side = "buy" if ask_vol > bid_vol elif bid_vol > ask_vol else
    "mid", size = ask_vol + bid_vol.
    """

    name = "hybrid"

    def __init__(self, tick_size: float):
        self.tick_size = tick_size

    def _snap(self, price: float) -> int:
        return round(price / self.tick_size)

    def classify_bar(self, ev: TickEvent) -> BarUpdate:
        return BarUpdate(buy_vol_delta=ev.ask_vol, sell_vol_delta=ev.bid_vol)

    def classify_footprint(self, ev: TickEvent) -> FootprintUpdate:
        key = self._snap(ev.last)
        return FootprintUpdate(rows=[(key, ev.ask_vol, ev.bid_vol)])

    def classify_tick_count(self, ev: TickEvent) -> TickCountUpdate:
        key = self._snap(ev.last)
        n_buy = 1 if ev.ask_vol > 0 else 0
        n_sell = 1 if ev.bid_vol > 0 else 0
        return TickCountUpdate(rows=[(key, 1, n_buy, n_sell)])


class SplitPolicy:
    """Aggressor classification policy: Option B (Split).

    Bar level:    buy_vol += ask_vol, sell_vol += bid_vol (same as Hybrid)
    Footprint:    row at snap(ask) += (ask_vol, 0) AND row at snap(bid) += (0, bid_vol)
    Tick counts:  row at snap(ask) += (n_trades=1, n_buy=1, n_sell=0) if ask_vol>0;
                  row at snap(bid) += (n_trades=1, n_buy=0, n_sell=1) if bid_vol>0.

    Distributes each record's volume between bid and ask price levels (separate
    cells), preserving Sierra ground-truth aggression at finer price granularity.
    Step 11 informational only — Hybrid stays locked as default.
    """

    name = "split"

    def __init__(self, tick_size: float):
        self.tick_size = tick_size

    def _snap(self, price: float) -> int:
        return round(price / self.tick_size)

    def classify_bar(self, ev: TickEvent) -> BarUpdate:
        return BarUpdate(buy_vol_delta=ev.ask_vol, sell_vol_delta=ev.bid_vol)

    def classify_footprint(self, ev: TickEvent) -> FootprintUpdate:
        rows = []
        if ev.ask_vol > 0:
            rows.append((self._snap(ev.ask), ev.ask_vol, 0))
        if ev.bid_vol > 0:
            rows.append((self._snap(ev.bid), 0, ev.bid_vol))
        return FootprintUpdate(rows=rows)

    def classify_tick_count(self, ev: TickEvent) -> TickCountUpdate:
        rows = []
        if ev.ask_vol > 0:
            rows.append((self._snap(ev.ask), 1, 1, 0))
        if ev.bid_vol > 0:
            rows.append((self._snap(ev.bid), 1, 0, 1))
        return TickCountUpdate(rows=rows)


class MajorityPolicy:
    """Aggressor classification policy: Option A (Majority).

    Bar level:    if ask_vol > bid_vol: buy_vol += (ask_vol + bid_vol)
                  if bid_vol > ask_vol: sell_vol += (ask_vol + bid_vol)
                  if equal: split evenly between buy and sell.
                  Mimics Lee-Ready "all-to-majority" — each record's TOTAL
                  volume goes to one side based on which volume dominates.
    Footprint:    one row at snap(last) += (total_vol, 0) or (0, total_vol)
                  based on majority side.
    Tick counts:  one row at snap(last); n_trades += 1; n_buy=1 or n_sell=1
                  based on majority.

    Step 11 informational only — Hybrid stays locked as default.
    """

    name = "majority"

    def __init__(self, tick_size: float):
        self.tick_size = tick_size

    def _snap(self, price: float) -> int:
        return round(price / self.tick_size)

    def _split_total(self, ev: TickEvent) -> tuple[int, int]:
        """Return (buy_vol_delta, sell_vol_delta) for this record under
        majority rules. On exact tie, split evenly (rounding down toward zero
        on the buy side)."""
        total = ev.ask_vol + ev.bid_vol
        if ev.ask_vol > ev.bid_vol:
            return total, 0
        if ev.bid_vol > ev.ask_vol:
            return 0, total
        # Exact tie: split evenly. If total is odd, give the extra to buy.
        half = total // 2
        return total - half, half

    def classify_bar(self, ev: TickEvent) -> BarUpdate:
        b, s = self._split_total(ev)
        return BarUpdate(buy_vol_delta=b, sell_vol_delta=s)

    def classify_footprint(self, ev: TickEvent) -> FootprintUpdate:
        key = self._snap(ev.last)
        b, s = self._split_total(ev)
        return FootprintUpdate(rows=[(key, b, s)])

    def classify_tick_count(self, ev: TickEvent) -> TickCountUpdate:
        key = self._snap(ev.last)
        if ev.ask_vol > ev.bid_vol:
            return TickCountUpdate(rows=[(key, 1, 1, 0)])
        if ev.bid_vol > ev.ask_vol:
            return TickCountUpdate(rows=[(key, 1, 0, 1)])
        # Tie: count as both (n_trades=1 but n_buy and n_sell each 1)
        return TickCountUpdate(rows=[(key, 1, 1, 1)])


# Policy registry (used by the CLI in Step 12 and validator's --all-policies).
POLICY_REGISTRY = {
    'hybrid':   HybridPolicy,
    'split':    SplitPolicy,
    'majority': MajorityPolicy,
}


def _minute_floor(ts: datetime) -> datetime:
    """Truncate timestamp to minute boundary."""
    return ts.replace(second=0, microsecond=0)


def _format_price(p: float, *, tick_size: float = 0.01) -> str:
    """Format a price for the bars CSV.

    Uses 4 decimals when the tick size is NOT a multiple of 0.01 — i.e.
    when the tick has sub-cent fractional precision. ZN/10Y at 0.015625
    (1/64) is the only such tick in our basket today. With 2 decimals a
    bar's OHLC would round 4.5625 → 4.56 while footprints kept 4.5625,
    making footprint.price > bar.high — which breaks the price-space
    invariant downstream consumers rely on. 2 decimals stays the default
    for ticks that ARE multiples of 0.01 (every non-treasury contract).
    """
    # tick_size * 100 should be an integer when the tick is a clean
    # multiple of 0.01. Tolerance handles float-rep noise on 0.10 etc.
    needs_subcent = abs(tick_size * 100 - round(tick_size * 100)) > 1e-9
    decimals = 4 if needs_subcent else 2
    return f"{p:.{decimals}f}"


def _format_price_f4(p: float) -> str:
    """Format a price for the footprint and big_trades CSVs (matches C# F4)."""
    return f"{p:.4f}"


# Hard cap from the C# strategy (FOOTPRINT_MAX_ROWS_PER_BAR).
FOOTPRINT_MAX_ROWS_PER_BAR = 2000


# Default big-trade thresholds by symbol class. Micros default 25 (matches the
# C# strategy's default), full-size index 10, treasuries 50.
BIG_TRADE_THRESHOLD_MAP = {
    'MES': 25, 'MNQ': 25, 'MYM': 25, 'MGC': 25, 'M2K': 25, 'MCL': 25,
    'ES': 10, 'NQ': 10, 'YM': 10, 'GC': 10, 'RTY': 10, 'CL': 10,
    'ZN': 50, '10Y': 50,
}


def _format_print_iso(ts: datetime) -> str:
    """ISO 8601 timestamp with 7-digit fractional seconds (matches C# F7).
    Python's %f gives 6 digits (microsecond precision); we append a trailing
    '0' to match C# DateTime.ToString('o') which emits 100-nanosecond ticks.
    SCID source resolution is microsecond, so the trailing digit is always 0.
    """
    return ts.strftime('%Y-%m-%dT%H:%M:%S.%f') + '0'


def _format_minute_iso(ts: datetime) -> str:
    """ISO 8601 minute-boundary timestamp matching C# `yyyy-MM-ddTHH:mm:00`."""
    return ts.strftime('%Y-%m-%dT%H:%M:00')


class BarAccumulator:
    """Accumulates per-minute bar state from a stream of TickEvents and
    flushes one row per bar to the CSV writer.

    State machine:
        - First event: initialize bar from event.
        - Same minute: update OHLC, accumulate buy_vol/sell_vol via policy.
        - New minute: flush previous bar, reset, initialize from event.
        - Empty intervening minutes are skipped (no synthetic bars), matching
          NT8 behavior.

    Step 3 emits bars only. Step 6 will extend with footprint + tick_counts;
    Step 7 with big-trades. The state below already includes future-step
    fields (footprint/tick_counts) but they are not populated yet.
    """

    def __init__(self, policy, writers: 'CsvWriters', tick_size: float,
                  big_trade_threshold: int = 25):
        self.policy = policy
        self.writers = writers
        self.tick_size = tick_size
        self.big_trade_threshold = big_trade_threshold

        # Bar state — None before first event.
        self.current_minute = None
        self.bar_open = 0.0
        self.bar_high = 0.0
        self.bar_low = 0.0
        self.bar_close = 0.0
        self.bar_buy_vol = 0
        self.bar_sell_vol = 0

        # Per-(bar, price) accumulators. Keys are integer tick counts to avoid
        # float-equality bugs. Reset on every bar flush.
        self.footprint: dict = {}      # int_ticks -> [buy_vol, sell_vol]
        self.tick_counts: dict = {}    # int_ticks -> [n_trades, n_buy, n_sell]

    def ingest(self, ev: TickEvent):
        event_minute = _minute_floor(ev.ts)

        if self.current_minute is None:
            self._init_bar(ev, event_minute)
        elif event_minute != self.current_minute:
            self._flush_bar()
            self._init_bar(ev, event_minute)
        else:
            # Update OHLC inside current bar.
            if ev.last > self.bar_high:
                self.bar_high = ev.last
            if ev.last < self.bar_low:
                self.bar_low = ev.last
            self.bar_close = ev.last

        # Apply policy updates.
        bar_upd = self.policy.classify_bar(ev)
        self.bar_buy_vol += bar_upd.buy_vol_delta
        self.bar_sell_vol += bar_upd.sell_vol_delta

        fp_upd = self.policy.classify_footprint(ev)
        for int_ticks, buy_d, sell_d in fp_upd.rows:
            cell = self.footprint.get(int_ticks)
            if cell is None:
                self.footprint[int_ticks] = [buy_d, sell_d]
            else:
                cell[0] += buy_d
                cell[1] += sell_d

        tc_upd = self.policy.classify_tick_count(ev)
        for int_ticks, n_d, nb_d, ns_d in tc_upd.rows:
            cell = self.tick_counts.get(int_ticks)
            if cell is None:
                self.tick_counts[int_ticks] = [n_d, nb_d, ns_d]
            else:
                cell[0] += n_d
                cell[1] += nb_d
                cell[2] += ns_d

        # Big trades — emitted streamed (not buffered), uniform across policies.
        size = ev.bid_vol + ev.ask_vol
        if size >= self.big_trade_threshold and self.current_minute is not None:
            if ev.ask_vol > ev.bid_vol:
                side = "buy"
            elif ev.bid_vol > ev.ask_vol:
                side = "sell"
            else:
                side = "mid"
            int_ticks = round(ev.last / self.tick_size)
            snapped_price = int_ticks * self.tick_size
            self.writers.write_big_trade([
                _format_minute_iso(self.current_minute),
                _format_print_iso(ev.ts),
                _format_price_f4(snapped_price),
                size,
                side,
            ])

    def _init_bar(self, ev: TickEvent, event_minute: datetime):
        self.current_minute = event_minute
        self.bar_open = ev.last
        self.bar_high = ev.last
        self.bar_low = ev.last
        self.bar_close = ev.last
        self.bar_buy_vol = 0
        self.bar_sell_vol = 0
        self.footprint.clear()
        self.tick_counts.clear()

    def _flush_bar(self):
        if self.current_minute is None:
            return
        ts_iso = _format_minute_iso(self.current_minute)

        # Bar row.
        volume = self.bar_buy_vol + self.bar_sell_vol
        delta = self.bar_buy_vol - self.bar_sell_vol
        self.writers.write_bar([
            ts_iso,
            _format_price(self.bar_open, tick_size=self.tick_size),
            _format_price(self.bar_high, tick_size=self.tick_size),
            _format_price(self.bar_low, tick_size=self.tick_size),
            _format_price(self.bar_close, tick_size=self.tick_size),
            volume,
            self.bar_buy_vol,
            self.bar_sell_vol,
            delta,
        ])

        # Footprint rows: only price levels with non-zero volume; sorted by
        # price ascending; capped at FOOTPRINT_MAX_ROWS_PER_BAR.
        for int_ticks in sorted(self.footprint.keys())[:FOOTPRINT_MAX_ROWS_PER_BAR]:
            buy, sell = self.footprint[int_ticks]
            if buy + sell <= 0:
                continue
            price = int_ticks * self.tick_size
            self.writers.write_footprint_row([
                ts_iso,
                _format_price_f4(price),
                buy,
                sell,
            ])

        # Tick count rows: one per (bar, price) ascending.
        for int_ticks in sorted(self.tick_counts.keys()):
            n_trades, n_buy, n_sell = self.tick_counts[int_ticks]
            if n_trades <= 0:
                continue
            price = int_ticks * self.tick_size
            self.writers.write_tick_count([
                ts_iso,
                _format_price_f4(price),
                n_trades,
                n_buy,
                n_sell,
            ])

    def close(self):
        """Flush the final bar. Caller must invoke after the iterator drains."""
        self._flush_bar()
        self.current_minute = None


def build_per_expiry(scid_path: str, output_dir: str, root: str,
                      expiry: str, policy=None, tick_size: float | None = None,
                      big_trade_threshold: int | None = None) -> dict:
    """Stage 1 (Steps 3, 6, 7 scope): SCID -> all 4 per-expiry orderflow CSVs.

    Emits bars + footprint + tick_counts + big_trades.

    `tick_size` is auto-resolved from TICK_SIZE_MAP[root.upper()] when None.
    `big_trade_threshold` is auto-resolved from BIG_TRADE_THRESHOLD_MAP when
    None (defaults: micros 25, full-size index 10, treasuries 50).
    `policy` defaults to HybridPolicy(tick_size).

    Returns summary dict with row counts per dataset.
    """
    root_u = root.upper()
    if tick_size is None:
        if root_u not in TICK_SIZE_MAP:
            raise ValueError(
                f"Unknown contract {root_u!r}. Pass tick_size= or extend "
                f"TICK_SIZE_MAP."
            )
        tick_size = TICK_SIZE_MAP[root_u]
    if big_trade_threshold is None:
        big_trade_threshold = BIG_TRADE_THRESHOLD_MAP.get(root_u, 25)
    if policy is None:
        policy = HybridPolicy(tick_size=tick_size)

    with CsvWriters(output_dir, root, expiry) as writers:
        accum = BarAccumulator(policy, writers, tick_size=tick_size,
                                big_trade_threshold=big_trade_threshold)
        for ev in iter_scid_records(scid_path):
            accum.ingest(ev)
        accum.close()

    prefix = f"{root.lower()}_{expiry.lower()}_1m"
    bars_path = os.path.join(output_dir, f"{prefix}.csv")
    footprint_path = os.path.join(output_dir, f"{prefix}_footprint.csv")
    tick_counts_path = os.path.join(output_dir, f"{prefix}_tick_counts.csv")
    big_trades_path = os.path.join(output_dir, f"{prefix}_big_trades.csv")

    def _count_rows(path):
        with open(path, 'r', encoding='utf-8') as f:
            return sum(1 for _ in f) - 1  # -1 for header

    return {
        'bars_csv': bars_path,
        'bars_emitted': _count_rows(bars_path),
        'footprint_emitted': _count_rows(footprint_path),
        'tick_counts_emitted': _count_rows(tick_counts_path),
        'big_trades_emitted': _count_rows(big_trades_path),
        'policy': policy.name,
        'tick_size': tick_size,
        'big_trade_threshold': big_trade_threshold,
    }


# Backwards-compatible alias used by Step 3 CLI.
build_per_expiry_bars = build_per_expiry


def verify_csv_headers(output_dir: str, root: str, expiry: str) -> dict:
    """Open CsvWriters, close it (writes headers only), then read back and
    compare each file's bytes to expected `header\\r\\n` in UTF-8.

    Returns a dict mapping filename -> 'OK' or a description of the mismatch.
    """
    with CsvWriters(output_dir, root, expiry):
        pass  # __enter__ wrote the 4 headers; __exit__ closes all

    prefix = f"{root.lower()}_{expiry.lower()}_1m"
    expected = {
        f"{prefix}.csv":             (BARS_HEADER + "\r\n").encode('utf-8'),
        f"{prefix}_footprint.csv":   (FOOTPRINT_HEADER + "\r\n").encode('utf-8'),
        f"{prefix}_big_trades.csv":  (BIG_TRADES_HEADER + "\r\n").encode('utf-8'),
        f"{prefix}_tick_counts.csv": (TICK_COUNTS_HEADER + "\r\n").encode('utf-8'),
    }

    results = {}
    for filename, expected_bytes in expected.items():
        path = os.path.join(output_dir, filename)
        with open(path, 'rb') as f:
            actual_bytes = f.read()
        if actual_bytes == expected_bytes:
            results[filename] = 'OK'
        else:
            results[filename] = (
                f"DIFF: expected {expected_bytes!r} ({len(expected_bytes)}B), "
                f"got {actual_bytes!r} ({len(actual_bytes)}B)"
            )
    return results


def smoke_test(path: str) -> dict:
    """Read a SCID file end-to-end and return summary stats for verification.

    Verifies:
      - Iterator runs to completion (no exception, no infinite loop).
      - Emitted record count <= max possible records from file size.
      - First 10 events have plausible shape.
    """
    file_size = os.path.getsize(path)
    max_records = (file_size - SIZE_HEADER) // SIZE_RECORD

    emitted = 0
    first_10 = []
    for ev in iter_scid_records(path):
        if emitted < 10:
            first_10.append(ev)
        emitted += 1

    return {
        'file_size': file_size,
        'max_records': max_records,
        'emitted_records': emitted,
        'first_10': first_10,
    }


_MONTH_CODE_MAP = {'F': 1, 'G': 2, 'H': 3, 'J': 4, 'K': 5, 'M': 6,
                    'N': 7, 'Q': 8, 'U': 9, 'V': 10, 'X': 11, 'Z': 12}


def expiry_from_scid_path(file_path: str, contract: str) -> str:
    """Extract lowercase 3-char expiry (e.g. 'h25') from a SCID basename.

    Mirrors scid_to_nt8_converter.scid_year — uses len(contract) as the
    offset into the basename so it works for any symbol length.
    """
    base = os.path.splitext(os.path.basename(file_path))[0]
    offset = len(contract)
    if len(base) < offset + 3:
        raise ValueError(f"Cannot extract expiry from filename: {base}")
    code = base[offset].upper()
    yy = base[offset + 1:offset + 3]
    if code not in _MONTH_CODE_MAP or not yy.isdigit():
        raise ValueError(
            f"Cannot parse expiry from {base!r}: month code={code!r} year={yy!r}"
        )
    return f"{code.lower()}{yy}"


def _scid_year_2digit(file_path: str, contract: str) -> str:
    """Return the 2-digit year from the SCID basename, or '' if unparseable."""
    try:
        return expiry_from_scid_path(file_path, contract)[1:]
    except ValueError:
        return ''


def per_expiry_output_dir(base_output: str, contract: str) -> str:
    """Resolve `<base_output>/<CONTRACT>/per_expiry`. The user-facing default
    base is `E:\\Data\\Orderflow`; final per-expiry CSVs land at
    `E:\\Data\\Orderflow\\<CONTRACT>\\per_expiry\\<root>_<expiry>_*.csv`.
    """
    return os.path.join(base_output, contract.upper(), 'per_expiry')


def _convert_one(scid_path: str, contract: str, options: dict) -> dict:
    """Convert one SCID file. Returns the build_per_expiry summary plus elapsed."""
    expiry = expiry_from_scid_path(scid_path, contract)
    out_dir = per_expiry_output_dir(options['output_dir'], contract)
    os.makedirs(out_dir, exist_ok=True)

    policy_cls = POLICY_REGISTRY[options['policy']]
    tick_size = options['tick_size']
    if tick_size is None:
        tick_size = TICK_SIZE_MAP.get(contract.upper())
        if tick_size is None:
            raise ValueError(
                f"No tick_size for {contract!r}; pass --tick-size or extend "
                f"TICK_SIZE_MAP."
            )
    policy = policy_cls(tick_size=tick_size)

    big_trade_threshold = options['big_trade_size']
    if big_trade_threshold is None:
        big_trade_threshold = BIG_TRADE_THRESHOLD_MAP.get(contract.upper(), 25)

    t0 = _time.time()
    summary = build_per_expiry(
        scid_path=scid_path,
        output_dir=out_dir,
        root=contract,
        expiry=expiry,
        policy=policy,
        tick_size=tick_size,
        big_trade_threshold=big_trade_threshold,
    )
    summary['elapsed_s'] = _time.time() - t0
    summary['expiry'] = expiry
    summary['scid_path'] = scid_path
    summary['out_dir'] = out_dir
    return summary


def parse_orderflow_args(argv: list) -> tuple:
    """Parse the converter-style orderflow CLI.

    Returns (contract, folder, work_type, options) where work_type is one of
    {'single', 'batch', 'all'}. options holds policy, tick_size, big_trade_size,
    output_dir, file_pattern, from_year.
    """
    if len(argv) < 4:
        print_orderflow_usage()
        sys.exit(1)

    contract = argv[1].upper()
    folder = argv[2]
    work_type = argv[3].lower()
    if work_type not in {'single', 'batch', 'all'}:
        print(f"Error: unknown work type {work_type!r}; expected single|batch|all")
        sys.exit(1)

    options = {
        'policy': 'hybrid',
        'tick_size': None,           # auto-resolved
        'big_trade_size': None,      # auto-resolved
        'output_dir': r'E:\Data\Orderflow',
        'from_year': None,
        'file_pattern': None,
    }

    i = 4
    while i < len(argv):
        arg = argv[i]
        if arg == '--policy' and i + 1 < len(argv):
            policy = argv[i + 1].lower()
            if policy not in POLICY_REGISTRY:
                print(f"Error: --policy must be one of {sorted(POLICY_REGISTRY)}")
                sys.exit(1)
            options['policy'] = policy
            i += 2
        elif arg == '--tick-size' and i + 1 < len(argv):
            try:
                options['tick_size'] = float(argv[i + 1])
            except ValueError:
                print("Error: --tick-size must be a float")
                sys.exit(1)
            i += 2
        elif arg == '--big-trade-size' and i + 1 < len(argv):
            try:
                options['big_trade_size'] = int(argv[i + 1])
            except ValueError:
                print("Error: --big-trade-size must be an integer")
                sys.exit(1)
            i += 2
        elif arg == '--output' and i + 1 < len(argv):
            options['output_dir'] = argv[i + 1]
            i += 2
        elif arg == '--from-year' and i + 1 < len(argv):
            yy = argv[i + 1].strip()
            if not (yy.isdigit() and len(yy) == 2):
                print("Error: --from-year takes a 2-digit year (e.g. 15 for 2015)")
                sys.exit(1)
            options['from_year'] = yy
            i += 2
        else:
            if work_type == 'single' and options['file_pattern'] is None:
                options['file_pattern'] = arg
                i += 1
            else:
                print(f"Error: unknown option {arg!r}")
                sys.exit(1)

    return contract, folder, work_type, options


def print_orderflow_usage():
    print(__doc__)
    print()
    print("Converter-style usage (Stage 1, per-expiry build):")
    print("  python scid_to_orderflow.py SYMBOL FOLDER {single|batch|all} \\")
    print("      [--policy {hybrid,split,majority}]  (default hybrid; locked)")
    print("      [--tick-size FLOAT]                 (default per TICK_SIZE_MAP)")
    print("      [--big-trade-size N]                (default per symbol class)")
    print("      [--output PATH]                     (default E:\\Data\\Orderflow)")
    print("      [--from-year YY]                    (2-digit year filter)")
    print()
    print("Examples:")
    print("  python scid_to_orderflow.py MES C:\\SierraChart\\Data single MESH25-CME.scid")
    print("  python scid_to_orderflow.py MES C:\\SierraChart\\Data batch --from-year 25")
    print("  python scid_to_orderflow.py MNQ C:\\SierraChart\\Data all --from-year 15")
    print()
    print("Dev/debug subcommands (kept for spot-checks):")
    print("  python scid_to_orderflow.py smoke-test <SCID_PATH>")
    print("  python scid_to_orderflow.py verify-headers <OUT_DIR> <ROOT> <EXPIRY>")
    print("  python scid_to_orderflow.py build-bars <SCID_PATH> <OUT_DIR> <ROOT> <EXPIRY>")


def execute_orderflow_work(contract: str, folder: str, work_type: str,
                            options: dict) -> int:
    """Run single/batch/all conversion serially. Returns process exit code.

    Per-symbol builds are intentionally serial — running multiple SCID
    iterators in parallel against the same source folder caused silent
    output truncation in audit testing (one of four parallel MNQ jobs
    produced a half-length CSV). Use a shell-level for-loop across
    symbols if cross-symbol parallelism is needed.
    """
    print(f"Contract:   {contract}")
    print(f"Folder:     {folder}")
    print(f"Work type:  {work_type}")
    print(f"Policy:     {options['policy']}")
    print(f"Output:     {per_expiry_output_dir(options['output_dir'], contract)}")
    if options['tick_size'] is not None:
        print(f"Tick size:  {options['tick_size']} (override)")
    if options['big_trade_size'] is not None:
        print(f"Big trade:  {options['big_trade_size']} (override)")
    if options['from_year']:
        print(f"From year:  {options['from_year']}")
    print('-' * 60)

    if work_type == 'single':
        if not options['file_pattern']:
            print("Error: single mode requires a SCID filename")
            return 1
        scid_path = os.path.join(folder, options['file_pattern'])
        if not os.path.exists(scid_path):
            print(f"Error: file not found: {scid_path}")
            return 1
        files = [scid_path]
    else:
        pattern = f"{contract}*.scid"
        if work_type == 'batch':
            files = sorted(glob.glob(os.path.join(folder, pattern)))
        else:  # 'all' — recurse
            files = sorted(glob.glob(os.path.join(folder, '**', pattern),
                                       recursive=True))
        if options['from_year']:
            fy = options['from_year']
            files = [f for f in files
                     if _scid_year_2digit(f, contract) >= fy]
        if not files:
            print(f"No {contract} files found in {folder}"
                  + (' (recursive)' if work_type == 'all' else ''))
            return 1

    print(f"Converting {len(files)} file(s) serially...\n")
    n_ok = 0
    n_fail = 0
    total_bars = 0
    total_fp = 0
    for i, scid_path in enumerate(files, 1):
        rel = os.path.relpath(scid_path, folder) if work_type == 'all' else os.path.basename(scid_path)
        print(f"[{i}/{len(files)}] {rel}")
        try:
            summary = _convert_one(scid_path, contract, options)
        except Exception as e:
            n_fail += 1
            print(f"  FAILED: {type(e).__name__}: {e}")
            continue
        n_ok += 1
        total_bars += summary['bars_emitted']
        total_fp += summary['footprint_emitted']
        print(f"  expiry={summary['expiry']}  "
              f"bars={summary['bars_emitted']:,}  "
              f"footprint={summary['footprint_emitted']:,}  "
              f"big_trades={summary['big_trades_emitted']:,}  "
              f"({summary['elapsed_s']:.1f}s)")

    print()
    print(f"=== {contract} {work_type} complete ===")
    print(f"  files ok:           {n_ok}/{len(files)}")
    if n_fail:
        print(f"  files failed:       {n_fail}")
    print(f"  total bars:         {total_bars:,}")
    print(f"  total footprint:    {total_fp:,}")
    return 0 if n_fail == 0 else 2


if __name__ == "__main__":
    if len(sys.argv) >= 6 and sys.argv[1] == "build-bars":
        # Steps 3 + 6 entry: SCID -> per-expiry bars + footprint + tick_counts.
        # Usage: build-bars <SCID_PATH> <OUT_DIR> <ROOT> <EXPIRY>
        scid_path, out_dir, root, expiry = sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5]
        if not os.path.exists(scid_path):
            print(f"Error: SCID file not found: {scid_path}")
            sys.exit(1)
        result = build_per_expiry(scid_path, out_dir, root, expiry)
        print(f"=== build per-expiry complete ===")
        print(f"  scid:                {scid_path}")
        print(f"  policy:              {result['policy']}")
        print(f"  tick_size:           {result['tick_size']}")
        print(f"  big_trade_threshold: {result['big_trade_threshold']}")
        print(f"  bars_emitted:        {result['bars_emitted']:,}")
        print(f"  footprint_emitted:   {result['footprint_emitted']:,}")
        print(f"  tick_counts_emitted: {result['tick_counts_emitted']:,}")
        print(f"  big_trades_emitted:  {result['big_trades_emitted']:,}")
        print(f"  bars_csv:            {result['bars_csv']}")
        sys.exit(0)

    if len(sys.argv) >= 4 and sys.argv[1] == "verify-headers":
        out_dir = sys.argv[2]
        # accept "MES h25" form: argv[3]="MES", argv[4]="h25"
        if len(sys.argv) >= 5:
            root, expiry = sys.argv[3], sys.argv[4]
        else:
            print("Usage: python scid_to_orderflow.py verify-headers <OUT_DIR> <ROOT> <EXPIRY>")
            sys.exit(1)
        results = verify_csv_headers(out_dir, root, expiry)
        all_ok = True
        print(f"=== CSV header byte-equality check ({root} {expiry}) ===")
        for filename, result in results.items():
            print(f"  {filename}: {result}")
            if result != 'OK':
                all_ok = False
        sys.exit(0 if all_ok else 1)

    if len(sys.argv) >= 3 and sys.argv[1] == "smoke-test":
        path = sys.argv[2]
        if not os.path.exists(path):
            print(f"Error: file not found: {path}")
            sys.exit(1)

        result = smoke_test(path)
        print(f"=== SCID iterator smoke test ===")
        print(f"file:            {path}")
        print(f"file_size:       {result['file_size']:,} bytes")
        print(f"max_records:     {result['max_records']:,} (upper bound)")
        print(f"emitted_records: {result['emitted_records']:,}")
        ratio = result['emitted_records'] / result['max_records'] if result['max_records'] > 0 else 0
        print(f"ratio:           {ratio:.4f} (expect < 1.0; sub-trades and invalid records skipped)")
        print(f"\nfirst 10 events:")
        for i, ev in enumerate(result['first_10'], 1):
            print(f"  [{i:2d}] {ev.ts.isoformat()} "
                  f"last={ev.last:8.2f} bid={ev.bid:8.2f} ask={ev.ask:8.2f} "
                  f"bid_vol={ev.bid_vol:6d} ask_vol={ev.ask_vol:6d}")
        sys.exit(0)

    if len(sys.argv) < 2 or sys.argv[1] in ('-h', '--help'):
        print_orderflow_usage()
        sys.exit(0 if len(sys.argv) >= 2 else 1)

    contract, folder, work_type, options = parse_orderflow_args(sys.argv)
    if not os.path.isdir(folder):
        print(f"Error: folder not found: {folder}")
        sys.exit(1)
    sys.exit(execute_orderflow_work(contract, folder, work_type, options))
