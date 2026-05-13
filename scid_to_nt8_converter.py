"""
SCID to NinjaTrader 8 Converter (TickStory Format)
===================================================
Converts Sierra Chart SCID tick data files to NinjaTrader 8 import format.
Includes automatic cleaning pass to remove outlier ticks (price spikes).

Pipeline:
1. Convert SCID binary to NT8 text format (parallel processing)
2. Clean outlier ticks using rolling median filter
3. Output final cleaned file ready for NT8 import

Format: yyyyMMdd HHmmss fffffff;last;bid;ask;volume
- Tick Replay format with bid/ask for volumetric analysis
- Sub-second precision (7 decimal places)
- Validates bid <= last <= ask
- Auto-detects cleaning threshold by symbol or use contract name parameter

Import Settings in NT8:
- Format: "NinjaTrader (start of bar timestamps)"
- Data Type: "Last"
- Time Zone: "UTC"

Usage:
    Single file:                python scid_to_nt8_converter.py MGC . single MGCZ25.scid
    Batch convert:               python scid_to_nt8_converter.py MNQ data/ batch
    Convert all:                 python scid_to_nt8_converter.py ES . all
    Custom options:              python scid_to_nt8_converter.py CL . batch --threshold 30.0 --output custom/

Supported Contracts (auto-detected or via --contract):
    25.0 points: MGC, MNQ, ES, NQ, GC, MES, RTY, M2K
    50.0 points: CL, MCL, YM, MYM, ZB, ZN, ZF, ZT, SI, HG, PL, PA, CU, AL, NI

Author: Created for SCID to NT8 tick data import
Date: January 2025
"""

import struct
import os
import glob
import sys
from datetime import date, datetime, time, timedelta
from multiprocessing import Pool, cpu_count
from collections import deque
from statistics import median

# SCID file format constants
SIZE_HEADER = 0x38  # 56 bytes
SIZE_RECORD = 0x28  # 40 bytes

# Sierra Chart epoch — SCID dt_raw is int64 microseconds since this UTC moment.
SIERRA_EPOCH = datetime(1899, 12, 30)


def scid_dt_raw_to_datetime(dt_raw):
    """Convert SCID raw int64 microsecond timestamp to naive datetime.

    Use this in preference to deserialize_datetime — it avoids the float
    roundtrip and is exact to microsecond precision.
    """
    try:
        return SIERRA_EPOCH + timedelta(microseconds=dt_raw)
    except (OverflowError, ValueError):
        return None


def deserialize_datetime(excel_date_time):
    """Convert SCID datetime (Excel days-since-1899-12-30 float) to Python datetime.

    Legacy interface kept for external callers. Internally now uses the
    corrected microsecond-direct path. The previous implementation had a
    rounding-overflow bug at midnight boundaries: a tick at 23:59:59.999998
    decoded as 00:00:00 of the SAME day, producing duplicate-timestamp ticks
    on the last record of every day. Returns naive datetime truncated to
    second precision (matching the legacy contract).
    """
    try:
        microseconds = round(excel_date_time * 86400 * 1_000_000)
        dt = SIERRA_EPOCH + timedelta(microseconds=microseconds)
        return dt.replace(microsecond=0)
    except (OverflowError, ValueError):
        return None

def process_chunk(input_file, start_record, num_records):
    """Process a chunk of records from the SCID file."""
    lines = []
    
    # Constants for unbundled trade detection
    FIRST_SUB_TRADE = -1.99900095e+37
    LAST_SUB_TRADE = -1.99900197e+37
    
    with open(input_file, 'rb') as f:
        f.seek(SIZE_HEADER + start_record * SIZE_RECORD)
        
        # State for aggregating unbundled trades
        in_unbundled_trade = False
        unbundled_dt_raw = 0
        unbundled_last = 0.0
        unbundled_bid = 0.0
        unbundled_ask = 0.0
        unbundled_vol = 0
        unbundled_trades = 0
        
        for _ in range(num_records):
            data = f.read(SIZE_RECORD)
            if len(data) < SIZE_RECORD:
                break
            
            dt_raw, o, h, l, c, vol, trades, bid_vol, ask_vol = struct.unpack('<q4f4I', data)

            # SCID datetime is microseconds since SIERRA_EPOCH; direct
            # arithmetic (no float roundtrip) avoids the midnight rounding bug.
            dt = scid_dt_raw_to_datetime(dt_raw)
            
            if dt is None or c <= 0:
                continue
            
            # Calculate actual volume from BidVolume + AskVolume
            actual_vol = bid_vol + ask_vol
            
            # Check if this is part of an unbundled trade
            is_first_sub = abs(o - FIRST_SUB_TRADE) < 1e30
            is_last_sub = abs(o - LAST_SUB_TRADE) < 1e30
            
            if is_first_sub:
                # Start accumulating unbundled trade
                in_unbundled_trade = True
                unbundled_dt_raw = dt_raw
                unbundled_last = c
                unbundled_bid = l
                unbundled_ask = h
                unbundled_vol = actual_vol
                unbundled_trades = 0
                continue
            elif in_unbundled_trade:
                # Accumulate volume from sub-trades
                unbundled_vol += actual_vol
                # Update prices (use last sub-trade's prices)
                unbundled_last = c
                unbundled_bid = l
                unbundled_ask = h
                
                if is_last_sub:
                    # End of unbundled trade - output combined tick
                    dt_raw = unbundled_dt_raw
                    c = unbundled_last
                    l = unbundled_bid
                    h = unbundled_ask
                    # Use summed volume for this aggregated trade
                    actual_vol = unbundled_vol
                    in_unbundled_trade = False
                    
                    # Recalculate datetime for output (using FIRST sub's dt_raw).
                    dt = scid_dt_raw_to_datetime(dt_raw)
                else:
                    # Middle sub-trade, keep accumulating
                    continue
            
            # Extract microseconds for sub-second precision
            microseconds = dt_raw % 1000000
            
            # Per Sierra Chart docs for tick data:
            # High = Ask price, Low = Bid price, Close = Last price
            last_price = c
            bid_price = l
            ask_price = h
            
            # Skip ticks with invalid bid/ask (0.00 or extreme spreads)
            # NT8 rejects these as "exceeds max spread"
            if bid_price <= 0 or ask_price <= 0:
                # Use last price for both bid and ask
                bid_price = last_price
                ask_price = last_price
            
            # Check for extreme spread (more than 20% of price)
            spread = ask_price - bid_price
            if spread > last_price * 0.2:
                # Spread too wide, use last price
                bid_price = last_price
                ask_price = last_price
            
            # Always use actual_vol (BidVolume + AskVolume or aggregated sum)
            output_vol = actual_vol
            
            # NT8 requires: Bid <= Last <= Ask
            if bid_price > last_price:
                bid_price = last_price
            if ask_price < last_price:
                ask_price = last_price
            if bid_price > ask_price:
                bid_price = ask_price = last_price
            
            # Format: yyyyMMdd HHmmss fffffff;last;bid;ask;volume
            # Match NT8 Tick Replay format exactly (no spaces after semicolons)
            datetime_str = dt.strftime('%Y%m%d %H%M%S')
            fractional = f"{microseconds:07d}"
            line = f"{datetime_str} {fractional};{last_price:.2f};{bid_price:.2f};{ask_price:.2f};{output_vol}\n"
            lines.append(line)
    
    return lines

def convert_scid_to_nt8(input_file, output_file=None, num_workers=None):
    """
    Convert a single SCID file to NT8 format.
    
    Args:
        input_file: Path to SCID file
        output_file: Optional output path (auto-generated if None)
        num_workers: Number of CPU cores to use (auto-detect if None)
    """
    if not os.path.exists(input_file):
        print(f"Error: {input_file} not found")
        return False
    
    file_size = os.path.getsize(input_file)
    total_records = (file_size - SIZE_HEADER) // SIZE_RECORD
    
    if num_workers is None:
        num_workers = cpu_count()
    
    # Determine output filename from SCID filename
    if output_file is None:
        base = os.path.splitext(os.path.basename(input_file))[0]
        # Extract contract info (e.g., MNQZ25-CME -> MNQ 12-25)
        if len(base) >= 6:
            symbol = base[:3]
            month_code = base[3]
            year = base[4:6]
            
            month_map = {'F':1,'G':2,'H':3,'J':4,'K':5,'M':6,'N':7,'Q':8,'U':9,'V':10,'X':11,'Z':12}
            month_num = month_map.get(month_code, 1)
            
            output_file = f"NT8_Imports/{symbol} {month_num:02d}-{year}.txt"
        else:
            output_file = f"NT8_Imports/{base}.txt"
    
    # Create output directory
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    print(f"\n{'='*80}")
    print(f"Converting: {os.path.basename(input_file)}")
    print(f"Output: {output_file}")
    print(f"Format: yyyyMMdd HHmmss fffffff;last;bid;ask;volume")
    print(f"Total records: {total_records:,}")
    print(f"Workers: {num_workers}")
    print(f"{'='*80}\n")
    
    # Create chunks for parallel processing
    chunk_size = max(1, total_records // num_workers)
    chunks = []
    for i in range(num_workers):
        start = i * chunk_size
        if i == num_workers - 1:
            count = total_records - start
        else:
            count = chunk_size
        if count > 0:
            chunks.append((input_file, start, count))
    
    print("Processing in parallel...")
    with Pool(num_workers) as pool:
        results = pool.starmap(process_chunk, chunks)
    
    # Combine and sort by timestamp
    print("Sorting and writing output...")
    all_lines = []
    for chunk_lines in results:
        all_lines.extend(chunk_lines)
    
    all_lines.sort(key=lambda x: x.split(';')[0])
    
    with open(output_file, 'w') as f:
        f.writelines(all_lines)
    
    print(f"✓ {output_file}: {len(all_lines):,} records\n")
    print(f"{'='*80}")
    print("COMPLETE!")
    print(f"{'='*80}\n")
    
    return output_file


def clean_tick_file(input_file, output_file, threshold=25.0, window_size=200, lookahead=10):
    """
    Clean an NT8 tick import file by removing:
    1. Ticks with illegal bid/ask values (0.00 or extreme spreads)
    2. Transient spike ticks (using rolling median + look-ahead confirmation)
    
    Args:
        input_file: Path to the raw NT8 tick import file
        output_file: Path for the cleaned output file
        threshold: Max allowed deviation from rolling median (in price points)
        window_size: Number of recent prices to use for median calculation
        lookahead: Number of ticks to look ahead for confirmation
    
    Returns:
        Tuple of (total_lines, kept_lines, dropped_lines)
    """
    # First pass: read all lines and parse data
    lines = []
    prices = []
    illegal_mask = []  # Track illegal bid/ask ticks
    
    with open(input_file, 'r') as f:
        for line in f:
            lines.append(line)
            try:
                parts = line.strip().split(';')
                if len(parts) >= 4:
                    last_price = float(parts[1])
                    bid_price = float(parts[2])
                    ask_price = float(parts[3])
                    
                    prices.append(last_price)
                    
                    # Check for illegal bid/ask values
                    # NT8 rejects spreads that are too wide
                    is_illegal = False
                    
                    # Zero or negative bid/ask
                    if bid_price <= 0 or ask_price <= 0:
                        is_illegal = True
                    
                    # Extreme spread (more than 5% of last price)
                    elif (ask_price - bid_price) > last_price * 0.05:
                        is_illegal = True
                    
                    # Bid/Ask way off from last (NT8 max spread check)
                    elif abs(last_price - bid_price) > last_price * 0.05:
                        is_illegal = True
                    elif abs(ask_price - last_price) > last_price * 0.05:
                        is_illegal = True
                    
                    illegal_mask.append(is_illegal)
                else:
                    prices.append(None)
                    illegal_mask.append(False)
            except (ValueError, IndexError):
                prices.append(None)
                illegal_mask.append(False)
    
    total_lines = len(lines)
    drop_mask = [False] * total_lines
    
    # Mark illegal ticks for dropping
    illegal_count = 0
    for i in range(total_lines):
        if illegal_mask[i]:
            drop_mask[i] = True
            illegal_count += 1
    
    # Second pass: identify transient spikes using rolling median + lookahead
    price_window = deque(maxlen=window_size)
    spike_count = 0
    
    for i in range(total_lines):
        price = prices[i]
        if price is None or drop_mask[i]:
            continue
        
        # Check against rolling median
        if len(price_window) >= 20:
            current_median = median(price_window)
            deviation = abs(price - current_median)
            
            if deviation > threshold:
                # Look ahead: do next N ticks return to median?
                future_prices = []
                for j in range(i + 1, min(i + 1 + lookahead, total_lines)):
                    if prices[j] is not None and not drop_mask[j]:
                        future_prices.append(prices[j])
                
                if future_prices:
                    # If future prices are close to the OLD median, this is a transient spike
                    future_median = median(future_prices)
                    future_deviation = abs(future_median - current_median)
                    
                    # Spike is transient if future returns to within threshold/2 of old median
                    if future_deviation < threshold / 2:
                        drop_mask[i] = True
                        spike_count += 1
                        # Also check if adjacent ticks are part of same spike cluster
                        for j in range(max(0, i - 3), i):
                            if prices[j] is not None and not drop_mask[j] and abs(prices[j] - current_median) > threshold:
                                drop_mask[j] = True
                                spike_count += 1
                        continue
        
        # Keep this tick and add to window
        price_window.append(price)
    
    # Write output
    kept_lines = 0
    dropped_lines = 0
    
    with open(output_file, 'w') as f:
        for i, line in enumerate(lines):
            if drop_mask[i]:
                dropped_lines += 1
            else:
                f.write(line)
                kept_lines += 1
    
    return total_lines, kept_lines, dropped_lines, illegal_count, spike_count


def convert_and_clean(input_file, output_file=None, num_workers=None, threshold=None, window_size=200, contract_name=None):
    """
    Convert SCID to NT8 format and clean outlier ticks in one pipeline.
    
    Args:
        input_file: Path to SCID file
        output_file: Final output path (optional, auto-generated if None)
        num_workers: Number of parallel workers
        threshold: Cleaning threshold (auto-detected by symbol if None)
        window_size: Rolling window size for median filter
        contract_name: Contract name for threshold lookup (e.g., 'MGC', 'MNQ', 'ES')
    
    Returns:
        True if successful, False otherwise
    """
    # Determine output filename
    if output_file is None:
        base = os.path.splitext(os.path.basename(input_file))[0]
        if len(base) >= 6:
            # Use provided contract_name or extract from filename
            symbol = contract_name if contract_name else base[:3]
            month_code = base[3]
            year = base[4:6]
            month_map = {'F':1,'G':2,'H':3,'J':4,'K':5,'M':6,'N':7,'Q':8,'U':9,'V':10,'X':11,'Z':12}
            month_num = month_map.get(month_code, 1)
            output_file = f"NT8_Imports/{symbol} {month_num:02d}-{year}.txt"
        else:
            symbol = contract_name if contract_name else base
            output_file = f"NT8_Imports/{symbol}.txt"
    
    # Create output directory if it doesn't exist
    output_dir = os.path.dirname(output_file)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)
        print(f"Created output directory: {output_dir}")
    
    # Auto-detect threshold based on symbol
    if threshold is None:
        # Use provided contract_name or extract from filename
        symbol = contract_name if contract_name else base[:3].upper()
        # MGC: spikes seen as low as 20 pts -> threshold 25
        # MNQ: spikes seen as low as 35 pts -> threshold 25
        threshold_map = {
            'MGC': 25.0, 'MNQ': 25.0, 'GC': 25.0, 'NQ': 25.0, 'ES': 25.0, 'MES': 25.0,
            'CL': 50.0, 'MCL': 50.0, 'YM': 50.0, 'MYM': 50.0, 'RTY': 25.0, 'M2K': 25.0,
            'ZB': 50.0, 'ZN': 50.0, 'ZF': 50.0, 'ZT': 50.0, 'GC': 25.0, 'SI': 50.0,
            'HG': 50.0, 'PL': 50.0, 'PA': 50.0, 'CU': 50.0, 'AL': 50.0, 'NI': 50.0
        }
        threshold = threshold_map.get(symbol, 50.0)
        print(f"Auto-detected symbol: {symbol}, threshold: {threshold} points")
    
    # Temp file for raw conversion
    raw_file = output_file.replace('.txt', '_raw.txt')
    
    # Step 1: Convert SCID to raw NT8 format
    result = convert_scid_to_nt8(input_file, output_file=raw_file, num_workers=num_workers)
    if not result:
        return False
    
    # Step 2: Clean the raw file
    print(f"\nCleaning ticks (threshold={threshold}, window={window_size})...")
    total, kept, dropped, illegal, spikes = clean_tick_file(raw_file, output_file, threshold=threshold, window_size=window_size)
    if total > 0:
        print(f"  Dropped {dropped:,} ticks ({100*dropped/total:.4f}%)")
        if illegal > 0:
            print(f"    - {illegal:,} illegal bid/ask")
        if spikes > 0:
            print(f"    - {spikes:,} transient spikes")
    else:
        print(f"  No ticks to clean (empty file)")
    
    # Step 3: Remove temp raw file
    try:
        os.remove(raw_file)
    except OSError:
        pass
    
    print(f"\n✓ Final output: {output_file} ({kept:,} ticks)")
    return True


def batch_convert(year_filter=None):
    """Batch convert multiple MNQ SCID files."""
    # Find all MNQ SCID files
    all_files = sorted(glob.glob("MNQ*-CME.scid"))

    if year_filter:
        # Filter for specific years
        scid_files = [f for f in all_files if any(f"MNQ{m}{y}" in f
                      for m in ['F','G','H','J','K','M','N','Q','U','V','X','Z']
                      for y in year_filter)]
        year_range = f"{year_filter[0]}-{year_filter[-1]}"
    else:
        scid_files = all_files
        year_range = "all"

    if not scid_files:
        print("No MNQ SCID files found!")
        return

    print(f"\n{'='*80}")
    print(f"BATCH CONVERSION: {len(scid_files)} MNQ contracts ({year_range})")
    print(f"{'='*80}\n")

    total_size = sum(os.path.getsize(f) for f in scid_files) / (1024**3)
    print(f"Total data size: {total_size:.2f} GB\n")

    success_count = 0
    for i, scid_file in enumerate(scid_files, 1):
        file_size_gb = os.path.getsize(scid_file) / (1024**3)
        print(f"\n[{i}/{len(scid_files)}] {scid_file} ({file_size_gb:.2f} GB)")
        print("-" * 80)

        if convert_and_clean(scid_file):
            success_count += 1
        else:
            print(f"❌ ERROR converting {scid_file}")

    print(f"\n{'='*80}")
    print("BATCH CONVERSION COMPLETE!")
    print(f"Successfully converted: {success_count}/{len(scid_files)} files")
    print("All files saved to: NT8_Imports\\ (cleaned)")
    print(f"{'='*80}\n")
    print("\nNext steps:")
    print("1. Open NT8: Tools → Import → Historical Data")
    print("2. Format: 'NinjaTrader (start of bar timestamps)'")
    print("3. Data Type: 'Last'")
    print("4. Time Zone: 'UTC'")
    print("5. Import all .txt files from NT8_Imports folder")
    print("\nNote: Tick Replay format with bid/ask for accurate volumetric analysis!")
    print(f"{'='*80}\n")


def batch_convert_mgc(year_filter=None):
    """Batch convert multiple MGC SCID files."""
    # Find all MGC SCID files (allow any exchange suffix)
    all_files = sorted(glob.glob("MGC*.scid"))

    if year_filter:
        scid_files = [f for f in all_files if any(f"MGC{m}{y}" in f
                      for m in ['F','G','H','J','K','M','N','Q','U','V','X','Z']
                      for y in year_filter)]
        year_range = f"{year_filter[0]}-{year_filter[-1]}"
    else:
        scid_files = all_files
        year_range = "all"

    if not scid_files:
        print("No MGC SCID files found!")
        return

    print(f"\n{'='*80}")
    print(f"BATCH CONVERSION: {len(scid_files)} MGC contracts ({year_range})")
    print(f"{'='*80}\n")

    total_size = sum(os.path.getsize(f) for f in scid_files) / (1024**3)
    print(f"Total data size: {total_size:.2f} GB\n")

    success_count = 0
    for i, scid_file in enumerate(scid_files, 1):
        file_size_gb = os.path.getsize(scid_file) / (1024**3)
        print(f"\n[{i}/{len(scid_files)}] {scid_file} ({file_size_gb:.2f} GB)")
        print("-" * 80)

        if convert_and_clean(scid_file):
            success_count += 1
        else:
            print(f"❌ ERROR converting {scid_file}")

    print(f"\n{'='*80}")
    print("BATCH CONVERSION COMPLETE!")
    print(f"Successfully converted: {success_count}/{len(scid_files)} files")
    print("All files saved to: NT8_Imports\\ (cleaned)")
    print(f"{'='*80}\n")
    print("\nNext steps:")
    print("1. Open NT8: Tools → Import → Historical Data")
    print("2. Format: 'NinjaTrader (start of bar timestamps)'")
    print("3. Data Type: 'Last'")
    print("4. Time Zone: 'UTC'")
    print("5. Import all .txt files from NT8_Imports folder")
    print("\nNote: Tick Replay format with bid/ask for accurate volumetric analysis!")
    print(f"{'='*80}\n")


def print_usage():
    """Print usage instructions."""
    print(__doc__)
    print("\nUsage:")
    print("  python scid_to_nt8_converter.py                    # Interactive mode")
    print("  python scid_to_nt8_converter.py <contract> <folder> <work_type> [options]")
    print("\nParameters:")
    print("  contract     Contract name (MGC, MNQ, ES, NQ, CL, etc.)")
    print("  folder       Source folder containing SCID files (default: current directory)")
    print("  work_type    Type of work to perform:")
    print("               single    - Convert single file")
    print("               batch     - Batch convert files in folder")
    print("               all       - Convert all files for contract")
    print("\nOptions:")
    print("  --threshold X    Custom cleaning threshold (points)")
    print("  --raw           Convert without cleaning")
    print("  --output DIR    Output directory (default: NT8_Imports)")
    print("  --workers N     Number of parallel workers (default: CPU count)")
    print("\nExamples:")
    print("  # Interactive mode (recommended for beginners)")
    print("  python scid_to_nt8_converter.py")
    print("  \n  # Command line mode")
    print("  # Convert single file")
    print("  python scid_to_nt8_converter.py MGC . single MGCZ25.scid")
    print("  python scid_to_nt8_converter.py ES data/ single ESH25.scid")
    print("  \n  # Batch convert files")
    print("  python scid_to_nt8_converter.py MNQ . batch")
    print("  python scid_to_nt8_converter.py CL scid_data/ batch")
    print("  \n  # Convert all files for contract")
    print("  python scid_to_nt8_converter.py MGC . all")
    print("  python scid_to_nt8_converter.py ES data/ all")
    print("  \n  # With custom options")
    print("  python scid_to_nt8_converter.py MGC . batch --threshold 30.0 --output custom/")
    print("  python scid_to_nt8_converter.py MNQ . single MNQZ25.scid --raw")
    print("\nContract Names and Thresholds:")
    print("  25.0 points: MGC, MNQ, ES, NQ, GC, MES, RTY, M2K")
    print("  50.0 points: CL, MCL, YM, MYM, ZB, ZN, ZF, ZT, SI, HG, PL, PA, CU, AL, NI")
    print()

def get_user_input():
    """Get user input through interactive prompts."""
    print("🚀 SCID to NinjaTrader 8 Converter - Interactive Mode")
    print("=" * 50)
    
    # Get contract
    contracts_25 = ['MGC', 'MNQ', 'ES', 'NQ', 'GC', 'MES', 'RTY', 'M2K']
    contracts_50 = ['CL', 'MCL', 'YM', 'MYM', 'ZB', 'ZN', 'ZF', 'ZT', 'SI', 'HG', 'PL', 'PA', 'CU', 'AL', 'NI']
    all_contracts = contracts_25 + contracts_50
    
    print("\n📋 Available Contracts:")
    print("  25.0 points: " + ", ".join(contracts_25))
    print("  50.0 points: " + ", ".join(contracts_50))
    
    while True:
        contract = input("\n📝 Enter contract name: ").strip().upper()
        if contract in all_contracts:
            break
        elif contract:
            print(f"❌ Unknown contract '{contract}'. Please choose from the list above.")
        else:
            print("❌ Contract name cannot be empty.")
    
    # Get folder
    while True:
        folder = input("📁 Enter source folder (press Enter for current directory): ").strip()
        if not folder:
            folder = "."
        
        if os.path.exists(folder):
            break
        else:
            print(f"❌ Folder '{folder}' does not exist. Please try again.")
    
    # Get work type
    print("\n🔧 Work Types:")
    print("  single - Convert a single file")
    print("  batch  - Batch convert files in folder")
    print("  all    - Convert all files recursively")
    
    while True:
        work_type = input("\n⚡ Enter work type (single/batch/all): ").strip().lower()
        if work_type in ['single', 'batch', 'all']:
            break
        elif work_type:
            print("❌ Invalid work type. Please choose: single, batch, or all.")
        else:
            print("❌ Work type cannot be empty.")
    
    # Get file pattern for single mode
    file_pattern = None
    if work_type == 'single':
        # List available files
        pattern = f"{contract}*.scid"
        files = glob.glob(os.path.join(folder, pattern))
        
        if files:
            print(f"\n📄 Found {len(files)} {contract} files:")
            for i, file in enumerate(files, 1):
                filename = os.path.basename(file)
                size_mb = os.path.getsize(file) / (1024 * 1024)
                print(f"  {i}. {filename} ({size_mb:.1f} MB)")
            
            while True:
                choice = input(f"\n📋 Enter file number (1-{len(files)}) or filename: ").strip()
                
                if choice.isdigit():
                    idx = int(choice) - 1
                    if 0 <= idx < len(files):
                        file_pattern = os.path.basename(files[idx])
                        break
                    else:
                        print(f"❌ Please enter a number between 1 and {len(files)}.")
                elif choice:
                    # Check if it's a valid filename
                    test_file = os.path.join(folder, choice)
                    if os.path.exists(test_file) and choice.endswith('.scid'):
                        file_pattern = choice
                        break
                    else:
                        print(f"❌ File '{choice}' not found.")
                else:
                    print("❌ Please select a file.")
        else:
            print(f"❌ No {contract} files found in '{folder}'.")
            return None
    
    # Get optional parameters
    options = {
        'threshold': None,
        'raw': False,
        'output_dir': 'NT8_Imports',
        'workers': None,
        'file_pattern': file_pattern
    }
    
    print("\n⚙️  Optional Parameters (press Enter to skip):")
    
    # Threshold
    threshold_input = input("🎯 Custom threshold (points) or press Enter for default: ").strip()
    if threshold_input:
        try:
            options['threshold'] = float(threshold_input)
        except ValueError:
            print(f"❌ Invalid threshold '{threshold_input}'. Using default.")
    
    # Raw mode
    raw_input = input("🧹 Raw mode (no cleaning)? (y/N): ").strip().lower()
    options['raw'] = raw_input in ['y', 'yes']
    
    # Output directory
    output_input = input("📤 Output directory or press Enter for 'NT8_Imports': ").strip()
    if output_input:
        options['output_dir'] = output_input
    
    # Workers
    workers_input = input(f"⚡ Number of workers or press Enter for CPU count: ").strip()
    if workers_input:
        try:
            options['workers'] = int(workers_input)
        except ValueError:
            print(f"❌ Invalid worker count '{workers_input}'. Using CPU count.")
    
    return contract, folder, work_type, options

def parse_arguments():
    """Parse command line arguments."""
    if len(sys.argv) < 2 or sys.argv[1] in ["-h", "--help"]:
        # Interactive mode
        if len(sys.argv) == 1:
            result = get_user_input()
            if result:
                return result
            else:
                print("\n❌ Interactive setup failed. Exiting.")
                sys.exit(1)
        else:
            print_usage()
            sys.exit(1)
    
    contract = sys.argv[1].upper()
    folder = sys.argv[2]
    work_type = sys.argv[3].lower()
    
    # Parse optional arguments
    options = {
        'threshold': None,
        'raw': False,
        'output_dir': 'NT8_Imports',
        'workers': None,
        'file_pattern': None,
        'from_year': None,
    }

    i = 4
    while i < len(sys.argv):
        arg = sys.argv[i]
        if arg == '--threshold' and i + 1 < len(sys.argv):
            try:
                options['threshold'] = float(sys.argv[i + 1])
                i += 2
            except ValueError:
                print("Error: Threshold must be a number")
                sys.exit(1)
        elif arg == '--raw':
            options['raw'] = True
            i += 1
        elif arg == '--output' and i + 1 < len(sys.argv):
            options['output_dir'] = sys.argv[i + 1]
            i += 2
        elif arg == '--workers' and i + 1 < len(sys.argv):
            try:
                options['workers'] = int(sys.argv[i + 1])
                i += 2
            except ValueError:
                print("Error: Workers must be an integer")
                sys.exit(1)
        elif arg == '--from-year' and i + 1 < len(sys.argv):
            yy = sys.argv[i + 1].strip()
            if not (yy.isdigit() and len(yy) == 2):
                print("Error: --from-year takes a 2-digit year (e.g. 15 for 2015)")
                sys.exit(1)
            options['from_year'] = yy
            i += 2
        else:
            # For single file mode, capture the filename
            if work_type == 'single':
                options['file_pattern'] = arg
                i += 1
            else:
                print(f"Error: Unknown option {arg}")
                sys.exit(1)
    
    return contract, folder, work_type, options

_MONTH_CODE_MAP = {'F':1,'G':2,'H':3,'J':4,'K':5,'M':6,'N':7,'Q':8,'U':9,'V':10,'X':11,'Z':12}


def nt8_output_filename(file_path, contract):
    """Return NT8-compliant filename ('CONTRACT MM-YY.txt') for a SCID input.

    Uses len(contract) as the offset into the basename so it works for any
    symbol length (ES=2, MGC=3, etc.)."""
    base = os.path.splitext(os.path.basename(file_path))[0]
    offset = len(contract)
    if len(base) >= offset + 3:
        month_num = _MONTH_CODE_MAP.get(base[offset], 1)
        year = base[offset + 1:offset + 3]
        return f"{contract} {month_num:02d}-{year}.txt"
    return f"{base}.txt"


def scid_year(file_path, contract):
    """Extract 2-digit year suffix from a SCID basename, e.g. 'ESH25-CME' -> '25'."""
    base = os.path.splitext(os.path.basename(file_path))[0]
    offset = len(contract)
    return base[offset + 1:offset + 3] if len(base) >= offset + 3 else ''


def execute_work(contract, folder, work_type, options):
    """Execute the requested work type."""
    # Isolate each contract's output in a per-ticker subfolder so re-runs of
    # different contracts don't share a directory.
    options['output_dir'] = os.path.join(options['output_dir'], contract)

    print(f"Contract: {contract}")
    print(f"Folder: {folder}")
    print(f"Work Type: {work_type}")
    print(f"Options: {options}")
    print("-" * 50)
    
    if work_type == 'single':
        if not options['file_pattern']:
            print("Error: Single file mode requires filename")
            sys.exit(1)
        
        file_path = os.path.join(folder, options['file_pattern'])
        if not os.path.exists(file_path):
            print(f"Error: File not found: {file_path}")
            sys.exit(1)
        
        if options['raw']:
            convert_scid_to_nt8(file_path, num_workers=options['workers'])
        else:
            output_file = os.path.join(options['output_dir'], nt8_output_filename(file_path, contract))
            convert_and_clean(
                file_path,
                output_file=output_file,
                contract_name=contract,
                threshold=options['threshold'],
                num_workers=options['workers']
            )
    
    elif work_type == 'batch':
        # Batch convert files for contract in folder
        pattern = f"{contract}*.scid"
        files = glob.glob(os.path.join(folder, pattern))

        if options.get('from_year'):
            fy = options['from_year']
            files = [f for f in files if scid_year(f, contract) >= fy]
            print(f"Year filter --from-year {fy}: {len(files)} files match")

        if not files:
            print(f"No {contract} files found in {folder}")
            sys.exit(1)

        print(f"Found {len(files)} {contract} files to convert...")
        
        for file_path in files:
            filename = os.path.basename(file_path)
            print(f"\nConverting: {filename}")
            
            if options['raw']:
                convert_scid_to_nt8(file_path, num_workers=options['workers'])
            else:
                convert_and_clean(
                    file_path,
                    output_file=os.path.join(options['output_dir'], nt8_output_filename(file_path, contract)),
                    contract_name=contract,
                    threshold=options['threshold'],
                    num_workers=options['workers']
                )

    elif work_type == 'all':
        # Convert all files for contract (recursive)
        pattern = f"{contract}*.scid"
        files = glob.glob(os.path.join(folder, '**', pattern), recursive=True)

        if options.get('from_year'):
            fy = options['from_year']
            files = [f for f in files if scid_year(f, contract) >= fy]
            print(f"Year filter --from-year {fy}: {len(files)} files match")

        if not files:
            print(f"No {contract} files found in {folder} (recursive)")
            sys.exit(1)
        
        print(f"Found {len(files)} {contract} files (recursive) to convert...")
        
        for file_path in files:
            filename = os.path.basename(file_path)
            rel_path = os.path.relpath(file_path, folder)
            print(f"\nConverting: {rel_path}")
            
            if options['raw']:
                convert_scid_to_nt8(file_path, num_workers=options['workers'])
            else:
                convert_and_clean(
                    file_path,
                    output_file=os.path.join(options['output_dir'], nt8_output_filename(file_path, contract)),
                    contract_name=contract,
                    threshold=options['threshold'],
                    num_workers=options['workers']
                )

    else:
        print(f"Error: Unknown work type '{work_type}'")
        print("Valid work types: single, batch, all")
        sys.exit(1)

if __name__ == "__main__":
    contract, folder, work_type, options = parse_arguments()
    execute_work(contract, folder, work_type, options)
