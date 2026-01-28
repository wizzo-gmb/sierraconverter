#!/usr/bin/env python3
"""
Batch Conversion Example
Demonstrates how to convert multiple SCID files with custom settings
"""

import os
import sys
import glob
from pathlib import Path

# Add parent directory to path to import the converter
sys.path.append(str(Path(__file__).parent.parent))

from scid_to_nt8_converter import convert_scid_file, SYMBOL_THRESHOLDS

def batch_convert_directory(directory=".", pattern="*.scid", output_dir="converted"):
    """
    Convert all SCID files in a directory with custom settings
    
    Args:
        directory: Directory containing SCID files
        pattern: File pattern to match (default: *.scid)
        output_dir: Output directory for converted files
    """
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Find all SCID files
    scid_files = glob.glob(os.path.join(directory, pattern))
    
    if not scid_files:
        print(f"No SCID files found in {directory} with pattern {pattern}")
        return
    
    print(f"Found {len(scid_files)} SCID files to convert...")
    
    for i, scid_file in enumerate(scid_files, 1):
        try:
            print(f"\n[{i}/{len(scid_files)}] Converting: {os.path.basename(scid_file)}")
            
            # Extract symbol from filename
            symbol = os.path.basename(scid_file).split('Z')[0]
            
            # Get symbol-specific threshold
            threshold = SYMBOL_THRESHOLDS.get(symbol, 25.0)
            
            # Convert file
            result = convert_scid_file(
                scid_file, 
                threshold=threshold,
                output_dir=output_dir,
                clean=True
            )
            
            if result['success']:
                print(f"✅ Success: {result['output_filename']}")
                print(f"   Records: {result['kept_records']:,} / {result['total_records']:,}")
                print(f"   Dropped: {result['dropped_records']:,} ({result['drop_pct']:.2f}%)")
            else:
                print(f"❌ Failed: {result['error']}")
                
        except Exception as e:
            print(f"❌ Error converting {scid_file}: {e}")

def convert_symbol_batch(symbol="MNQ", base_dir="."):
    """
    Convert all files for a specific symbol
    
    Args:
        symbol: Symbol to convert (e.g., "MNQ", "ES", "MGC")
        base_dir: Base directory containing SCID files
    """
    
    pattern = f"{symbol}*.scid"
    output_dir = f"converted_{symbol}"
    
    print(f"Converting all {symbol} files...")
    batch_convert_directory(base_dir, pattern, output_dir)

if __name__ == "__main__":
    # Example 1: Convert all SCID files in current directory
    print("=== Batch Conversion Example ===")
    batch_convert_directory()
    
    # Example 2: Convert only MNQ files
    print("\n=== MNQ Batch Conversion ===")
    convert_symbol_batch("MNQ")
    
    # Example 3: Convert with custom settings
    print("\n=== Custom Threshold Conversion ===")
    custom_threshold = 30.0
    custom_output = "converted_custom"
    
    os.makedirs(custom_output, exist_ok=True)
    scid_files = glob.glob("ES*.scid")[:2]  # Convert first 2 ES files
    
    for scid_file in scid_files:
        print(f"Converting {scid_file} with threshold {custom_threshold}")
        result = convert_scid_file(
            scid_file,
            threshold=custom_threshold,
            output_dir=custom_output,
            clean=True
        )
        print(f"Result: {result['success']}")
