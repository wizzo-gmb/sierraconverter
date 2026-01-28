#!/usr/bin/env python3
"""
Custom Threshold Example
Demonstrates how to use custom cleaning thresholds for different symbols
"""

import os
import sys
from pathlib import Path

# Add parent directory to path to import the converter
sys.path.append(str(Path(__file__).parent.parent))

from scid_to_nt8_converter import convert_scid_file, convert_and_clean, SYMBOL_THRESHOLDS

def custom_threshold_conversion():
    """
    Example of converting files with custom thresholds based on market conditions
    """
    
    # Custom thresholds for different volatility regimes
    custom_thresholds = {
        'ES': 50.0,   # Higher threshold for ES during high volatility
        'NQ': 40.0,   # Moderate threshold for NQ
        'MGC': 30.0,  # Lower threshold for Gold (more sensitive)
        'MNQ': 20.0,  # Very low threshold for Micro NQ
        'CL': 100.0,  # High threshold for Crude Oil (volatile)
    }
    
    # Example files to convert
    test_files = [
        ('ESH25-CME.scid', 'ES'),
        ('NQH25-CME.scid', 'NQ'),
        ('MGCH25-COMEX.scid', 'MGC'),
        ('MNQH25-CME.scid', 'MNQ'),
        ('CLH25-NYMEX.scid', 'CL')
    ]
    
    print("=== Custom Threshold Conversion ===\n")
    
    for filename, contract_name in test_files:
        if not os.path.exists(filename):
            print(f"⚠️  File not found: {filename}")
            continue
            
        # Get custom threshold or use default
        threshold = custom_thresholds.get(contract_name, SYMBOL_THRESHOLDS.get(contract_name, 25.0))
        
        print(f"Converting {filename}")
        print(f"Contract: {contract_name}")
        print(f"Threshold: {threshold} points")
        print("-" * 40)
        
        try:
            # Convert with contract name and custom threshold
            success = convert_and_clean(
                filename,
                threshold=threshold,
                contract_name=contract_name
            )
            
            if success:
                print(f"✅ Success! Converted with contract name: {contract_name}")
            else:
                print(f"❌ Conversion failed")
                
        except Exception as e:
            print(f"❌ Error: {e}")
        
        print()

def contract_name_examples():
    """
    Demonstrate using contract names for files with non-standard naming
    """
    
    print("=== Contract Name Examples ===\n")
    
    # Example: File with non-standard name but known contract
    non_standard_files = [
        ('my_gold_data.scid', 'MGC'),
        ('es_data_2025.scid', 'ES'),
        ('crude_oil_tick.scid', 'CL'),
        ('micro_nasdaq.scid', 'MNQ')
    ]
    
    for filename, contract_name in non_standard_files:
        if not os.path.exists(filename):
            print(f"⚠️  File not found: {filename}")
            continue
            
        print(f"Converting {filename} as {contract_name}")
        print("-" * 30)
        
        try:
            success = convert_and_clean(
                filename,
                contract_name=contract_name
            )
            
            if success:
                print(f"✅ Success! Used contract name: {contract_name}")
            else:
                print(f"❌ Conversion failed")
                
        except Exception as e:
            print(f"❌ Error: {e}")
        
        print()

def threshold_comparison():
    """
    Compare results with different thresholds for the same file
    """
    
    test_file = 'MNQH25-CME.scid'
    
    if not os.path.exists(test_file):
        print(f"⚠️  Test file not found: {test_file}")
        return
    
    thresholds = [15.0, 25.0, 50.0, 100.0]  # Different thresholds to test
    
    print(f"=== Threshold Comparison for {test_file} ===\n")
    
    for threshold in thresholds:
        print(f"Testing threshold: {threshold} points")
        
        try:
            result = convert_scid_file(
                test_file,
                threshold=threshold,
                output_dir=f"converted_threshold_{int(threshold)}",
                clean=True
            )
            
            if result['success']:
                print(f"   Records kept: {result['kept_records']:,} / {result['total_records']:,}")
                print(f"   Drop rate: {result['drop_pct']:.2f}%")
                print(f"   Illegal ticks: {result['illegal_ticks']:,}")
                print(f"   Spike ticks: {result['spike_ticks']:,}")
            else:
                print(f"   Failed: {result['error']}")
                
        except Exception as e:
            print(f"   Error: {e}")
        
        print()

def raw_vs_cleaned_comparison():
    """
    Compare raw conversion vs cleaned conversion
    """
    
    test_file = 'ESH25-CME.scid'
    
    if not os.path.exists(test_file):
        print(f"⚠️  Test file not found: {test_file}")
        return
    
    print(f"=== Raw vs Cleaned Comparison for {test_file} ===\n")
    
    # Raw conversion (no cleaning)
    print("Raw conversion (no cleaning):")
    try:
        raw_result = convert_scid_file(
            test_file,
            threshold=None,
            output_dir="converted_raw",
            clean=False
        )
        
        if raw_result['success']:
            print(f"   Records: {raw_result['total_records']:,}")
            print(f"   File size: {raw_result['file_size_mb']:.2f} MB")
    except Exception as e:
        print(f"   Error: {e}")
    
    print()
    
    # Cleaned conversion
    print("Cleaned conversion:")
    try:
        clean_result = convert_scid_file(
            test_file,
            threshold=25.0,
            output_dir="converted_clean",
            clean=True
        )
        
        if clean_result['success']:
            print(f"   Records: {clean_result['kept_records']:,} / {clean_result['total_records']:,}")
            print(f"   Dropped: {clean_result['dropped_records']:,} ({clean_result['drop_pct']:.2f}%)")
            print(f"   Illegal ticks: {clean_result['illegal_ticks']:,}")
            print(f"   Spike ticks: {clean_result['spike_ticks']:,}")
    except Exception as e:
        print(f"   Error: {e}")

if __name__ == "__main__":
    # Run all examples
    custom_threshold_conversion()
    threshold_comparison()
    raw_vs_cleaned_comparison()
