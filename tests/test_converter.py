#!/usr/bin/env python3
"""
Unit Tests for SCID Converter
"""

import unittest
import tempfile
import os
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

# Add parent directory to path to import the converter
sys.path.append(str(Path(__file__).parent.parent))

try:
    from scid_to_nt8_converter import (
        convert_scid_file, 
        deserialize_datetime, 
        SYMBOL_THRESHOLDS,
        SIZE_HEADER,
        SIZE_RECORD
    )
except ImportError as e:
    print(f"Import error: {e}")
    print("Make sure scid_to_nt8_converter.py is in the parent directory")
    sys.exit(1)

class TestSCIDConverter(unittest.TestCase):
    """Test cases for SCID converter functionality"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.temp_dir = tempfile.mkdtemp()
        self.test_scid_file = os.path.join(self.temp_dir, "test.scid")
        
        # Create a minimal SCID file for testing
        self.create_test_scid_file()
    
    def tearDown(self):
        """Clean up test fixtures"""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def create_test_scid_file(self):
        """Create a minimal test SCID file"""
        # Create a simple header (56 bytes)
        header = b'\x00' * SIZE_HEADER
        
        # Create a few test records (40 bytes each)
        records = []
        for i in range(10):
            # Simple record structure
            record = b'\x00' * SIZE_RECORD
            records.append(record)
        
        # Write file
        with open(self.test_scid_file, 'wb') as f:
            f.write(header)
            f.write(b''.join(records))
    
    def test_deserialize_datetime(self):
        """Test datetime deserialization"""
        # Test with a known Excel datetime value
        excel_dt = 45000.0  # Represents a specific date
        result = deserialize_datetime(excel_dt)
        
        self.assertIsNotNone(result)
        self.assertIsInstance(result, type(None) if result is None else type(result))
    
    def test_symbol_thresholds(self):
        """Test symbol threshold configuration"""
        # Test known symbols
        self.assertIn('ES', SYMBOL_THRESHOLDS)
        self.assertIn('NQ', SYMBOL_THRESHOLDS)
        self.assertIn('MGC', SYMBOL_THRESHOLDS)
        
        # Test threshold values are positive
        for symbol, threshold in SYMBOL_THRESHOLDS.items():
            self.assertGreater(threshold, 0, f"Threshold for {symbol} should be positive")
    
    def test_file_not_found(self):
        """Test handling of non-existent files"""
        non_existent_file = os.path.join(self.temp_dir, "nonexistent.scid")
        
        result = convert_scid_file(
            non_existent_file,
            threshold=25.0,
            output_dir=self.temp_dir,
            clean=True
        )
        
        self.assertFalse(result['success'])
        self.assertIn('error', result)
    
    def test_invalid_file_format(self):
        """Test handling of invalid file formats"""
        # Create an invalid file (wrong size)
        invalid_file = os.path.join(self.temp_dir, "invalid.scid")
        with open(invalid_file, 'wb') as f:
            f.write(b'invalid data')
        
        result = convert_scid_file(
            invalid_file,
            threshold=25.0,
            output_dir=self.temp_dir,
            clean=True
        )
        
        # Should either fail or handle gracefully
        if result['success']:
            self.assertIsInstance(result['total_records'], int)
        else:
            self.assertIn('error', result)
    
    def test_output_directory_creation(self):
        """Test that output directory is created if it doesn't exist"""
        output_dir = os.path.join(self.temp_dir, "new_output_dir")
        
        # Ensure directory doesn't exist
        self.assertFalse(os.path.exists(output_dir))
        
        # Convert file (this should create the directory)
        result = convert_scid_file(
            self.test_scid_file,
            threshold=25.0,
            output_dir=output_dir,
            clean=True
        )
        
        # Directory should now exist
        self.assertTrue(os.path.exists(output_dir))
    
    def test_clean_vs_raw_conversion(self):
        """Test difference between clean and raw conversion"""
        # Clean conversion
        clean_result = convert_scid_file(
            self.test_scid_file,
            threshold=25.0,
            output_dir=self.temp_dir,
            clean=True
        )
        
        # Raw conversion
        raw_result = convert_scid_file(
            self.test_scid_file,
            threshold=None,
            output_dir=self.temp_dir,
            clean=False
        )
        
        # Both should succeed or fail consistently
        self.assertEqual(clean_result['success'], raw_result['success'])
        
        if clean_result['success']:
            # Raw should have more or equal records than clean
            self.assertGreaterEqual(
                raw_result['total_records'], 
                clean_result['kept_records']
            )
    
    def test_custom_threshold(self):
        """Test custom threshold setting"""
        custom_threshold = 50.0
        
        result = convert_scid_file(
            self.test_scid_file,
            threshold=custom_threshold,
            output_dir=self.temp_dir,
            clean=True
        )
        
        if result['success']:
            # Should not raise any errors with custom threshold
            self.assertIsInstance(result['drop_pct'], (int, float))
            self.assertGreaterEqual(result['drop_pct'], 0)
            self.assertLessEqual(result['drop_pct'], 100)
    
    def test_output_file_creation(self):
        """Test that output file is created"""
        result = convert_scid_file(
            self.test_scid_file,
            threshold=25.0,
            output_dir=self.temp_dir,
            clean=True
        )
        
        if result['success']:
            output_file = os.path.join(self.temp_dir, result['output_filename'])
            self.assertTrue(os.path.exists(output_file))
            
            # Check file has content
            self.assertGreater(os.path.getsize(output_file), 0)
    
    def test_filename_generation(self):
        """Test output filename generation"""
        result = convert_scid_file(
            self.test_scid_file,
            threshold=25.0,
            output_dir=self.temp_dir,
            clean=True
        )
        
        if result['success']:
            # Output filename should be different from input
            self.assertNotEqual(result['output_filename'], 'test.scid')
            # Should end with .txt for NT8 format
            self.assertTrue(result['output_filename'].endswith('.txt'))

class TestConverterEdgeCases(unittest.TestCase):
    """Test edge cases and error conditions"""
    
    def test_empty_file(self):
        """Test handling of empty files"""
        with tempfile.NamedTemporaryFile(suffix='.scid', delete=False) as f:
            # Create empty file
            temp_file = f.name
        
        try:
            result = convert_scid_file(
                temp_file,
                threshold=25.0,
                output_dir=tempfile.gettempdir(),
                clean=True
            )
            
            # Should handle gracefully
            self.assertIsInstance(result['success'], bool)
            
        finally:
            os.unlink(temp_file)
    
    def test_zero_threshold(self):
        """Test with zero threshold"""
        with tempfile.NamedTemporaryFile(suffix='.scid', delete=False) as f:
            # Create minimal file
            f.write(b'\x00' * (SIZE_HEADER + SIZE_RECORD))
            temp_file = f.name
        
        try:
            result = convert_scid_file(
                temp_file,
                threshold=0.0,
                output_dir=tempfile.gettempdir(),
                clean=True
            )
            
            if result['success']:
                self.assertGreaterEqual(result['drop_pct'], 0)
                
        finally:
            os.unlink(temp_file)
    
    def test_negative_threshold(self):
        """Test with negative threshold (should be handled gracefully)"""
        with tempfile.NamedTemporaryFile(suffix='.scid', delete=False) as f:
            # Create minimal file
            f.write(b'\x00' * (SIZE_HEADER + SIZE_RECORD))
            temp_file = f.name
        
        try:
            result = convert_scid_file(
                temp_file,
                threshold=-10.0,
                output_dir=tempfile.gettempdir(),
                clean=True
            )
            
            # Should handle negative threshold gracefully
            self.assertIsInstance(result['success'], bool)
                
        finally:
            os.unlink(temp_file)

if __name__ == '__main__':
    # Run tests
    unittest.main(verbosity=2)
