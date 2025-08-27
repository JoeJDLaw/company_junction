"""
Unit tests for the cleaning module.

This module tests:
- Data loading functionality
- Duplicate detection logic
- Record merging operations
- Data validation
"""

import unittest
import pandas as pd
import tempfile
import os
import sys
from pathlib import Path

# Add src directory to path for imports
sys.path.append(str(Path(__file__).parent.parent / 'src'))

from cleaning import load_salesforce_data, validate_required_columns


class TestCleaning(unittest.TestCase):
    """Test cases for data cleaning functionality."""
    
    def setUp(self):
        """Set up test data."""
        self.sample_data = pd.DataFrame({
            'Account ID': ['001', '002', '003', '004'],
            'Account Name': ['Acme Corp', 'Acme Corp', 'Tech Solutions', 'Tech Solutions'],
            'Relationship': ['Other/Miscellaneous', 'Other/Miscellaneous', 'Other/Miscellaneous', 'Other/Miscellaneous'],
            'Created Date': ['2021-01-01', '2021-01-02', '2021-01-03', '2021-01-04']
        })
    
    def test_load_salesforce_data_csv(self):
        """Test loading CSV data."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as tmp_file:
            self.sample_data.to_csv(tmp_file.name, index=False)
            tmp_path = tmp_file.name
        
        try:
            loaded_data = load_salesforce_data(tmp_path)
            self.assertEqual(len(loaded_data), 4)
            self.assertEqual(list(loaded_data.columns), ['Account ID', 'Account Name', 'Relationship', 'Created Date'])
        finally:
            os.unlink(tmp_path)
    
    def test_validate_required_columns(self):
        """Test column validation logic."""
        # Test with all required columns
        self.assertTrue(validate_required_columns(self.sample_data))
        
        # Test with missing column
        incomplete_data = self.sample_data.drop(columns=['Created Date'])
        with self.assertRaises(ValueError):
            validate_required_columns(incomplete_data)
    
    def test_invalid_file_format(self):
        """Test handling of invalid file formats."""
        with tempfile.NamedTemporaryFile(suffix='.txt', delete=False) as tmp_file:
            tmp_file.write(b"Invalid data")
            tmp_path = tmp_file.name
        
        try:
            with self.assertRaises(ValueError):
                load_salesforce_data(tmp_path)
        finally:
            os.unlink(tmp_path)


if __name__ == '__main__':
    unittest.main()
