"""
Tests for name normalization functionality.
"""

import unittest
import pandas as pd
import sys
from pathlib import Path

# Add src directory to path for imports
sys.path.append(str(Path(__file__).parent.parent / 'src'))

from normalize import (
    normalize_name, extract_suffix, excel_serial_to_datetime,
    normalize_dataframe, NameNorm
)


class TestNormalize(unittest.TestCase):
    """Test cases for name normalization."""
    
    def test_basic_normalization(self):
        """Test basic name normalization."""
        # Test symbol mapping
        result = normalize_name("20-20 Plumbing & Heating, Inc.")
        self.assertEqual(result.name_base, "20 20 plumbing and heating inc")
        self.assertEqual(result.suffix_class, "INC")
        self.assertEqual(result.name_core, "20 20 plumbing and heating")
        
        # Test numeric style unification
        result2 = normalize_name("20/20 Plumbing & Heating Inc")
        self.assertEqual(result2.name_base, "20 20 plumbing and heating inc")
        self.assertEqual(result2.suffix_class, "INC")
        self.assertEqual(result2.name_core, "20 20 plumbing and heating")
        
        # Test space-separated numeric
        result3 = normalize_name("20 20 Plumbing & Heating Inc")
        self.assertEqual(result3.name_base, "20 20 plumbing and heating inc")
        self.assertEqual(result3.suffix_class, "INC")
        self.assertEqual(result3.name_core, "20 20 plumbing and heating")
    
    def test_suffix_detection(self):
        """Test legal suffix detection."""
        # Test different INC variations
        inc_variants = [
            "Acme Inc",
            "Acme Inc.",
            "Acme Incorporated"
        ]
        
        for variant in inc_variants:
            result = normalize_name(variant)
            self.assertEqual(result.suffix_class, "INC")
            self.assertEqual(result.name_core, "acme")
        
        # Test LLC
        result = normalize_name("Acme LLC")
        self.assertEqual(result.suffix_class, "LLC")
        self.assertEqual(result.name_core, "acme")
        
        # Test no suffix
        result = normalize_name("Acme Company")
        self.assertEqual(result.suffix_class, "CO")  # "Company" is recognized as CO suffix
        self.assertEqual(result.name_core, "acme")
    
    def test_suffix_mismatch_detection(self):
        """Test that different suffixes are properly distinguished."""
        inc_result = normalize_name("20-20 Plumbing & Heating Inc")
        llc_result = normalize_name("20-20 Plumbing & Heating LLC")
        
        # Same core name
        self.assertEqual(inc_result.name_core, llc_result.name_core)
        
        # Different suffix classes
        self.assertEqual(inc_result.suffix_class, "INC")
        self.assertEqual(llc_result.suffix_class, "LLC")
        
        # This should NOT be considered a match due to suffix mismatch
        self.assertNotEqual(inc_result.suffix_class, llc_result.suffix_class)
    
    def test_edge_cases(self):
        """Test edge cases and error handling."""
        # Empty/None values
        result = normalize_name(None)
        self.assertEqual(result.suffix_class, "NONE")
        self.assertEqual(result.name_core, "")
        
        result = normalize_name("")
        self.assertEqual(result.suffix_class, "NONE")
        self.assertEqual(result.name_core, "")
        
        # Very short names
        result = normalize_name("A")
        self.assertEqual(result.suffix_class, "NONE")
        self.assertEqual(result.name_core, "a")
        
        # Names with special characters
        result = normalize_name("Company@123.com")
        self.assertEqual(result.name_base, "company at 123 com")
        self.assertEqual(result.suffix_class, "NONE")
    
    def test_numeric_style_unification(self):
        """Test numeric style unification."""
        test_cases = [
            ("20-20 Vision", "20 20 vision"),
            ("20/20 Vision", "20 20 vision"),
            ("20 20 Vision", "20 20 vision"),
            ("100-200 Company", "100 200 company"),
            ("A1-B2-C3", "a1 b2 c3"),  # Should not be unified
        ]
        
        for input_name, expected_base in test_cases:
            result = normalize_name(input_name)
            self.assertEqual(result.name_base, expected_base)
    
    def test_excel_serial_conversion(self):
        """Test Excel serial number to datetime conversion."""
        # Test Excel serial dates
        # Excel serial 44197 = 2021-01-01
        result = excel_serial_to_datetime(44197)
        self.assertIsInstance(result, pd.Timestamp)
        self.assertEqual(result.date(), pd.Timestamp('2021-01-01').date())
        
        # Test None/NaN
        result = excel_serial_to_datetime(None)
        self.assertIsNone(result)
        
        result = excel_serial_to_datetime(pd.NA)
        self.assertIsNone(result)
    
    def test_dataframe_normalization(self):
        """Test DataFrame normalization."""
        df = pd.DataFrame({
            'Account Name': [
                '20-20 Plumbing & Heating Inc',
                '20/20 Plumbing & Heating LLC',
                'Acme Corporation'
            ]
        })
        
        result = normalize_dataframe(df, 'Account Name')
        
        # Check that new columns were added
        expected_columns = ['Account Name', 'name_raw', 'name_base', 'name_core', 'suffix_class']
        for col in expected_columns:
            self.assertIn(col, result.columns)
        
        # Check specific values
        self.assertEqual(result.iloc[0]['suffix_class'], 'INC')
        self.assertEqual(result.iloc[0]['name_core'], '20 20 plumbing and heating')
        
        self.assertEqual(result.iloc[1]['suffix_class'], 'LLC')
        self.assertEqual(result.iloc[1]['name_core'], '20 20 plumbing and heating')
        
        self.assertEqual(result.iloc[2]['suffix_class'], 'CORP')
        self.assertEqual(result.iloc[2]['name_core'], 'acme')
    
    def test_three_variants_same_core(self):
        """Test that three '20/20 ... Inc' variants share same name_core."""
        variants = [
            "20-20 Plumbing and Heating Inc",
            "20/20 Plumbing & Heating, Inc.",
            "20 20 Plumbing & Heating Inc"
        ]
        
        results = [normalize_name(variant) for variant in variants]
        
        # All should have same core name
        core_names = [r.name_core for r in results]
        self.assertEqual(len(set(core_names)), 1)
        self.assertEqual(core_names[0], "20 20 plumbing and heating")
        
        # All should have INC suffix
        suffix_classes = [r.suffix_class for r in results]
        self.assertEqual(len(set(suffix_classes)), 1)
        self.assertEqual(suffix_classes[0], "INC")
    
    def test_underscore_normalization(self):
        """Test underscore normalization."""
        result = normalize_name("__Don_Roberto__")
        self.assertEqual(result.name_base, "don roberto")
        self.assertEqual(result.name_core, "don roberto")
    
    def test_parentheses_detection(self):
        """Test parentheses detection."""
        result = normalize_name("Diamond Foods (Express Staffing)")
        self.assertTrue(result.has_parentheses)
        self.assertEqual(result.name_core, "diamond foods express staffing")
    
    def test_semicolon_detection(self):
        """Test semicolon detection."""
        result = normalize_name("Company A; Company B")
        self.assertTrue(result.has_semicolon)
        self.assertTrue(result.has_multiple_names)
    
    def test_multiple_names_detection(self):
        """Test multiple names detection."""
        # Test numbered pattern
        result = normalize_name("(1) Don Roberto; (2) BYD Auto")
        self.assertTrue(result.has_multiple_names)
        
        # Test multiple "and" separators
        result = normalize_name("Company A and Company B and Company C")
        self.assertTrue(result.has_multiple_names)
    
    def test_alias_extraction(self):
        """Test alias candidate extraction."""
        # Test semicolon aliases
        result = normalize_name("(1)Don Roberto Jewelers; (2) BYD Auto")
        self.assertEqual(len(result.alias_candidates), 2)
        self.assertIn("Don Roberto Jewelers", result.alias_candidates)
        self.assertIn("BYD Auto", result.alias_candidates)
        self.assertEqual(result.alias_sources, ['semicolon', 'semicolon'])
        
        # Test parentheses with company name
        result = normalize_name("BMW of Ontario (Penske Auto Group Ontario B1)")
        self.assertEqual(len(result.alias_candidates), 1)
        self.assertIn("Penske Auto Group Ontario B1", result.alias_candidates)
        self.assertEqual(result.alias_sources, ['parentheses'])
        
        # Test parentheses with blacklist word (should not create alias)
        result = normalize_name("Acme Corp (Delaware)")
        self.assertEqual(len(result.alias_candidates), 0)
        
        # Test parentheses with paystub (should not create alias)
        result = normalize_name("Test Company (paystub)")
        self.assertEqual(len(result.alias_candidates), 0)
    
    def test_numbered_marker_removal(self):
        """Test that numbered markers are removed from scoring."""
        result = normalize_name("(1)Don Roberto Jewelers")
        self.assertEqual(result.name_core, "don roberto jewelers")
        # The (1) should be removed from the core name for scoring


if __name__ == '__main__':
    unittest.main()
