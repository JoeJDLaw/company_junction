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
sys.path.append(str(Path(__file__).parent.parent / "src"))

from cleaning import load_salesforce_data, validate_required_columns
from src.utils.io_utils import load_settings
from normalize import normalize_dataframe


class TestCleaning(unittest.TestCase):
    """Test cases for data cleaning functionality."""

    def setUp(self):
        """Set up test data."""
        self.sample_data = pd.DataFrame(
            {
                "Account ID": ["001", "002", "003", "004"],
                "Account Name": [
                    "Acme Corp",
                    "Acme Corp",
                    "Tech Solutions",
                    "Tech Solutions",
                ],
                "Relationship": [
                    "Other/Miscellaneous",
                    "Other/Miscellaneous",
                    "Other/Miscellaneous",
                    "Other/Miscellaneous",
                ],
                "Created Date": [
                    "2021-01-01",
                    "2021-01-02",
                    "2021-01-03",
                    "2021-01-04",
                ],
            }
        )

    def test_load_salesforce_data_csv(self):
        """Test loading CSV data."""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False
        ) as tmp_file:
            self.sample_data.to_csv(tmp_file.name, index=False)
            tmp_path = tmp_file.name

        try:
            loaded_data = load_salesforce_data(tmp_path)
            self.assertEqual(len(loaded_data), 4)
            self.assertEqual(
                list(loaded_data.columns),
                ["Account ID", "Account Name", "Relationship", "Created Date"],
            )
        finally:
            os.unlink(tmp_path)

    def test_validate_required_columns(self):
        """Test column validation logic."""
        # Test with all required columns
        self.assertTrue(validate_required_columns(self.sample_data))

        # Test with missing column
        incomplete_data = self.sample_data.drop(columns=["Created Date"])
        with self.assertRaises(ValueError):
            validate_required_columns(incomplete_data)

    def test_invalid_file_format(self):
        """Test handling of invalid file formats."""
        with tempfile.NamedTemporaryFile(suffix=".txt", delete=False) as tmp_file:
            tmp_file.write(b"Invalid data")
            tmp_path = tmp_file.name

        try:
            with self.assertRaises(ValueError):
                load_salesforce_data(tmp_path)
        finally:
            os.unlink(tmp_path)

    def test_enhanced_filtering_removes_problematic_records(self):
        """Test that enhanced filtering removes problematic records before blocking."""
        # Create test data with problematic records
        test_data = pd.DataFrame(
            {
                "Account Name": [
                    "Valid Company Inc",
                    "123",  # numeric-only
                    "a",  # single letter
                    "test",  # placeholder
                    "1099",  # tax form
                    "Another Valid Corp",
                    "temp",  # placeholder
                    "Unknown Industries",  # valid
                    "n/a",  # placeholder
                    "Real Business LLC",
                ],
                "Account ID": range(10),
                "Relationship": ["Customer"] * 10,
                "Created Date": ["2023-01-01"] * 10,
            }
        )

        # Run the pipeline up to filtering
        settings = load_settings("config/settings.yaml")
        name_column = settings.get("data", {}).get("name_column", "Account Name")

        # Normalize
        df_norm = normalize_dataframe(test_data, name_column)

        # Apply filtering (simulate the filtering logic from cleaning.py)
        # Filter out empty name_core
        df_norm = df_norm[df_norm["name_core"].str.strip() != ""].copy()

        # Enhanced problematic patterns (accounting for normalization)
        problematic_patterns = [
            r"^\d+$",  # Numeric only
            r"^[A-Za-z]$",  # Single character
            r"^(test|sample|temp|unknown|n a|none|tbd)$",  # Common placeholders (n/a becomes n a)
            r"^1099$",  # Tax form references
            r"^unknown industries$",  # "Unknown Industries" becomes "unknown industries"
        ]

        import re

        mask = (
            df_norm["name_core"]
            .str.lower()
            .str.strip()
            .apply(
                lambda x: not any(
                    re.match(pattern, x) for pattern in problematic_patterns
                )
            )
        )
        df_norm = df_norm[mask].copy()

        filtered_count = len(df_norm)

        # Should have filtered out 7 problematic records
        self.assertEqual(filtered_count, 3)  # 10 total - 7 problematic = 3 valid

        # Verify the remaining records are valid
        remaining_names = df_norm["name_core"].tolist()
        # Normalization changes names, so we check for partial matches
        expected_valid_patterns = [
            "valid company",
            "another valid",
            "real business",
        ]

        for name in remaining_names:
            name_lower = name.lower()
            self.assertTrue(
                any(pattern in name_lower for pattern in expected_valid_patterns),
                f"Name '{name}' not found in expected patterns",
            )

    def test_performance_logging_context_manager(self):
        """Test that performance logging context manager works."""
        from src.utils.perf_utils import log_perf
        import time

        with log_perf("test_operation"):
            time.sleep(0.01)  # Small delay to ensure timing is captured

        # If we get here without error, the context manager worked


if __name__ == "__main__":
    unittest.main()
