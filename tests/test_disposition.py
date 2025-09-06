"""Tests for disposition logic functionality."""

import sys
import unittest
from pathlib import Path

import pandas as pd

# Add src directory to path for imports
sys.path.append(str(Path(__file__).parent.parent / "src"))

from src.disposition import (
    _is_blacklisted,
    _is_suspicious_singleton,
    apply_dispositions,
    classify_disposition,
    compute_group_metadata,
)
from src.normalize import normalize_dataframe


class TestDisposition(unittest.TestCase):
    """Test cases for disposition logic."""

    def setUp(self) -> None:
        """Set up test data."""
        self.test_data = pd.DataFrame(
            {
                "Account Name": [
                    "20-20 Plumbing & Heating Inc",
                    "20/20 Plumbing & Heating LLC",
                    "PNC is not sure",
                    "1099, no paystubs",
                    "Acme Corporation",
                    "Test Company",
                    "Unknown Company",
                    "N/A",
                ],
                "Account ID": ["001", "002", "003", "004", "005", "006", "007", "008"],
                "Relationship": [
                    "Company Name on Paystubs",
                    "Company Name on Paystubs",
                    "Other/Miscellaneous",
                    "Other/Miscellaneous",
                    "Company Name on W-2",
                    "Other/Miscellaneous",
                    "Other/Miscellaneous",
                    "Other/Miscellaneous",
                ],
            },
        )

        # Normalize the test data
        self.df_norm = normalize_dataframe(self.test_data, "Account Name")

        # Test settings
        self.settings = {
            "similarity": {
                "high": 92,
                "medium": 84,
                "penalty": {"suffix_mismatch": 25, "num_style_mismatch": 5},
            },
            "llm": {"enabled": False, "delete_threshold": 85},
        }

    def test_blacklist_detection(self) -> None:
        """Test blacklist detection."""
        # Test blacklisted names
        blacklisted_names = [
            "PNC is not sure",
            "1099, no paystubs",
            "N/A",
            "Test Company",
            "Unknown Company",
            "TBD",
            "Sample Company",
        ]

        for name in blacklisted_names:
            self.assertTrue(_is_blacklisted(name), f"'{name}' should be blacklisted")

        # Test non-blacklisted names
        good_names = [
            "Acme Corporation",
            "20-20 Plumbing & Heating Inc",
            "Tech Solutions LLC",
            "Global Industries",
        ]

        for name in good_names:
            self.assertFalse(
                _is_blacklisted(name),
                f"'{name}' should not be blacklisted",
            )

    def test_short_long_name_detection(self) -> None:
        """Test detection of very short or long names."""
        # Very short names
        self.assertTrue(_is_blacklisted("A"))
        self.assertTrue(_is_blacklisted("AB"))

        # Very long names (over 100 chars)
        long_name = "A" * 101
        self.assertTrue(_is_blacklisted(long_name))

        # Normal length names
        self.assertFalse(_is_blacklisted("Acme Corp"))
        self.assertFalse(_is_blacklisted("20-20 Plumbing & Heating Inc"))

    def test_punctuation_stopword_detection(self) -> None:
        """Test detection of names that are mostly punctuation or stopwords."""
        # Mostly punctuation
        self.assertTrue(_is_blacklisted("..."))
        self.assertTrue(_is_blacklisted("---"))
        self.assertTrue(_is_blacklisted("   "))

        # Mostly stopwords
        self.assertTrue(_is_blacklisted("The And Or"))
        self.assertTrue(_is_blacklisted("In On At"))

        # Normal names
        self.assertFalse(_is_blacklisted("Acme Corporation"))
        self.assertFalse(_is_blacklisted("20-20 Plumbing"))

    def test_suspicious_singleton_detection(self) -> None:
        """Test detection of suspicious singleton records."""
        # Suspicious patterns
        suspicious_data = [
            {"Account Name": "Unknown Company"},
            {"Account Name": "Test Sample"},
            {"Account Name": "Temp Agency"},
            {"Account Name": "To Be Determined"},
            {"Account Name": "Do Not Use"},
        ]

        for record_data in suspicious_data:
            row = pd.Series(record_data)
            self.assertTrue(_is_suspicious_singleton(row, self.settings))

        # Normal names
        normal_data = [
            {"Account Name": "Acme Corporation"},
            {"Account Name": "20-20 Plumbing & Heating Inc"},
        ]

        for record_data in normal_data:
            row = pd.Series(record_data)
            self.assertFalse(_is_suspicious_singleton(row, self.settings))

    def test_disposition_classification(self) -> None:
        """Test disposition classification logic."""
        # Test blacklisted -> Delete
        blacklisted_row = pd.Series({"Account Name": "PNC is not sure"})
        group_meta = {"group_size": 1, "has_suffix_mismatch": False}

        disposition = classify_disposition(blacklisted_row, group_meta, self.settings)
        self.assertEqual(disposition, "Delete")

        # Test suffix mismatch -> Verify
        normal_row = pd.Series({"Account Name": "Acme Corp"})
        group_meta = {"group_size": 2, "has_suffix_mismatch": True}

        disposition = classify_disposition(normal_row, group_meta, self.settings)
        self.assertEqual(disposition, "Verify")

        # Test singleton -> Keep
        normal_row = pd.Series({"Account Name": "Acme Corporation"})
        group_meta = {"group_size": 1, "has_suffix_mismatch": False}

        disposition = classify_disposition(normal_row, group_meta, self.settings)
        self.assertEqual(disposition, "Keep")

        # Test primary in group -> Keep
        primary_row = pd.Series({"Account Name": "Acme Corp", "is_primary": True})
        group_meta = {"group_size": 3, "has_suffix_mismatch": False}

        disposition = classify_disposition(primary_row, group_meta, self.settings)
        self.assertEqual(disposition, "Keep")

        # Test non-primary in group -> Update
        non_primary_row = pd.Series({"Account Name": "Acme Corp", "is_primary": False})
        group_meta = {"group_size": 3, "has_suffix_mismatch": False}

        disposition = classify_disposition(non_primary_row, group_meta, self.settings)
        self.assertEqual(disposition, "Update")

    def test_group_metadata_computation(self) -> None:
        """Test group metadata computation."""
        # Create test groups
        df_groups = self.df_norm.copy()
        df_groups["group_id"] = [0, 1, 2, 3, 4, 5, 6, 7]  # All singletons
        df_groups["is_primary"] = [True] * 8
        df_groups["weakest_edge_to_primary"] = [0.0] * 8

        # Create a group with suffix mismatch
        df_groups.loc[0, "group_id"] = 0
        df_groups.loc[1, "group_id"] = 0  # Same group, different suffix
        df_groups.loc[1, "is_primary"] = False

        metadata = compute_group_metadata(df_groups)

        # Check group 0 (has suffix mismatch)
        self.assertTrue(metadata[0]["has_suffix_mismatch"])
        self.assertEqual(metadata[0]["group_size"], 2)

        # Check other groups (singletons)
        for group_id in [2, 3, 4, 5, 6, 7]:
            self.assertFalse(metadata[group_id]["has_suffix_mismatch"])
            self.assertEqual(metadata[group_id]["group_size"], 1)

    def test_apply_dispositions(self) -> None:
        """Test applying dispositions to a DataFrame."""
        # Create test groups
        df_groups = self.df_norm.copy()
        df_groups["group_id"] = [0, 1, 2, 3, 4, 5, 6, 7]  # All singletons
        df_groups["is_primary"] = [True] * 8
        df_groups["weakest_edge_to_primary"] = [0.0] * 8

        # Apply dispositions
        df_dispositions = apply_dispositions(df_groups, self.settings)

        # Check that dispositions were assigned
        self.assertIn("disposition", df_dispositions.columns)

        # Check specific dispositions
        # Blacklisted names should be Delete
        self.assertEqual(
            df_dispositions.iloc[2]["disposition"],
            "Delete",
        )  # 'PNC is not sure'
        self.assertEqual(
            df_dispositions.iloc[3]["disposition"],
            "Delete",
        )  # '1099, no paystubs'
        self.assertEqual(df_dispositions.iloc[7]["disposition"], "Delete")  # 'N/A'

    def test_multiple_names_verification(self) -> None:
        """Test that records with multiple names are marked as Verify."""
        # Create test data with multiple names
        df_groups = self.df_norm.copy()
        df_groups["group_id"] = [0, 1, 2, 3, 4, 5, 6, 7]
        df_groups["is_primary"] = [True] * 8
        df_groups["weakest_edge_to_primary"] = [0.0] * 8

        # Add multiple names flag
        df_groups.loc[0, "has_multiple_names"] = True
        df_groups.loc[1, "has_multiple_names"] = True

        df_dispositions = apply_dispositions(df_groups, self.settings)

        # Check that multiple names are marked as Verify
        self.assertEqual(df_dispositions.iloc[0]["disposition"], "Verify")
        self.assertEqual(df_dispositions.iloc[1]["disposition"], "Verify")
        self.assertEqual(
            df_dispositions.iloc[0]["disposition_reason"],
            "multi_name_string_requires_split",
        )

    def test_manual_override_application(self) -> None:
        """Test that manual overrides are applied correctly."""
        import json
        from pathlib import Path

        # Create test data
        test_data = pd.DataFrame(
            {
                "group_id": [1, 1],
                "Account Name": ["Acme Corp Inc", "Acme Corp Inc"],
                "is_primary": [True, False],
                "weakest_edge_to_primary": [100, 95],
                "suffix_class": ["INC", "INC"],
                "has_multiple_names": [False, False],
            },
        )

        # Create manual override file
        manual_dir = Path("data/manual")
        manual_dir.mkdir(parents=True, exist_ok=True)

        override_data = [
            {
                "record_id": "0",  # First record
                "account_id": "001Hs000054S8kI",
                "account_name": "Acme Corp Inc",
                "name_core": "acme corp",
                "override": "Delete",
                "reason": "Test override",
                "ts": "2024-01-01T00:00:00",
            },
        ]

        with open(manual_dir / "manual_dispositions.json", "w") as f:
            json.dump(override_data, f)

        try:
            # Apply dispositions
            result = apply_dispositions(test_data, self.settings)

            # Check that manual override was applied
            self.assertEqual(result.iloc[0]["disposition"], "Delete")
            self.assertEqual(
                result.iloc[0]["disposition_reason"],
                "manual_override:Delete",
            )

            # Check that other records were processed normally
            self.assertEqual(result.iloc[1]["disposition"], "Update")

        finally:
            # Clean up
            if (manual_dir / "manual_dispositions.json").exists():
                (manual_dir / "manual_dispositions.json").unlink()

    def test_manual_blacklist_application(self) -> None:
        """Test that manual blacklist terms are applied correctly."""
        import json
        from pathlib import Path

        # Create manual blacklist file
        manual_dir = Path("data/manual")
        manual_dir.mkdir(parents=True, exist_ok=True)

        blacklist_data = {
            "terms": ["test_blacklist_term"],
            "last_updated": "2024-01-01T00:00:00",
        }

        with open(manual_dir / "manual_blacklist.json", "w") as f:
            json.dump(blacklist_data, f)

        try:
            # Test that the term is blacklisted
            self.assertTrue(_is_blacklisted("Company with test_blacklist_term"))
            self.assertFalse(_is_blacklisted("Normal Company Name"))

        finally:
            # Clean up
            if (manual_dir / "manual_blacklist.json").exists():
                (manual_dir / "manual_blacklist.json").unlink()

    def test_blacklist_word_boundaries(self) -> None:
        """Test that word-boundary matching works correctly."""
        from src.disposition import _is_blacklisted_improved

        # Test word boundaries - should NOT match
        self.assertFalse(_is_blacklisted_improved("Tempest Company"))

        # Test word boundaries - SHOULD match (these contain blacklisted words as standalone terms)
        self.assertTrue(
            _is_blacklisted_improved("Temporary Solutions Inc"),
        )  # contains "temporary"
        self.assertTrue(
            _is_blacklisted_improved("Unknown Industries"),
        )  # contains "unknown"
        self.assertTrue(_is_blacklisted_improved("temp staffing"))
        self.assertTrue(_is_blacklisted_improved("temporary agency"))
        self.assertTrue(_is_blacklisted_improved("unknown company"))
        self.assertTrue(_is_blacklisted_improved("N/A"))
        self.assertTrue(_is_blacklisted_improved("TBD"))

        # Test multi-word phrases
        self.assertTrue(_is_blacklisted_improved("pnc is not sure"))
        self.assertTrue(_is_blacklisted_improved("no paystub"))
        self.assertTrue(_is_blacklisted_improved("1099"))

    def test_blacklist_caching(self) -> None:
        """Test that manual blacklist terms are cached properly."""
        from src.disposition import _is_blacklisted_improved

        # Test with manual terms
        manual_terms = {"test_term", "another_term"}

        # Should match manual terms
        self.assertTrue(
            _is_blacklisted_improved("Company with test_term", manual_terms),
        )
        self.assertTrue(_is_blacklisted_improved("another_term company", manual_terms))

        # Should not match other terms
        self.assertFalse(_is_blacklisted_improved("Normal Company", manual_terms))

    def test_disposition_reasons(self) -> None:
        """Test that disposition reasons are correctly assigned."""
        df_groups = self.df_norm.copy()
        df_groups["group_id"] = [0, 1, 2, 3, 4, 5, 6, 7]
        df_groups["is_primary"] = [True] * 8
        df_groups["weakest_edge_to_primary"] = [0.0] * 8

        df_dispositions = apply_dispositions(df_groups, self.settings)

        # Check that reasons are assigned
        self.assertIn("disposition_reason", df_dispositions.columns)

        # Check specific reasons
        self.assertEqual(
            df_dispositions.iloc[2]["disposition_reason"],
            "blacklisted_name",
        )  # 'PNC is not sure'
        self.assertEqual(
            df_dispositions.iloc[0]["disposition_reason"],
            "clean_singleton",
        )  # Normal record

        # Normal names should be Keep (singletons)
        self.assertEqual(
            df_dispositions.iloc[0]["disposition"],
            "Keep",
        )  # '20-20 Plumbing & Heating Inc'
        self.assertEqual(
            df_dispositions.iloc[4]["disposition"],
            "Keep",
        )  # 'Acme Corporation'

    def test_suffix_mismatch_verification(self) -> None:
        """Test that suffix mismatches are marked as Verify."""
        # Create a group with suffix mismatch
        df_groups = self.df_norm.copy()
        df_groups["group_id"] = [0, 0, 2, 3, 4, 5, 6, 7]  # First two in same group
        df_groups["is_primary"] = [True, False] + [True] * 6
        df_groups["weakest_edge_to_primary"] = [0.0, 95.0] + [0.0] * 6

        # Apply dispositions
        df_dispositions = apply_dispositions(df_groups, self.settings)

        # Both records in the suffix mismatch group should be Verify
        self.assertEqual(df_dispositions.iloc[0]["disposition"], "Verify")
        self.assertEqual(df_dispositions.iloc[1]["disposition"], "Verify")

    def test_strong_match_same_suffix(self) -> None:
        """Test that strong matches with same suffix get proper dispositions."""
        # Create a group with same suffix
        df_groups = self.df_norm.copy()
        df_groups["group_id"] = [0, 0, 2, 3, 4, 5, 6, 7]  # First two in same group
        df_groups["is_primary"] = [True, False] + [True] * 6
        df_groups["weakest_edge_to_primary"] = [0.0, 95.0] + [0.0] * 6

        # Force same suffix for the group
        df_groups.loc[0, "suffix_class"] = "INC"
        df_groups.loc[1, "suffix_class"] = "INC"

        # Apply dispositions
        df_dispositions = apply_dispositions(df_groups, self.settings)

        # Primary should be Keep, non-primary should be Update
        self.assertEqual(df_dispositions.iloc[0]["disposition"], "Keep")
        self.assertEqual(df_dispositions.iloc[1]["disposition"], "Update")


if __name__ == "__main__":
    unittest.main()
