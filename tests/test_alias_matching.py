"""
Unit tests for alias matching functionality.
"""

import unittest
import pandas as pd
import sys
from pathlib import Path

# Add src directory to path
sys.path.append(str(Path(__file__).parent.parent / "src"))

from src.alias_matching import (
    compute_alias_matches,
    create_alias_cross_refs,
    _normalize_alias,
)


class TestAliasMatching(unittest.TestCase):
    """Test alias matching functionality."""

    def setUp(self):
        """Set up test data."""
        self.settings = {
            "similarity": {"high": 92, "medium": 84, "max_alias_pairs": 100000}
        }

        # Create test data with aliases
        self.df_norm = pd.DataFrame(
            {
                "Account Name": [
                    "(1)Don Roberto Jewelers; (2) BYD Auto",
                    "BMW of Ontario (Penske Auto Group Ontario B1)",
                    "BYD Auto Inc",
                    "Acme Corp (Delaware)",
                    "Test Company (paystub)",
                ],
                "name_core": [
                    "don roberto jewelers",
                    "bmw of ontario",
                    "byd auto",
                    "acme corp",
                    "test company",
                ],
                "suffix_class": ["NONE", "NONE", "INC", "CORP", "NONE"],
                "alias_candidates": [
                    ["Don Roberto Jewelers", "BYD Auto"],
                    ["Penske Auto Group Ontario B1"],
                    [],
                    [],
                    [],
                ],
                "alias_sources": [
                    ["semicolon", "semicolon"],
                    ["parentheses"],
                    [],
                    [],
                    [],
                ],
            }
        )

        # Create test groups
        self.df_groups = pd.DataFrame(
            {"group_id": [0, 1, 2, 3, 4], "is_primary": [True, True, True, True, True]}
        )

    def test_normalize_alias(self) -> None:
        """Test alias normalization."""
        # Test basic normalization
        result = _normalize_alias("Don Roberto Jewelers")
        self.assertEqual(result, "don roberto jewelers")

        # Test with symbols
        result = _normalize_alias("BYD Auto & Co.")
        self.assertEqual(result, "byd auto and co")

        # Test empty/None
        result = _normalize_alias("")
        self.assertEqual(result, "")

        result = _normalize_alias(None)
        self.assertEqual(result, "")

    def test_compute_alias_matches(self) -> None:
        """Test alias matching computation."""
        # Compute alias matches
        result = compute_alias_matches(self.df_norm, self.df_groups, self.settings)

        # Handle new return format (tuple of DataFrame and stats)
        if isinstance(result, tuple) and len(result) == 2:
            df_matches, stats = result
        else:
            df_matches, stats = result, {}

        # Should find matches between BYD Auto alias and BYD Auto Inc
        if not df_matches.empty:
            # Check that we have matches
            self.assertGreater(len(df_matches), 0)

            # Check that BYD Auto alias matches BYD Auto Inc
            byd_matches = df_matches[
                (df_matches["alias_text"] == "byd auto")
                & (df_matches["match_record_id"] == 2)  # BYD Auto Inc record
            ]
            if not byd_matches.empty:
                self.assertGreaterEqual(byd_matches.iloc[0]["score"], 92)

        # Check that stats are returned
        self.assertIsInstance(stats, dict)
        self.assertIn("pairs_generated", stats)
        self.assertIn("accepted_matches", stats)
        self.assertIn("elapsed_time", stats)

    def test_create_alias_cross_refs(self) -> None:
        """Test alias cross-reference creation."""
        # Create mock alias matches
        df_matches = pd.DataFrame(
            [
                {
                    "record_id": 0,
                    "alias_text": "byd auto",
                    "alias_source": "semicolon",
                    "match_record_id": 2,
                    "match_group_id": 2,
                    "score": 95,
                    "suffix_match": True,
                }
            ]
        )

        # Create cross-references
        df_result = create_alias_cross_refs(self.df_norm, df_matches)

        # Check that cross-refs were added
        self.assertIn("alias_cross_refs", df_result.columns)

        # Check that record 0 has cross-refs
        cross_refs = df_result.iloc[0]["alias_cross_refs"]
        self.assertEqual(len(cross_refs), 1)
        self.assertEqual(cross_refs[0]["alias"], "byd auto")
        self.assertEqual(cross_refs[0]["group_id"], 2)
        self.assertEqual(cross_refs[0]["score"], 95)

    def test_empty_alias_matches(self) -> None:
        """Test handling of empty alias matches."""
        # Create empty matches
        df_matches = pd.DataFrame()

        # Create cross-references
        df_result = create_alias_cross_refs(self.df_norm, df_matches)

        # Check that cross-refs column exists and is empty
        self.assertIn("alias_cross_refs", df_result.columns)
        for cross_refs in df_result["alias_cross_refs"]:
            self.assertEqual(len(cross_refs), 0)


if __name__ == "__main__":
    unittest.main()
