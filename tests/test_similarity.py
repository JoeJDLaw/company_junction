"""
Tests for similarity scoring functionality.
"""

import unittest
import pandas as pd
import sys
from pathlib import Path

# Add src directory to path for imports
sys.path.append(str(Path(__file__).parent.parent / "src"))

from similarity import (
    pair_scores,
    _compute_pair_score,
    _check_numeric_style_match,
    save_candidate_pairs,
    load_candidate_pairs,
)
from normalize import normalize_dataframe


class TestSimilarity(unittest.TestCase):
    """Test cases for similarity scoring."""

    def setUp(self):
        """Set up test data."""
        self.test_data = pd.DataFrame(
            {
                "Account Name": [
                    "20-20 Plumbing & Heating Inc",
                    "20/20 Plumbing & Heating LLC",
                    "20 20 Plumbing & Heating Inc",
                    "Acme Corporation",
                    "Acme Corp",
                    "Tech Solutions Inc",
                    "Tech Solutions LLC",
                ],
                "Account ID": ["001", "002", "003", "004", "005", "006", "007"],
                "Created Date": [
                    "2021-01-01",
                    "2021-01-02",
                    "2021-01-03",
                    "2021-01-04",
                    "2021-01-05",
                    "2021-01-06",
                    "2021-01-07",
                ],
            }
        )

        # Normalize the test data
        self.df_norm = normalize_dataframe(self.test_data, "Account Name")

        # Test settings
        self.settings = {
            "similarity": {
                "high": 92,
                "medium": 84,
                "penalty": {"suffix_mismatch": 25, "num_style_mismatch": 5},
            }
        }

    def test_numeric_style_matching(self):
        """Test numeric style matching."""
        # Same numeric style
        self.assertTrue(_check_numeric_style_match("20 20 plumbing", "20 20 heating"))

        # Different numeric styles
        self.assertFalse(_check_numeric_style_match("20 20 plumbing", "30 30 heating"))

        # One has numeric, other doesn't
        self.assertFalse(_check_numeric_style_match("20 20 plumbing", "acme corp"))

        # Neither has numeric
        self.assertTrue(_check_numeric_style_match("acme corp", "tech solutions"))

    def test_pair_score_computation(self):
        """Test pair score computation."""
        # Test INC vs INC (should be high score)
        row_a = self.df_norm.iloc[0]  # 20-20 Plumbing & Heating Inc
        row_b = self.df_norm.iloc[2]  # 20 20 Plumbing & Heating Inc

        score_data = _compute_pair_score(
            row_a, row_b, self.settings["similarity"]["penalty"]
        )

        self.assertGreater(score_data["score"], 90)  # Should be high
        self.assertTrue(score_data["suffix_match"])  # Both INC
        self.assertTrue(score_data["num_style_match"])  # Same numeric style

    def test_suffix_mismatch_penalty(self):
        """Test that suffix mismatches are penalized."""
        # Test INC vs LLC (should be lower score due to suffix penalty)
        row_a = self.df_norm.iloc[0]  # 20-20 Plumbing & Heating Inc
        row_b = self.df_norm.iloc[1]  # 20/20 Plumbing & Heating LLC

        score_data = _compute_pair_score(
            row_a, row_b, self.settings["similarity"]["penalty"]
        )

        self.assertFalse(score_data["suffix_match"])  # Different suffixes
        self.assertLess(score_data["score"], 90)  # Should be lower due to penalty

    def test_candidate_pair_generation(self):
        """Test candidate pair generation."""
        pairs_df = pair_scores(self.df_norm, self.settings)

        # Should generate some pairs
        self.assertGreater(len(pairs_df), 0)

        # Check that pairs are above medium threshold
        if not pairs_df.empty:
            self.assertTrue(
                all(pairs_df["score"] >= self.settings["similarity"]["medium"])
            )

    def test_inc_vs_inc_high_score(self):
        """Test that INC vs INC examples score >= high threshold."""
        # Create test data with INC variants
        inc_data = pd.DataFrame(
            {
                "Account Name": [
                    "20-20 Plumbing & Heating Inc",
                    "20/20 Plumbing & Heating Inc",
                    "20 20 Plumbing & Heating Inc",
                ],
                "Account ID": ["001", "002", "003"],
            }
        )

        df_norm = normalize_dataframe(inc_data, "Account Name")
        pairs_df = pair_scores(df_norm, self.settings)

        # Should have pairs
        self.assertGreater(len(pairs_df), 0)

        # All pairs should be above high threshold (same suffix, high similarity)
        if not pairs_df.empty:
            self.assertTrue(
                all(pairs_df["score"] >= self.settings["similarity"]["high"])
            )

    def test_inc_vs_llc_verification_needed(self):
        """Test that INC vs LLC stays below high or is flagged for verify."""
        # Create test data with INC vs LLC
        mixed_data = pd.DataFrame(
            {
                "Account Name": [
                    "20-20 Plumbing & Heating Inc",
                    "20-20 Plumbing & Heating LLC",
                ],
                "Account ID": ["001", "002"],
            }
        )

        df_norm = normalize_dataframe(mixed_data, "Account Name")
        pairs_df = pair_scores(df_norm, self.settings)

        if not pairs_df.empty:
            # Either score should be below high threshold OR suffix_match should be False
            for _, pair in pairs_df.iterrows():
                score_high = pair["score"] >= self.settings["similarity"]["high"]
                suffix_match = pair["suffix_match"]

                # If score is high, suffix should NOT match (forcing verify)
                # If suffix matches, score should be below high
                self.assertTrue(
                    (not score_high) or (not suffix_match),
                    f"High score ({pair['score']}) with suffix match ({suffix_match}) should not happen",
                )

    def test_save_load_candidate_pairs(self):
        """Test saving and loading candidate pairs."""
        pairs_df = pair_scores(self.df_norm, self.settings)

        if not pairs_df.empty:
            # Test save/load
            test_path = "test_candidate_pairs.parquet"

            try:
                save_candidate_pairs(pairs_df, test_path)
                loaded_df = load_candidate_pairs(test_path)

                # Should have same data
                self.assertEqual(len(pairs_df), len(loaded_df))
                self.assertTrue(all(pairs_df.columns == loaded_df.columns))

            finally:
                # Clean up
                import os

                if os.path.exists(test_path):
                    os.remove(test_path)

    def test_empty_dataframe(self):
        """Test handling of empty DataFrame."""
        empty_df = pd.DataFrame()
        pairs_df = pair_scores(empty_df, self.settings)

        self.assertTrue(pairs_df.empty)


if __name__ == "__main__":
    unittest.main()
