"""Tests for refactored similarity module."""

import sys
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.similarity import get_stop_tokens, pair_scores


class TestSimilarityRefactor:
    """Test cases for refactored similarity module."""

    def test_pair_scores_no_mutation(self):
        """Test that pair_scores doesn't mutate the input DataFrame."""
        # Create test data
        df_norm = pd.DataFrame(
            {
                "account_id": ["A1", "A2", "A3"],
                "name_core": [
                    "test company inc",
                    "test corp llc",
                    "sample business ltd",
                ],
                "suffix_class": ["INC", "LLC", "LTD"],
            },
        )

        # Store original columns
        original_columns = set(df_norm.columns)

        # Mock settings
        settings = {
            "similarity": {
                "medium": 50,  # Low threshold for testing
                "blocking": {
                    "allowlist_tokens": [],
                    "denylist_tokens": [],
                    "stop_tokens": ["inc", "llc", "ltd"],
                },
                "scoring": {
                    "use_bulk_cdist": False,  # Use parallel scoring for small dataset
                },
                "penalty": {"suffix_mismatch": 25, "num_style_mismatch": 5},
            },
        }

        # Call pair_scores
        result = pair_scores(df_norm, settings)

        # Check that input DataFrame wasn't mutated
        assert set(df_norm.columns) == original_columns
        assert "block_key" not in df_norm.columns
        assert "bigram_key" not in df_norm.columns

        # Check that result is a DataFrame
        assert isinstance(result, pd.DataFrame)

    def test_pair_scores_sort_order(self):
        """Test that pair_scores returns results in correct sort order."""
        # Create test data with known pairs
        df_norm = pd.DataFrame(
            {
                "account_id": ["A1", "A2", "A3", "A4"],
                "name_core": [
                    "test company",
                    "test corp",
                    "sample business",
                    "sample inc",
                ],
                "suffix_class": ["INC", "LLC", "LTD", "INC"],
            },
        )

        settings = {
            "similarity": {
                "medium": 50,  # Low threshold for testing
                "blocking": {
                    "allowlist_tokens": [],
                    "denylist_tokens": [],
                    "stop_tokens": ["inc", "llc", "ltd"],
                },
                "scoring": {"use_bulk_cdist": False},
                "penalty": {"suffix_mismatch": 25, "num_style_mismatch": 5},
            },
        }

        result = pair_scores(df_norm, settings)

        if len(result) > 1:
            # Check sort order: id_a, id_b ascending, score descending
            for i in range(len(result) - 1):
                row1 = result.iloc[i]
                row2 = result.iloc[i + 1]

                # id_a should be ascending
                if row1["id_a"] != row2["id_a"]:
                    assert row1["id_a"] <= row2["id_a"]
                # If id_a is same, id_b should be ascending
                elif row1["id_b"] != row2["id_b"]:
                    assert row1["id_b"] <= row2["id_b"]
                else:
                    # If both ids are same, score should be descending
                    assert row1["score"] >= row2["score"]

    def test_get_stop_tokens_from_config(self):
        """Test that stop tokens are read from configuration."""
        settings = {
            "similarity": {"blocking": {"stop_tokens": ["inc", "llc", "ltd", "corp"]}},
        }

        stop_tokens = get_stop_tokens(settings)
        assert stop_tokens == {"inc", "llc", "ltd", "corp"}

    def test_get_stop_tokens_default(self):
        """Test that default stop tokens are used when not in config."""
        settings: dict[str, Any] = {"similarity": {"blocking": {}}}

        stop_tokens = get_stop_tokens(settings)
        assert stop_tokens == {"inc", "llc", "ltd"}

    def test_suffix_class_default_handling(self):
        """Test that missing suffix_class column is handled gracefully."""
        # Create test data without suffix_class
        df_norm = pd.DataFrame(
            {"account_id": ["A1", "A2"], "name_core": ["test company", "test corp"]},
        )

        settings = {
            "similarity": {
                "medium": 50,
                "blocking": {
                    "allowlist_tokens": [],
                    "denylist_tokens": [],
                    "stop_tokens": ["inc", "llc", "ltd"],
                },
                "scoring": {"use_bulk_cdist": False},
                "penalty": {"suffix_mismatch": 25, "num_style_mismatch": 5},
            },
        }

        # Should not raise an error
        result = pair_scores(df_norm, settings)
        assert isinstance(result, pd.DataFrame)

    def test_empty_dataframe_handling(self):
        """Test that empty DataFrame is handled gracefully."""
        df_norm = pd.DataFrame(columns=["account_id", "name_core"])

        settings = {
            "similarity": {
                "medium": 50,
                "blocking": {
                    "allowlist_tokens": [],
                    "denylist_tokens": [],
                    "stop_tokens": ["inc", "llc", "ltd"],
                },
                "scoring": {"use_bulk_cdist": False},
                "penalty": {"suffix_mismatch": 25, "num_style_mismatch": 5},
            },
        }

        result = pair_scores(df_norm, settings)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0


if __name__ == "__main__":
    pytest.main([__file__])
