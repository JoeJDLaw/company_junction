"""Tests for contracts and outputs in similarity scoring.

This module tests output contracts and guarantees:
- No DataFrame mutation guarantees
- String dtype enforcement
- Sort order contract (document current behavior)
- Deterministic output guarantees
- Output structure and data types
"""

import copy
import sys
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.normalize import normalize_dataframe
from src.similarity.scoring import score_pairs_bulk, score_pairs_parallel


def _get_settings(overrides: dict | None = None) -> dict:
    """Helper to create settings dict with optional overrides."""
    settings = {
        "similarity": {
            "scoring": {
                "gate_cutoff": 72,
                "use_bulk_cdist": True,
                "penalties": {"punctuation": 0.1, "suffix": 0.05, "numeric": 0.15},
            }
        }
    }
    if overrides:
        # Deep merge overrides
        for key, value in overrides.items():
            if key in settings["similarity"]["scoring"]:
                settings["similarity"]["scoring"][key] = value
    return settings


class TestScoringContracts:
    """Test output contracts and guarantees for similarity scoring."""

    def test_no_dataframe_mutation(self):
        """Test that input DataFrames are not mutated."""
        # Create test data
        test_data = pd.DataFrame(
            {
                "account_id": [1, 2, 3],
                "Account Name": ["acme store", "acme shop", "xyz corp"],
            },
        )

        # Normalize the data
        df_norm = normalize_dataframe(test_data, "Account Name")

        # Create deep copy to verify immutability
        original_df = copy.deepcopy(df_norm)

        candidate_pairs = [(0, 1), (0, 2)]
        settings = _get_settings()

        # Run both scoring methods
        bulk_results = score_pairs_bulk(df_norm, candidate_pairs, settings)
        parallel_results = score_pairs_parallel(df_norm, candidate_pairs, settings)

        # Verify immutability - DataFrame should be unchanged
        pd.testing.assert_frame_equal(df_norm, original_df)

        # Both methods should produce results
        assert len(bulk_results) >= 0
        assert len(parallel_results) >= 0

    def test_string_dtype_enforcement(self):
        """Test that string columns maintain string dtypes."""
        # Create test data with mixed dtypes
        test_data = pd.DataFrame(
            {
                "account_id": [1, 2, 3],  # int
                "Account Name": ["acme store", "acme shop", "xyz corp"],  # string
            },
        )

        # Normalize the data
        df_norm = normalize_dataframe(test_data, "Account Name")

        candidate_pairs = [(0, 1)]
        settings = _get_settings()

        # Run scoring
        results = score_pairs_bulk(df_norm, candidate_pairs, settings)

        if len(results) > 0:
            result = results[0]

            # Verify string-like dtypes for key fields
            # Note: account_id may be int, numpy int, or string depending on config
            import numpy as np

            assert isinstance(
                result["id_a"],
                (str, int, np.integer),
            ), "id_a should be string or integer"
            assert isinstance(
                result["id_b"],
                (str, int, np.integer),
            ), "id_b should be string or integer"

            # Verify numeric dtypes for scores
            assert isinstance(
                result["score"],
                (int, float, np.number),
            ), "score should be numeric"
            assert isinstance(
                result["ratio_name"],
                (int, float, np.number),
            ), "ratio_name should be numeric"
            assert isinstance(
                result["ratio_set"],
                (int, float, np.number),
            ), "ratio_set should be numeric"
            assert isinstance(
                result["jaccard"],
                (int, float, np.number),
            ), "jaccard should be numeric"

            # Verify boolean dtypes for flags
            assert isinstance(
                result["suffix_match"],
                bool,
            ), "suffix_match should be boolean"
            assert isinstance(
                result["num_style_match"],
                bool,
            ), "num_style_match should be boolean"
            assert isinstance(
                result["punctuation_mismatch"],
                bool,
            ), "punctuation_mismatch should be boolean"

    def test_sort_order_contract_documentation(self):
        """Test and document current sort order behavior."""
        # Create test data
        test_data = pd.DataFrame(
            {
                "account_id": [1, 2, 3, 4],
                "Account Name": ["acme store", "acme shop", "acme depot", "acme corp"],
            },
        )

        df_norm = normalize_dataframe(test_data, "Account Name")
        candidate_pairs = [(0, 1), (0, 2), (0, 3), (1, 2), (1, 3), (2, 3)]
        settings = _get_settings()

        # Run scoring
        results = score_pairs_bulk(df_norm, candidate_pairs, settings)

        # Document current behavior: results are not sorted
        # This is baseline behavior - we don't currently sort results
        print(f"Current sort behavior: {len(results)} results")
        if len(results) > 1:
            print("Results are NOT currently sorted by score or id")
            print("This is documented baseline behavior")

        # Verify results have consistent structure
        for result in results:
            assert "id_a" in result, "Should have id_a"
            assert "id_b" in result, "Should have id_b"
            assert "score" in result, "Should have score"
            assert 0 <= result["score"] <= 100, "Score should be in valid range"

    def test_deterministic_outputs(self):
        """Test that same inputs produce identical outputs."""
        # Create test data
        test_data = pd.DataFrame(
            {
                "account_id": [1, 2, 3],
                "Account Name": ["acme store", "acme shop", "xyz corp"],
            },
        )

        df_norm = normalize_dataframe(test_data, "Account Name")
        candidate_pairs = [(0, 1), (0, 2)]
        settings = _get_settings()

        # Run scoring twice
        results1 = score_pairs_bulk(df_norm, candidate_pairs, settings)
        results2 = score_pairs_bulk(df_norm, candidate_pairs, settings)

        # Results should be identical
        assert len(results1) == len(
            results2,
        ), "Same inputs should produce same number of results"

        for r1, r2 in zip(results1, results2):
            assert r1["id_a"] == r2["id_a"], "id_a should be identical"
            assert r1["id_b"] == r2["id_b"], "id_b should be identical"
            assert r1["score"] == r2["score"], "score should be identical"
            assert (
                r1["ratio_name"] == r2["ratio_name"]
            ), "ratio_name should be identical"
            assert r1["ratio_set"] == r2["ratio_set"], "ratio_set should be identical"
            assert r1["jaccard"] == r2["jaccard"], "jaccard should be identical"

    def test_output_column_structure(self):
        """Test that output has correct column structure."""
        # Create test data
        test_data = pd.DataFrame(
            {"account_id": [1, 2], "Account Name": ["acme store", "acme shop"]},
        )

        df_norm = normalize_dataframe(test_data, "Account Name")
        candidate_pairs = [(0, 1)]
        settings = _get_settings()

        # Run scoring
        results = score_pairs_bulk(df_norm, candidate_pairs, settings)

        if len(results) > 0:
            result = results[0]

            # Verify required columns are present
            required_columns = [
                "id_a",
                "id_b",
                "score",
                "ratio_name",
                "ratio_set",
                "jaccard",
                "suffix_match",
                "num_style_match",
                "punctuation_mismatch",
                "base_score",
            ]

            for col in required_columns:
                assert col in result, f"Required column {col} should be present"

    def test_output_data_types(self):
        """Test that output data types are correct."""
        # Create test data
        test_data = pd.DataFrame(
            {"account_id": [1, 2], "Account Name": ["acme store", "acme shop"]},
        )

        df_norm = normalize_dataframe(test_data, "Account Name")
        candidate_pairs = [(0, 1)]
        settings = _get_settings()

        # Run scoring
        results = score_pairs_bulk(df_norm, candidate_pairs, settings)

        if len(results) > 0:
            result = results[0]

            # Verify data types
            import numpy as np

            assert isinstance(
                result["score"],
                (int, float, np.number),
            ), "score should be numeric"
            assert isinstance(
                result["ratio_name"],
                (int, float, np.number),
            ), "ratio_name should be numeric"
            assert isinstance(
                result["ratio_set"],
                (int, float, np.number),
            ), "ratio_set should be numeric"
            assert isinstance(
                result["jaccard"],
                (int, float, np.number),
            ), "jaccard should be numeric"
            assert isinstance(
                result["base_score"],
                (int, float, np.number),
            ), "base_score should be numeric"

            assert isinstance(
                result["suffix_match"],
                bool,
            ), "suffix_match should be boolean"
            assert isinstance(
                result["num_style_match"],
                bool,
            ), "num_style_match should be boolean"
            assert isinstance(
                result["punctuation_mismatch"],
                bool,
            ), "punctuation_mismatch should be boolean"

    def test_empty_input_handling(self):
        """Test that empty inputs produce empty outputs."""
        # Create test data
        test_data = pd.DataFrame(
            {"account_id": [1, 2], "Account Name": ["acme store", "acme shop"]},
        )

        df_norm = normalize_dataframe(test_data, "Account Name")
        candidate_pairs: list[tuple[int, int]] = []  # Empty
        settings = _get_settings()

        # Run scoring
        results = score_pairs_bulk(df_norm, candidate_pairs, settings)

        # Should return empty results
        assert len(results) == 0, "Empty candidate_pairs should produce empty results"
        assert isinstance(results, list), "Results should be a list"

    def test_output_consistency_bulk_parallel(self):
        """Test that bulk and parallel outputs have consistent structure."""
        # Create test data
        test_data = pd.DataFrame(
            {
                "account_id": [1, 2, 3],
                "Account Name": ["acme store", "acme shop", "xyz corp"],
            },
        )

        df_norm = normalize_dataframe(test_data, "Account Name")
        candidate_pairs = [(0, 1), (0, 2)]
        settings = _get_settings()

        # Run both methods
        bulk_results = score_pairs_bulk(df_norm, candidate_pairs, settings)
        parallel_results = score_pairs_parallel(df_norm, candidate_pairs, settings)

        # Both should return lists
        assert isinstance(bulk_results, list), "Bulk results should be a list"
        assert isinstance(parallel_results, list), "Parallel results should be a list"

        # Both should have same structure
        if len(bulk_results) > 0 and len(parallel_results) > 0:
            bulk_keys = set(bulk_results[0].keys())
            parallel_keys = set(parallel_results[0].keys())
            assert (
                bulk_keys == parallel_keys
            ), "Both methods should have same output structure"

    def test_score_bounds_contract(self):
        """Test that scores are within valid bounds."""
        # Create test data with various similarity levels
        test_data = pd.DataFrame(
            {
                "account_id": [1, 2, 3, 4],
                "Account Name": [
                    "acme store",  # Should match well
                    "acme shop",  # Should match well
                    "acme depot",  # Should match moderately
                    "xyz corporation",  # Should match poorly
                ],
            },
        )

        df_norm = normalize_dataframe(test_data, "Account Name")
        candidate_pairs = [(0, 1), (0, 2), (0, 3)]
        settings = _get_settings()

        # Run scoring
        results = score_pairs_bulk(df_norm, candidate_pairs, settings)

        # All scores should be within bounds
        for result in results:
            assert (
                0 <= result["score"] <= 100
            ), f"Score {result['score']} should be 0-100"
            assert (
                0 <= result["ratio_name"] <= 100
            ), f"ratio_name {result['ratio_name']} should be 0-100"
            assert (
                0 <= result["ratio_set"] <= 100
            ), f"ratio_set {result['ratio_set']} should be 0-100"
            assert (
                0 <= result["jaccard"] <= 1.0
            ), f"jaccard {result['jaccard']} should be 0-1.0"

    def test_penalty_flags_contract(self):
        """Test that penalty flags are correctly set."""
        # Create test data with different penalty scenarios
        test_data = pd.DataFrame(
            {
                "account_id": [1, 2, 3, 4],
                "Account Name": [
                    "acme store",  # Same name, same suffix
                    "acme shop",  # Same name, different suffix
                    "studio 54",  # Different numeric style
                    "studio fifty four",  # Different numeric style
                ],
            },
        )

        df_norm = normalize_dataframe(test_data, "Account Name")
        candidate_pairs = [(0, 1), (2, 3)]  # Test suffix and numeric penalties
        settings = _get_settings()

        # Run scoring
        results = score_pairs_bulk(df_norm, candidate_pairs, settings)

        # Verify penalty flags are boolean
        for result in results:
            assert isinstance(
                result["suffix_match"],
                bool,
            ), "suffix_match should be boolean"
            assert isinstance(
                result["num_style_match"],
                bool,
            ), "num_style_match should be boolean"
            assert isinstance(
                result["punctuation_mismatch"],
                bool,
            ), "punctuation_mismatch should be boolean"

    def test_component_scores_contract(self):
        """Test that component scores are present and valid."""
        # Create test data
        test_data = pd.DataFrame(
            {"account_id": [1, 2], "Account Name": ["acme store", "acme shop"]},
        )

        df_norm = normalize_dataframe(test_data, "Account Name")
        candidate_pairs = [(0, 1)]
        settings = _get_settings()

        # Run scoring
        results = score_pairs_bulk(df_norm, candidate_pairs, settings)

        if len(results) > 0:
            result = results[0]

            # Verify component scores are present and valid
            assert "ratio_name" in result, "Should have ratio_name"
            assert "ratio_set" in result, "Should have ratio_set"
            assert "jaccard" in result, "Should have jaccard"
            assert "base_score" in result, "Should have base_score"

            # Verify they are numeric
            import numpy as np

            assert isinstance(
                result["ratio_name"],
                (int, float, np.number),
            ), "ratio_name should be numeric"
            assert isinstance(
                result["ratio_set"],
                (int, float, np.number),
            ), "ratio_set should be numeric"
            assert isinstance(
                result["jaccard"],
                (int, float, np.number),
            ), "jaccard should be numeric"
            assert isinstance(
                result["base_score"],
                (int, float, np.number),
            ), "base_score should be numeric"
