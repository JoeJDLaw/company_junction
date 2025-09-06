"""Tests for output persistence in similarity scoring.

This module tests output persistence behavior:
- Output format consistency
- Output data types
- Output structure
"""

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


class TestScoringOutputPersistence:
    """Test output persistence for similarity scoring."""

    def test_output_format_consistency(self):
        """Test output format consistency."""
        # Create test data
        test_data = pd.DataFrame(
            {
                "account_id": [1, 2, 3, 4],
                "Account Name": ["acme store", "acme shop", "acme depot", "xyz corp"],
            }
        )

        df_norm = normalize_dataframe(test_data, "Account Name")
        candidate_pairs = [(0, 1), (0, 2), (0, 3)]
        settings = _get_settings()

        # Test bulk scoring
        bulk_results = score_pairs_bulk(df_norm, candidate_pairs, settings)

        # Test parallel scoring
        parallel_results = score_pairs_parallel(df_norm, candidate_pairs, settings)

        # Both should have consistent format
        assert isinstance(bulk_results, list), "Bulk results should be a list"
        assert isinstance(parallel_results, list), "Parallel results should be a list"

        # If we have results, check structure
        if bulk_results:
            result = bulk_results[0]
            expected_keys = [
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
            for key in expected_keys:
                assert key in result, f"Result should contain key '{key}'"

    def test_output_data_types(self):
        """Test output data types."""
        # Create test data
        test_data = pd.DataFrame(
            {
                "account_id": [1, 2, 3],
                "Account Name": ["acme store", "acme shop", "acme depot"],
            }
        )

        df_norm = normalize_dataframe(test_data, "Account Name")
        candidate_pairs = [(0, 1), (0, 2)]
        settings = _get_settings()

        # Test bulk scoring
        results = score_pairs_bulk(df_norm, candidate_pairs, settings)

        # Check data types
        if results:
            result = results[0]

            # Check numeric types
            assert isinstance(result["score"], (int, float)), "Score should be numeric"
            assert isinstance(
                result["ratio_name"], (int, float)
            ), "ratio_name should be numeric"
            assert isinstance(
                result["ratio_set"], (int, float)
            ), "ratio_set should be numeric"
            assert isinstance(
                result["jaccard"], (int, float)
            ), "jaccard should be numeric"
            assert isinstance(
                result["base_score"], (int, float)
            ), "base_score should be numeric"

            # Check boolean types
            assert isinstance(
                result["suffix_match"], bool
            ), "suffix_match should be boolean"
            assert isinstance(
                result["num_style_match"], bool
            ), "num_style_match should be boolean"
            assert isinstance(
                result["punctuation_mismatch"], bool
            ), "punctuation_mismatch should be boolean"

            # Check ID types
            import numpy as np

            assert isinstance(
                result["id_a"], (int, str, np.integer)
            ), "id_a should be int, string, or numpy integer"
            assert isinstance(
                result["id_b"], (int, str, np.integer)
            ), "id_b should be int, string, or numpy integer"

    def test_output_structure(self):
        """Test output structure."""
        # Create test data
        test_data = pd.DataFrame(
            {
                "account_id": [1, 2, 3, 4],
                "Account Name": ["acme store", "acme shop", "acme depot", "xyz corp"],
            }
        )

        df_norm = normalize_dataframe(test_data, "Account Name")
        candidate_pairs = [(0, 1), (0, 2), (0, 3)]
        settings = _get_settings()

        # Test bulk scoring
        results = score_pairs_bulk(df_norm, candidate_pairs, settings)

        # Check structure
        if results:
            result = results[0]

            # Should have all required fields
            required_fields = [
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

            for field in required_fields:
                assert field in result, f"Result should contain field '{field}'"
                assert result[field] is not None, f"Field '{field}' should not be None"

    def test_output_consistency_bulk_parallel(self):
        """Test output consistency between bulk and parallel."""
        # Create test data
        test_data = pd.DataFrame(
            {
                "account_id": [1, 2, 3, 4],
                "Account Name": ["acme store", "acme shop", "acme depot", "xyz corp"],
            }
        )

        df_norm = normalize_dataframe(test_data, "Account Name")
        candidate_pairs = [(0, 1), (0, 2), (0, 3)]
        settings = _get_settings()

        # Test both methods
        bulk_results = score_pairs_bulk(df_norm, candidate_pairs, settings)
        parallel_results = score_pairs_parallel(df_norm, candidate_pairs, settings)

        # Both should have same structure
        if bulk_results and parallel_results:
            bulk_result = bulk_results[0]
            parallel_result = parallel_results[0]

            # Should have same keys
            assert set(bulk_result.keys()) == set(
                parallel_result.keys()
            ), "Results should have same keys"

            # Should have same data types
            for key in bulk_result.keys():
                assert isinstance(
                    bulk_result[key], type(parallel_result[key])
                ), f"Field '{key}' should have same type"

    def test_output_empty_input(self):
        """Test output with empty input."""
        # Create test data
        test_data = pd.DataFrame(
            {"account_id": [1, 2], "Account Name": ["acme store", "acme shop"]}
        )

        df_norm = normalize_dataframe(test_data, "Account Name")
        candidate_pairs: list[tuple[int, int]] = []  # Empty list
        settings = _get_settings()

        # Test bulk scoring
        results = score_pairs_bulk(df_norm, candidate_pairs, settings)

        # Should return empty list
        assert isinstance(results, list), "Results should be a list"
        assert len(results) == 0, "Results should be empty for empty input"

    def test_output_determinism(self):
        """Test output determinism."""
        # Create test data
        test_data = pd.DataFrame(
            {
                "account_id": [1, 2, 3],
                "Account Name": ["acme store", "acme shop", "acme depot"],
            }
        )

        df_norm = normalize_dataframe(test_data, "Account Name")
        candidate_pairs = [(0, 1), (0, 2)]
        settings = _get_settings()

        # Run multiple times
        results1 = score_pairs_bulk(df_norm, candidate_pairs, settings)
        results2 = score_pairs_bulk(df_norm, candidate_pairs, settings)

        # Should be identical
        assert len(results1) == len(results2), "Results should have same length"

        if results1 and results2:
            for r1, r2 in zip(results1, results2):
                assert r1["score"] == r2["score"], "Scores should be identical"
                assert (
                    r1["ratio_name"] == r2["ratio_name"]
                ), "ratio_name should be identical"
                assert (
                    r1["ratio_set"] == r2["ratio_set"]
                ), "ratio_set should be identical"
                assert r1["jaccard"] == r2["jaccard"], "jaccard should be identical"
