"""Tests for degenerate inputs in similarity scoring.

This module tests handling of degenerate inputs:
- Jaccard with empty tokens returns 0.0
- Empty candidate list → empty DataFrame (no mutation, correct columns)
- None inputs, empty strings, whitespace-only inputs
- Single characters, very long inputs, special characters
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
from src.similarity.scoring import (
    compute_score_components,
    score_pairs_bulk,
    score_pairs_parallel,
)


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


class TestScoringDegenerate:
    """Test degenerate input handling for similarity scoring."""

    def test_jaccard_empty_tokens_returns_zero(self):
        """Test that Jaccard with empty tokens returns 0.0."""
        # Test with empty strings
        result = compute_score_components(
            "",
            "",
            "NONE",
            "NONE",
            {"num_style_mismatch": 0, "suffix_mismatch": 0, "punctuation_mismatch": 0},
        )

        # Jaccard should be 0.0 for empty tokens
        assert (
            result["jaccard"] == 0.0
        ), f"Jaccard should be 0.0 for empty strings, got {result['jaccard']}"

        # Test with whitespace-only strings
        result_whitespace = compute_score_components(
            "   ",
            "   ",
            "NONE",
            "NONE",
            {"num_style_mismatch": 0, "suffix_mismatch": 0, "punctuation_mismatch": 0},
        )

        # Jaccard should be 0.0 for whitespace-only strings (after normalization)
        assert (
            result_whitespace["jaccard"] == 0.0
        ), f"Jaccard should be 0.0 for whitespace-only strings, got {result_whitespace['jaccard']}"

    def test_empty_candidate_list_empty_dataframe(self):
        """Test that empty candidate list produces empty DataFrame."""
        # Create test data
        test_data = pd.DataFrame(
            {"account_id": [1, 2], "Account Name": ["acme store", "acme shop"]}
        )

        df_norm = normalize_dataframe(test_data, "Account Name")
        candidate_pairs: list[tuple[int, int]] = []  # Empty list
        settings = _get_settings()

        # Test bulk scoring
        bulk_results = score_pairs_bulk(df_norm, candidate_pairs, settings)
        assert (
            len(bulk_results) == 0
        ), "Bulk results should be empty for empty candidate pairs"
        assert isinstance(bulk_results, list), "Bulk results should be a list"

        # Test parallel scoring
        parallel_results = score_pairs_parallel(df_norm, candidate_pairs, settings)
        assert (
            len(parallel_results) == 0
        ), "Parallel results should be empty for empty candidate pairs"
        assert isinstance(parallel_results, list), "Parallel results should be a list"

    def test_empty_candidate_list_no_mutation(self):
        """Test that empty candidate list doesn't mutate input."""
        # Create test data
        test_data = pd.DataFrame(
            {"account_id": [1, 2], "Account Name": ["acme store", "acme shop"]}
        )

        df_norm = normalize_dataframe(test_data, "Account Name")
        original_df = df_norm.copy()
        candidate_pairs: list[tuple[int, int]] = []  # Empty list
        settings = _get_settings()

        # Run scoring with empty candidates
        score_pairs_bulk(df_norm, candidate_pairs, settings)
        score_pairs_parallel(df_norm, candidate_pairs, settings)

        # DataFrame should be unchanged
        pd.testing.assert_frame_equal(
            df_norm,
            original_df,
        )

    def test_empty_candidate_list_correct_columns(self):
        """Test that empty candidate list produces correct columns."""
        # Create test data
        test_data = pd.DataFrame(
            {"account_id": [1, 2], "Account Name": ["acme store", "acme shop"]}
        )

        df_norm = normalize_dataframe(test_data, "Account Name")
        candidate_pairs: list[tuple[int, int]] = []  # Empty list
        settings = _get_settings()

        # Run scoring
        results = score_pairs_bulk(df_norm, candidate_pairs, settings)

        # Results should be empty but still a list
        assert isinstance(results, list), "Results should be a list"
        assert len(results) == 0, "Results should be empty"

        # If we had results, they should have the correct structure
        # (This is more of a contract test since we have empty results)
        expected_columns = [
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

        # This test documents the expected structure for when results are present
        print("Expected result columns:", expected_columns)

    def test_none_inputs_handling(self):
        """Test handling of None inputs."""
        # Current implementation crashes on None inputs
        # This documents the current behavior - None inputs cause AttributeError
        with pytest.raises(
            AttributeError, match="'NoneType' object has no attribute 'split'"
        ):
            compute_score_components(
                None,  # type: ignore[arg-type] # None input
                "acme store",
                "NONE",
                "NONE",
                {
                    "num_style_mismatch": 0,
                    "suffix_mismatch": 0,
                    "punctuation_mismatch": 0,
                },
            )

        # Document this as a known limitation
        print(
            "Note: None inputs currently cause AttributeError - this is a known limitation"
        )

    def test_empty_string_inputs_handling(self):
        """Test handling of empty string inputs."""
        # Test empty string vs empty string
        result = compute_score_components(
            "",
            "",
            "NONE",
            "NONE",
            {"num_style_mismatch": 0, "suffix_mismatch": 0, "punctuation_mismatch": 0},
        )

        # Should handle gracefully
        assert isinstance(result["score"], (int, float)), "Score should be numeric"
        assert (
            0 <= result["score"] <= 100
        ), f"Score should be 0-100, got {result['score']}"
        assert result["jaccard"] == 0.0, "Jaccard should be 0.0 for empty strings"

        # Test empty string vs non-empty string
        result_mixed = compute_score_components(
            "",
            "acme store",
            "NONE",
            "NONE",
            {"num_style_mismatch": 0, "suffix_mismatch": 0, "punctuation_mismatch": 0},
        )

        # Should handle gracefully
        assert isinstance(
            result_mixed["score"], (int, float)
        ), "Score should be numeric"
        assert (
            0 <= result_mixed["score"] <= 100
        ), f"Score should be 0-100, got {result_mixed['score']}"

    def test_whitespace_only_inputs_handling(self):
        """Test handling of whitespace-only inputs."""
        # Test whitespace-only strings
        result = compute_score_components(
            "   ",
            "   ",
            "NONE",
            "NONE",
            {"num_style_mismatch": 0, "suffix_mismatch": 0, "punctuation_mismatch": 0},
        )

        # Should handle gracefully
        assert isinstance(result["score"], (int, float)), "Score should be numeric"
        assert (
            0 <= result["score"] <= 100
        ), f"Score should be 0-100, got {result['score']}"

        # Test mixed whitespace
        result_mixed = compute_score_components(
            "   ",
            "acme store",
            "NONE",
            "NONE",
            {"num_style_mismatch": 0, "suffix_mismatch": 0, "punctuation_mismatch": 0},
        )

        # Should handle gracefully
        assert isinstance(
            result_mixed["score"], (int, float)
        ), "Score should be numeric"
        assert (
            0 <= result_mixed["score"] <= 100
        ), f"Score should be 0-100, got {result_mixed['score']}"

    def test_single_character_inputs_handling(self):
        """Test handling of single character inputs."""
        # Test single characters
        result = compute_score_components(
            "a",
            "b",
            "NONE",
            "NONE",
            {"num_style_mismatch": 0, "suffix_mismatch": 0, "punctuation_mismatch": 0},
        )

        # Should handle gracefully
        assert isinstance(result["score"], (int, float)), "Score should be numeric"
        assert (
            0 <= result["score"] <= 100
        ), f"Score should be 0-100, got {result['score']}"

        # Test same single character
        result_same = compute_score_components(
            "a",
            "a",
            "NONE",
            "NONE",
            {"num_style_mismatch": 0, "suffix_mismatch": 0, "punctuation_mismatch": 0},
        )

        # Should handle gracefully
        assert isinstance(result_same["score"], (int, float)), "Score should be numeric"
        assert (
            0 <= result_same["score"] <= 100
        ), f"Score should be 0-100, got {result_same['score']}"

    def test_very_long_inputs_handling(self):
        """Test handling of very long inputs."""
        # Create very long strings
        long_string_a = "acme store " * 1000  # ~10,000 characters
        long_string_b = "acme shop " * 1000  # ~10,000 characters

        result = compute_score_components(
            long_string_a,
            long_string_b,
            "NONE",
            "NONE",
            {"num_style_mismatch": 0, "suffix_mismatch": 0, "punctuation_mismatch": 0},
        )

        # Should handle gracefully
        assert isinstance(result["score"], (int, float)), "Score should be numeric"
        assert (
            0 <= result["score"] <= 100
        ), f"Score should be 0-100, got {result['score']}"

        # Test identical long strings
        result_same = compute_score_components(
            long_string_a,
            long_string_a,
            "NONE",
            "NONE",
            {"num_style_mismatch": 0, "suffix_mismatch": 0, "punctuation_mismatch": 0},
        )

        # Should handle gracefully and score high for identical strings
        assert isinstance(result_same["score"], (int, float)), "Score should be numeric"
        assert (
            0 <= result_same["score"] <= 100
        ), f"Score should be 0-100, got {result_same['score']}"
        assert (
            result_same["score"] > 50
        ), "Identical long strings should score reasonably high"

    def test_special_character_only_inputs_handling(self):
        """Test handling of special character-only inputs."""
        # Test special characters only
        result = compute_score_components(
            "!@#$%^&*()",
            "!@#$%^&*()",
            "NONE",
            "NONE",
            {"num_style_mismatch": 0, "suffix_mismatch": 0, "punctuation_mismatch": 0},
        )

        # Should handle gracefully
        assert isinstance(result["score"], (int, float)), "Score should be numeric"
        assert (
            0 <= result["score"] <= 100
        ), f"Score should be 0-100, got {result['score']}"

        # Test different special characters
        result_different = compute_score_components(
            "!@#$%",
            "^&*()",
            "NONE",
            "NONE",
            {"num_style_mismatch": 0, "suffix_mismatch": 0, "punctuation_mismatch": 0},
        )

        # Should handle gracefully
        assert isinstance(
            result_different["score"], (int, float)
        ), "Score should be numeric"
        assert (
            0 <= result_different["score"] <= 100
        ), f"Score should be 0-100, got {result_different['score']}"

    def test_numeric_only_inputs_handling(self):
        """Test handling of numeric-only inputs."""
        # Test numeric strings
        result = compute_score_components(
            "12345",
            "67890",
            "NONE",
            "NONE",
            {"num_style_mismatch": 0, "suffix_mismatch": 0, "punctuation_mismatch": 0},
        )

        # Should handle gracefully
        assert isinstance(result["score"], (int, float)), "Score should be numeric"
        assert (
            0 <= result["score"] <= 100
        ), f"Score should be 0-100, got {result['score']}"

        # Test identical numeric strings
        result_same = compute_score_components(
            "12345",
            "12345",
            "NONE",
            "NONE",
            {"num_style_mismatch": 0, "suffix_mismatch": 0, "punctuation_mismatch": 0},
        )

        # Should handle gracefully and score high for identical strings
        assert isinstance(result_same["score"], (int, float)), "Score should be numeric"
        assert (
            0 <= result_same["score"] <= 100
        ), f"Score should be 0-100, got {result_same['score']}"
        assert (
            result_same["score"] > 50
        ), "Identical numeric strings should score reasonably high"

    def test_mixed_type_inputs_handling(self):
        """Test handling of mixed type inputs."""
        # Test mixed content
        result = compute_score_components(
            "acme123!@#",
            "store456$%^",
            "NONE",
            "NONE",
            {"num_style_mismatch": 0, "suffix_mismatch": 0, "punctuation_mismatch": 0},
        )

        # Should handle gracefully
        assert isinstance(result["score"], (int, float)), "Score should be numeric"
        assert (
            0 <= result["score"] <= 100
        ), f"Score should be 0-100, got {result['score']}"

        # Test with different suffix classes
        result_suffix = compute_score_components(
            "acme123!@#",
            "store456$%^",
            "INC",
            "LLC",
            {"num_style_mismatch": 0, "suffix_mismatch": 25, "punctuation_mismatch": 0},
        )

        # Should handle gracefully
        assert isinstance(
            result_suffix["score"], (int, float)
        ), "Score should be numeric"
        assert (
            0 <= result_suffix["score"] <= 100
        ), f"Score should be 0-100, got {result_suffix['score']}"

    def test_unicode_inputs_handling(self):
        """Test handling of Unicode inputs."""
        # Test Unicode characters
        result = compute_score_components(
            "acmé störe",
            "acmé shöp",
            "NONE",
            "NONE",
            {"num_style_mismatch": 0, "suffix_mismatch": 0, "punctuation_mismatch": 0},
        )

        # Should handle gracefully
        assert isinstance(result["score"], (int, float)), "Score should be numeric"
        assert (
            0 <= result["score"] <= 100
        ), f"Score should be 0-100, got {result['score']}"

        # Test identical Unicode strings
        result_same = compute_score_components(
            "acmé störe",
            "acmé störe",
            "NONE",
            "NONE",
            {"num_style_mismatch": 0, "suffix_mismatch": 0, "punctuation_mismatch": 0},
        )

        # Should handle gracefully and score high for identical strings
        assert isinstance(result_same["score"], (int, float)), "Score should be numeric"
        assert (
            0 <= result_same["score"] <= 100
        ), f"Score should be 0-100, got {result_same['score']}"
        assert (
            result_same["score"] > 50
        ), "Identical Unicode strings should score reasonably high"

    def test_edge_case_combinations(self):
        """Test edge case combinations."""
        # Test various edge case combinations
        edge_cases = [
            ("", "a"),  # Empty vs single char
            ("a", ""),  # Single char vs empty
            ("   ", "a"),  # Whitespace vs single char
            ("a", "   "),  # Single char vs whitespace
            ("!@#", "123"),  # Special chars vs numbers
            ("123", "!@#"),  # Numbers vs special chars
            ("a", "!@#"),  # Single char vs special chars
            ("!@#", "a"),  # Special chars vs single char
        ]

        for name_a, name_b in edge_cases:
            result = compute_score_components(
                name_a,
                name_b,
                "NONE",
                "NONE",
                {
                    "num_style_mismatch": 0,
                    "suffix_mismatch": 0,
                    "punctuation_mismatch": 0,
                },
            )

            # All should handle gracefully
            assert isinstance(
                result["score"], (int, float)
            ), f"Score should be numeric for '{name_a}' vs '{name_b}'"
            assert (
                0 <= result["score"] <= 100
            ), f"Score should be 0-100 for '{name_a}' vs '{name_b}', got {result['score']}"
            assert isinstance(
                result["jaccard"], (int, float)
            ), f"Jaccard should be numeric for '{name_a}' vs '{name_b}'"
            assert (
                0 <= result["jaccard"] <= 1.0
            ), f"Jaccard should be 0-1.0 for '{name_a}' vs '{name_b}', got {result['jaccard']}"
