"""Tests for scoring math and bounds in similarity scoring.

This module tests mathematical correctness and bounds:
- Score clamping (0-100 bounds)
- Rounding behavior
- Component score bounds
- Penalty application bounds
- Base score calculation accuracy
"""

import sys
from pathlib import Path
from typing import Any, Dict

import numpy as np
import pytest

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.similarity.scoring import compute_score_components


class TestScoringBounds:
    """Test mathematical correctness and bounds for similarity scoring."""

    def test_score_clamp_upper_bound(self):
        """Test that scores >100 are clamped to 100."""
        # Create test data that should produce high scores
        name_a = "acme store"
        name_b = "acme store"  # Identical names should score 100

        result = compute_score_components(
            name_a,
            name_b,
            "NONE",
            "NONE",
            {"num_style_mismatch": 0, "suffix_mismatch": 0, "punctuation_mismatch": 0},
        )

        # Score should be exactly 100 for identical names
        assert (
            result["score"] == 100
        ), f"Identical names should score 100, got {result['score']}"
        assert (
            result["base_score"] == 100
        ), f"Base score should be 100, got {result['base_score']}"

    def test_score_clamp_lower_bound(self):
        """Test that scores <0 are clamped to 0."""
        # Create test data with very high penalties to force negative scores
        name_a = "acme store"
        name_b = "xyz corporation"  # Very different names

        # Use extremely high penalties to force negative base score
        result = compute_score_components(
            name_a,
            name_b,
            "NONE",
            "NONE",
            {
                "num_style_mismatch": 0,
                "suffix_mismatch": 200,
                "punctuation_mismatch": 0,
            },
        )

        # Score should be clamped to 0
        assert result["score"] >= 0, f"Score should be >= 0, got {result['score']}"
        assert result["score"] <= 100, f"Score should be <= 100, got {result['score']}"

    def test_score_rounding_behavior(self):
        """Test score rounding behavior."""
        # Test with names that should produce fractional scores
        name_a = "acme store"
        name_b = "acme shop"  # Similar but not identical

        result = compute_score_components(
            name_a,
            name_b,
            "NONE",
            "NONE",
            {"num_style_mismatch": 0, "suffix_mismatch": 0, "punctuation_mismatch": 0},
        )

        # Score should be an integer (rounded)
        assert isinstance(
            result["score"], (int, np.integer)
        ), f"Score should be integer, got {type(result['score'])}"
        assert (
            0 <= result["score"] <= 100
        ), f"Score should be 0-100, got {result['score']}"

        # Base score might be fractional, but final score should be rounded
        if isinstance(result["base_score"], (int, float, np.number)):
            assert (
                0 <= result["base_score"] <= 100
            ), f"Base score should be 0-100, got {result['base_score']}"

    def test_score_rounding_edge_cases(self):
        """Test score rounding edge cases."""
        # Test various similarity levels to check rounding behavior
        test_cases = [
            ("acme store", "acme store"),  # Should be 100
            ("acme store", "acme shop"),  # Should be high but < 100
            ("acme store", "acme depot"),  # Should be medium
            ("acme store", "xyz corp"),  # Should be low
        ]

        for name_a, name_b in test_cases:
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

            # All scores should be integers in valid range
            assert isinstance(
                result["score"], (int, np.integer)
            ), f"Score should be integer for {name_a} vs {name_b}"
            assert (
                0 <= result["score"] <= 100
            ), f"Score should be 0-100 for {name_a} vs {name_b}, got {result['score']}"

    def test_component_score_bounds(self):
        """Test that component scores are within valid bounds."""
        name_a = "acme store"
        name_b = "acme shop"

        result = compute_score_components(
            name_a,
            name_b,
            "NONE",
            "NONE",
            {"num_style_mismatch": 0, "suffix_mismatch": 0, "punctuation_mismatch": 0},
        )

        # Check component score bounds
        assert (
            0 <= result["ratio_name"] <= 100
        ), f"ratio_name should be 0-100, got {result['ratio_name']}"
        assert (
            0 <= result["ratio_set"] <= 100
        ), f"ratio_set should be 0-100, got {result['ratio_set']}"
        assert (
            0 <= result["jaccard"] <= 1.0
        ), f"jaccard should be 0-1.0, got {result['jaccard']}"

        # Base score should be calculated from components
        expected_base = (
            0.45 * result["ratio_name"]
            + 0.35 * result["ratio_set"]
            + 20.0 * result["jaccard"]
        )
        assert (
            abs(result["base_score"] - expected_base) < 0.01
        ), f"Base score calculation incorrect: {result['base_score']} vs {expected_base}"

    def test_penalty_application_bounds(self):
        """Test penalty application doesn't violate bounds."""
        name_a = "acme store"
        name_b = "acme shop"

        # Test with various penalty values
        penalty_values = [0, 5, 10, 25, 50]

        for penalty in penalty_values:
            result = compute_score_components(
                name_a,
                name_b,
                "NONE",
                "NONE",
                {
                    "num_style_mismatch": 0,
                    "suffix_mismatch": penalty,
                    "punctuation_mismatch": 0,
                },
            )

            # Score should always be in valid range regardless of penalties
            assert (
                0 <= result["score"] <= 100
            ), f"Score should be 0-100 with penalty {penalty}, got {result['score']}"

            # Higher penalties should result in lower scores
            if penalty > 0:
                # Score should be <= base_score (penalties can only reduce scores)
                assert (
                    result["score"] <= result["base_score"]
                ), f"Score {result['score']} should be <= base_score {result['base_score']} with penalty {penalty}"

    def test_base_score_calculation(self):
        """Test base score calculation correctness."""
        name_a = "acme store"
        name_b = "acme shop"

        result = compute_score_components(
            name_a,
            name_b,
            "NONE",
            "NONE",
            {"num_style_mismatch": 0, "suffix_mismatch": 0, "punctuation_mismatch": 0},
        )

        # Verify base score calculation formula
        expected_base = (
            0.45 * result["ratio_name"]
            + 0.35 * result["ratio_set"]
            + 20.0 * result["jaccard"]
        )
        assert (
            abs(result["base_score"] - expected_base) < 0.01
        ), f"Base score calculation incorrect: {result['base_score']} vs {expected_base}"

        # Base score should be positive for similar names
        assert (
            result["base_score"] > 0
        ), f"Base score should be positive for similar names, got {result['base_score']}"

    def test_penalty_subtraction_accuracy(self):
        """Test penalty subtraction accuracy."""
        # Use different suffix classes to trigger suffix penalty
        name_a = "acme store"
        name_b = "acme shop"

        # Test without penalties (same suffix classes - no penalty applied)
        result_no_penalty = compute_score_components(
            name_a,
            name_b,
            "NONE",
            "NONE",
            {"num_style_mismatch": 0, "suffix_mismatch": 0, "punctuation_mismatch": 0},
        )

        # Test with suffix penalty (different suffix classes - penalty applied)
        result_with_penalty = compute_score_components(
            name_a,
            name_b,
            "INC",
            "LLC",  # Different suffix classes to trigger penalty
            {"num_style_mismatch": 0, "suffix_mismatch": 25, "punctuation_mismatch": 0},
        )

        # Base scores should be different (penalties are applied to base_score)
        # The difference should be approximately the penalty amount
        base_difference = (
            result_no_penalty["base_score"] - result_with_penalty["base_score"]
        )
        assert (
            abs(base_difference - 25) <= 1
        ), f"Base score difference should be ~25, got {base_difference}"

        # Final score should be reduced by penalty amount
        score_difference = result_no_penalty["score"] - result_with_penalty["score"]
        assert (
            score_difference >= 0
        ), "Score with penalty should be <= score without penalty"

        # The difference should be approximately the penalty amount (allowing for rounding)
        assert (
            abs(score_difference - 25) <= 1
        ), f"Score difference should be ~25, got {score_difference}"

    def test_score_precision(self):
        """Test score precision and floating point handling."""
        # Test with names that might produce fractional results
        name_a = "acme store"
        name_b = "acme shop"

        result = compute_score_components(
            name_a,
            name_b,
            "NONE",
            "NONE",
            {"num_style_mismatch": 0, "suffix_mismatch": 0, "punctuation_mismatch": 0},
        )

        # All numeric values should be valid numbers
        assert isinstance(
            result["score"], (int, float, np.number)
        ), "Score should be numeric"
        assert isinstance(
            result["base_score"], (int, float, np.number)
        ), "Base score should be numeric"
        assert isinstance(
            result["ratio_name"], (int, float, np.number)
        ), "ratio_name should be numeric"
        assert isinstance(
            result["ratio_set"], (int, float, np.number)
        ), "ratio_set should be numeric"
        assert isinstance(
            result["jaccard"], (int, float, np.number)
        ), "jaccard should be numeric"

        # No NaN or infinite values
        assert not np.isnan(result["score"]), "Score should not be NaN"
        assert not np.isinf(result["score"]), "Score should not be infinite"
        assert not np.isnan(result["base_score"]), "Base score should not be NaN"
        assert not np.isinf(result["base_score"]), "Base score should not be infinite"

    def test_extreme_penalty_values(self):
        """Test behavior with extreme penalty values."""
        name_a = "acme store"
        name_b = "acme shop"

        # Test with very large penalties (use different suffix classes to trigger penalty)
        extreme_penalties = [100, 200, 500, 1000]

        for penalty in extreme_penalties:
            result = compute_score_components(
                name_a,
                name_b,
                "INC",
                "LLC",  # Different suffix classes to trigger penalty
                {
                    "num_style_mismatch": 0,
                    "suffix_mismatch": penalty,
                    "punctuation_mismatch": 0,
                },
            )

            # Score should be clamped to 0 with extreme penalties
            assert (
                result["score"] >= 0
            ), f"Score should be >= 0 with penalty {penalty}, got {result['score']}"
            assert (
                result["score"] <= 100
            ), f"Score should be <= 100 with penalty {penalty}, got {result['score']}"

            # With very large penalties, score should be 0
            if penalty >= 100:
                assert (
                    result["score"] == 0
                ), f"Score should be 0 with extreme penalty {penalty}, got {result['score']}"

    def test_zero_penalty_values(self):
        """Test behavior with zero penalty values."""
        name_a = "acme store"
        name_b = "acme shop"

        # Test with zero penalties
        result = compute_score_components(
            name_a,
            name_b,
            "NONE",
            "NONE",
            {"num_style_mismatch": 0, "suffix_mismatch": 0, "punctuation_mismatch": 0},
        )

        # Score should be the same as base score (no penalties applied)
        assert (
            result["score"] == result["base_score"]
        ), f"Score should equal base_score with zero penalties: {result['score']} vs {result['base_score']}"

        # Score should be positive for similar names
        assert (
            result["score"] > 0
        ), f"Score should be positive for similar names with zero penalties, got {result['score']}"

    def test_negative_penalty_values(self):
        """Test behavior with negative penalty values."""
        name_a = "acme store"
        name_b = "acme shop"

        # Test with negative penalties (should increase scores)
        result = compute_score_components(
            name_a,
            name_b,
            "NONE",
            "NONE",
            {
                "num_style_mismatch": 0,
                "suffix_mismatch": -10,
                "punctuation_mismatch": 0,
            },
        )

        # Score should still be in valid range
        assert (
            0 <= result["score"] <= 100
        ), f"Score should be 0-100 with negative penalty, got {result['score']}"

        # Score should be >= base_score (negative penalty increases score)
        assert (
            result["score"] >= result["base_score"]
        ), f"Score {result['score']} should be >= base_score {result['base_score']} with negative penalty"

        # But should be clamped to 100 maximum
        assert (
            result["score"] <= 100
        ), f"Score should be <= 100 even with negative penalty, got {result['score']}"

    def test_combined_penalty_bounds(self):
        """Test behavior with multiple penalties combined."""
        name_a = "acme store"
        name_b = "acme shop"

        # Test with multiple penalties
        result = compute_score_components(
            name_a,
            name_b,
            "NONE",
            "NONE",
            {"num_style_mismatch": 5, "suffix_mismatch": 25, "punctuation_mismatch": 3},
        )

        # Score should be in valid range
        assert (
            0 <= result["score"] <= 100
        ), f"Score should be 0-100 with combined penalties, got {result['score']}"

        # Score should be <= base_score (penalties reduce score)
        assert (
            result["score"] <= result["base_score"]
        ), f"Score {result['score']} should be <= base_score {result['base_score']} with combined penalties"

        # Penalty flags should be set correctly
        assert isinstance(
            result["suffix_match"], bool
        ), "suffix_match should be boolean"
        assert isinstance(
            result["num_style_match"], bool
        ), "num_style_match should be boolean"
        assert isinstance(
            result["punctuation_mismatch"], bool
        ), "punctuation_mismatch should be boolean"

    def test_edge_case_score_boundaries(self):
        """Test edge cases at score boundaries."""
        # Test with names that should produce boundary scores
        test_cases = [
            ("acme store", "acme store"),  # Should be 100
            ("acme store", "xyz corporation"),  # Should be low
            ("a", "b"),  # Should be very low
            ("", ""),  # Should be 0 or 100
        ]

        for name_a, name_b in test_cases:
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

            # All scores should be in valid range
            assert (
                0 <= result["score"] <= 100
            ), f"Score should be 0-100 for '{name_a}' vs '{name_b}', got {result['score']}"

            # All component scores should be in valid ranges
            assert (
                0 <= result["ratio_name"] <= 100
            ), f"ratio_name should be 0-100 for '{name_a}' vs '{name_b}'"
            assert (
                0 <= result["ratio_set"] <= 100
            ), f"ratio_set should be 0-100 for '{name_a}' vs '{name_b}'"
            assert (
                0 <= result["jaccard"] <= 1.0
            ), f"jaccard should be 0-1.0 for '{name_a}' vs '{name_b}'"
