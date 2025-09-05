"""Tests for base signal correctness in similarity scoring.

This module tests the fundamental scoring components:
- token_sort_ratio sensitivity and order-insensitive behavior
- token_set_ratio resilience and subset handling
- Jaccard similarity with enhanced normalization
- Plural→singular mapping behavior
- Canonical retail terms mapping
"""

import sys
from pathlib import Path

import pytest

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.similarity.scoring import compute_score_components


class TestScoringComponents:
    """Test base signal correctness for similarity scoring components."""

    def test_token_sort_ratio_sensitivity(self):
        """Test that token_sort_ratio is order-insensitive."""
        # Test order-insensitive behavior
        name_a = "acme holdings"
        name_b = "holdings acme"

        result = compute_score_components(
            name_a,
            name_b,
            "NONE",
            "NONE",
            {"num_style_mismatch": 5, "suffix_mismatch": 25, "punctuation_mismatch": 3},
        )

        # token_sort_ratio should be high (order-insensitive)
        assert (
            result["ratio_name"] >= 90
        ), f"token_sort_ratio {result['ratio_name']} should be >= 90 for order-insensitive match"
        assert (
            result["score"] >= 80
        ), f"Score {result['score']} should be >= 80 for order-insensitive match"

    def test_token_sort_ratio_vs_set_difference(self):
        """Test token_sort_ratio vs token_set_ratio behavior differences."""
        # Test different ratio behaviors - use more distinct words
        name_a = "acme shop"
        name_b = "acme store"

        result = compute_score_components(
            name_a,
            name_b,
            "NONE",
            "NONE",
            {"num_style_mismatch": 5, "suffix_mismatch": 25, "punctuation_mismatch": 3},
        )

        # Both ratios should be high due to shared "acme" and similar length
        # token_set_ratio should be >= token_sort_ratio (they can be equal)
        assert result["ratio_set"] >= result["ratio_name"], (
            f"token_set_ratio {result['ratio_set']} should be >= "
            f"token_sort_ratio {result['ratio_name']}"
        )
        assert (
            result["ratio_set"] >= 50
        ), f"token_set_ratio {result['ratio_set']} should be >= 50 (shared 'acme')"
        assert (
            result["ratio_name"] >= 50
        ), f"token_sort_ratio {result['ratio_name']} should be >= 50 (shared 'acme')"

    def test_token_set_ratio_resilience(self):
        """Test that token_set_ratio handles subsets well."""
        # Test subset handling
        name_a = "acme store"
        name_b = "acme store west"

        result = compute_score_components(
            name_a,
            name_b,
            "NONE",
            "NONE",
            {"num_style_mismatch": 5, "suffix_mismatch": 25, "punctuation_mismatch": 3},
        )

        # token_set_ratio should be high for subset relationship
        assert (
            result["ratio_set"] >= 80
        ), f"token_set_ratio {result['ratio_set']} should be >= 80 for subset relationship"
        # token_sort_ratio should be lower due to different word order/length
        assert result["ratio_name"] < result["ratio_set"], (
            f"token_sort_ratio {result['ratio_name']} should be < "
            f"token_set_ratio {result['ratio_set']}"
        )

    def test_token_set_ratio_weak_token_handling(self):
        """Test token_set_ratio resilience to weak tokens."""
        # Test weak token handling - "the" is typically a weak token
        name_a = "acme"
        name_b = "acme the store"

        result = compute_score_components(
            name_a,
            name_b,
            "NONE",
            "NONE",
            {"num_style_mismatch": 5, "suffix_mismatch": 25, "punctuation_mismatch": 3},
        )

        # token_set_ratio should be high despite "the" being a weak token
        assert (
            result["ratio_set"] >= 60
        ), f"token_set_ratio {result['ratio_set']} should be >= 60 despite weak token 'the'"
        # token_sort_ratio should be lower due to length difference
        assert result["ratio_name"] < result["ratio_set"], (
            f"token_sort_ratio {result['ratio_name']} should be < "
            f"token_set_ratio {result['ratio_set']}"
        )

    def test_jaccard_enhanced_tokens(self):
        """Test Jaccard similarity with enhanced normalization."""
        # Test enhanced token Jaccard - "only" should be filtered out as weak token
        name_a = "99 cents only store"
        name_b = "99 cents store"

        result = compute_score_components(
            name_a,
            name_b,
            "NONE",
            "NONE",
            {"num_style_mismatch": 5, "suffix_mismatch": 25, "punctuation_mismatch": 3},
        )

        # Jaccard should be 1.0 because "only" is filtered out as weak token
        assert (
            result["jaccard"] == 1.0
        ), f"Jaccard {result['jaccard']} should be 1.0 (weak token 'only' filtered)"
        assert (
            result["score"] >= 80
        ), f"Score {result['score']} should be >= 80 with perfect Jaccard"

    def test_jaccard_plural_singular_mapping(self):
        """Test Jaccard with plural→singular normalization."""
        # Test plural→singular mapping
        name_a = "acme stores"
        name_b = "acme store"

        result = compute_score_components(
            name_a,
            name_b,
            "NONE",
            "NONE",
            {"num_style_mismatch": 5, "suffix_mismatch": 25, "punctuation_mismatch": 3},
        )

        # Jaccard should be 1.0 because "stores" → "store" after normalization
        assert (
            result["jaccard"] == 1.0
        ), f"Jaccard {result['jaccard']} should be 1.0 (plural→singular mapping)"
        assert (
            result["score"] >= 80
        ), f"Score {result['score']} should be >= 80 with perfect Jaccard"

    def test_jaccard_canonical_retail_terms(self):
        """Test Jaccard with canonical retail term mapping."""
        # Test canonical retail term mapping - "shop" and "store" should map to same canonical term
        name_a = "acme shop"
        name_b = "acme store"

        result = compute_score_components(
            name_a,
            name_b,
            "NONE",
            "NONE",
            {"num_style_mismatch": 5, "suffix_mismatch": 25, "punctuation_mismatch": 3},
        )

        # Jaccard should be 1.0 because "shop" and "store" map to same canonical term
        assert (
            result["jaccard"] == 1.0
        ), f"Jaccard {result['jaccard']} should be 1.0 (canonical retail terms mapping)"
        assert (
            result["score"] >= 80
        ), f"Score {result['score']} should be >= 80 with perfect Jaccard"

    def test_jaccard_weak_token_removal(self):
        """Test that weak tokens are excluded from Jaccard calculation."""
        # Test weak token exclusion - "the" should be excluded from Jaccard calculation
        name_a = "acme the store"
        name_b = "acme store"

        result = compute_score_components(
            name_a,
            name_b,
            "NONE",
            "NONE",
            {"num_style_mismatch": 5, "suffix_mismatch": 25, "punctuation_mismatch": 3},
        )

        # Jaccard should be 1.0 because "the" is excluded as weak token
        assert (
            result["jaccard"] == 1.0
        ), f"Jaccard {result['jaccard']} should be 1.0 (weak token 'the' excluded)"
        assert (
            result["score"] >= 80
        ), f"Score {result['score']} should be >= 80 with perfect Jaccard"

    def test_jaccard_empty_tokens(self):
        """Test Jaccard behavior with empty token sets."""
        # Test empty token handling
        name_a = ""
        name_b = "acme store"

        result = compute_score_components(
            name_a,
            name_b,
            "NONE",
            "NONE",
            {"num_style_mismatch": 5, "suffix_mismatch": 25, "punctuation_mismatch": 3},
        )

        # Jaccard should be 0.0 for empty tokens
        assert (
            result["jaccard"] == 0.0
        ), f"Jaccard {result['jaccard']} should be 0.0 for empty tokens"
        assert (
            result["score"] == 0
        ), f"Score {result['score']} should be 0 for empty input"

    def test_enhanced_normalization_fallback(self):
        """Test fallback behavior when enhanced normalization fails."""
        # Test fallback behavior by mocking import failure
        import unittest.mock

        with unittest.mock.patch(
            "src.normalize.enhance_name_core",
            side_effect=ImportError("Module not found"),
        ):
            name_a = "acme store"
            name_b = "acme shop"

            result = compute_score_components(
                name_a,
                name_b,
                "NONE",
                "NONE",
                {
                    "num_style_mismatch": 5,
                    "suffix_mismatch": 25,
                    "punctuation_mismatch": 3,
                },
            )

            # Should still work with fallback to basic tokenization
            assert "score" in result, "Should return score even with import failure"
            assert (
                "ratio_name" in result
            ), "Should return ratio_name even with import failure"
            assert (
                "ratio_set" in result
            ), "Should return ratio_set even with import failure"
            assert "jaccard" in result, "Should return jaccard even with import failure"
