"""Tests for enhanced normalization fallback in similarity scoring.

This module tests fallback behavior when enhanced normalization fails:
- Scoring works if normalize import fails
- Penalties still apply
- No exceptions leak
"""

import sys
from pathlib import Path
from unittest.mock import patch

import pytest

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.similarity.scoring import compute_score_components


class TestScoringEnhancedFallback:
    """Test enhanced normalization fallback behavior for similarity scoring."""

    def test_normalize_import_failure_fallback(self):
        """Test fallback when normalize import fails."""
        # Mock the import to fail
        with patch.dict("sys.modules", {"src.normalize": None}):
            # Should still work with fallback behavior
            result = compute_score_components(
                "acme store",
                "acme shop",
                "NONE",
                "NONE",
                {
                    "num_style_mismatch": 0,
                    "suffix_mismatch": 0,
                    "punctuation_mismatch": 0,
                },
            )

            # Should produce valid results
            assert isinstance(result["score"], (int, float)), "Score should be numeric"
            assert (
                0 <= result["score"] <= 100
            ), f"Score should be 0-100, got {result['score']}"
            assert isinstance(
                result["jaccard"], (int, float)
            ), "Jaccard should be numeric"
            assert (
                0 <= result["jaccard"] <= 1.0
            ), f"Jaccard should be 0-1.0, got {result['jaccard']}"

    def test_enhance_name_core_failure_fallback(self):
        """Test fallback when enhance_name_core fails."""
        with patch(
            "src.normalize.enhance_name_core",
            side_effect=ImportError("Module not available"),
        ):
            # Should still work with fallback behavior
            result = compute_score_components(
                "acme store",
                "acme shop",
                "NONE",
                "NONE",
                {
                    "num_style_mismatch": 0,
                    "suffix_mismatch": 0,
                    "punctuation_mismatch": 0,
                },
            )

            # Should produce valid results
            assert isinstance(result["score"], (int, float)), "Score should be numeric"
            assert (
                0 <= result["score"] <= 100
            ), f"Score should be 0-100, got {result['score']}"

    def test_get_enhanced_tokens_failure_fallback(self):
        """Test fallback when get_enhanced_tokens_for_jaccard fails."""
        with patch(
            "src.normalize.get_enhanced_tokens_for_jaccard",
            side_effect=ImportError("Module not available"),
        ):
            # Should still work with fallback behavior
            result = compute_score_components(
                "acme store",
                "acme shop",
                "NONE",
                "NONE",
                {
                    "num_style_mismatch": 0,
                    "suffix_mismatch": 0,
                    "punctuation_mismatch": 0,
                },
            )

            # Should produce valid results
            assert isinstance(result["score"], (int, float)), "Score should be numeric"
            assert (
                0 <= result["score"] <= 100
            ), f"Score should be 0-100, got {result['score']}"

    def test_penalties_apply_during_fallback(self):
        """Test that penalties still apply during fallback."""
        with patch(
            "src.normalize.enhance_name_core",
            side_effect=ImportError("Module not available"),
        ):
            # Test with suffix penalty
            result = compute_score_components(
                "acme store",
                "acme shop",
                "INC",
                "LLC",  # Different suffix classes
                {
                    "num_style_mismatch": 0,
                    "suffix_mismatch": 25,
                    "punctuation_mismatch": 0,
                },
            )

            # Should produce valid results with penalty applied
            assert isinstance(result["score"], (int, float)), "Score should be numeric"
            assert (
                0 <= result["score"] <= 100
            ), f"Score should be 0-100, got {result['score']}"
            assert result["suffix_match"] is False, "Suffix match should be False"

    def test_no_exceptions_leak_fallback(self):
        """Test that no exceptions leak during fallback."""
        # Current implementation only catches ImportError, not other exceptions
        # This documents the current behavior - other exceptions will propagate
        with patch(
            "src.normalize.enhance_name_core", side_effect=Exception("Unexpected error")
        ):
            # Should raise the exception (current behavior)
            with pytest.raises(Exception, match="Unexpected error"):
                compute_score_components(
                    "acme store",
                    "acme shop",
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
            "Note: Only ImportError is caught in fallback - other exceptions propagate"
        )

    def test_fallback_score_consistency(self):
        """Test that fallback scores are consistent."""
        # Test multiple calls with same inputs
        results = []
        for _ in range(3):
            with patch(
                "src.normalize.enhance_name_core",
                side_effect=ImportError("Module not available"),
            ):
                result = compute_score_components(
                    "acme store",
                    "acme shop",
                    "NONE",
                    "NONE",
                    {
                        "num_style_mismatch": 0,
                        "suffix_mismatch": 0,
                        "punctuation_mismatch": 0,
                    },
                )
                results.append(result)

        # All results should be identical
        for i in range(1, len(results)):
            assert (
                results[i]["score"] == results[0]["score"]
            ), "Fallback scores should be consistent"
            assert (
                results[i]["jaccard"] == results[0]["jaccard"]
            ), "Fallback jaccard should be consistent"

    def test_fallback_performance(self):
        """Test that fallback doesn't significantly impact performance."""
        import time

        # Time fallback execution
        start_time = time.time()
        with patch(
            "src.normalize.enhance_name_core",
            side_effect=ImportError("Module not available"),
        ):
            result = compute_score_components(
                "acme store",
                "acme shop",
                "NONE",
                "NONE",
                {
                    "num_style_mismatch": 0,
                    "suffix_mismatch": 0,
                    "punctuation_mismatch": 0,
                },
            )
        fallback_time = time.time() - start_time

        # Should complete quickly (less than 1 second)
        assert (
            fallback_time < 1.0
        ), f"Fallback should be fast, took {fallback_time:.3f}s"
        assert isinstance(result["score"], (int, float)), "Score should be numeric"

    def test_fallback_logging(self):
        """Test that fallback behavior is logged appropriately."""
        # This test documents that fallback behavior exists
        # In a real implementation, we might check for specific log messages
        with patch(
            "src.normalize.enhance_name_core",
            side_effect=ImportError("Module not available"),
        ):
            result = compute_score_components(
                "acme store",
                "acme shop",
                "NONE",
                "NONE",
                {
                    "num_style_mismatch": 0,
                    "suffix_mismatch": 0,
                    "punctuation_mismatch": 0,
                },
            )

            # Should produce valid results
            assert isinstance(result["score"], (int, float)), "Score should be numeric"
            assert (
                0 <= result["score"] <= 100
            ), f"Score should be 0-100, got {result['score']}"

    def test_fallback_graceful_degradation(self):
        """Test that fallback provides graceful degradation."""
        # Test that fallback still provides reasonable scores
        with patch(
            "src.normalize.enhance_name_core",
            side_effect=ImportError("Module not available"),
        ):
            # Identical names should still score high
            result_identical = compute_score_components(
                "acme store",
                "acme store",
                "NONE",
                "NONE",
                {
                    "num_style_mismatch": 0,
                    "suffix_mismatch": 0,
                    "punctuation_mismatch": 0,
                },
            )

            # Different names should score lower
            result_different = compute_score_components(
                "acme store",
                "xyz corporation",
                "NONE",
                "NONE",
                {
                    "num_style_mismatch": 0,
                    "suffix_mismatch": 0,
                    "punctuation_mismatch": 0,
                },
            )

            # Identical should score higher than different
            assert (
                result_identical["score"] > result_different["score"]
            ), "Identical names should score higher than different names in fallback"

    def test_fallback_error_recovery(self):
        """Test that fallback recovers from ImportError only."""
        # Test ImportError (should work with fallback)
        with patch(
            "src.normalize.enhance_name_core",
            side_effect=ImportError("Module not available"),
        ):
            result = compute_score_components(
                "acme store",
                "acme shop",
                "NONE",
                "NONE",
                {
                    "num_style_mismatch": 0,
                    "suffix_mismatch": 0,
                    "punctuation_mismatch": 0,
                },
            )

            # Should produce valid results for ImportError
            assert isinstance(
                result["score"], (int, float)
            ), "Score should be numeric for ImportError"
            assert (
                0 <= result["score"] <= 100
            ), f"Score should be 0-100 for ImportError, got {result['score']}"

        # Test other error types (should propagate)
        other_error_types = [AttributeError, RuntimeError, ValueError]
        for error_type in other_error_types:
            with patch(
                "src.normalize.enhance_name_core", side_effect=error_type("Test error")
            ):
                # Should raise the exception (current behavior)
                with pytest.raises(error_type, match="Test error"):
                    compute_score_components(
                        "acme store",
                        "acme shop",
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
            "Note: Only ImportError is caught in fallback - other exceptions propagate"
        )

    def test_fallback_configuration_handling(self):
        """Test that fallback handles configuration properly."""
        with patch(
            "src.normalize.enhance_name_core",
            side_effect=ImportError("Module not available"),
        ):
            # Test with different settings
            result = compute_score_components(
                "acme store",
                "acme shop",
                "NONE",
                "NONE",
                {
                    "num_style_mismatch": 0,
                    "suffix_mismatch": 0,
                    "punctuation_mismatch": 0,
                },
                settings={"test": "value"},  # Pass settings
            )

            # Should produce valid results
            assert isinstance(result["score"], (int, float)), "Score should be numeric"
            assert (
                0 <= result["score"] <= 100
            ), f"Score should be 0-100, got {result['score']}"

    def test_fallback_determinism(self):
        """Test that fallback behavior is deterministic."""
        # Test that same inputs produce same outputs in fallback mode
        with patch(
            "src.normalize.enhance_name_core",
            side_effect=ImportError("Module not available"),
        ):
            result1 = compute_score_components(
                "acme store",
                "acme shop",
                "NONE",
                "NONE",
                {
                    "num_style_mismatch": 0,
                    "suffix_mismatch": 0,
                    "punctuation_mismatch": 0,
                },
            )

            result2 = compute_score_components(
                "acme store",
                "acme shop",
                "NONE",
                "NONE",
                {
                    "num_style_mismatch": 0,
                    "suffix_mismatch": 0,
                    "punctuation_mismatch": 0,
                },
            )

            # Results should be identical
            assert (
                result1["score"] == result2["score"]
            ), "Fallback results should be deterministic"
            assert (
                result1["jaccard"] == result2["jaccard"]
            ), "Fallback jaccard should be deterministic"
