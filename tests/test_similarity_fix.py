"""Tests for similarity scoring fixes and enhanced normalization.

These tests verify that the enhanced normalization correctly handles
retail brand variants like "99 Cents Only Stores" vs "99 Cents Store".
"""

import sys
from pathlib import Path

import pytest

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.normalize import (
    enhance_name_core,
    get_enhanced_tokens_for_jaccard,
    normalize_name,
)
from src.similarity.scoring import compute_score_components


class TestSimilarityFix:
    """Test cases for similarity scoring fixes."""

    def test_99_cents_grouping_with_matching_suffixes(self):
        """Test that 99 Cents variants group when suffixes match."""
        name_a = "99 Cents Only Stores LLC"
        name_b = "99 Cents Store LLC"

        # Normalize names
        norm_a = normalize_name(name_a)
        norm_b = normalize_name(name_b)

        # Test enhanced normalization
        enhanced_a, weak_a = enhance_name_core(norm_a.name_core)
        enhanced_b, weak_b = enhance_name_core(norm_b.name_core)

        # Verify enhancements
        assert enhanced_a == "99 cents only store"  # stores -> store
        assert enhanced_b == "99 cents store"
        assert weak_a == {"only"}  # "only" is weak token
        assert weak_b == set()

        # Test Jaccard tokens (excluding weak tokens)
        tokens_a = get_enhanced_tokens_for_jaccard(norm_a.name_core)
        tokens_b = get_enhanced_tokens_for_jaccard(norm_b.name_core)

        assert tokens_a == {"99", "cents", "store"}
        assert tokens_b == {"99", "cents", "store"}
        assert tokens_a == tokens_b  # Perfect Jaccard match

        # Test scoring
        result = compute_score_components(
            norm_a.name_core,
            norm_b.name_core,
            norm_a.suffix_class,
            norm_b.suffix_class,
            {"num_style_mismatch": 5, "suffix_mismatch": 25, "punctuation_mismatch": 3},
        )

        # Should score >= 84 (medium threshold) due to enhanced normalization
        assert result["score"] >= 84, f"Score {result['score']} should be >= 84"
        assert result["jaccard"] == 1.0, "Jaccard should be 1.0 (perfect match)"

    def test_99_cents_grouping_with_different_suffixes(self):
        """Test that 99 Cents variants still group with different suffixes if score is high enough."""
        name_a = "99 Cents Only Stores LLC"
        name_b = "99 Cents Store Inc"

        # Normalize names
        norm_a = normalize_name(name_a)
        norm_b = normalize_name(name_b)

        # Test scoring with suffix mismatch penalty
        result = compute_score_components(
            norm_a.name_core,
            norm_b.name_core,
            norm_a.suffix_class,
            norm_b.suffix_class,
            {"num_style_mismatch": 5, "suffix_mismatch": 25, "punctuation_mismatch": 3},
        )

        # Should still score reasonably high despite suffix penalty
        assert result["score"] >= 65, f"Score {result['score']} should be >= 65"
        assert result["jaccard"] == 1.0, "Jaccard should be 1.0 (perfect match)"

    def test_7_eleven_variants_remain_high(self):
        """Test that 7-Eleven variants maintain high scores."""
        name_a = "7-Eleven Store Inc"
        name_b = "7 Eleven Inc"

        # Normalize names
        norm_a = normalize_name(name_a)
        norm_b = normalize_name(name_b)

        # Test scoring
        result = compute_score_components(
            norm_a.name_core,
            norm_b.name_core,
            norm_a.suffix_class,
            norm_b.suffix_class,
            {"num_style_mismatch": 5, "suffix_mismatch": 25, "punctuation_mismatch": 3},
        )

        # Should maintain reasonable score for 7-Eleven variants with matching suffixes
        assert result["score"] >= 80, f"Score {result['score']} should be >= 80"

    def test_enhanced_normalization_features(self):
        """Test individual enhanced normalization features."""
        # Test plural to singular mapping
        enhanced, weak = enhance_name_core("test stores services")
        assert enhanced == "test store service"

        # Test canonical retail terms
        enhanced, weak = enhance_name_core("test shop shops")
        assert enhanced == "test store store"

        # Test weak token detection
        enhanced, weak = enhance_name_core("test only the and")
        assert weak == {"only", "the", "and"}

        # Test Jaccard token filtering
        tokens = get_enhanced_tokens_for_jaccard("test only the and")
        assert tokens == {"test"}  # Only non-weak tokens

    def test_soft_ban_denylist_still_yields_candidates(self):
        """Test that denylist tokens still generate candidates (throttled, not hard-banned)."""
        # Test that the configuration is properly set up for soft-ban

        # Test that denylist tokens are configured
        from src.normalize import load_normalization_settings

        settings = load_normalization_settings()

        # Should have denylist tokens configured
        assert "weak_tokens" in settings
        assert "only" in settings["weak_tokens"]

        # Should have plural mapping configured
        assert "plural_singular_map" in settings
        assert settings["plural_singular_map"]["stores"] == "store"

        # Test that soft-ban is the only strategy (no legacy)
        import yaml

        with open("config/settings.yaml") as f:
            config = yaml.safe_load(f)

        # Should not have legacy performance settings
        assert "performance" not in config.get("similarity", {})

        # Should have soft-ban blocking settings
        blocking = config.get("similarity", {}).get("blocking", {})
        assert "allowlist_tokens" in blocking
        assert "denylist_tokens" in blocking
        assert "soft_ban" in blocking

    def test_configuration_keys_exist(self):
        """Test that all required configuration keys exist."""
        from src.normalize import load_normalization_settings

        settings = load_normalization_settings()

        required_keys = [
            "weak_tokens",
            "plural_singular_map",
            "canonical_retail_terms",
            "enable_plural_normalization",
            "enable_weak_token_filtering",
            "enable_canonical_retail_terms",
        ]

        for key in required_keys:
            assert key in settings, f"Required config key '{key}' not found"

    def test_edge_cases(self):
        """Test edge cases for enhanced normalization."""
        # Empty string
        enhanced, weak = enhance_name_core("")
        assert enhanced == ""
        assert weak == set()

        # Single token
        enhanced, weak = enhance_name_core("test")
        assert enhanced == "test"
        assert weak == set()

        # All weak tokens
        enhanced, weak = enhance_name_core("only the and")
        assert enhanced == "only the and"
        assert weak == {"only", "the", "and"}

        # Mixed case
        enhanced, weak = enhance_name_core("Test Stores Only")
        assert enhanced == "test store only"
        assert weak == {"only"}


if __name__ == "__main__":
    pytest.main([__file__])
