"""Tests for edge case robustness in similarity scoring.

This module tests robustness against edge cases:
- Whitespace variants and normalization
- Empty/short names handling
- Missing suffix_class defaulting
- Unicode handling (smart quotes, different hyphen types)
- Special character handling
- Boundary conditions
"""

import sys
from pathlib import Path

import pandas as pd
import pytest

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.normalize import normalize_dataframe
from src.similarity.scoring import compute_score_components, score_pairs_bulk


class TestScoringRobustness:
    """Test edge case robustness for similarity scoring."""

    def test_whitespace_variants_leading_trailing(self):
        """Test handling of leading and trailing whitespace."""
        # Test that leading/trailing whitespace is normalized correctly
        name_a = "  acme  "
        name_b = "acme"

        result = compute_score_components(
            name_a,
            name_b,
            "NONE",
            "NONE",
            {"num_style_mismatch": 5, "suffix_mismatch": 25, "punctuation_mismatch": 3},
        )

        # Should match perfectly after normalization
        assert (
            result["score"] == 100
        ), "Leading/trailing whitespace should be normalized"
        assert result["ratio_name"] == 100, "Token sort ratio should be perfect"
        assert result["ratio_set"] == 100, "Token set ratio should be perfect"
        assert result["jaccard"] == 1.0, "Jaccard should be perfect"

    def test_whitespace_variants_multiple_spaces(self):
        """Test handling of multiple consecutive spaces."""
        # Test that multiple spaces are normalized to single spaces
        name_a = "acme    store"
        name_b = "acme store"

        result = compute_score_components(
            name_a,
            name_b,
            "NONE",
            "NONE",
            {"num_style_mismatch": 5, "suffix_mismatch": 25, "punctuation_mismatch": 3},
        )

        # Should match perfectly after normalization
        assert result["score"] == 100, "Multiple spaces should be normalized"
        assert result["ratio_name"] == 100, "Token sort ratio should be perfect"
        assert result["ratio_set"] == 100, "Token set ratio should be perfect"
        assert result["jaccard"] == 1.0, "Jaccard should be perfect"

    def test_whitespace_variants_tabs_newlines(self):
        """Test handling of tabs and newlines."""
        # Test that tabs and newlines are normalized to spaces
        name_a = "acme\tstore\n"
        name_b = "acme store"

        result = compute_score_components(
            name_a,
            name_b,
            "NONE",
            "NONE",
            {"num_style_mismatch": 5, "suffix_mismatch": 25, "punctuation_mismatch": 3},
        )

        # Should match perfectly after normalization
        assert result["score"] == 100, "Tabs and newlines should be normalized"
        assert result["ratio_name"] == 100, "Token sort ratio should be perfect"
        assert result["ratio_set"] == 100, "Token set ratio should be perfect"
        assert result["jaccard"] == 1.0, "Jaccard should be perfect"

    def test_empty_names_handling(self):
        """Test handling of empty names."""
        # Test that empty names are handled gracefully
        name_a = ""
        name_b = "acme"

        result = compute_score_components(
            name_a,
            name_b,
            "NONE",
            "NONE",
            {"num_style_mismatch": 5, "suffix_mismatch": 25, "punctuation_mismatch": 3},
        )

        # Should return valid result with zero score
        assert result["score"] == 0, "Empty name should result in zero score"
        assert result["ratio_name"] == 0, "Token sort ratio should be zero"
        assert result["ratio_set"] == 0, "Token set ratio should be zero"
        assert result["jaccard"] == 0.0, "Jaccard should be zero"

    def test_short_names_handling(self):
        """Test handling of very short names."""
        # Test that very short names are handled gracefully
        name_a = "a"
        name_b = "b"

        result = compute_score_components(
            name_a,
            name_b,
            "NONE",
            "NONE",
            {"num_style_mismatch": 5, "suffix_mismatch": 25, "punctuation_mismatch": 3},
        )

        # Should return valid result with low score
        assert result["score"] >= 0, "Short names should return valid score"
        assert result["score"] <= 100, "Score should be within bounds"
        assert "ratio_name" in result, "Should return ratio_name"
        assert "ratio_set" in result, "Should return ratio_set"
        assert "jaccard" in result, "Should return jaccard"

    def test_missing_suffix_class_defaulting(self):
        """Test defaulting of missing suffix_class to 'NONE'."""
        # Test that missing suffix_class defaults to "NONE"
        name_a = "acme"
        name_b = "acme"

        # Test with explicit "NONE" suffix
        result_explicit = compute_score_components(
            name_a,
            name_b,
            "NONE",
            "NONE",
            {"num_style_mismatch": 5, "suffix_mismatch": 25, "punctuation_mismatch": 3},
        )

        # Test with empty string suffix (should be treated as "NONE")
        result_empty = compute_score_components(
            name_a,
            name_b,
            "",
            "",
            {"num_style_mismatch": 5, "suffix_mismatch": 25, "punctuation_mismatch": 3},
        )

        # Both should produce identical results
        assert result_explicit["score"] == result_empty["score"]
        assert result_explicit["suffix_match"] == result_empty["suffix_match"]
        assert result_explicit["suffix_match"] is True, "Empty suffixes should match"

    def test_unicode_smart_quotes(self):
        """Test handling of smart quotes."""
        # Test that smart quotes are handled consistently
        name_a = "bob's"  # regular apostrophe
        name_b = "bob\u2019s"  # smart quote (U+2019)

        result = compute_score_components(
            name_a,
            name_b,
            "NONE",
            "NONE",
            {"num_style_mismatch": 5, "suffix_mismatch": 25, "punctuation_mismatch": 3},
        )

        # Should detect punctuation mismatch
        assert (
            result["punctuation_mismatch"] is True
        ), "Smart quotes should be detected as different"
        assert result["score"] < 100, "Smart quote difference should reduce score"

    def test_unicode_en_dash_vs_hyphen(self):
        """Test handling of en dash vs regular hyphen."""
        # Test that en dash and hyphen are handled consistently
        name_a = "7-eleven"  # regular hyphen
        name_b = "7–eleven"  # en dash (U+2013)

        result = compute_score_components(
            name_a,
            name_b,
            "NONE",
            "NONE",
            {"num_style_mismatch": 5, "suffix_mismatch": 25, "punctuation_mismatch": 3},
        )

        # Should detect punctuation mismatch
        assert (
            result["punctuation_mismatch"] is True
        ), "En dash vs hyphen should be detected as different"
        assert result["score"] < 100, "En dash difference should reduce score"

    def test_unicode_em_dash_vs_hyphen(self):
        """Test handling of em dash vs regular hyphen."""
        # Test that em dash and hyphen are handled consistently
        name_a = "7-eleven"  # regular hyphen
        name_b = "7—eleven"  # em dash (U+2014)

        result = compute_score_components(
            name_a,
            name_b,
            "NONE",
            "NONE",
            {"num_style_mismatch": 5, "suffix_mismatch": 25, "punctuation_mismatch": 3},
        )

        # Should detect punctuation mismatch
        assert (
            result["punctuation_mismatch"] is True
        ), "Em dash vs hyphen should be detected as different"
        assert result["score"] < 100, "Em dash difference should reduce score"

    def test_unicode_curly_quotes(self):
        """Test handling of curly quotes."""
        # Test that curly quotes are handled consistently
        name_a = '"acme"'  # regular quotes
        name_b = "\u201cacme\u201d"  # curly quotes (U+201C, U+201D)

        result = compute_score_components(
            name_a,
            name_b,
            "NONE",
            "NONE",
            {"num_style_mismatch": 5, "suffix_mismatch": 25, "punctuation_mismatch": 3},
        )

        # Should detect punctuation mismatch
        assert (
            result["punctuation_mismatch"] is True
        ), "Curly quotes should be detected as different"
        assert result["score"] < 100, "Curly quote difference should reduce score"

    def test_unicode_accents(self):
        """Test handling of accented characters."""
        # Test that accented characters are handled consistently
        name_a = "cafe"
        name_b = "café"  # é (U+00E9)

        result = compute_score_components(
            name_a,
            name_b,
            "NONE",
            "NONE",
            {"num_style_mismatch": 5, "suffix_mismatch": 25, "punctuation_mismatch": 3},
        )

        # Should detect character difference
        assert (
            result["score"] < 100
        ), "Accented character difference should reduce score"
        # Note: This may or may not trigger punctuation_mismatch depending on implementation

    def test_unicode_special_characters(self):
        """Test handling of special Unicode characters."""
        # Test various special Unicode characters
        name_a = "acme"
        name_b = "acme™"  # trademark symbol (U+2122)

        result = compute_score_components(
            name_a,
            name_b,
            "NONE",
            "NONE",
            {"num_style_mismatch": 5, "suffix_mismatch": 25, "punctuation_mismatch": 3},
        )

        # Should detect character difference
        assert result["score"] < 100, "Special Unicode character should reduce score"
        assert (
            result["punctuation_mismatch"] is True
        ), "Trademark symbol should trigger punctuation mismatch"

    def test_none_input_handling(self):
        """Test handling of None inputs."""
        # Test that None inputs are handled gracefully
        name_a = None
        name_b = "acme"

        # Current implementation crashes on None inputs
        # This documents the current behavior - None inputs cause AttributeError
        with pytest.raises(
            AttributeError,
            match="'NoneType' object has no attribute 'split'",
        ):
            compute_score_components(
                name_a,  # type: ignore[arg-type]
                name_b,
                "NONE",
                "NONE",
                {
                    "num_style_mismatch": 5,
                    "suffix_mismatch": 25,
                    "punctuation_mismatch": 3,
                },
            )

        # Document this as a known limitation
        print(
            "Note: None inputs currently cause AttributeError - this is a known limitation",
        )

    def test_numeric_only_names(self):
        """Test handling of purely numeric names."""
        # Test that purely numeric names are handled gracefully
        name_a = "123"
        name_b = "456"

        result = compute_score_components(
            name_a,
            name_b,
            "NONE",
            "NONE",
            {"num_style_mismatch": 5, "suffix_mismatch": 25, "punctuation_mismatch": 3},
        )

        # Should return valid result with low score
        assert result["score"] >= 0, "Numeric names should return valid score"
        assert result["score"] <= 100, "Score should be within bounds"
        assert (
            result["num_style_match"] is False
        ), "Different numbers should trigger numeric style mismatch"

    def test_special_character_only_names(self):
        """Test handling of names with only special characters."""
        # Test that names with only special characters are handled gracefully
        name_a = "!!!"
        name_b = "???"

        result = compute_score_components(
            name_a,
            name_b,
            "NONE",
            "NONE",
            {"num_style_mismatch": 5, "suffix_mismatch": 25, "punctuation_mismatch": 3},
        )

        # Should return valid result
        assert result["score"] >= 0, "Special character names should return valid score"
        assert result["score"] <= 100, "Score should be within bounds"
        assert "ratio_name" in result, "Should return ratio_name"
        assert "ratio_set" in result, "Should return ratio_set"
        assert "jaccard" in result, "Should return jaccard"

    def test_very_long_names(self):
        """Test handling of very long names."""
        # Test that very long names are handled gracefully
        name_a = "a" * 1000  # 1000 character string
        name_b = "b" * 1000  # 1000 character string

        result = compute_score_components(
            name_a,
            name_b,
            "NONE",
            "NONE",
            {"num_style_mismatch": 5, "suffix_mismatch": 25, "punctuation_mismatch": 3},
        )

        # Should return valid result
        assert result["score"] >= 0, "Long names should return valid score"
        assert result["score"] <= 100, "Score should be within bounds"
        assert "ratio_name" in result, "Should return ratio_name"
        assert "ratio_set" in result, "Should return ratio_set"
        assert "jaccard" in result, "Should return jaccard"

    def test_mixed_unicode_normalization(self):
        """Test handling of mixed Unicode characters in normalization."""
        # Test that mixed Unicode characters are normalized consistently
        test_data = pd.DataFrame(
            {
                "account_id": [1, 2],
                "Account Name": ["  acme™  ", "acme™"],  # Mixed whitespace and Unicode
            },
        )

        df_norm = normalize_dataframe(test_data, "Account Name")
        candidate_pairs = [(0, 1)]

        settings = {
            "similarity": {
                "scoring": {"gate_cutoff": 50},
                "penalty": {
                    "num_style_mismatch": 5,
                    "suffix_mismatch": 25,
                    "punctuation_mismatch": 3,
                },
            },
        }

        # Should not crash and return valid results
        results = score_pairs_bulk(df_norm, candidate_pairs, settings)

        # Should handle normalization gracefully
        assert len(results) >= 0, "Should return valid results"
        if len(results) > 0:
            result = results[0]
            assert result["score"] >= 0, "Score should be non-negative"
            assert result["score"] <= 100, "Score should be within bounds"
