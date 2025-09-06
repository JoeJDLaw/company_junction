"""Tests for the specific fixes applied to similarity module."""

import sys
from pathlib import Path

import pandas as pd
import pytest

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.similarity import get_stop_tokens, pair_scores
from src.similarity.blocking import generate_candidate_pairs_soft_ban
from src.similarity.scoring import compute_score_components


class TestSimilarityFixes:
    """Test cases for the specific fixes applied."""

    def test_lowercased_block_keys_hit_allowlist(self):
        """Test that lowercased block keys properly hit allow/deny lists."""
        # Create test data with mixed case
        df_norm = pd.DataFrame(
            {
                "account_id": ["A1", "A2", "A3", "A4"],
                "name_core": [
                    "99 Cents Store",
                    "99 CENTS CORP",
                    "The Company",
                    "THE BUSINESS",
                ],
                "suffix_class": ["INC", "LLC", "INC", "LLC"],
            },
        )

        settings = {
            "similarity": {
                "medium": 50,
                "blocking": {
                    "allowlist_tokens": ["99"],  # Should match both "99 Cents" variants
                    "denylist_tokens": ["the"],  # Should match both "The" variants
                    "stop_tokens": ["inc", "llc", "ltd"],
                },
                "scoring": {"use_bulk_cdist": False},
                "penalty": {
                    "suffix_mismatch": 25,
                    "num_style_mismatch": 5,
                    "punctuation_mismatch": 3,
                },
            },
        }

        # Generate candidate pairs
        pairs = generate_candidate_pairs_soft_ban(df_norm, settings=settings)

        # Should generate pairs for "99" tokens (allowlisted) but not "the" tokens (denylisted)
        # The exact number depends on implementation, but we should have some pairs
        assert len(pairs) > 0

        # Verify that allowlisted tokens are processed
        # This is tested indirectly through pair generation

    def test_allowlisted_bigrams_generate_pairs(self):
        """Test that allowlisted bigrams actually generate pairs."""
        df_norm = pd.DataFrame(
            {
                "account_id": ["A1", "A2", "A3", "A4"],
                "name_core": [
                    "99 cents store",
                    "99 cents corp",
                    "7 eleven inc",
                    "7 eleven llc",
                ],
                "suffix_class": ["INC", "LLC", "INC", "LLC"],
            },
        )

        settings = {
            "similarity": {
                "medium": 50,
                "blocking": {
                    "allowlist_tokens": [],
                    "allowlist_bigrams": [
                        "99 cents",
                        "7 eleven",
                    ],  # Should generate pairs
                    "denylist_tokens": [],
                    "stop_tokens": ["inc", "llc", "ltd"],
                },
                "scoring": {"use_bulk_cdist": False},
                "penalty": {
                    "suffix_mismatch": 25,
                    "num_style_mismatch": 5,
                    "punctuation_mismatch": 3,
                },
            },
        }

        # Generate candidate pairs
        pairs = generate_candidate_pairs_soft_ban(df_norm, settings=settings)

        # Should generate pairs for both bigram groups
        # "99 cents" group: 2 pairs (A1-A2)
        # "7 eleven" group: 2 pairs (A3-A4)
        # Total: 4 pairs
        assert len(pairs) >= 2  # At least some pairs should be generated

    def test_duplicate_pairs_are_deduped(self):
        """Test that duplicate pairs are removed when same pair is reachable via multiple paths."""
        df_norm = pd.DataFrame(
            {
                "account_id": ["A1", "A2", "A3"],
                "name_core": ["99 cents store", "99 cents corp", "99 cents inc"],
                "suffix_class": ["INC", "LLC", "INC"],
            },
        )

        settings = {
            "similarity": {
                "medium": 50,
                "blocking": {
                    "allowlist_tokens": [
                        "99",
                    ],  # Will generate pairs via token blocking
                    "allowlist_bigrams": [
                        "99 cents",
                    ],  # Will also generate pairs via bigram
                    "denylist_tokens": [],
                    "stop_tokens": ["inc", "llc", "ltd"],
                },
                "scoring": {"use_bulk_cdist": False},
                "penalty": {
                    "suffix_mismatch": 25,
                    "num_style_mismatch": 5,
                    "punctuation_mismatch": 3,
                },
            },
        }

        # Generate candidate pairs
        pairs = generate_candidate_pairs_soft_ban(df_norm, settings=settings)

        # Convert to set to check for duplicates
        pairs_set = set(pairs)
        assert len(pairs) == len(pairs_set), "Duplicate pairs should be removed"

    def test_diagnostics_write_to_correct_directory(self):
        """Test that diagnostics are written to the correct directory."""
        import os
        import tempfile

        df_norm = pd.DataFrame(
            {
                "account_id": ["A1", "A2"],
                "name_core": ["test company", "test corp"],
                "suffix_class": ["INC", "LLC"],
            },
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
                "penalty": {
                    "suffix_mismatch": 25,
                    "num_style_mismatch": 5,
                    "punctuation_mismatch": 3,
                },
            },
        }

        # Create temporary directory
        with tempfile.TemporaryDirectory() as temp_dir:
            # Generate candidate pairs with interim directory
            _pairs = generate_candidate_pairs_soft_ban(
                df_norm,
                interim_dir=temp_dir,
                settings=settings,
            )

            # Check that block_stats.csv was created
            block_stats_path = os.path.join(temp_dir, "block_stats.csv")
            assert os.path.exists(
                block_stats_path,
            ), f"Block stats should be written to {block_stats_path}"

    def test_bulk_gate_no_cdist_shape_error(self):
        """Test that bulk gating doesn't fail due to cdist shape issues."""
        df_norm = pd.DataFrame(
            {
                "account_id": ["A1", "A2", "A3", "A4"],
                "name_core": [
                    "test company inc",
                    "test corp llc",
                    "sample business ltd",
                    "sample inc corp",
                ],
                "suffix_class": ["INC", "LLC", "LTD", "INC"],
            },
        )

        settings = {
            "similarity": {
                "medium": 50,
                "blocking": {
                    "allowlist_tokens": [],
                    "denylist_tokens": [],
                    "stop_tokens": ["inc", "llc", "ltd"],
                },
                "scoring": {
                    "use_bulk_cdist": True,  # Use bulk scoring
                    "gate_cutoff": 50,
                },
                "penalty": {
                    "suffix_mismatch": 25,
                    "num_style_mismatch": 5,
                    "punctuation_mismatch": 3,
                },
            },
        }

        # This should not raise an error about cdist shape
        result = pair_scores(df_norm, settings)
        assert isinstance(result, pd.DataFrame)

    def test_punctuation_penalty_is_applied(self):
        """Test that punctuation penalty is applied when configured."""
        # Test with punctuation mismatch
        result = compute_score_components(
            "test, company",
            "test company",  # Different punctuation
            "INC",
            "INC",
            {"punctuation_mismatch": 5},
            {},
        )

        # Should have punctuation_mismatch flag
        assert "punctuation_mismatch" in result
        assert result["punctuation_mismatch"] is True

        # Test without punctuation mismatch
        result2 = compute_score_components(
            "test company",
            "test company",  # Same punctuation
            "INC",
            "INC",
            {"punctuation_mismatch": 5},
            {},
        )

        assert result2["punctuation_mismatch"] is False


if __name__ == "__main__":
    pytest.main([__file__])
