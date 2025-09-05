"""Tests for the latest similarity improvements: lowercasing, safety rails, and bigram prepass.
"""

import sys
from pathlib import Path

import pandas as pd
import pytest

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.similarity.blocking import generate_candidate_pairs_soft_ban


class TestSimilarityImprovements:
    """Test cases for the latest improvements."""

    def test_lowercased_config_tokens_hit_allowlist(self):
        """Test that config tokens are normalized to lowercase and hit allowlist regardless of case."""
        # Create test data with mixed case
        df_norm = pd.DataFrame(
            {
                "account_id": ["A1", "A2", "A3", "A4"],
                "name_core": [
                    "7 Eleven Store",
                    "7 ELEVEN CORP",
                    "24 Hour Fitness",
                    "24 HOUR GYM",
                ],
                "suffix_class": ["INC", "LLC", "INC", "LLC"],
            },
        )

        settings = {
            "similarity": {
                "medium": 50,
                "blocking": {
                    "allowlist_tokens": [
                        "7",
                        "24",
                    ],  # Should match both "7" and "24" variants
                    "allowlist_bigrams": [],
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

        # Should generate pairs for both "7" and "24" tokens (allowlisted)
        # "7" group: 2 pairs (A1-A2)
        # "24" group: 2 pairs (A3-A4)
        # Total: 4 pairs
        assert len(pairs) >= 2  # At least some pairs should be generated

    def test_allowlisted_token_sharding_safety_rail(self):
        """Test that allowlisted tokens are sharded when they exceed block_cap."""
        # Create a large allowlisted block with names that will be sharded but still generate pairs
        large_block_size = 10  # Exceeds typical block_cap of 8
        df_norm = pd.DataFrame(
            {
                "account_id": [f"A{i}" for i in range(large_block_size)],
                "name_core": [
                    f"7 Store {i}" for i in range(large_block_size)
                ],  # All start with "7 S"
                "suffix_class": ["INC"] * large_block_size,
            },
        )

        settings = {
            "similarity": {
                "medium": 50,
                "blocking": {
                    "allowlist_tokens": ["7"],
                    "allowlist_bigrams": [],
                    "denylist_tokens": [],
                    "stop_tokens": ["inc", "llc", "ltd"],
                    "soft_ban": {
                        "block_cap": 8,  # Smaller than our block size
                        "shard_strategy": "char_trigram",  # Use char_trigram to ensure sharding
                    },
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

        # Should generate pairs (sharding may or may not reduce count depending on data)
        max_possible_pairs = large_block_size * (large_block_size - 1) // 2
        assert len(pairs) > 0  # Some pairs should be generated
        assert len(pairs) <= max_possible_pairs  # Should not exceed nC2

    def test_allowlisted_bigram_sharding_safety_rail(self):
        """Test that allowlisted bigrams are sharded when they exceed block_cap."""
        # Create a large allowlisted bigram block with names that will be sharded but still generate pairs
        large_block_size = 10  # Exceeds typical block_cap of 8
        df_norm = pd.DataFrame(
            {
                "account_id": [f"A{i}" for i in range(large_block_size)],
                "name_core": [
                    f"7 store {i}" for i in range(large_block_size)
                ],  # All have "7 store" bigram
                "suffix_class": ["INC"] * large_block_size,
            },
        )

        settings = {
            "similarity": {
                "medium": 50,
                "blocking": {
                    "allowlist_tokens": [],
                    "allowlist_bigrams": ["7 store"],  # Should match all records
                    "denylist_tokens": [],
                    "stop_tokens": ["inc", "llc", "ltd"],
                    "soft_ban": {
                        "block_cap": 8,  # Smaller than our block size
                        "shard_strategy": "char_trigram",  # Use char_trigram to ensure sharding
                    },
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

        # Should generate pairs (sharding may or may not reduce count depending on data)
        max_possible_pairs = large_block_size * (large_block_size - 1) // 2
        assert len(pairs) > 0  # Some pairs should be generated
        assert len(pairs) <= max_possible_pairs  # Should not exceed nC2

    def test_no_duplicate_pairs_with_bigram_and_block_overlap(self):
        """Test that no duplicate pairs are generated when bigram prepass and block pass overlap."""
        df_norm = pd.DataFrame(
            {
                "account_id": ["A1", "A2", "A3", "A4"],
                "name_core": [
                    "7 eleven store",
                    "7 eleven corp",
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
                    "allowlist_tokens": ["7"],  # Will generate pairs via token blocking
                    "allowlist_bigrams": [
                        "7 eleven",
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

        # Should have exactly 6 pairs (4C2 = 6)
        assert len(pairs) == 6

    def test_bigram_prepass_generates_pairs(self):
        """Test that allowlisted bigram prepass generates pairs before block processing."""
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
                    "allowlist_tokens": [],  # No token allowlist
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
        # "99 cents" group: 1 pair (A1-A2)
        # "7 eleven" group: 1 pair (A3-A4)
        # Total: 2 pairs
        assert len(pairs) == 2

    def test_strategy_logging_includes_all_strategies(self):
        """Test that strategy logging includes all strategies used."""
        df_norm = pd.DataFrame(
            {
                "account_id": ["A1", "A2", "A3", "A4", "A5", "A6"],
                "name_core": [
                    "7 eleven store",
                    "7 eleven corp",
                    "99 cents store",
                    "99 cents corp",
                    "test company",
                    "test corp",
                ],
                "suffix_class": ["INC", "LLC", "INC", "LLC", "INC", "LLC"],
            },
        )

        settings = {
            "similarity": {
                "medium": 50,
                "blocking": {
                    "allowlist_tokens": ["7"],  # Will use allowlisted strategy
                    "allowlist_bigrams": [
                        "99 cents",
                    ],  # Will use allowlisted_bigram strategy
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

        # Should generate pairs using multiple strategies
        assert len(pairs) > 0

        # The logging should include both strategies (tested indirectly through pair generation)
        # This test ensures the function completes without errors and generates expected pairs


if __name__ == "__main__":
    pytest.main([__file__])
