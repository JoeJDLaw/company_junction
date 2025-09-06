"""Tests for brand suggestions functionality."""

import sys
from pathlib import Path

import pandas as pd
import pytest

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.similarity.diagnostics import generate_brand_suggestions


class TestBrandSuggestions:
    """Test cases for brand suggestions."""

    def test_generate_brand_suggestions_basic(self):
        """Test basic brand suggestions generation."""
        # Mock block stats
        block_stats = [
            {"token": "test", "count": 15, "pairs_generated": 100, "pairs_capped": 0},
            {"token": "sample", "count": 5, "pairs_generated": 10, "pairs_capped": 0},
            {"token": "demo", "count": 20, "pairs_generated": 200, "pairs_capped": 50},
        ]

        # Mock groups data
        groups_df = pd.DataFrame(
            {
                "group_id": [1, 2, 3, 4, 5],
                "group_size": [1, 2, 1, 3, 1],  # 3 singletons out of 5 groups
                "name_core": [
                    "test company",
                    "sample corp",
                    "demo inc",
                    "test llc",
                    "demo corp",
                ],
            },
        )

        # Mock settings
        settings = {
            "similarity": {
                "blocking": {
                    "min_suggestion_count": 10,
                    "min_singleton_pct": 0.1,  # Lower threshold for testing
                    "allowlist_tokens": [],
                    "denylist_tokens": [],
                },
            },
        }

        suggestions = generate_brand_suggestions(block_stats, groups_df, settings)

        # Should have suggestions for tokens meeting criteria
        assert len(suggestions) > 0

        # Check suggestion structure
        for suggestion in suggestions:
            assert "token" in suggestion
            assert "count" in suggestion
            assert "pct_singletons" in suggestion
            assert "suggestion_confidence" in suggestion
            assert 0.0 <= suggestion["suggestion_confidence"] <= 1.0

    def test_generate_brand_suggestions_exclude_configured_tokens(self):
        """Test that configured tokens are excluded from suggestions."""
        block_stats = [
            {"token": "test", "count": 15, "pairs_generated": 100, "pairs_capped": 0},
            {"token": "sample", "count": 20, "pairs_generated": 200, "pairs_capped": 0},
        ]

        groups_df = pd.DataFrame(
            {
                "group_id": [1, 2],
                "group_size": [1, 1],
                "name_core": ["test company", "sample corp"],
            },
        )

        settings = {
            "similarity": {
                "blocking": {
                    "min_suggestion_count": 10,
                    "min_singleton_pct": 0.6,
                    "allowlist_tokens": ["test"],  # Already in allowlist
                    "denylist_tokens": ["sample"],  # Already in denylist
                },
            },
        }

        suggestions = generate_brand_suggestions(block_stats, groups_df, settings)

        # Should have no suggestions since both tokens are already configured
        assert len(suggestions) == 0

    def test_generate_brand_suggestions_minimum_criteria(self):
        """Test that minimum criteria are applied correctly."""
        block_stats = [
            {
                "token": "low_count",
                "count": 5,
                "pairs_generated": 10,
                "pairs_capped": 0,
            },
            {
                "token": "high_count",
                "count": 15,
                "pairs_generated": 100,
                "pairs_capped": 0,
            },
        ]

        groups_df = pd.DataFrame(
            {
                "group_id": [1, 2],
                "group_size": [1, 1],
                "name_core": ["low count corp", "high count inc"],
            },
        )

        settings = {
            "similarity": {
                "blocking": {
                    "min_suggestion_count": 10,  # Only high_count meets this
                    "min_singleton_pct": 0.1,  # Lower threshold for testing
                    "allowlist_tokens": [],
                    "denylist_tokens": [],
                },
            },
        }

        suggestions = generate_brand_suggestions(block_stats, groups_df, settings)

        # Should only suggest high_count (meets minimum count)
        suggested_tokens = [s["token"] for s in suggestions]
        assert "high_count" in suggested_tokens
        assert "low_count" not in suggested_tokens

    def test_generate_brand_suggestions_empty_inputs(self):
        """Test handling of empty inputs."""
        # Empty block stats
        suggestions = generate_brand_suggestions([], pd.DataFrame(), {})
        assert suggestions == []

        # Empty groups
        block_stats = [
            {"token": "test", "count": 15, "pairs_generated": 100, "pairs_capped": 0},
        ]
        suggestions = generate_brand_suggestions(block_stats, pd.DataFrame(), {})
        assert suggestions == []

    def test_generate_brand_suggestions_confidence_calculation(self):
        """Test that confidence scores are calculated correctly."""
        block_stats = [
            {
                "token": "high_conf",
                "count": 50,
                "pairs_generated": 500,
                "pairs_capped": 0,
            },
            {
                "token": "low_conf",
                "count": 10,
                "pairs_generated": 50,
                "pairs_capped": 0,
            },
        ]

        groups_df = pd.DataFrame(
            {
                "group_id": [1, 2],
                "group_size": [1, 1],
                "name_core": ["high conf corp", "low conf inc"],
            },
        )

        settings = {
            "similarity": {
                "blocking": {
                    "min_suggestion_count": 10,
                    "min_singleton_pct": 0.6,
                    "allowlist_tokens": [],
                    "denylist_tokens": [],
                },
            },
        }

        suggestions = generate_brand_suggestions(block_stats, groups_df, settings)

        # Should be sorted by confidence (highest first)
        if len(suggestions) >= 2:
            assert (
                suggestions[0]["suggestion_confidence"]
                >= suggestions[1]["suggestion_confidence"]
            )

        # High count should have higher confidence
        high_conf_suggestion = next(
            (s for s in suggestions if s["token"] == "high_conf"),
            None,
        )
        low_conf_suggestion = next(
            (s for s in suggestions if s["token"] == "low_conf"),
            None,
        )

        if high_conf_suggestion and low_conf_suggestion:
            assert (
                high_conf_suggestion["suggestion_confidence"]
                > low_conf_suggestion["suggestion_confidence"]
            )


if __name__ == "__main__":
    pytest.main([__file__])
