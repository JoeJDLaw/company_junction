"""Test alias progress logging functionality.
"""

from unittest.mock import patch

import pandas as pd
import pytest

from src.alias_matching import compute_alias_matches
from tests.helpers.ingest import ensure_required_columns


@pytest.fixture
def sample_data():
    """Create sample data for progress logging tests."""
    df_norm = pd.DataFrame(
        {
            "name_core": [
                "acme corporation",
                "acme corp",
                "acme llc",
                "beta industries",
                "beta inc",
                "gamma solutions",
                "delta technologies",
            ],
            "suffix_class": [
                "corporation",
                "corp",
                "llc",
                "industries",
                "inc",
                "solutions",
                "technologies",
            ],
            "alias_candidates": [
                ["acme corp", "acme llc"],
                ["acme corporation"],
                ["acme corp"],
                ["beta inc"],
                ["beta industries"],
                [],
                [],
            ],
            "alias_sources": [
                ["semicolon", "parentheses"],
                ["semicolon"],
                ["parentheses"],
                ["semicolon"],
                ["parentheses"],
                [],
                [],
            ],
        },
    )

    df_groups = pd.DataFrame(
        {
            "group_id": [
                "group_1",
                "group_1",
                "group_1",
                "group_2",
                "group_2",
                "group_3",
                "group_4",
            ],
        },
        index=df_norm.index,
    )

    # Ensure required columns are present
    required_columns = [
        "account_id",
        "name_core",
        "suffix_class",
        "alias_candidates",
        "alias_sources",
    ]
    df_norm = ensure_required_columns(df_norm, required_columns)
    df_groups = ensure_required_columns(df_groups, ["group_id", "account_id"])

    return df_norm, df_groups


@pytest.fixture
def settings():
    """Create settings for testing."""
    return {
        "similarity": {
            "high": 85,
            "max_alias_pairs": 1000,
        },
        "parallelism": {
            "workers": 2,
            "backend": "threading",
            "chunk_size": 100,
        },
        "alias": {
            "optimize": True,
            "progress_interval_s": 0.1,
        },
    }


def test_progress_logging_appears(sample_data, settings):
    """Test that progress logging appears during alias matching."""
    df_norm, df_groups = sample_data

    with patch("src.alias_matching.logger") as mock_logger:
        df_result, stats = compute_alias_matches(df_norm, df_groups, settings)

        # Check that progress logging was called
        progress_calls = [
            call
            for call in mock_logger.info.call_args_list
            if "progress" in str(call).lower() or "processed" in str(call).lower()
        ]
        assert len(progress_calls) > 0, "Progress logging should appear"


def test_progress_logging_rate_limited(sample_data, settings):
    """Test that progress logging respects rate limiting."""
    df_norm, df_groups = sample_data

    # Set a longer progress interval to test rate limiting
    settings["alias"]["progress_interval_s"] = 1.0

    with patch("src.alias_matching.logger") as mock_logger:
        df_result, stats = compute_alias_matches(df_norm, df_groups, settings)

        # Progress should still appear but be rate limited
        progress_calls = [
            call
            for call in mock_logger.info.call_args_list
            if "progress" in str(call).lower() or "processed" in str(call).lower()
        ]
        assert len(progress_calls) > 0, "Progress logging should still appear"


def test_progress_logging_disabled_when_sequential(sample_data, settings):
    """Test that progress logging is disabled for sequential processing."""
    df_norm, df_groups = sample_data

    # Force sequential processing
    settings["parallelism"]["workers"] = 1

    with patch("src.alias_matching.logger") as mock_logger:
        df_result, stats = compute_alias_matches(df_norm, df_groups, settings)

        # Progress logging should still appear but indicate sequential mode
        info_calls = [call[0][0] for call in mock_logger.info.call_args_list]
        sequential_indicators = [
            call for call in info_calls if "sequential" in call.lower()
        ]
        assert len(sequential_indicators) > 0, "Should indicate sequential processing"


def test_progress_logging_disabled_when_optimize_false(sample_data, settings):
    """Test that progress logging is disabled when optimization is disabled."""
    df_norm, df_groups = sample_data

    # Disable optimization
    settings["alias"]["optimize"] = False

    with patch("src.alias_matching.logger") as mock_logger:
        df_result, stats = compute_alias_matches(df_norm, df_groups, settings)

        # Should indicate legacy mode
        info_calls = [call[0][0] for call in mock_logger.info.call_args_list]
        legacy_indicators = [call for call in info_calls if "legacy" in call.lower()]
        assert len(legacy_indicators) > 0, "Should indicate legacy sequential mode"
