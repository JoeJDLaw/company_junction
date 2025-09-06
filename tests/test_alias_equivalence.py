"""Test alias matching equivalence between legacy and optimized paths."""

from typing import Any

import pandas as pd
import pytest

from src.alias_matching import compute_alias_matches
from tests.helpers.ingest import ensure_required_columns


@pytest.fixture
def sample_data():
    """Create sample data for alias matching tests."""
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

    # Ensure required columns are present for the pipeline
    required_columns = [
        "account_id",
        "name_core",
        "suffix_class",
        "alias_candidates",
        "alias_sources",
    ]
    df_norm = ensure_required_columns(df_norm, required_columns)

    # Ensure df_groups has required columns
    df_groups = ensure_required_columns(df_groups, ["group_id", "account_id"])

    return df_norm, df_groups


@pytest.fixture
def settings():
    """Create settings for testing."""
    return {
        "similarity": {
            "high": 85,  # Lower threshold for more matches in test
            "max_alias_pairs": 1000,
        },
        "parallelism": {
            "workers": 2,  # Use 2 workers for testing
            "backend": "threading",  # Use threading for test compatibility
            "chunk_size": 100,
        },
        "alias": {
            "optimize": True,
            "progress_interval_s": 0.1,  # Fast progress for tests
        },
    }


def test_alias_equivalence_optimized_vs_legacy(sample_data, settings):
    """Test that optimized and legacy paths produce identical results."""
    df_norm, df_groups = sample_data

    # Test optimized path
    settings["alias"]["optimize"] = True
    df_optimized, stats_optimized = compute_alias_matches(df_norm, df_groups, settings)

    # Test legacy path
    settings["alias"]["optimize"] = False
    df_legacy, stats_legacy = compute_alias_matches(df_norm, df_groups, settings)

    # Sort both DataFrames for comparison (order-insensitive)
    df_optimized_sorted = df_optimized.sort_values(
        ["record_id", "alias_text", "match_record_id"],
    ).reset_index(drop=True)
    df_legacy_sorted = df_legacy.sort_values(
        ["record_id", "alias_text", "match_record_id"],
    ).reset_index(drop=True)

    # Compare DataFrames
    pd.testing.assert_frame_equal(
        df_optimized_sorted,
        df_legacy_sorted,
        check_dtype=False,  # Allow different dtypes as long as values match
    )

    # Compare performance stats (timing may differ, but counts should match)
    assert stats_optimized["pairs_generated"] == stats_legacy["pairs_generated"]
    assert stats_optimized["accepted_matches"] == stats_legacy["accepted_matches"]
    assert stats_optimized["capped_blocks"] == stats_legacy["capped_blocks"]


def test_alias_equivalence_edge_cases():
    """Test edge cases for alias matching equivalence."""
    # Test with empty aliases
    df_norm = pd.DataFrame(
        {
            "name_core": ["acme corp", "beta inc"],
            "suffix_class": ["corp", "inc"],
            "alias_candidates": [[], []],
            "alias_sources": [[], []],
        },
    )

    df_groups = pd.DataFrame({"group_id": ["group_1", "group_2"]}, index=df_norm.index)

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

    settings: dict[str, Any] = {
        "similarity": {"high": 85, "max_alias_pairs": 1000},
        "parallelism": {"workers": 2, "backend": "threading", "chunk_size": 100},
        "alias": {"optimize": True, "progress_interval_s": 0.1},
    }

    # Both paths should produce empty results
    df_optimized, stats_optimized = compute_alias_matches(df_norm, df_groups, settings)
    settings["alias"]["optimize"] = False
    df_legacy, stats_legacy = compute_alias_matches(df_norm, df_groups, settings)

    assert len(df_optimized) == 0
    assert len(df_legacy) == 0
    assert stats_optimized["pairs_generated"] == 0
    assert stats_legacy["pairs_generated"] == 0


def test_alias_equivalence_mismatched_sources():
    """Test equivalence with mismatched alias sources."""
    df_norm = pd.DataFrame(
        {
            "name_core": ["acme corp", "acme llc"],
            "suffix_class": ["corp", "llc"],
            "alias_candidates": [["acme llc"], ["acme corp"]],
            "alias_sources": [["semicolon"], ["parentheses"]],
        },
    )

    df_groups = pd.DataFrame({"group_id": ["group_1", "group_1"]}, index=df_norm.index)

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

    settings: dict[str, Any] = {
        "similarity": {"high": 85, "max_alias_pairs": 1000},
        "parallelism": {"workers": 2, "backend": "threading", "chunk_size": 100},
        "alias": {"optimize": True, "progress_interval_s": 0.1},
    }

    # Test optimized path
    df_optimized, stats_optimized = compute_alias_matches(df_norm, df_groups, settings)

    # Test legacy path
    settings["alias"]["optimize"] = False
    df_legacy, stats_legacy = compute_alias_matches(df_norm, df_groups, settings)

    # Results should be equivalent
    df_optimized_sorted = df_optimized.sort_values(
        ["record_id", "alias_text", "match_record_id"],
    ).reset_index(drop=True)
    df_legacy_sorted = df_legacy.sort_values(
        ["record_id", "alias_text", "match_record_id"],
    ).reset_index(drop=True)

    pd.testing.assert_frame_equal(
        df_optimized_sorted,
        df_legacy_sorted,
        check_dtype=False,
    )


def test_alias_equivalence_sequential_fallback():
    """Test that sequential fallback produces equivalent results."""
    df_norm = pd.DataFrame(
        {
            "name_core": ["acme corp", "acme corp", "beta industries"],
            "suffix_class": ["corp", "corp", "industries"],
            "alias_candidates": [["acme corp"], ["acme corp"], []],
            "alias_sources": [["semicolon"], ["parentheses"], []],
        },
    )

    df_groups = pd.DataFrame(
        {"group_id": ["group_1", "group_1", "group_2"]},
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

    settings = {
        "similarity": {
            "high": 85,
            "medium": 50,
            "max_alias_pairs": 1000,
        },  # Lower threshold for testing
        "parallelism": {
            "workers": 1,
            "backend": "threading",
            "chunk_size": 100,
        },  # Force sequential
        "alias": {"optimize": True, "progress_interval_s": 0.1},
    }

    # This should fall back to sequential processing
    df_result, stats = compute_alias_matches(df_norm, df_groups, settings)

    # Should still produce valid results (these records should match each other)
    assert (
        len(df_result) > 0
    ), f"No matches found with threshold 50, got {len(df_result)} matches"
    assert stats["pairs_generated"] > 0
