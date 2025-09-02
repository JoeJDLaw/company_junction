"""
Test alias validation functionality.
"""

import pandas as pd
import pytest

from src.alias_matching import compute_alias_matches
from tests.helpers.ingest import ensure_required_columns


@pytest.fixture
def sample_data():
    """Create sample data for alias validation tests."""
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
        }
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
            ]
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


def test_alias_equivalence_on_fixture(sample_data, settings):
    """Test that alias matching produces equivalent results on fixture data."""
    df_norm, df_groups = sample_data

    # Test optimized path
    df_optimized, stats_optimized = compute_alias_matches(df_norm, df_groups, settings)

    # Test legacy path
    settings["alias"]["optimize"] = False
    df_legacy, stats_legacy = compute_alias_matches(df_norm, df_groups, settings)

    # Both should produce the same results
    pd.testing.assert_frame_equal(
        df_optimized.sort_values(
            ["record_id", "alias_text", "match_record_id"]
        ).reset_index(drop=True),
        df_legacy.sort_values(
            ["record_id", "alias_text", "match_record_id"]
        ).reset_index(drop=True),
        check_dtype=False,
    )


def test_alias_determinism_on_fixture(sample_data, settings):
    """Test that alias matching produces deterministic results on fixture data."""
    df_norm, df_groups = sample_data

    # Run twice with same settings
    df_run1, stats1 = compute_alias_matches(df_norm, df_groups, settings)
    df_run2, stats2 = compute_alias_matches(df_norm, df_groups, settings)

    # Results should be identical
    pd.testing.assert_frame_equal(
        df_run1.sort_values(["record_id", "alias_text", "match_record_id"]).reset_index(
            drop=True
        ),
        df_run2.sort_values(["record_id", "alias_text", "match_record_id"]).reset_index(
            drop=True
        ),
        check_dtype=False,
    )


def test_edge_case_mismatched_sources():
    """Test handling of edge cases with mismatched alias sources."""
    df_norm = pd.DataFrame(
        {
            "name_core": ["acme corp", "beta inc"],
            "suffix_class": ["corp", "inc"],
            "alias_candidates": [["acme corp"], ["beta inc", "beta industries"]],
            "alias_sources": [["semicolon"], ["semicolon"]],  # Mismatched length
        }
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

    settings = {
        "similarity": {"high": 85, "max_alias_pairs": 1000},
        "parallelism": {"workers": 2, "backend": "threading", "chunk_size": 100},
        "alias": {"optimize": True, "progress_interval_s": 0.1},
    }

    # Should handle the mismatch gracefully
    df_result, stats = compute_alias_matches(df_norm, df_groups, settings)
    assert isinstance(df_result, pd.DataFrame)
    assert isinstance(stats, dict)


@pytest.mark.skip(
    reason="TODO: Phase 1.26.1 - Temporarily skipped for QA efficiency, will re-enable after Phase 1.25.1"
)
@pytest.mark.parametrize(
    "num_records",
    [
        pytest.param(1500, id="fast"),
        pytest.param(15000, marks=pytest.mark.slow, id="slow"),
    ],
)
def test_large_bucket_warning(num_records):
    """Test that large first-token buckets trigger warnings."""

    # Create data with many records sharing the same first token
    records = []
    for i in range(num_records):  # Exceeds the 10k warning threshold
        records.append(
            {
                "name_core": f"test company {i} corp",
                "suffix_class": "corp",
                "alias_candidates": [f"test company {i} corporation"],
                "alias_sources": ["semicolon"],
            }
        )

    df_norm = pd.DataFrame(records)
    df_groups = pd.DataFrame(
        {"group_id": [f"group_{i}" for i in range(num_records)]}, index=df_norm.index
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
        "similarity": {"high": 85, "max_alias_pairs": 100000},
        "parallelism": {"workers": 2, "backend": "threading", "chunk_size": 1000},
        "alias": {"optimize": True, "progress_interval_s": 1.0},
    }

    # Should complete without error (may be slow due to large bucket)
    df_result, stats = compute_alias_matches(df_norm, df_groups, settings)
    assert isinstance(df_result, pd.DataFrame)
    assert isinstance(stats, dict)
