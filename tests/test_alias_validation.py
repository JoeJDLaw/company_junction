"""
Tests for alias validation and benchmarking tools.

This module tests the validation scripts and tools created in Phase 1.21.2.
"""

import tempfile
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from src.alias_matching import compute_alias_matches


@pytest.fixture
def sample_data_with_aliases():
    """Create sample data with aliases for testing."""
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

    return df_norm, df_groups


@pytest.fixture
def sample_data_mismatched_sources():
    """Create sample data with mismatched alias_candidates vs alias_sources."""
    df_norm = pd.DataFrame(
        {
            "name_core": ["acme corp", "beta inc"],
            "suffix_class": ["corp", "inc"],
            "alias_candidates": [["acme corp"], ["beta inc", "beta industries"]],
            "alias_sources": [["semicolon"], ["semicolon"]],  # Mismatched length
        }
    )

    df_groups = pd.DataFrame({"group_id": ["group_1", "group_2"]}, index=df_norm.index)

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
            "workers": 2,
            "backend": "threading",
            "chunk_size": 100,
        },
        "alias": {
            "optimize": True,
            "progress_interval_s": 0.1,
        },
    }


def test_equivalence_script_import():
    """Test that the equivalence script can be imported."""
    try:
        from scripts.check_alias_results import check_equivalence, check_determinism

        assert callable(check_equivalence)
        assert callable(check_determinism)
    except ImportError as e:
        pytest.skip(f"Equivalence script not available: {e}")


def test_benchmark_script_import():
    """Test that the benchmark script can be imported."""
    try:
        from scripts.bench_alias import run_pipeline_with_settings

        assert callable(run_pipeline_with_settings)
    except ImportError as e:
        pytest.skip(f"Benchmark script not available: {e}")


def test_bucket_script_import():
    """Test that the bucket script can be imported."""
    try:
        from scripts.check_alias_buckets import extract_first_tokens, analyze_buckets

        assert callable(extract_first_tokens)
        assert callable(analyze_buckets)
    except ImportError as e:
        pytest.skip(f"Bucket script not available: {e}")


def test_alias_equivalence_on_fixture(sample_data_with_aliases, settings):
    """Test that legacy and optimized paths produce equivalent results on fixture."""
    df_norm, df_groups = sample_data_with_aliases

    # Test optimized path
    settings["alias"]["optimize"] = True
    df_optimized, stats_optimized = compute_alias_matches(df_norm, df_groups, settings)

    # Test legacy path
    settings["alias"]["optimize"] = False
    df_legacy, stats_legacy = compute_alias_matches(df_norm, df_groups, settings)

    # Sort both DataFrames for comparison (order-insensitive)
    df_optimized_sorted = df_optimized.sort_values(
        ["record_id", "alias_text", "match_record_id"]
    ).reset_index(drop=True)
    df_legacy_sorted = df_legacy.sort_values(
        ["record_id", "alias_text", "match_record_id"]
    ).reset_index(drop=True)

    # Compare DataFrames
    pd.testing.assert_frame_equal(
        df_optimized_sorted, df_legacy_sorted, check_dtype=False
    )

    # Compare performance stats (timing may differ, but counts should match)
    assert stats_optimized["pairs_generated"] == stats_legacy["pairs_generated"]
    assert stats_optimized["accepted_matches"] == stats_legacy["accepted_matches"]
    assert stats_optimized["capped_blocks"] == stats_legacy["capped_blocks"]


def test_alias_determinism_on_fixture(sample_data_with_aliases, settings):
    """Test that optimized runs produce deterministic results."""
    df_norm, df_groups = sample_data_with_aliases

    # Run optimized path twice
    settings["alias"]["optimize"] = True

    df_run1, stats1 = compute_alias_matches(df_norm, df_groups, settings)
    df_run2, stats2 = compute_alias_matches(df_norm, df_groups, settings)

    # Sort both DataFrames for comparison
    df_run1_sorted = df_run1.sort_values(
        ["record_id", "alias_text", "match_record_id"]
    ).reset_index(drop=True)
    df_run2_sorted = df_run2.sort_values(
        ["record_id", "alias_text", "match_record_id"]
    ).reset_index(drop=True)

    # Results should be identical
    pd.testing.assert_frame_equal(df_run1_sorted, df_run2_sorted, check_dtype=False)

    # Performance stats should be similar (timing may vary slightly)
    assert stats1["pairs_generated"] == stats2["pairs_generated"]
    assert stats1["accepted_matches"] == stats2["accepted_matches"]


def test_edge_case_mismatched_sources(sample_data_mismatched_sources, settings):
    """Test handling of mismatched alias_candidates vs alias_sources lengths."""
    df_norm, df_groups = sample_data_mismatched_sources

    # Both paths should handle the mismatch gracefully
    settings["alias"]["optimize"] = True
    df_optimized, stats_optimized = compute_alias_matches(df_norm, df_groups, settings)

    settings["alias"]["optimize"] = False
    df_legacy, stats_legacy = compute_alias_matches(df_norm, df_groups, settings)

    # Results should be equivalent
    df_optimized_sorted = df_optimized.sort_values(
        ["record_id", "alias_text", "match_record_id"]
    ).reset_index(drop=True)
    df_legacy_sorted = df_legacy.sort_values(
        ["record_id", "alias_text", "match_record_id"]
    ).reset_index(drop=True)

    pd.testing.assert_frame_equal(
        df_optimized_sorted, df_legacy_sorted, check_dtype=False
    )


def test_bucket_analysis_functions():
    """Test the bucket analysis utility functions."""
    try:
        from scripts.check_alias_buckets import extract_first_tokens, analyze_buckets
    except ImportError:
        pytest.skip("Bucket script not available")

    # Test first token extraction
    names = pd.Series(
        [
            "acme corporation",
            "acme corp",
            "beta industries",
            "beta inc",
            "",
            None,
            "gamma solutions",
        ]
    )

    tokens = extract_first_tokens(names)

    # Should have 3 unique tokens with counts
    assert len(tokens) == 3  # 3 unique tokens: acme, beta, gamma
    assert tokens["acme"] == 2  # acme appears twice
    assert tokens["beta"] == 2  # beta appears twice
    assert tokens["gamma"] == 1  # gamma appears once

    # Test bucket analysis with temporary CSV
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write("name_core,suffix_class\n")
        f.write("acme corporation,corp\n")
        f.write("acme corp,corp\n")
        f.write("beta industries,inc\n")
        f.write("beta inc,inc\n")
        f.write("gamma solutions,solutions\n")
        temp_csv = Path(f.name)

    try:
        buckets = analyze_buckets(temp_csv, top_n=3)

        # Should return top 3 buckets
        assert len(buckets) == 3

        # acme and beta should be the top buckets
        top_tokens = [token for token, _ in buckets]
        assert "acme" in top_tokens
        assert "beta" in top_tokens

    finally:
        temp_csv.unlink(missing_ok=True)


def test_checksum_computation():
    """Test checksum computation for determinism checking."""
    try:
        from scripts.check_alias_results import compute_checksum
    except ImportError:
        pytest.skip("Equivalence script not available")

    # Create test DataFrames
    df1 = pd.DataFrame(
        {
            "record_id": [1, 2, 3],
            "alias_text": ["acme corp", "beta inc", "gamma solutions"],
            "score": [95, 87, 92],
        }
    )

    df2 = pd.DataFrame(
        {
            "record_id": [1, 2, 3],
            "alias_text": ["acme corp", "beta inc", "gamma solutions"],
            "score": [95, 87, 92],
        }
    )

    # Same data should produce same checksum
    checksum1 = compute_checksum(df1)
    checksum2 = compute_checksum(df2)
    assert checksum1 == checksum2

    # Different data should produce different checksums
    df3 = pd.DataFrame(
        {
            "record_id": [1, 2, 3],
            "alias_text": ["acme corp", "beta inc", "delta tech"],  # Different
            "score": [95, 87, 92],
        }
    )

    checksum3 = compute_checksum(df3)
    assert checksum3 != checksum1


def test_large_bucket_warning():
    """Test that large bucket warnings are triggered appropriately."""
    # Create data with a large bucket
    large_bucket_size = 15000

    # Create many records with the same first token
    names = ["acme corporation"] * large_bucket_size
    names.extend(["beta industries", "gamma solutions"])  # Add a few others

    df_norm = pd.DataFrame(
        {
            "name_core": names,
            "suffix_class": ["corp"] * large_bucket_size + ["inc", "solutions"],
            "alias_candidates": [["acme corp"]] * large_bucket_size + [[], []],
            "alias_sources": [["semicolon"]] * large_bucket_size + [[], []],
        }
    )

    df_groups = pd.DataFrame(
        {"group_id": ["group_1"] * large_bucket_size + ["group_2", "group_3"]},
        index=df_norm.index,
    )

    settings = {
        "similarity": {"high": 85, "max_alias_pairs": 100000},
        "parallelism": {"workers": 2, "backend": "threading", "chunk_size": 1000},
        "alias": {"optimize": True, "progress_interval_s": 0.1},
    }

    # This should trigger a large bucket warning
    with patch("src.alias_matching.logger") as mock_logger:
        df_result, stats = compute_alias_matches(df_norm, df_groups, settings)

        # Check that warning was logged
        warning_calls = [
            call
            for call in mock_logger.warning.call_args_list
            if "Large first-token bucket" in str(call)
        ]
        assert len(warning_calls) > 0, "Large bucket warning should be logged"


def test_blas_environment_clamping():
    """Test that BLAS environment variables are clamped appropriately."""
    try:
        from src.utils.parallel_utils import ensure_single_thread_blas
    except ImportError:
        pytest.skip("Parallel utils not available")

    import os

    # Test that unset variables are set to "1"
    original_env = os.environ.copy()

    try:
        # Clear BLAS variables
        blas_vars = ["OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS"]
        for var in blas_vars:
            if var in os.environ:
                del os.environ[var]

        # Call the function
        ensure_single_thread_blas()

        # Check that variables are set
        for var in blas_vars:
            assert os.environ[var] == "1"

        # Test that existing values are preserved
        os.environ["OMP_NUM_THREADS"] = "4"
        ensure_single_thread_blas()
        assert os.environ["OMP_NUM_THREADS"] == "4"  # Should be preserved

    finally:
        # Restore original environment
        os.environ.clear()
        os.environ.update(original_env)
