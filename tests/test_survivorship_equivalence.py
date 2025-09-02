#!/usr/bin/env python3
"""
Tests for survivorship equivalence between optimized and original implementations.

Ensures that the hybrid optimization produces identical results to the original logic.
"""

import pandas as pd
import pytest
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.survivorship import select_primary_records


@pytest.fixture
def sample_groups_df():
    """Create a test DataFrame with mixed group sizes and various scenarios."""
    data = {
        "group_id": [
            # Singleton groups
            1,
            2,
            3,
            # Multi-record groups
            10,
            10,
            10,  # 3 records
            20,
            20,  # 2 records
            30,
            30,
            30,
            30,
            30,  # 5 records
            # Edge cases
            -1,
            -1,  # Unassigned (should be skipped)
        ],
        "Relationship": [
            # Singletons
            "Per PNC, Company Name",
            "Per PNC, Company Name",
            "Unknown",
            # Multi-record groups
            "Per PNC, Company Name",
            "Per PNC, Company Name",
            "Unknown",
            "Per PNC, Company Name",
            "Unknown",
            "Per PNC, Company Name",
            "Unknown",
            "Unknown",
            "Unknown",
            "Unknown",
            # Edge cases
            "Unknown",
            "Unknown",
        ],
        "created_date": [
            # Singletons
            "2025-06-14",
            "2025-06-15",
            "2025-06-16",
            # Multi-record groups
            "2025-06-14",
            "2025-06-15",
            "2025-06-16",
            "2025-06-14",
            "2025-06-15",
            "2025-06-14",
            "2025-06-15",
            "2025-06-16",
            "2025-06-17",
            "2025-06-18",
            # Edge cases
            "2025-06-14",
            "2025-06-15",
        ],
        "account_id": [
            # Singletons
            "001Hs000054SD8IIAW",
            "001Hs000054SD8IIBW",
            "001Hs000054SD8IICW",
            # Multi-record groups
            "001Hs000054SD8IIDW",
            "001Hs000054SD8IIEW",
            "001Hs000054SD8IIFW",
            "001Hs000054SD8IIGW",
            "001Hs000054SD8IIHW",
            "001Hs000054SD8IIHW",
            "001Hs000054SD8IIIW",
            "001Hs000054SD8IIJW",
            "001Hs000054SD8IIKW",
            "001Hs000054SD8IILW",
            # Edge cases
            "001Hs000054SD8IIMW",
            "001Hs000054SD8IINW",
        ],
        "account_name": [
            # Singletons
            "Company A",
            "Company B",
            "Company C",
            # Multi-record groups
            "Company D",
            "Company D",
            "Company D",
            "Company E",
            "Company E",
            "Company F",
            "Company F",
            "Company F",
            "Company F",
            "Company F",
            # Edge cases
            "Company G",
            "Company H",
        ],
    }

    df = pd.DataFrame(data)
    df["is_primary"] = False  # Initialize primary flag
    return df


@pytest.fixture
def relationship_ranks():
    """Standard relationship ranking for tests."""
    return {
        "Per PNC, Company Name": 10,  # Highest priority
        "Unknown": 60,  # Default rank
    }


@pytest.fixture
def settings():
    """Test settings with survivorship configuration."""
    return {
        "survivorship": {
            "tie_breakers": ["created_date", "account_id"],
            "optimized": True,  # Will be toggled in tests
        }
    }


class TestSurvivorshipEquivalence:
    """Test that optimized and original implementations produce identical results."""

    def test_singleton_groups_identical(
        self, sample_groups_df, relationship_ranks, settings
    ):
        """Test that singleton groups get identical primary selection."""
        # Test with optimization enabled
        settings["survivorship"]["optimized"] = True
        df_optimized = select_primary_records(
            sample_groups_df.copy(), relationship_ranks, settings
        )

        # Test with optimization disabled
        settings["survivorship"]["optimized"] = False
        df_original = select_primary_records(
            sample_groups_df.copy(), relationship_ranks, settings
        )

        # Check singleton groups (1, 2, 3)
        for group_id in [1, 2, 3]:
            opt_primary = df_optimized[df_optimized["group_id"] == group_id][
                "is_primary"
            ].iloc[0]
            orig_primary = df_original[df_original["group_id"] == group_id][
                "is_primary"
            ].iloc[0]
            assert (
                opt_primary == orig_primary
            ), f"Singleton group {group_id} primary selection differs"
            assert opt_primary, f"Singleton group {group_id} should be primary"

    def test_multi_record_groups_identical(
        self, sample_groups_df, relationship_ranks, settings
    ):
        """Test that multi-record groups get identical primary selection."""
        # Test with optimization enabled
        settings["survivorship"]["optimized"] = True
        df_optimized = select_primary_records(
            sample_groups_df.copy(), relationship_ranks, settings
        )

        # Test with optimization disabled
        settings["survivorship"]["optimized"] = False
        df_original = select_primary_records(
            sample_groups_df.copy(), relationship_ranks, settings
        )

        # Check multi-record groups (10, 20, 30)
        for group_id in [10, 20, 30]:
            opt_primary_idx = df_optimized[df_optimized["group_id"] == group_id][
                "is_primary"
            ].idxmax()
            orig_primary_idx = df_original[df_original["group_id"] == group_id][
                "is_primary"
            ].idxmax()

            # Should select same record as primary
            opt_primary_record = df_optimized.loc[opt_primary_idx]
            orig_primary_record = df_original.loc[orig_primary_idx]

            assert (
                opt_primary_record["account_id"] == orig_primary_record["account_id"]
            ), f"Group {group_id} primary record differs"

            # Verify only one primary per group
            opt_primary_count = df_optimized[df_optimized["group_id"] == group_id][
                "is_primary"
            ].sum()
            orig_primary_count = df_original[df_original["group_id"] == group_id][
                "is_primary"
            ].sum()
            assert (
                opt_primary_count == 1
            ), f"Group {group_id} should have exactly 1 primary (optimized)"
            assert (
                orig_primary_count == 1
            ), f"Group {group_id} should have exactly 1 primary (original)"

    def test_unassigned_groups_skipped(
        self, sample_groups_df, relationship_ranks, settings
    ):
        """Test that unassigned groups (group_id == -1) are properly skipped."""
        # Test with optimization enabled
        settings["survivorship"]["optimized"] = True
        df_optimized = select_primary_records(
            sample_groups_df.copy(), relationship_ranks, settings
        )

        # Test with optimization disabled
        settings["survivorship"]["optimized"] = False
        df_original = select_primary_records(
            sample_groups_df.copy(), relationship_ranks, settings
        )

        # Check unassigned groups
        unassigned_optimized = df_optimized[df_optimized["group_id"] == -1][
            "is_primary"
        ].sum()
        unassigned_original = df_original[df_original["group_id"] == -1][
            "is_primary"
        ].sum()

        # Unassigned groups should not have any primary records
        assert (
            unassigned_optimized == 0
        ), "Unassigned groups should not have primary records (optimized)"
        assert (
            unassigned_original == 0
        ), "Unassigned groups should not have primary records (original)"

    def test_relationship_ranking_identical(
        self, sample_groups_df, relationship_ranks, settings
    ):
        """Test that relationship ranking produces identical results."""
        # Test with optimization enabled
        settings["survivorship"]["optimized"] = True
        df_optimized = select_primary_records(
            sample_groups_df.copy(), relationship_ranks, settings
        )

        # Test with optimization disabled
        settings["survivorship"]["optimized"] = False
        df_original = select_primary_records(
            sample_groups_df.copy(), relationship_ranks, settings
        )

        # Check that relationship_rank column is identical
        # Sort by group_id for consistent comparison (index may differ due to sorting)
        opt_rank_sorted = (
            df_optimized[["group_id", "relationship_rank"]]
            .sort_values("group_id")
            .reset_index(drop=True)
        )
        orig_rank_sorted = (
            df_original[["group_id", "relationship_rank"]]
            .sort_values("group_id")
            .reset_index(drop=True)
        )

        pd.testing.assert_series_equal(
            opt_rank_sorted["relationship_rank"],
            orig_rank_sorted["relationship_rank"],
            check_names=False,
            check_dtype=False,
        )

    def test_tie_breaker_logic_identical(
        self, sample_groups_df, relationship_ranks, settings
    ):
        """Test that tie-breaker logic (created_date, account_id) produces identical results."""
        # Create a scenario where relationship_rank is tied
        test_df = sample_groups_df.copy()
        # Force same relationship rank for group 10
        test_df.loc[test_df["group_id"] == 10, "Relationship"] = "Unknown"

        # Test with optimization enabled
        settings["survivorship"]["optimized"] = True
        df_optimized = select_primary_records(
            test_df.copy(), relationship_ranks, settings
        )

        # Test with optimization disabled
        settings["survivorship"]["optimized"] = False
        df_original = select_primary_records(
            test_df.copy(), relationship_ranks, settings
        )

        # Check that group 10 gets same primary record
        opt_primary_idx = df_optimized[df_optimized["group_id"] == 10][
            "is_primary"
        ].idxmax()
        orig_primary_idx = df_original[df_original["group_id"] == 10][
            "is_primary"
        ].idxmax()

        opt_primary_record = df_optimized.loc[opt_primary_idx]
        orig_primary_record = df_original.loc[orig_primary_idx]

        assert (
            opt_primary_record["account_id"] == orig_primary_record["account_id"]
        ), "Tie-breaker logic should produce identical results"

    def test_unknown_relationship_default_rank(
        self, sample_groups_df, relationship_ranks, settings
    ):
        """Test that unknown relationships get default rank 60."""
        # Test with optimization enabled
        settings["survivorship"]["optimized"] = True
        df_optimized = select_primary_records(
            sample_groups_df.copy(), relationship_ranks, settings
        )

        # Check that unknown relationships get rank 60
        unknown_mask = df_optimized["Relationship"] == "Unknown"
        unknown_ranks = df_optimized.loc[unknown_mask, "relationship_rank"]

        assert all(
            rank == 60 for rank in unknown_ranks
        ), "Unknown relationships should get default rank 60"

    def test_feature_flag_control(self, sample_groups_df, relationship_ranks, settings):
        """Test that the feature flag properly controls optimization."""
        # Test with optimization enabled
        settings["survivorship"]["optimized"] = True
        df_optimized = select_primary_records(
            sample_groups_df.copy(), relationship_ranks, settings
        )

        # Test with optimization disabled
        settings["survivorship"]["optimized"] = False
        df_original = select_primary_records(
            sample_groups_df.copy(), relationship_ranks, settings
        )

        # Results should be identical regardless of optimization setting
        # Sort by group_id for consistent comparison (index may differ due to sorting)
        opt_sorted = (
            df_optimized[["group_id", "is_primary"]]
            .sort_values("group_id")
            .reset_index(drop=True)
        )
        orig_sorted = (
            df_original[["group_id", "is_primary"]]
            .sort_values("group_id")
            .reset_index(drop=True)
        )

        pd.testing.assert_frame_equal(opt_sorted, orig_sorted, check_dtype=False)

    def test_deterministic_results(
        self, sample_groups_df, relationship_ranks, settings
    ):
        """Test that results are deterministic (same input = same output)."""
        # Run optimization multiple times
        results = []
        for _ in range(3):
            df_result = select_primary_records(
                sample_groups_df.copy(), relationship_ranks, settings
            )
            results.append(df_result[["group_id", "is_primary"]])

        # All results should be identical
        for i in range(1, len(results)):
            pd.testing.assert_frame_equal(results[0], results[i], check_dtype=False)


if __name__ == "__main__":
    pytest.main([__file__])
