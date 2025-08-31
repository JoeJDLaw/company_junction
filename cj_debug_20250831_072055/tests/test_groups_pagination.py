"""
Tests for groups pagination functionality (Phase 1.17.5).

This module tests the PyArrow-based pagination helpers and ensures
stable sorting, proper cache key generation, and correct pagination limits.
"""

import pyarrow as pa
import tempfile
import os
from typing import Dict

from src.utils.ui_helpers import (
    build_sort_expression,
    get_groups_page_pyarrow,
    build_cache_key,
    apply_filters_pyarrow,
    compute_group_stats_pyarrow,
)


class TestSortExpression:
    """Test sort expression generation."""

    def test_build_sort_expression_group_size_desc(self) -> None:
        """Test group size descending sort expression."""
        sort_keys = build_sort_expression("Group Size (Desc)")
        assert sort_keys == [("group_size", "descending"), ("group_id", "ascending")]

    def test_build_sort_expression_group_size_asc(self) -> None:
        """Test group size ascending sort expression."""
        sort_keys = build_sort_expression("Group Size (Asc)")
        assert sort_keys == [("group_size", "ascending"), ("group_id", "ascending")]

    def test_build_sort_expression_max_score_desc(self) -> None:
        """Test max score descending sort expression."""
        sort_keys = build_sort_expression("Max Score (Desc)")
        assert sort_keys == [("max_score", "descending"), ("group_id", "ascending")]

    def test_build_sort_expression_max_score_asc(self) -> None:
        """Test max score ascending sort expression."""
        sort_keys = build_sort_expression("Max Score (Asc)")
        assert sort_keys == [("max_score", "ascending"), ("group_id", "ascending")]

    def test_build_sort_expression_account_name_asc(self) -> None:
        """Test account name ascending sort expression."""
        sort_keys = build_sort_expression("Account Name (Asc)")
        assert sort_keys == [("primary_name", "ascending"), ("group_id", "ascending")]

    def test_build_sort_expression_account_name_desc(self) -> None:
        """Test account name descending sort expression."""
        sort_keys = build_sort_expression("Account Name (Desc)")
        assert sort_keys == [("primary_name", "descending"), ("group_id", "ascending")]

    def test_build_sort_expression_unknown(self) -> None:
        """Test unknown sort expression defaults to group_id."""
        sort_keys = build_sort_expression("Unknown Sort")
        assert sort_keys == [("group_id", "ascending"), ("group_id", "ascending")]


class TestCacheKeyGeneration:
    """Test cache key generation."""

    def test_build_cache_key_basic(self) -> None:
        """Test basic cache key generation."""
        run_id = "test_run_123"
        sort_key = "Group Size (Desc)"
        page = 1
        page_size = 500
        filters = {"dispositions": ["Keep", "Update"], "min_group_size": 2}

        cache_key = build_cache_key(run_id, sort_key, page, page_size, filters)

        # Should contain all components
        assert run_id in cache_key
        assert sort_key in cache_key
        assert "1" in cache_key  # page
        assert "500" in cache_key  # page_size
        assert "|" in cache_key  # separator

    def test_build_cache_key_filter_changes(self) -> None:
        """Test that filter changes generate different cache keys."""
        run_id = "test_run_123"
        sort_key = "Group Size (Desc)"
        page = 1
        page_size = 500

        filters1 = {"dispositions": ["Keep"], "min_group_size": 1}
        filters2 = {"dispositions": ["Keep", "Update"], "min_group_size": 1}

        key1 = build_cache_key(run_id, sort_key, page, page_size, filters1)
        key2 = build_cache_key(run_id, sort_key, page, page_size, filters2)

        assert key1 != key2

    def test_build_cache_key_sort_changes(self) -> None:
        """Test that sort key changes generate different cache keys."""
        run_id = "test_run_123"
        page = 1
        page_size = 500
        filters = {"dispositions": ["Keep"]}

        key1 = build_cache_key(run_id, "Group Size (Desc)", page, page_size, filters)
        key2 = build_cache_key(run_id, "Group Size (Asc)", page, page_size, filters)

        assert key1 != key2

    def test_build_cache_key_page_changes(self) -> None:
        """Test that page changes generate different cache keys."""
        run_id = "test_run_123"
        sort_key = "Group Size (Desc)"
        page_size = 500
        filters = {"dispositions": ["Keep"]}

        key1 = build_cache_key(run_id, sort_key, 1, page_size, filters)
        key2 = build_cache_key(run_id, sort_key, 2, page_size, filters)

        assert key1 != key2


class TestGroupStatsComputation:
    """Test group statistics computation."""

    def test_compute_group_stats_pyarrow_basic(self) -> None:
        """Test basic group stats computation."""
        # Create test data
        data = {
            "group_id": ["group1", "group1", "group2", "group2", "group2"],
            "account_name": [
                "Company A",
                "Company B",
                "Company C",
                "Company D",
                "Company E",
            ],
            "is_primary": [True, False, True, False, False],
            "weakest_edge_to_primary": [95.0, 85.0, 92.0, 88.0, 90.0],
        }

        table = pa.Table.from_pydict(data)
        stats_table = compute_group_stats_pyarrow(table)

        # Convert to pandas for easier testing
        stats_df = stats_table.to_pandas()

        assert len(stats_df) == 2  # Two groups
        assert "group1" in stats_df["group_id"].values
        assert "group2" in stats_df["group_id"].values

        # Check group sizes
        group1_stats = stats_df[stats_df["group_id"] == "group1"].iloc[0]
        group2_stats = stats_df[stats_df["group_id"] == "group2"].iloc[0]

        assert group1_stats["group_size"] == 2
        assert group2_stats["group_size"] == 3

        # Check max scores
        assert group1_stats["max_score"] == 95.0
        assert group2_stats["max_score"] == 92.0

    def test_compute_group_stats_pyarrow_primary_names(self) -> None:
        """Test primary name extraction."""
        data = {
            "group_id": ["group1", "group1"],
            "account_name": ["Primary Company", "Secondary Company"],
            "is_primary": [True, False],
            "weakest_edge_to_primary": [95.0, 85.0],
        }

        table = pa.Table.from_pydict(data)
        stats_table = compute_group_stats_pyarrow(table)
        stats_df = stats_table.to_pandas()

        group1_stats = stats_df[stats_df["group_id"] == "group1"].iloc[0]
        assert group1_stats["primary_name"] == "Primary Company"

    def test_compute_group_stats_pyarrow_no_primary(self) -> None:
        """Test handling when no primary record exists."""
        data = {
            "group_id": ["group1", "group1"],
            "account_name": ["Company A", "Company B"],
            "is_primary": [False, False],
            "weakest_edge_to_primary": [95.0, 85.0],
        }

        table = pa.Table.from_pydict(data)
        stats_table = compute_group_stats_pyarrow(table)
        stats_df = stats_table.to_pandas()

        group1_stats = stats_df[stats_df["group_id"] == "group1"].iloc[0]
        assert group1_stats["primary_name"] == "Company A"  # First record


class TestFilterApplication:
    """Test filter application to PyArrow tables."""

    def test_apply_filters_pyarrow_disposition(self) -> None:
        """Test disposition filtering."""
        data = {
            "group_id": ["group1", "group2", "group3"],
            "Disposition": ["Keep", "Update", "Delete"],
            "weakest_edge_to_primary": [95.0, 85.0, 75.0],
        }

        table = pa.Table.from_pydict(data)
        filters = {"dispositions": ["Keep", "Update"]}

        filtered_table = apply_filters_pyarrow(table, filters)
        filtered_df = filtered_table.to_pandas()

        assert len(filtered_df) == 2
        assert "Delete" not in filtered_df["Disposition"].values

    def test_apply_filters_pyarrow_edge_strength(self) -> None:
        """Test edge strength filtering."""
        data = {
            "group_id": ["group1", "group2", "group3"],
            "weakest_edge_to_primary": [95.0, 85.0, 75.0],
        }

        table = pa.Table.from_pydict(data)
        filters = {"min_edge_strength": 80.0}

        filtered_table = apply_filters_pyarrow(table, filters)
        filtered_df = filtered_table.to_pandas()

        assert len(filtered_df) == 2
        assert filtered_df["weakest_edge_to_primary"].min() >= 80.0

    def test_apply_filters_pyarrow_no_filters(self) -> None:
        """Test that no filters returns original table."""
        data = {
            "group_id": ["group1", "group2"],
            "weakest_edge_to_primary": [95.0, 85.0],
        }

        table = pa.Table.from_pydict(data)
        filters = {}

        filtered_table = apply_filters_pyarrow(table, filters)
        assert filtered_table.num_rows == table.num_rows


class TestPaginationLimits:
    """Test pagination limits and ranges."""

    def test_pagination_limits_empty_data(self) -> None:
        """Test pagination with empty data."""
        # Create temporary parquet file with empty data
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create empty parquet file
            empty_data = {
                "group_id": [],
                "account_name": [],
                "is_primary": [],
                "weakest_edge_to_primary": [],
            }
            empty_table = pa.Table.from_pydict(empty_data)

            parquet_path = os.path.join(temp_dir, "review_ready.parquet")
            pa.parquet.write_table(empty_table, parquet_path)

            # Mock artifact paths
            def mock_get_artifact_paths(run_id: str) -> Dict[str, str]:
                return {"review_ready_parquet": parquet_path}

            # Patch the function
            import src.utils.ui_helpers

            original_get_artifact_paths = src.utils.ui_helpers.get_artifact_paths
            src.utils.ui_helpers.get_artifact_paths = mock_get_artifact_paths

            try:
                page_groups, total_count = get_groups_page_pyarrow(
                    "test_run", "Group Size (Desc)", 1, 500, {}
                )

                assert page_groups == []
                assert total_count == 0
            finally:
                src.utils.ui_helpers.get_artifact_paths = original_get_artifact_paths

    def test_pagination_limits_page_bounds(self) -> None:
        """Test pagination with page bounds."""
        # Create test data with known number of groups
        data = {
            "group_id": [f"group{i}" for i in range(10)],
            "account_name": [f"Company {i}" for i in range(10)],
            "is_primary": [True] + [False] * 9,
            "weakest_edge_to_primary": [95.0] * 10,
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            table = pa.Table.from_pydict(data)
            parquet_path = os.path.join(temp_dir, "review_ready.parquet")
            pa.parquet.write_table(table, parquet_path)

            # Mock artifact paths
            def mock_get_artifact_paths(run_id: str) -> Dict[str, str]:
                return {"review_ready_parquet": parquet_path}

            import src.utils.ui_helpers

            original_get_artifact_paths = src.utils.ui_helpers.get_artifact_paths
            src.utils.ui_helpers.get_artifact_paths = mock_get_artifact_paths

            try:
                # Test first page
                page_groups, total_count = get_groups_page_pyarrow(
                    "test_run", "Group Size (Desc)", 1, 5, {}
                )

                assert len(page_groups) == 5
                assert total_count == 10

                # Test second page
                page_groups, total_count = get_groups_page_pyarrow(
                    "test_run", "Group Size (Desc)", 2, 5, {}
                )

                assert len(page_groups) == 5
                assert total_count == 10

                # Test page beyond bounds
                page_groups, total_count = get_groups_page_pyarrow(
                    "test_run", "Group Size (Desc)", 3, 5, {}
                )

                assert page_groups == []
                assert total_count == 10
            finally:
                src.utils.ui_helpers.get_artifact_paths = original_get_artifact_paths


class TestSortingStability:
    """Test sorting stability."""

    def test_sorting_stability_group_id_tiebreaker(self) -> None:
        """Test that group_id tiebreaker ensures stable sorting."""
        # Create test data with same values but different group_ids
        data = {
            "group_id": [
                "group_b",
                "group_b",
                "group_a",
                "group_a",
                "group_c",
                "group_c",
            ],
            "account_name": [
                "Company B1",
                "Company B2",
                "Company A1",
                "Company A2",
                "Company C1",
                "Company C2",
            ],
            "is_primary": [True, False, True, False, True, False],
            "weakest_edge_to_primary": [90.0, 90.0, 90.0, 90.0, 90.0, 90.0],
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            table = pa.Table.from_pydict(data)
            parquet_path = os.path.join(temp_dir, "review_ready.parquet")
            pa.parquet.write_table(table, parquet_path)

            # Mock artifact paths
            def mock_get_artifact_paths(run_id: str) -> Dict[str, str]:
                return {"review_ready_parquet": parquet_path}

            import src.utils.ui_helpers

            original_get_artifact_paths = src.utils.ui_helpers.get_artifact_paths
            src.utils.ui_helpers.get_artifact_paths = mock_get_artifact_paths

            try:
                # Test ascending sort - should be stable by group_id
                page_groups, _ = get_groups_page_pyarrow(
                    "test_run", "Group Size (Asc)", 1, 10, {}
                )

                # Should be sorted by group_id (ascending) as tiebreaker
                group_ids = [group["group_id"] for group in page_groups]
                assert group_ids == ["group_a", "group_b", "group_c"]

                # Test descending sort - should still be stable by group_id
                page_groups, _ = get_groups_page_pyarrow(
                    "test_run", "Group Size (Desc)", 1, 10, {}
                )

                # Should still be sorted by group_id (ascending) as tiebreaker
                group_ids = [group["group_id"] for group in page_groups]
                assert group_ids == ["group_a", "group_b", "group_c"]
            finally:
                src.utils.ui_helpers.get_artifact_paths = original_get_artifact_paths


class TestCacheKeyInvalidation:
    """Test cache key invalidation."""

    def test_cache_key_changes_on_sort(self) -> None:
        """Test that cache key changes when sort key changes."""
        run_id = "test_run"
        page = 1
        page_size = 500
        filters = {"dispositions": ["Keep"]}

        key1 = build_cache_key(run_id, "Group Size (Desc)", page, page_size, filters)
        key2 = build_cache_key(run_id, "Group Size (Asc)", page, page_size, filters)
        key3 = build_cache_key(run_id, "Max Score (Desc)", page, page_size, filters)

        assert key1 != key2
        assert key1 != key3
        assert key2 != key3

    def test_cache_key_changes_on_filters(self) -> None:
        """Test that cache key changes when filters change."""
        run_id = "test_run"
        sort_key = "Group Size (Desc)"
        page = 1
        page_size = 500

        filters1 = {"dispositions": ["Keep"]}
        filters2 = {"dispositions": ["Keep", "Update"]}
        filters3 = {"dispositions": ["Keep"], "min_group_size": 2}

        key1 = build_cache_key(run_id, sort_key, page, page_size, filters1)
        key2 = build_cache_key(run_id, sort_key, page, page_size, filters2)
        key3 = build_cache_key(run_id, sort_key, page, page_size, filters3)

        assert key1 != key2
        assert key1 != key3
        assert key2 != key3

    def test_cache_key_changes_on_page(self) -> None:
        """Test that cache key changes when page changes."""
        run_id = "test_run"
        sort_key = "Group Size (Desc)"
        page_size = 500
        filters = {"dispositions": ["Keep"]}

        key1 = build_cache_key(run_id, sort_key, 1, page_size, filters)
        key2 = build_cache_key(run_id, sort_key, 2, page_size, filters)
        key3 = build_cache_key(run_id, sort_key, 3, page_size, filters)

        assert key1 != key2
        assert key1 != key3
        assert key2 != key3
