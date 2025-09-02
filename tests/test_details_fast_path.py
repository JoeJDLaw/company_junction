"""
Tests for Phase 1.23.1 Details Fast Path functionality.

Tests the new group_details.parquet generation, DuckDB-first loading,
caching behavior, auto-load functionality, and error handling.
"""

import pytest
import pandas as pd
from unittest.mock import patch, MagicMock

from src.utils.ui_helpers import (
    DetailsCache,
    get_group_details_duckdb,
    _get_parquet_fingerprint,
)
from src.utils.schema_utils import (
    GROUP_ID,
    ACCOUNT_ID,
    ACCOUNT_NAME,
    SUFFIX_CLASS,
    CREATED_DATE,
    DISPOSITION,
)


class TestDetailsCache:
    """Test the DetailsCache LRU implementation."""

    def test_cache_initialization(self) -> None:
        """Test cache initialization with default capacity."""
        cache = DetailsCache()
        assert cache.capacity == 16
        assert len(cache.cache) == 0
        assert len(cache.access_order) == 0

    def test_cache_put_and_get(self) -> None:
        """Test basic put and get operations."""
        cache = DetailsCache(capacity=2)
        key1 = ("run1", "fp1", "group1", "duckdb")
        key2 = ("run1", "fp1", "group2", "duckdb")

        # Put items
        cache.put(key1, [{"id": 1}])
        cache.put(key2, [{"id": 2}])

        # Get items
        result1 = cache.get(key1)
        result2 = cache.get(key2)

        assert result1 == [{"id": 1}]
        assert result2 == [{"id": 2}]
        assert len(cache.cache) == 2

    def test_cache_lru_eviction(self) -> None:
        """Test LRU eviction when capacity is exceeded."""
        cache = DetailsCache(capacity=2)
        key1 = ("run1", "fp1", "group1", "duckdb")
        key2 = ("run1", "fp1", "group2", "duckdb")
        key3 = ("run1", "fp1", "group3", "duckdb")

        # Fill cache
        cache.put(key1, [{"id": 1}])
        cache.put(key2, [{"id": 2}])

        # Access key1 to make it most recently used
        cache.get(key1)

        # Add key3, should evict key2 (least recently used)
        cache.put(key3, [{"id": 3}])

        assert key1 in cache.cache
        assert key3 in cache.cache
        assert key2 not in cache.cache
        assert len(cache.cache) == 2

    def test_cache_invalidate_run(self) -> None:
        """Test cache invalidation for a specific run."""
        cache = DetailsCache(capacity=10)

        # Add items for different runs
        key1 = ("run1", "fp1", "group1", "duckdb")
        key2 = ("run1", "fp1", "group2", "duckdb")
        key3 = ("run2", "fp1", "group1", "duckdb")

        cache.put(key1, [{"id": 1}])
        cache.put(key2, [{"id": 2}])
        cache.put(key3, [{"id": 3}])

        # Invalidate run1
        cache.invalidate_run("run1")

        assert key1 not in cache.cache
        assert key2 not in cache.cache
        assert key3 in cache.cache  # run2 should remain
        assert len(cache.cache) == 1

    def test_cache_clear(self) -> None:
        """Test cache clearing."""
        cache = DetailsCache(capacity=10)

        # Add some items
        key1 = ("run1", "fp1", "group1", "duckdb")
        key2 = ("run1", "fp1", "group2", "duckdb")

        cache.put(key1, [{"id": 1}])
        cache.put(key2, [{"id": 2}])

        # Clear cache
        cache.clear()

        assert len(cache.cache) == 0
        assert len(cache.access_order) == 0


class TestParquetFingerprint:
    """Test parquet fingerprint generation."""

    def test_parquet_fingerprint_success(self, tmp_path) -> None:
        """Test successful fingerprint generation."""
        # Create a temporary file
        test_file = tmp_path / "test.parquet"
        test_file.write_text("test content")

        # Get fingerprint
        fingerprint = _get_parquet_fingerprint(str(test_file))

        # Should be in format "mtime_size"
        assert "_" in fingerprint
        parts = fingerprint.split("_")
        assert len(parts) == 2
        assert parts[1].isdigit()  # size should be numeric

    def test_parquet_fingerprint_file_not_found(self) -> None:
        """Test fingerprint generation for non-existent file."""
        fingerprint = _get_parquet_fingerprint("/nonexistent/file.parquet")
        assert fingerprint == "unknown"


class TestGroupDetailsDuckDB:
    """Test DuckDB group details loading."""

    @patch("src.utils.ui_helpers.duckdb.connect")
    @patch("src.utils.ui_helpers.get_artifact_paths")
    @patch("src.utils.ui_helpers.os.path.exists")
    def test_get_group_details_duckdb_success(
        self, mock_exists, mock_get_paths, mock_duckdb_connect
    ) -> None:
        """Test successful DuckDB details loading."""
        # Mock artifact paths
        mock_get_paths.return_value = {
            "group_details_parquet": "/test/path/group_details.parquet"
        }
        mock_exists.return_value = True

        # Mock DuckDB connection and execution
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.rowcount = 2
        mock_result.df.return_value = pd.DataFrame(
            {
                GROUP_ID: ["group1", "group1"],
                ACCOUNT_ID: ["acc1", "acc2"],
                ACCOUNT_NAME: ["Company 1", "Company 2"],
                SUFFIX_CLASS: ["INC", "INC"],
                CREATED_DATE: ["2023-01-01", "2023-01-02"],
                DISPOSITION: ["Keep", "Update"],
            }
        )

        mock_conn.execute.return_value = mock_result
        mock_duckdb_connect.return_value = mock_conn

        # Test the function
        result = get_group_details_duckdb("run1", "group1")

        # Verify result
        assert len(result) == 2
        assert result[0][ACCOUNT_NAME] == "Company 1"
        assert result[1][ACCOUNT_NAME] == "Company 2"

        # Verify DuckDB was called correctly
        mock_duckdb_connect.assert_called_once_with(":memory:")
        mock_conn.execute.assert_called()

    @patch("src.utils.ui_helpers.get_artifact_paths")
    @patch("src.utils.ui_helpers.os.path.exists")
    def test_get_group_details_duckdb_file_not_found(
        self, mock_exists, mock_get_paths
    ) -> None:
        """Test DuckDB details loading when file not found."""
        # Mock artifact paths
        mock_get_paths.return_value = {
            "group_details_parquet": "/test/path/group_details.parquet"
        }
        mock_exists.return_value = False

        # Test the function should raise FileNotFoundError
        with pytest.raises(FileNotFoundError):
            get_group_details_duckdb("run1", "group1")

    @patch("src.utils.ui_helpers.duckdb.connect")
    @patch("src.utils.ui_helpers.get_artifact_paths")
    @patch("src.utils.ui_helpers.os.path.exists")
    def test_get_group_details_duckdb_query_error(
        self, mock_exists, mock_get_paths, mock_duckdb_connect
    ) -> None:
        """Test DuckDB details loading with query execution error."""
        # Mock artifact paths
        mock_get_paths.return_value = {
            "group_details_parquet": "/test/path/group_details.parquet"
        }
        mock_exists.return_value = True

        # Mock DuckDB connection that fails on execute
        mock_conn = MagicMock()
        mock_conn.execute.side_effect = Exception("SQL syntax error")
        mock_duckdb_connect.return_value = mock_conn

        # Test the function should raise the exception
        with pytest.raises(Exception, match="SQL syntax error"):
            get_group_details_duckdb("run1", "group1")


class TestGroupsRouting:
    """Test groups page routing to ensure DuckDB-first behavior."""

    @patch("src.utils.ui_helpers.get_artifact_paths")
    @patch("src.utils.ui_helpers.os.path.exists")
    @patch("src.utils.ui_helpers.DUCKDB_AVAILABLE", True)
    def test_groups_use_duckdb_when_stats_parquet_exists(
        self, mock_exists, mock_get_paths
    ) -> None:
        """Test that groups page uses DuckDB when group_stats.parquet exists."""
        from src.utils.ui_helpers import get_groups_page

        # Mock artifact paths with group_stats.parquet
        mock_get_paths.return_value = {
            "group_stats_parquet": "/test/path/group_stats.parquet"
        }
        mock_exists.return_value = True

        # Mock the DuckDB function to return test data
        with patch(
            "src.utils.ui_helpers.get_groups_page_from_stats_duckdb"
        ) as mock_duckdb:
            mock_duckdb.return_value = ([{"group_id": "test"}], 1)

            # Call the function
            result, total = get_groups_page("run1", "Group Size (Desc)", 1, 50, {})

            # Verify DuckDB was called
            mock_duckdb.assert_called_once()
            assert result == [{"group_id": "test"}]
            assert total == 1

    @patch("src.utils.ui_helpers.get_artifact_paths")
    @patch("src.utils.ui_helpers.os.path.exists")
    @patch("src.utils.ui_helpers.DUCKDB_AVAILABLE", False)
    def test_groups_fallback_to_pyarrow_when_duckdb_unavailable(
        self, mock_exists, mock_get_paths
    ) -> None:
        """Test that groups page falls back to PyArrow when DuckDB unavailable."""
        from src.utils.ui_helpers import get_groups_page

        # Mock artifact paths with group_stats.parquet
        mock_get_paths.return_value = {
            "group_stats_parquet": "/test/path/group_stats.parquet"
        }
        mock_exists.return_value = True

        # Mock the PyArrow function to return test data
        with patch("src.utils.ui_helpers.get_groups_page_pyarrow") as mock_pyarrow:
            mock_pyarrow.return_value = ([{"group_id": "test"}], 1)

            # Call the function
            result, total = get_groups_page("run1", "Group Size (Desc)", 1, 50, {})

            # Verify PyArrow was called
            mock_pyarrow.assert_called_once()
            assert result == [{"group_id": "test"}]
            assert total == 1


if __name__ == "__main__":
    pytest.main([__file__])
