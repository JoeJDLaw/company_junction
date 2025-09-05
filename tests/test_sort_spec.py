"""Tests for sort specification functionality in filtering.py.
"""

import pytest

from src.utils.filtering import (
    SortSpec,
    resolve_sort,
    to_duckdb_order_by,
    to_pyarrow_sort_by,
)
from src.utils.schema_utils import GROUP_ID, GROUP_SIZE, MAX_SCORE, PRIMARY_NAME


class TestSortSpec:
    """Test SortSpec dataclass."""

    def test_sort_spec_creation(self):
        """Test SortSpec creation with valid parameters."""
        spec = SortSpec(
            field=GROUP_SIZE, direction="desc", tie_breaker=(GROUP_ID, "asc"),
        )
        assert spec.field == GROUP_SIZE
        assert spec.direction == "desc"
        assert spec.tie_breaker == (GROUP_ID, "asc")


class TestResolveSort:
    """Test resolve_sort function."""

    def test_group_size_desc(self):
        """Test 'Group Size (Desc)' sort key."""
        spec = resolve_sort("Group Size (Desc)")
        assert spec.field == GROUP_SIZE
        assert spec.direction == "desc"
        assert spec.tie_breaker == (GROUP_ID, "asc")

    def test_group_size_asc(self):
        """Test 'Group Size (Asc)' sort key."""
        spec = resolve_sort("Group Size (Asc)")
        assert spec.field == GROUP_SIZE
        assert spec.direction == "asc"
        assert spec.tie_breaker == (GROUP_ID, "asc")

    def test_max_score_desc(self):
        """Test 'Max Score (Desc)' sort key."""
        spec = resolve_sort("Max Score (Desc)")
        assert spec.field == MAX_SCORE
        assert spec.direction == "desc"
        assert spec.tie_breaker == (GROUP_ID, "asc")

    def test_max_score_asc(self):
        """Test 'Max Score (Asc)' sort key."""
        spec = resolve_sort("Max Score (Asc)")
        assert spec.field == MAX_SCORE
        assert spec.direction == "asc"
        assert spec.tie_breaker == (GROUP_ID, "asc")

    def test_account_name_asc(self):
        """Test 'Account Name (Asc)' sort key."""
        spec = resolve_sort("Account Name (Asc)")
        assert spec.field == PRIMARY_NAME
        assert spec.direction == "asc"
        assert spec.tie_breaker == (GROUP_ID, "asc")

    def test_account_name_desc(self):
        """Test 'Account Name (Desc)' sort key."""
        spec = resolve_sort("Account Name (Desc)")
        assert spec.field == PRIMARY_NAME
        assert spec.direction == "desc"
        assert spec.tie_breaker == (GROUP_ID, "asc")

    def test_unknown_sort_key_fallback(self):
        """Test unknown sort key falls back to default."""
        spec = resolve_sort("Unknown Sort Key")
        # Should fall back to GROUP_SIZE DESC due to config unavailability
        assert spec.field == GROUP_SIZE
        assert spec.direction == "desc"
        assert spec.tie_breaker == (GROUP_ID, "asc")


class TestToDuckDBOrderBy:
    """Test to_duckdb_order_by function."""

    def test_group_size_desc_to_duckdb(self):
        """Test converting Group Size Desc to DuckDB ORDER BY."""
        spec = SortSpec(
            field=GROUP_SIZE, direction="desc", tie_breaker=(GROUP_ID, "asc"),
        )
        result = to_duckdb_order_by(spec)
        expected = f"{GROUP_SIZE} DESC, {GROUP_ID} ASC"
        assert result == expected

    def test_max_score_asc_to_duckdb(self):
        """Test converting Max Score Asc to DuckDB ORDER BY."""
        spec = SortSpec(field=MAX_SCORE, direction="asc", tie_breaker=(GROUP_ID, "asc"))
        result = to_duckdb_order_by(spec)
        expected = f"{MAX_SCORE} ASC, {GROUP_ID} ASC"
        assert result == expected


class TestToPyArrowSortBy:
    """Test to_pyarrow_sort_by function."""

    def test_group_size_desc_to_pyarrow(self):
        """Test converting Group Size Desc to PyArrow sort spec."""
        spec = SortSpec(
            field=GROUP_SIZE, direction="desc", tie_breaker=(GROUP_ID, "asc"),
        )
        result = to_pyarrow_sort_by(spec)
        expected = [(GROUP_SIZE, "descending"), (GROUP_ID, "ascending")]
        assert result == expected

    def test_max_score_asc_to_pyarrow(self):
        """Test converting Max Score Asc to PyArrow sort spec."""
        spec = SortSpec(field=MAX_SCORE, direction="asc", tie_breaker=(GROUP_ID, "asc"))
        result = to_pyarrow_sort_by(spec)
        expected = [(MAX_SCORE, "ascending"), (GROUP_ID, "ascending")]
        assert result == expected


if __name__ == "__main__":
    pytest.main([__file__])
