"""
Tests for dtype validation and memory optimization functionality.
"""

import pytest
import pandas as pd
import sys
from pathlib import Path

# Add src to path for imports
sys.path.append(str(Path(__file__).parent.parent / "src"))

from utils.dtypes import (
    apply_dtypes,
    assert_no_unexpected_object_columns,
    drop_intermediate_columns,
    optimize_dataframe_memory,
    get_dtypes_for_schema,
)
from dtypes_map import DTYPES, ALLOWED_OBJECT_COLUMNS, INTERMEDIATE_COLUMNS_TO_DROP


class TestDtypeApplication:
    """Test dtype application functionality."""

    def test_apply_dtypes_basic(self):
        """Test basic dtype application."""
        df = pd.DataFrame(
            {
                "account_id": ["123", "456"],
                "name_core": ["company inc", "company llc"],
                "score": [95.5, 87.2],
            }
        )

        schema = {"account_id": "string", "name_core": "string", "score": "float32"}

        result = apply_dtypes(df, schema)

        assert result["account_id"].dtype == "string"
        assert result["name_core"].dtype == "string"
        assert result["score"].dtype == "float32"

    def test_apply_dtypes_missing_columns(self):
        """Test dtype application with missing columns."""
        df = pd.DataFrame(
            {"account_id": ["123", "456"], "name_core": ["company inc", "company llc"]}
        )

        schema = {
            "account_id": "string",
            "name_core": "string",
            "score": "float32",  # Not in dataframe
        }

        result = apply_dtypes(df, schema)

        # Should only apply to existing columns
        assert result["account_id"].dtype == "string"
        assert result["name_core"].dtype == "string"
        assert "score" not in result.columns

    def test_apply_dtypes_empty_dataframe(self):
        """Test dtype application with empty dataframe."""
        df = pd.DataFrame()
        schema = {"account_id": "string"}

        result = apply_dtypes(df, schema)

        assert result.empty
        assert len(result.columns) == 0


class TestObjectColumnValidation:
    """Test object column validation."""

    def test_assert_no_unexpected_object_columns_pass(self):
        """Test validation passes with allowed object columns."""
        df = pd.DataFrame(
            {
                "account_id": ["123", "456"],
                "name_core": ["company inc", "company llc"],  # Allowed object column
                "score": [95.5, 87.2],
            }
        )

        # Should not raise exception
        assert_no_unexpected_object_columns(df, context="test")

    def test_assert_no_unexpected_object_columns_fail(self):
        """Test validation fails with unexpected object columns."""
        df = pd.DataFrame(
            {
                "account_id": ["123", "456"],
                "unexpected_col": ["value1", "value2"],  # Unexpected object column
                "score": [95.5, 87.2],
            }
        )

        with pytest.raises(AssertionError, match="Unexpected object columns"):
            assert_no_unexpected_object_columns(df, context="test")

    def test_assert_no_unexpected_object_columns_empty(self):
        """Test validation with empty dataframe."""
        df = pd.DataFrame()

        # Should not raise exception
        assert_no_unexpected_object_columns(df, context="test")

    def test_assert_no_unexpected_object_columns_custom_allowed(self):
        """Test validation with custom allowed columns."""
        df = pd.DataFrame(
            {"account_id": ["123", "456"], "custom_col": ["value1", "value2"]}
        )

        # Should not raise exception with custom allowed set
        assert_no_unexpected_object_columns(
            df, allowed={"custom_col", "account_id"}, context="test"
        )


class TestIntermediateColumnDropping:
    """Test intermediate column dropping."""

    def test_drop_intermediate_columns(self):
        """Test dropping intermediate columns."""
        df = pd.DataFrame(
            {
                "account_id": ["123", "456"],
                "name_core": ["company inc", "company llc"],
                "raw_name_tokens": [
                    ["company", "inc"],
                    ["company", "llc"],
                ],  # Intermediate
                "temp_group_assignments": [1, 2],  # Intermediate
            }
        )

        result = drop_intermediate_columns(df, context="test")

        assert "account_id" in result.columns
        assert "name_core" in result.columns
        assert "raw_name_tokens" not in result.columns
        assert "temp_group_assignments" not in result.columns

    def test_drop_intermediate_columns_none_present(self):
        """Test dropping when no intermediate columns present."""
        df = pd.DataFrame(
            {"account_id": ["123", "456"], "name_core": ["company inc", "company llc"]}
        )

        result = drop_intermediate_columns(df, context="test")

        # Should be unchanged
        assert len(result.columns) == 2
        assert "account_id" in result.columns
        assert "name_core" in result.columns


class TestMemoryOptimization:
    """Test memory optimization functionality."""

    def test_optimize_dataframe_memory_basic(self):
        """Test basic memory optimization."""
        df = pd.DataFrame(
            {
                "account_id": ["123", "456"],
                "name_core": ["company inc", "company llc"],
                "score": [95.5, 87.2],
                "is_primary": [True, False],
            }
        )

        result = optimize_dataframe_memory(df, context="test")

        # Should have same data but potentially different dtypes
        assert len(result) == len(df)
        assert list(result.columns) == list(df.columns)

    def test_optimize_dataframe_memory_empty(self):
        """Test memory optimization with empty dataframe."""
        df = pd.DataFrame()

        result = optimize_dataframe_memory(df, context="test")

        assert result.empty

    def test_optimize_dataframe_memory_with_intermediate_columns(self):
        """Test memory optimization with intermediate columns."""
        df = pd.DataFrame(
            {
                "account_id": ["123", "456"],
                "name_core": ["company inc", "company llc"],
                "raw_name_tokens": [
                    ["company", "inc"],
                    ["company", "llc"],
                ],  # Intermediate
                "temp_group_assignments": [1, 2],  # Intermediate
            }
        )

        result = optimize_dataframe_memory(df, context="test")

        # Intermediate columns should be dropped
        assert "raw_name_tokens" not in result.columns
        assert "temp_group_assignments" not in result.columns
        assert "account_id" in result.columns
        assert "name_core" in result.columns


class TestSchemaDetection:
    """Test schema detection functionality."""

    def test_get_dtypes_for_schema_accounts(self):
        """Test getting dtypes for accounts schema."""
        schema = get_dtypes_for_schema("accounts")

        expected_keys = {
            "account_id",
            "account_name",
            "created_date",
            "name_core",
            "name_core_tokens",
            "suffix_class",
            "has_parentheses",
            "has_semicolon",
            "has_multiple_names",
            "relationship_rank",
        }

        assert all(key in schema for key in expected_keys)
        assert schema["account_id"] == "string"
        assert schema["suffix_class"] == "category"

    def test_get_dtypes_for_schema_pairs(self):
        """Test getting dtypes for pairs schema."""
        schema = get_dtypes_for_schema("pairs")

        expected_keys = {
            "account_id",
            "name_core",
            "suffix_class",
            "score",
            "block_key",
            "block_size",
        }

        assert all(key in schema for key in expected_keys)
        assert schema["score"] == "float32"

    def test_get_dtypes_for_schema_groups(self):
        """Test getting dtypes for groups schema."""
        schema = get_dtypes_for_schema("groups")

        expected_keys = {
            "account_id",
            "group_id",
            "name_core",
            "suffix_class",
            "score_to_primary",
            "is_primary",
            "group_size",
            "group_rank",
            "group_join_reason",
            "weakest_edge_to_primary",
            "shared_tokens_count",
        }

        assert all(key in schema for key in expected_keys)
        assert schema["group_id"] == "string"
        assert schema["is_primary"] == "boolean"

    def test_get_dtypes_for_schema_review_ready(self):
        """Test getting dtypes for review_ready schema."""
        schema = get_dtypes_for_schema("review_ready")

        expected_keys = {
            "account_id",
            "account_name",
            "created_date",
            "group_id",
            "name_core",
            "suffix_class",
            "is_primary",
            "group_size",
            "disposition",
            "disposition_reason",
            "applied_penalties",
            "alias_candidates",
            "alias_matches_count",
            "relationship_rank",
            "group_join_reason",
            "weakest_edge_to_primary",
            "shared_tokens_count",
        }

        assert all(key in schema for key in expected_keys)
        assert schema["disposition"] == "category"
        assert schema["group_size"] == "int16"

    def test_get_dtypes_for_schema_unknown(self):
        """Test getting dtypes for unknown schema."""
        schema = get_dtypes_for_schema("unknown")

        # Should return full DTYPES dict
        assert schema == DTYPES


class TestDtypeMapConstants:
    """Test dtype map constants."""

    def test_dtypes_structure(self):
        """Test that DTYPES has expected structure."""
        assert isinstance(DTYPES, dict)
        assert len(DTYPES) > 0

        # Check some key dtypes
        assert DTYPES["account_id"] == "string"
        assert DTYPES["score"] == "float32"
        assert DTYPES["is_primary"] == "boolean"
        assert DTYPES["disposition"] == "category"

    def test_allowed_object_columns(self):
        """Test ALLOWED_OBJECT_COLUMNS structure."""
        assert isinstance(ALLOWED_OBJECT_COLUMNS, set)
        assert len(ALLOWED_OBJECT_COLUMNS) > 0

        # Check some expected allowed columns
        assert "name_core" in ALLOWED_OBJECT_COLUMNS
        assert "disposition_reason" in ALLOWED_OBJECT_COLUMNS

    def test_intermediate_columns_to_drop(self):
        """Test INTERMEDIATE_COLUMNS_TO_DROP structure."""
        assert isinstance(INTERMEDIATE_COLUMNS_TO_DROP, set)
        assert len(INTERMEDIATE_COLUMNS_TO_DROP) > 0

        # Check some expected intermediate columns
        assert "raw_name_tokens" in INTERMEDIATE_COLUMNS_TO_DROP
        assert "temp_group_assignments" in INTERMEDIATE_COLUMNS_TO_DROP
