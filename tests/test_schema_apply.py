"""
Tests for schema application functionality.

This module tests the new helper functions for applying canonical column renames.
"""

import pytest
import pandas as pd
from src.utils.schema_utils import invert_mapping, apply_canonical_rename


def test_invert_mapping():
    """Test that invert_mapping correctly inverts canonical -> actual to actual -> canonical."""
    # Test case: canonical -> actual mapping
    canonical_to_actual = {
        "account_name": "Account Name",
        "account_id": "Account ID",
        "created_date": "Created Date",
    }

    # Expected: actual -> canonical mapping
    expected = {
        "Account Name": "account_name",
        "Account ID": "account_id",
        "Created Date": "created_date",
    }

    result = invert_mapping(canonical_to_actual)
    assert result == expected


def test_apply_canonical_rename_success():
    """Test successful application of canonical rename."""
    # Create DataFrame with actual column names
    df = pd.DataFrame(
        {
            "Account ID": ["001", "002", "003"],
            "Account Name": ["Company A", "Company B", "Company C"],
            "Created Date": ["2023-01-01", "2023-01-02", "2023-01-03"],
        }
    )

    # Schema mapping from canonical to actual
    schema_mapping = {
        "account_name": "Account Name",
        "account_id": "Account ID",
        "created_date": "Created Date",
    }

    # Apply rename
    df_renamed = apply_canonical_rename(df, schema_mapping)

    # Verify columns are now canonical
    assert "account_id" in df_renamed.columns
    assert "account_name" in df_renamed.columns
    assert "created_date" in df_renamed.columns

    # Verify old column names are gone
    assert "Account ID" not in df_renamed.columns
    assert "Account Name" not in df_renamed.columns
    assert "Created Date" not in df_renamed.columns

    # Verify data is preserved
    assert df_renamed["account_id"].iloc[0] == "001"
    assert df_renamed["account_name"].iloc[0] == "Company A"


def test_apply_canonical_rename_missing_required_column():
    """Test that apply_canonical_rename raises error when required columns are missing."""
    # Create DataFrame missing required ACCOUNT_NAME column
    df = pd.DataFrame(
        {
            "Account ID": ["001", "002"],
            "Created Date": ["2023-01-01", "2023-01-02"],
            # Missing "Account Name" column
        }
    )

    # Schema mapping missing account_name
    schema_mapping = {
        "account_id": "Account ID",
        "created_date": "Created Date",
        # Missing account_name mapping
    }

    # Should raise ValueError due to missing required column
    with pytest.raises(ValueError) as exc_info:
        apply_canonical_rename(df, schema_mapping)

    assert "Required canonical columns missing after renaming" in str(exc_info.value)
    assert "account_name" in str(exc_info.value)


def test_apply_canonical_rename_partial_mapping():
    """Test that apply_canonical_rename works with partial column mapping."""
    # Create DataFrame with extra columns
    df = pd.DataFrame(
        {
            "Account ID": ["001", "002"],
            "Account Name": ["Company A", "Company B"],
            "Created Date": ["2023-01-01", "2023-01-02"],
            "Extra Column": ["extra1", "extra2"],  # Extra column not in mapping
        }
    )

    # Schema mapping for only some columns
    schema_mapping = {
        "account_name": "Account Name",
        "account_id": "Account ID",
        # Missing created_date mapping
    }

    # Apply rename
    df_renamed = apply_canonical_rename(df, schema_mapping)

    # Verify mapped columns are canonical
    assert "account_id" in df_renamed.columns
    assert "account_name" in df_renamed.columns

    # Verify unmapped columns remain unchanged
    assert "Created Date" in df_renamed.columns
    assert "Extra Column" in df_renamed.columns

    # Verify data is preserved
    assert df_renamed["account_id"].iloc[0] == "001"
    assert df_renamed["account_name"].iloc[0] == "Company A"
    assert df_renamed["Created Date"].iloc[0] == "2023-01-01"
    assert df_renamed["Extra Column"].iloc[0] == "extra1"
