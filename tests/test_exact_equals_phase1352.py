"""Tests for Phase 1.35.2 Exact Equals Phase-0 functionality.

This module tests:
- Raw exact key building
- Exact equals group detection
- Representative selection
- Artifact generation
"""

import pandas as pd
import pytest

from src.utils.exact_equals import (
    build_raw_exact_key,
    create_unique_normalized,
    find_exact_equals_groups,
)


def test_build_raw_exact_key() -> None:
    """Test raw exact key building with trim and whitespace collapse."""
    settings = {"pipeline": {"exact_equals_first_pass": {"key_trim": True}}}

    # Test basic trimming
    assert build_raw_exact_key("  Walmart  ", settings) == "Walmart"

    # Test whitespace collapse
    assert build_raw_exact_key("Walmart   Inc", settings) == "Walmart Inc"

    # Test multiple spaces
    assert build_raw_exact_key("Walmart    Inc    Corp", settings) == "Walmart Inc Corp"

    # Test no trimming when disabled
    settings["pipeline"]["exact_equals_first_pass"]["key_trim"] = False
    assert build_raw_exact_key("  Walmart  ", settings) == "  Walmart  "

    # Test edge cases
    assert build_raw_exact_key("", settings) == ""
    assert build_raw_exact_key(None, settings) == ""  # type: ignore[arg-type]
    assert build_raw_exact_key("   ", settings) == ""


def test_find_exact_equals_groups() -> None:
    """Test exact equals group detection."""
    settings = {
        "pipeline": {
            "exact_equals_first_pass": {
                "enable": True,
                "min_group_size": 2,
                "representative_policy": "min_account_id",
            },
        },
    }

    # Create test data
    df = pd.DataFrame(
        {
            "account_id": ["A1", "A2", "A3", "A4", "A5"],
            "Account Name": [
                "Walmart",
                "Walmart",
                "Walmart Inc",
                "Walmart Inc",
                "Target",
            ],
        },
    )

    # Find exact equals groups
    exact_groups, raw_map, candidate_pairs = find_exact_equals_groups(df, settings)

    # Verify exact groups
    assert len(exact_groups) == 2  # Walmart (2) and Walmart Inc (2)
    assert len(raw_map) == 4  # 4 records in groups

    # Verify candidate pairs
    assert len(candidate_pairs) == 2  # 1 pair for each group

    # Verify representative selection (min account_id)
    walmart_group = exact_groups[exact_groups["raw_exact_key"] == "Walmart"].iloc[0]
    assert walmart_group["representative_id"] == "A1"  # min account_id

    walmart_inc_group = exact_groups[
        exact_groups["raw_exact_key"] == "Walmart Inc"
    ].iloc[0]
    assert walmart_inc_group["representative_id"] == "A3"  # min account_id


def test_create_unique_normalized() -> None:
    """Test creation of unique normalized dataset."""
    settings = {"pipeline": {"exact_equals_first_pass": {"enable": True}}}

    # Create test data
    df = pd.DataFrame(
        {
            "account_id": ["A1", "A2", "A3", "A4", "A5"],
            "Account Name": [
                "Walmart",
                "Walmart",
                "Walmart Inc",
                "Walmart Inc",
                "Target",
            ],
        },
    )

    # Create raw exact map
    raw_map = pd.DataFrame(
        {
            "account_id": ["A1", "A2", "A3", "A4"],
            "representative_id": ["A1", "A1", "A3", "A3"],
        },
    )

    # Create unique normalized dataset
    unique_df = create_unique_normalized(df, raw_map, settings)

    # Should have 3 records: 2 representatives + 1 singleton
    assert len(unique_df) == 3

    # Should contain representatives and singletons
    account_ids = set(unique_df["account_id"])
    assert "A1" in account_ids  # Walmart representative
    assert "A3" in account_ids  # Walmart Inc representative
    assert "A5" in account_ids  # Target singleton


def test_no_exact_groups() -> None:
    """Test behavior when no exact groups exist."""
    settings = {
        "pipeline": {"exact_equals_first_pass": {"enable": True, "min_group_size": 2}},
    }

    # Create test data with no duplicates
    df = pd.DataFrame(
        {
            "account_id": ["A1", "A2", "A3"],
            "Account Name": ["Walmart", "Target", "Costco"],
        },
    )

    # Find exact equals groups
    exact_groups, raw_map, candidate_pairs = find_exact_equals_groups(df, settings)

    # Should return empty DataFrames
    assert len(exact_groups) == 0
    assert len(raw_map) == 0
    assert len(candidate_pairs) == 0


def test_min_group_size_filtering() -> None:
    """Test minimum group size filtering."""
    settings = {
        "pipeline": {
            "exact_equals_first_pass": {
                "enable": True,
                "min_group_size": 3,  # Require 3+ records
            },
        },
    }

    # Create test data with groups of size 2 and 3
    df = pd.DataFrame(
        {
            "account_id": ["A1", "A2", "A3", "A4", "A5", "A6"],
            "Account Name": [
                "Walmart",
                "Walmart",  # Size 2 (should be filtered out)
                "Target",
                "Target",
                "Target",  # Size 3 (should be kept)
                "Costco",  # Singleton
            ],
        },
    )

    # Find exact equals groups
    exact_groups, raw_map, candidate_pairs = find_exact_equals_groups(df, settings)

    # Should only have the Target group (size 3)
    assert len(exact_groups) == 1
    assert exact_groups.iloc[0]["raw_exact_key"] == "Target"
    assert exact_groups.iloc[0]["group_size"] == 3


if __name__ == "__main__":
    pytest.main([__file__])
