"""Tests for src.utils.sort_utils module.

This module tests the sort utilities for Phase 1.18.1 refactor.
"""

from src.utils.sort_utils import (
    build_order_by_clause,
    build_stable_sort_key,
    coalesce_primary_name,
    validate_sort_key,
)


def test_build_stable_sort_key_group_size_desc() -> None:
    """Test build_stable_sort_key with Group Size (Desc)."""
    result = build_stable_sort_key("Group Size (Desc)", "group123")
    assert result == "group_size_desc_group123"


def test_build_stable_sort_key_group_size_asc() -> None:
    """Test build_stable_sort_key with Group Size (Asc)."""
    result = build_stable_sort_key("Group Size (Asc)", "group456")
    assert result == "group_size_asc_group456"


def test_build_stable_sort_key_max_score_desc() -> None:
    """Test build_stable_sort_key with Max Score (Desc)."""
    result = build_stable_sort_key("Max Score (Desc)", "group789")
    assert result == "max_score_desc_group789"


def test_build_stable_sort_key_max_score_asc() -> None:
    """Test build_stable_sort_key with Max Score (Asc)."""
    result = build_stable_sort_key("Max Score (Asc)", "group012")
    assert result == "max_score_asc_group012"


def test_build_stable_sort_key_account_name_asc() -> None:
    """Test build_stable_sort_key with Account Name (Asc)."""
    result = build_stable_sort_key("Account Name (Asc)", "group345")
    assert result == "primary_name_asc_group345"


def test_build_stable_sort_key_account_name_desc() -> None:
    """Test build_stable_sort_key with Account Name (Desc)."""
    result = build_stable_sort_key("Account Name (Desc)", "group678")
    assert result == "primary_name_desc_group678"


def test_build_stable_sort_key_default() -> None:
    """Test build_stable_sort_key with default case."""
    result = build_stable_sort_key("Unknown Sort", "group999")
    assert result == "group_size_asc_group999"


def test_coalesce_primary_name_with_value() -> None:
    """Test coalesce_primary_name with a valid value."""
    result = coalesce_primary_name("Test Company")
    assert result == "Test Company"


def test_coalesce_primary_name_none() -> None:
    """Test coalesce_primary_name with None."""
    result = coalesce_primary_name(None)
    assert result == ""


def test_coalesce_primary_name_empty_string() -> None:
    """Test coalesce_primary_name with empty string."""
    result = coalesce_primary_name("")
    assert result == ""


def test_build_order_by_clause_group_size_desc() -> None:
    """Test build_order_by_clause with Group Size (Desc)."""
    result = build_order_by_clause("Group Size (Desc)")
    assert result == "s.group_size DESC, s.group_id ASC"


def test_build_order_by_clause_group_size_asc() -> None:
    """Test build_order_by_clause with Group Size (Asc)."""
    result = build_order_by_clause("Group Size (Asc)")
    assert result == "s.group_size ASC, s.group_id ASC"


def test_build_order_by_clause_max_score_desc() -> None:
    """Test build_order_by_clause with Max Score (Desc)."""
    result = build_order_by_clause("Max Score (Desc)")
    assert result == "s.max_score DESC, s.group_id ASC"


def test_build_order_by_clause_max_score_asc() -> None:
    """Test build_order_by_clause with Max Score (Asc)."""
    result = build_order_by_clause("Max Score (Asc)")
    assert result == "s.max_score ASC, s.group_id ASC"


def test_build_order_by_clause_account_name_asc() -> None:
    """Test build_order_by_clause with Account Name (Asc)."""
    result = build_order_by_clause("Account Name (Asc)")
    assert result == "COALESCE(p.primary_name, '') ASC, s.group_id ASC"


def test_build_order_by_clause_account_name_desc() -> None:
    """Test build_order_by_clause with Account Name (Desc)."""
    result = build_order_by_clause("Account Name (Desc)")
    assert result == "COALESCE(p.primary_name, '') DESC, s.group_id ASC"


def test_build_order_by_clause_default() -> None:
    """Test build_order_by_clause with default case."""
    result = build_order_by_clause("Unknown Sort")
    assert result == "s.group_size DESC, s.group_id ASC"


def test_validate_sort_key_valid_keys() -> None:
    """Test validate_sort_key with valid sort keys."""
    valid_keys = [
        "Group Size (Desc)",
        "Group Size (Asc)",
        "Max Score (Desc)",
        "Max Score (Asc)",
        "Account Name (Asc)",
        "Account Name (Desc)",
    ]

    for key in valid_keys:
        assert validate_sort_key(key) is True


def test_validate_sort_key_invalid_keys() -> None:
    """Test validate_sort_key with invalid sort keys."""
    invalid_keys = ["Invalid Sort", "Group Size", "Max Score", "Account Name", ""]

    for key in invalid_keys:
        assert validate_sort_key(key) is False


def test_validate_sort_key_case_sensitive() -> None:
    """Test validate_sort_key is case sensitive."""
    assert validate_sort_key("group size (desc)") is False
    assert validate_sort_key("Group Size (desc)") is False
    assert validate_sort_key("GROUP SIZE (DESC)") is False
