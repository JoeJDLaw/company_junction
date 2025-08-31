"""
Sort utilities for Phase 1.18.1 refactor.

This module provides stable sort key builders and name coalescing functions.
"""

from typing import Optional


def build_stable_sort_key(sort_by: str, group_id: str) -> str:
    """
    Build a stable sort key with group_id tie-breaker.

    Args:
        sort_by: The sort key from dropdown
        group_id: The group ID for tie-breaking

    Returns:
        A string sort key for stable sorting
    """
    # Map sort keys to their corresponding field names
    sort_mapping = {
        "Group Size (Desc)": "group_size",
        "Group Size (Asc)": "group_size",
        "Max Score (Desc)": "max_score",
        "Max Score (Asc)": "max_score",
        "Account Name (Asc)": "primary_name",
        "Account Name (Desc)": "primary_name",
    }

    field = sort_mapping.get(sort_by, "group_size")

    # Determine sort direction
    if "(Desc)" in sort_by:
        direction = "desc"
    else:
        direction = "asc"

    return f"{field}_{direction}_{group_id}"


def coalesce_primary_name(primary_name: Optional[str]) -> str:
    """
    Coalesce primary name to empty string if None.

    Args:
        primary_name: The primary name to coalesce

    Returns:
        The primary name or empty string
    """
    return primary_name or ""


def build_order_by_clause(sort_by: str) -> str:
    """
    Build SQL ORDER BY clause for DuckDB queries.

    Args:
        sort_by: The sort key from dropdown

    Returns:
        SQL ORDER BY clause with stable tie-breaker
    """
    if "Group Size" in sort_by:
        if "(Desc)" in sort_by:
            return "s.group_size DESC, s.group_id ASC"
        else:
            return "s.group_size ASC, s.group_id ASC"
    elif "Max Score" in sort_by:
        if "(Desc)" in sort_by:
            return "s.max_score DESC, s.group_id ASC"
        else:
            return "s.max_score ASC, s.group_id ASC"
    elif "Account Name" in sort_by:
        if "(Desc)" in sort_by:
            return "COALESCE(p.primary_name, '') DESC, s.group_id ASC"
        else:
            return "COALESCE(p.primary_name, '') ASC, s.group_id ASC"
    else:
        # Default to group size descending
        return "s.group_size DESC, s.group_id ASC"


def validate_sort_key(sort_by: str) -> bool:
    """
    Validate that a sort key is supported.

    Args:
        sort_by: The sort key to validate

    Returns:
        True if valid, False otherwise
    """
    valid_keys = [
        "Group Size (Desc)",
        "Group Size (Asc)",
        "Max Score (Desc)",
        "Max Score (Asc)",
        "Account Name (Asc)",
        "Account Name (Desc)",
    ]
    return sort_by in valid_keys
