"""Sort utilities for Phase 1.18.1 refactor.

This module provides stable sort key builders and name coalescing functions.
"""

from typing import Optional

from src.utils.schema_utils import GROUP_ID, GROUP_SIZE, MAX_SCORE, PRIMARY_NAME


def build_stable_sort_key(sort_by: str, group_id: str) -> str:
    """Build a stable sort key with group_id tie-breaker.

    Args:
        sort_by: The sort key from dropdown
        group_id: The group ID for tie-breaking

    Returns:
        A string sort key for stable sorting

    """
    # Map sort keys to their corresponding field names
    sort_mapping = {
        "Group Size (Desc)": GROUP_SIZE,
        "Group Size (Asc)": GROUP_SIZE,
        "Max Score (Desc)": MAX_SCORE,
        "Max Score (Asc)": MAX_SCORE,
        "Account Name (Asc)": PRIMARY_NAME,
        "Account Name (Desc)": PRIMARY_NAME,
    }

    field = sort_mapping.get(sort_by, GROUP_SIZE)

    # Determine sort direction
    if "(Desc)" in sort_by:
        direction = "desc"
    else:
        direction = "asc"

    return f"{field}_{direction}_{group_id}"


def coalesce_primary_name(primary_name: Optional[str]) -> str:
    """Coalesce primary name to empty string if None.

    Args:
        primary_name: The primary name to coalesce

    Returns:
        The primary name or empty string

    """
    return primary_name or ""


def build_order_by_clause(sort_by: str) -> str:
    """Build SQL ORDER BY clause for DuckDB queries.

    Args:
        sort_by: The sort key from dropdown

    Returns:
        SQL ORDER BY clause with stable tie-breaker

    """
    if "Group Size" in sort_by:
        if "(Desc)" in sort_by:
            return f"s.{GROUP_SIZE} DESC, s.{GROUP_ID} ASC"
        return f"s.{GROUP_SIZE} ASC, s.{GROUP_ID} ASC"
    if "Max Score" in sort_by:
        if "(Desc)" in sort_by:
            return f"s.{MAX_SCORE} DESC, s.{GROUP_ID} ASC"
        return f"s.{MAX_SCORE} ASC, s.{GROUP_ID} ASC"
    if "Account Name" in sort_by:
        if "(Desc)" in sort_by:
            return f"COALESCE(p.{PRIMARY_NAME}, '') DESC, s.{GROUP_ID} ASC"
        return f"COALESCE(p.{PRIMARY_NAME}, '') ASC, s.{GROUP_ID} ASC"
    # Default to group size descending
    return f"s.{GROUP_SIZE} DESC, s.{GROUP_ID} ASC"


def validate_sort_key(sort_by: str) -> bool:
    """Validate that a sort key is supported.

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
