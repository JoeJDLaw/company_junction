"""
Filtering and sorting utilities for ui_helpers refactor.

This module provides unified models for sort/filter operations.
"""

import logging
from dataclasses import dataclass
from typing import Literal, Dict, Any, List, Tuple
from src.utils.schema_utils import (
    GROUP_SIZE, MAX_SCORE, PRIMARY_NAME, GROUP_ID
)
from src.utils.logging_utils import get_logger

logger = get_logger(__name__)

@dataclass(frozen=True)
class SortSpec:
    """Unified sort specification."""
    field: str
    direction: Literal["asc", "desc"]
    tie_breaker: tuple[str, Literal["asc", "desc"]] = ("group_id", "asc")

def get_order_by(sort_key: str) -> str:
    """
    Centralized sort key to ORDER BY mapping.

    This is the single source of truth for all sort key mappings used by both
    DuckDB backend functions to ensure consistency.

    Args:
        sort_key: The sort key from the UI

    Returns:
        The ORDER BY clause for DuckDB queries

    Raises:
        ValueError: If sort_key is not recognized
    """
    order_by_map = {
        "Group Size (Desc)": f"{GROUP_SIZE} DESC",
        "Group Size (Asc)": f"{GROUP_SIZE} ASC",
        "Max Score (Desc)": f"{MAX_SCORE} DESC",
        "Max Score (Asc)": f"{MAX_SCORE} ASC",
        "Account Name (Asc)": f"{PRIMARY_NAME} ASC",
        "Account Name (Desc)": f"{PRIMARY_NAME} DESC",
    }

    if sort_key not in order_by_map:
        # Load default from config instead of hardcoded fallback
        try:
            from src.utils.io_utils import load_settings
            from src.utils.path_utils import get_config_path

            settings = load_settings(str(get_config_path()))
            default_sort = (
                settings.get("ui", {})
                .get("sort", {})
                .get("default", f"{GROUP_SIZE} DESC")
            )
            logger.error(
                f"Unknown sort_key='{sort_key}', falling back to config default: {default_sort}"
            )
            return default_sort
        except Exception as e:
            logger.error(
                f"Failed to load config default sort, using hardcoded fallback: {e}"
            )
            return f"{GROUP_SIZE} DESC"

    return order_by_map[sort_key]


def build_sort_expression(sort_key: str) -> List[Tuple[str, str]]:
    """
    Build PyArrow sort keys for stable sorting.

    Args:
        sort_key: Sort key from dropdown (e.g., "Group Size (Desc)")

    Returns:
        List of (field, direction) tuples for sorting
    """
    # Extract sort field and direction
    if "Group Size" in sort_key:
        field = GROUP_SIZE
    elif "Max Score" in sort_key:
        field = MAX_SCORE
    elif "Account Name" in sort_key:
        field = PRIMARY_NAME
    else:
        # Default to group_id for stability
        field = GROUP_ID

    # Determine sort direction
    direction = (
        "ascending" if "(Asc)" in sort_key or "(Desc)" not in sort_key else "descending"
    )

    # Return sort keys with group_id tiebreaker for stability
    return [(field, direction), (GROUP_ID, "ascending")]


def apply_filters_pyarrow(table: Any, filters: Dict[str, Any]) -> Any:
    """
    Apply filters to PyArrow table.
    
    Args:
        table: PyArrow table to filter
        filters: Dictionary of filters to apply
        
    Returns:
        Filtered PyArrow table
    """
    if not filters:
        return table

    # Apply disposition filter
    if "dispositions" in filters and filters["dispositions"]:
        # TODO: Import pc when opt_deps is implemented
        # disposition_mask = pc.is_in(pc.field("disposition"), pc.scalar(filters["dispositions"]))
        # table = table.filter(disposition_mask)
        pass

    # Apply edge strength filter
    if "min_edge_strength" in filters and filters["min_edge_strength"] is not None:
        # TODO: Import pc when opt_deps is implemented
        # edge_mask = pc.greater_equal(pc.field("weakest_edge_to_primary"), pc.scalar(filters["min_edge_strength"]))
        # table = table.filter(edge_mask)
        pass

    # Apply group size filter
    if "min_group_size" in filters and filters["min_group_size"] is not None:
        # This would require group stats, so we'll skip for now
        pass

    return table


def apply_filters_duckdb(table: Any, filters: Dict[str, Any]) -> Any:
    """
    Apply filters to DuckDB table/DataFrame.
    
    Args:
        table: DuckDB table or DataFrame to filter
        filters: Dictionary of filters to apply
        
    Returns:
        Filtered table/DataFrame
    """
    if not filters:
        return table

    # For DuckDB, we'll return the table as-is for now
    # The actual filtering should be done in SQL queries
    return table


# TODO: Implement unified sort resolution using the above functions
def resolve_sort(sort_key: str) -> SortSpec:
    """Convert sort key to unified SortSpec."""
    # TODO: Implement actual logic using get_order_by and build_sort_expression
    pass

def to_duckdb_order_by(spec: SortSpec) -> str:
    """Convert SortSpec to DuckDB ORDER BY clause."""
    # TODO: Implement actual logic
    pass

def to_pyarrow_sort_by(spec: SortSpec) -> list[tuple[str, str]]:
    """Convert SortSpec to PyArrow sort specification."""
    # TODO: Implement actual logic
    pass
