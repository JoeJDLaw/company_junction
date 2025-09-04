"""
Filtering and sorting utilities for ui_helpers refactor.

This module provides unified models for sort/filter operations.
"""

from dataclasses import dataclass
from typing import Literal, Dict, Any, List, Tuple
from src.utils.schema_utils import (
    GROUP_SIZE, MAX_SCORE, PRIMARY_NAME, GROUP_ID, DISPOSITION, WEAKEST_EDGE_TO_PRIMARY
)
from src.utils.opt_deps import PC
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
    Apply filters to PyArrow table using Arrow-native operations.
    
    This provides a pure PyArrow implementation without DuckDB dependency.
    
    Args:
        table: PyArrow table to filter
        filters: Dictionary of filters to apply
        
    Returns:
        Filtered PyArrow table
    """
    if not filters or PC is None:
        return table
    
    exprs = []
    
    # dispositions: IN (...)
    dispositions = filters.get("dispositions")
    if dispositions:
        import pyarrow as pa
        exprs.append(PC.is_in(PC.field(DISPOSITION), value_set=pa.array(dispositions)))
    
    # min_edge_strength: >= threshold
    min_es = filters.get("min_edge_strength", 0.0)
    if min_es not in (None, 0.0):
        exprs.append(PC.greater_equal(PC.field(WEAKEST_EDGE_TO_PRIMARY), PC.scalar(float(min_es))))
    
    if not exprs:
        return table
    
    # Combine expressions with AND
    predicate = exprs[0]
    for e in exprs[1:]:
        predicate = PC.and_(predicate, e)
    
    return table.filter(predicate)


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


def resolve_sort(sort_key: str) -> SortSpec:
    """
    Convert sort key to unified SortSpec.
    
    Args:
        sort_key: Sort key from UI dropdown
        
    Returns:
        Unified SortSpec with field, direction, and tie-breaker
    """
    # Extract sort field and direction
    if "Group Size" in sort_key:
        field = GROUP_SIZE
    elif "Max Score" in sort_key:
        field = MAX_SCORE
    elif "Account Name" in sort_key:
        field = PRIMARY_NAME
    else:
        # Unknown sort_key, fall back to config default
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
            # Parse the default to extract field and direction
            if "group_size" in default_sort.lower():
                field = GROUP_SIZE
            elif "max_score" in default_sort.lower():
                field = MAX_SCORE
            elif "primary_name" in default_sort.lower():
                field = PRIMARY_NAME
            else:
                field = GROUP_SIZE
            
            # Parse direction from default
            direction = "desc" if "desc" in default_sort.lower() else "asc"
        except Exception as e:
            logger.error(f"Failed to load config default sort, using hardcoded fallback: {e}")
            field = GROUP_SIZE
            direction = "desc"  # Default to DESC for GROUP_SIZE

    # Determine sort direction (only if not already set by fallback)
    if 'direction' not in locals():
        direction = "desc" if "(Desc)" in sort_key else "asc"
    
    # Return SortSpec with group_id tie-breaker for stability
    return SortSpec(
        field=field,
        direction=direction,
        tie_breaker=(GROUP_ID, "asc")
    )


def to_duckdb_order_by(spec: SortSpec) -> str:
    """
    Convert SortSpec to DuckDB ORDER BY clause.
    
    Args:
        spec: SortSpec to convert
        
    Returns:
        DuckDB ORDER BY clause string
    """
    # Convert direction to SQL syntax
    sql_direction = "DESC" if spec.direction == "desc" else "ASC"
    tie_direction = "DESC" if spec.tie_breaker[1] == "desc" else "ASC"
    
    return f"{spec.field} {sql_direction}, {spec.tie_breaker[0]} {tie_direction}"


def to_pyarrow_sort_by(spec: SortSpec) -> list[tuple[str, str]]:
    """
    Convert SortSpec to PyArrow sort specification.
    
    Args:
        spec: SortSpec to convert
        
    Returns:
        PyArrow sort specification list
    """
    # Convert direction to PyArrow syntax
    pyarrow_direction = "descending" if spec.direction == "desc" else "ascending"
    tie_direction = "descending" if spec.tie_breaker[1] == "desc" else "ascending"
    
    return [
        (spec.field, pyarrow_direction),
        (spec.tie_breaker[0], tie_direction)
    ]
