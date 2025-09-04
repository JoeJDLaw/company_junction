"""
Filtering and sorting utilities for ui_helpers refactor.

This module provides unified models for sort/filter operations.
"""

from dataclasses import dataclass
from typing import Literal

@dataclass(frozen=True)
class SortSpec:
    """Unified sort specification."""
    field: str
    direction: Literal["asc", "desc"]
    tie_breaker: tuple[str, Literal["asc", "desc"]] = ("group_id", "asc")

# TODO: Implement unified sort resolution
def resolve_sort(sort_key: str) -> SortSpec:
    """Convert sort key to unified SortSpec."""
    # TODO: Implement actual logic
    pass

def to_duckdb_order_by(spec: SortSpec) -> str:
    """Convert SortSpec to DuckDB ORDER BY clause."""
    # TODO: Implement actual logic
    pass

def to_pyarrow_sort_by(spec: SortSpec) -> list[tuple[str, str]]:
    """Convert SortSpec to PyArrow sort specification."""
    # TODO: Implement actual logic
    pass

# TODO: Move apply_filters functions here
def apply_filters_pyarrow(table: Any, filters: Dict[str, Any]) -> Any:
    """Apply filters to PyArrow table."""
    # TODO: Implement actual logic
    pass

def apply_filters_duckdb(table: Any, filters: Dict[str, Any]) -> Any:
    """Apply filters to DuckDB table/DataFrame."""
    # TODO: Implement actual logic
    pass
