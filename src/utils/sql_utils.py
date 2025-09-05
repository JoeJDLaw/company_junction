"""SQL utility functions for safe query building.

This module provides helper functions for building parameterized SQL queries
that prevent SQL injection while maintaining readability.
"""

from typing import List, Tuple


def in_clause(values: List) -> Tuple[str, List]:
    """Return 'IN (?,?,...)' and corresponding params, for DuckDB.

    Args:
        values: List of values to include in the IN clause

    Returns:
        Tuple of (sql_fragment, parameter_list)

    Examples:
        >>> in_clause(["A", "B", "C"])
        ('IN (?,?,?)', ['A', 'B', 'C'])
        >>> in_clause([])
        ('IN (NULL)', [])
        >>> in_clause([" keep ", "merge"])
        ('IN (?,?)', [' keep ', 'merge'])

    """
    if not values:
        return "IN (NULL)", []  # empty never matches
    placeholders = ",".join(["?"] * len(values))
    return "IN (" + placeholders + ")", list(values)


# Backward compatibility alias
_in_clause = in_clause

__all__ = ["_in_clause", "in_clause"]
