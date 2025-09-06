"""Group statistics utilities for ui_helpers refactor.

This module handles statistical computations for groups.
"""

from typing import Any

import pandas as pd

from .logging_utils import get_logger
from .opt_deps import DUCKDB
from .schema_utils import (
    ACCOUNT_NAME,
    GROUP_ID,
    GROUP_SIZE,
    IS_PRIMARY,
    MAX_SCORE,
    PRIMARY_NAME,
    WEAKEST_EDGE_TO_PRIMARY,
)

logger = get_logger(__name__)


def compute_group_stats(
    table: pd.DataFrame | Any,
    backend: str = "auto",
) -> pd.DataFrame:
    """Compute group statistics for sorting with backend selection.

    Args:
        table: DataFrame or PyArrow table with group data
        backend: Backend to use ("auto", "duckdb", or "pandas")

    Returns:
        DataFrame with group statistics

    """
    if backend == "duckdb" or (backend == "auto" and DUCKDB is not None):
        return compute_group_stats_duckdb(table)
    # Use pandas fallback
    return compute_group_stats_duckdb(table)  # This will fall back to pandas


def compute_group_stats_duckdb(table: pd.DataFrame) -> pd.DataFrame:
    """Compute group statistics for sorting using DuckDB.

    Args:
        table: DataFrame or PyArrow table with group data

    Returns:
        DataFrame with group statistics

    """
    # Check if DuckDB is available
    if DUCKDB is None:
        logger.warning("DuckDB not available, falling back to pandas for group stats")
        # Fall through to pandas implementation below
    else:
        try:
            # Convert to pandas if needed
            if hasattr(table, "to_pandas"):
                df = table.to_pandas()  # type: ignore[operator]
            else:
                df = table

            # Create DuckDB connection
            conn = DUCKDB.connect(":memory:")

            # Register DataFrame with DuckDB
            conn.register("groups_df", df)

            # Execute aggregation query - using schema constants (safe, not user input)
            query = (
                """
            SELECT
                """
                + GROUP_ID
                + """ as group_id,
                COUNT(*) as """
                + GROUP_SIZE
                + """,
                MAX(CASE WHEN """
                + WEAKEST_EDGE_TO_PRIMARY
                + """ IS NOT NULL THEN """
                + WEAKEST_EDGE_TO_PRIMARY
                + """ ELSE 0.0 END) as """
                + MAX_SCORE
                + """,
                FIRST(CASE WHEN """
                + IS_PRIMARY
                + """ THEN """
                + ACCOUNT_NAME
                + """ ELSE NULL END) as """
                + PRIMARY_NAME
                + """
            FROM groups_df
            GROUP BY """
                + GROUP_ID
                + """
            ORDER BY """
                + GROUP_ID
                + """
            """
            )

            result: pd.DataFrame = conn.execute(query).df()
            conn.close()

            return result

        except Exception as e:
            logger.warning(f"DuckDB execution failed: {e}, falling back to pandas")
            # Fall through to pandas implementation below

    # Fallback to pandas if DuckDB not available
    logger.warning("DuckDB not available, falling back to pandas for group stats")

    # Convert to pandas if needed
    if hasattr(table, "to_pandas"):
        df = table.to_pandas()  # type: ignore[operator]
    else:
        df = table

    # Compute group statistics
    stats_data = []
    for group_id in df[GROUP_ID].unique():
        group_data = df[df[GROUP_ID] == group_id]

        # Get group size
        group_size = len(group_data)

        # Get max score
        max_score = (
            group_data[WEAKEST_EDGE_TO_PRIMARY].max()
            if WEAKEST_EDGE_TO_PRIMARY in group_data.columns
            else 0.0
        )

        # Get primary record's account name
        primary_record = (
            group_data[group_data[IS_PRIMARY]].iloc[0]
            if group_data[IS_PRIMARY].any()
            else group_data.iloc[0]
        )
        primary_name = primary_record.get(ACCOUNT_NAME, "")

        stats_data.append(
            {
                GROUP_ID: group_id,
                GROUP_SIZE: group_size,
                MAX_SCORE: max_score,
                PRIMARY_NAME: primary_name or "",
            },
        )

    return pd.DataFrame(stats_data)


def _get_parquet_fingerprint(file_path: str) -> str:
    """Get a fingerprint for a parquet file based on mtime and size.

    Args:
        file_path: Path to the parquet file

    Returns:
        Fingerprint string

    """
    try:
        import os

        stat = os.stat(file_path)
        fingerprint = f"{stat.st_mtime}_{stat.st_size}"
        return fingerprint
    except OSError:
        return "unknown"
