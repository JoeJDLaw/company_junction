"""
Group pagination utilities for ui_helpers refactor.

This module handles pagination logic for groups.
"""

import os
import time
from dataclasses import dataclass
from typing import Dict, Any, List, Tuple

from .opt_deps import DUCKDB, DUCKDB_AVAILABLE
from .schema_utils import (
    GROUP_ID, GROUP_SIZE, MAX_SCORE, PRIMARY_NAME, 
    IS_PRIMARY, ACCOUNT_NAME, WEAKEST_EDGE_TO_PRIMARY, DISPOSITION
)
from .artifact_management import get_artifact_paths
from .filtering import get_order_by, build_sort_expression, apply_filters_duckdb
from .group_stats import compute_group_stats_duckdb
from .logging_utils import get_logger

logger = get_logger(__name__)


def _in_clause(values: list) -> tuple[str, list]:
    """Return 'IN (?,?,...)' and corresponding params, for DuckDB."""
    if not values:
        return "IN (NULL)", []  # empty never matches
    placeholders = ",".join(["?"] * len(values))
    return "IN (" + placeholders + ")", list(values)


class PageFetchTimeout(Exception):
    """Exception raised when page fetch exceeds timeout."""
    pass


@dataclass
class PaginationSpec:
    """Logical pagination model."""
    filters: Dict[str, Any]
    sort: "SortSpec"  # TODO: Import from filtering when available
    offset: int
    limit: int


def _set_backend_choice(run_id: str, backend: str) -> None:
    """Set backend choice in session state for consistency."""
    try:
        from .ui_session import SessionState
        session_state = SessionState()
        session_state.set_backend_choice(run_id, backend)
    except Exception as e:
        logger.debug(f"Failed to set backend choice in session state: {e}")


def get_groups_page(
    run_id: str, sort_key: str, page: int, page_size: int, filters: Dict[str, Any]
) -> Tuple[List[Dict[str, Any]], int]:
    """
    Get a page of groups using the configured backend (PyArrow or DuckDB).

    Args:
        run_id: The run ID
        sort_key: The sort key
        page: The page number
        page_size: The page size
        filters: The filters dictionary

    Returns:
        Tuple of (page_data, total_groups)
    """
    logger.info(
        f"get_groups_page called | run_id={run_id} sort_key='{sort_key}' page={page} page_size={page_size}"
    )

    # Log the ORDER BY clause that will be used
    order_by = get_order_by(sort_key)
    logger.info(
        f"get_groups_page | run_id={run_id} sort_key='{sort_key}' clause='{order_by}'"
    )

    # Load settings
    try:
        from .settings import get_settings
        settings = get_settings()
        # Settings loading is now logged by the get_settings function itself
    except Exception as e:
        logger.error(f"Failed to load settings: {e}")
        # Fallback to default settings
        settings = {
            "ui_perf": {
                "groups": {"use_stats_parquet": True},
                "details": {
                    "use_details_parquet": True,
                    "allow_pyarrow_fallback": False,
                },
            }
        }

    # Simplified backend selection logic to prevent fallback issues

    # Phase 1: Check for group_stats.parquet first (highest priority)
    artifact_paths = get_artifact_paths(run_id)
    group_stats_path = artifact_paths.get("group_stats_parquet")

    logger.info(
        f"Backend selection | run_id={run_id} group_stats_path={group_stats_path} exists={os.path.exists(group_stats_path) if group_stats_path else False}"
    )

    if (
        group_stats_path
        and os.path.exists(group_stats_path)
        and settings.get("ui_perf", {}).get("groups", {}).get("use_stats_parquet", True)
        and DUCKDB_AVAILABLE
    ):
        logger.info(
            f"Using persisted group stats | run_id={run_id} path={group_stats_path}"
        )

        try:
            logger.info(
                f"groups_perf: backend=duckdb reason=stats_parquet_available | run_id={run_id}"
            )

            # Persist backend choice in session state
            _set_backend_choice(run_id, "duckdb")

            # Use DuckDB for fast pagination from group_stats.parquet
            logger.info(
                f"get_groups_page: stats path selected | run_id={run_id} sort_key='{sort_key}' backend=duckdb source=stats"
            )
            return get_groups_page_from_stats_duckdb(
                run_id, sort_key, page, page_size, filters, group_stats_path
            )
        except Exception as e:
            logger.error(
                f"DuckDB stats query failed, falling back to PyArrow | run_id={run_id} error='{str(e)}'"
            )
            # Continue to next backend option instead of immediate fallback

    # Phase 2: Check ui.use_duckdb_for_groups flag (second priority)
    use_duckdb_flag = settings.get("ui", {}).get("use_duckdb_for_groups", False)

    if use_duckdb_flag and DUCKDB_AVAILABLE:
        logger.info(f"groups_perf: backend=duckdb reason=flag_true | run_id={run_id}")

        # Persist backend choice in session state
        _set_backend_choice(run_id, "duckdb")

        logger.info(
            f"get_groups_page: non-stats path selected | run_id={run_id} sort_key='{sort_key}' backend=duckdb source=review_ready"
        )
        return get_groups_page_duckdb(run_id, sort_key, page, page_size, filters)

    # Phase 3: Check threshold-based routing (third priority)
    use_duckdb_threshold = (
        settings.get("ui_perf", {})
        .get("groups", {})
        .get("duckdb_prefer_over_pyarrow", False)
    )
    rows_threshold = (
        settings.get("ui_perf", {})
        .get("groups", {})
        .get("rows_duckdb_threshold", 30000)
    )

    if use_duckdb_threshold and DUCKDB_AVAILABLE:
        # Quick check of data size to determine backend
        try:
            review_path = artifact_paths.get("review_ready_parquet")
            if review_path and os.path.exists(review_path):
                # Quick row count check
                from .opt_deps import DS
                if DS is not None:
                    dataset = DS.dataset(review_path)
                    total_rows = dataset.count_rows()

                    if total_rows > rows_threshold:
                        logger.info(
                            f"groups_perf: backend=duckdb reason=threshold rows={total_rows} > {rows_threshold} | run_id={run_id}"
                        )

                        # Persist backend choice in session state
                        _set_backend_choice(run_id, "duckdb")

                        logger.info(
                            f"get_groups_page: threshold path selected | run_id={run_id} sort_key='{sort_key}' backend=duckdb source=review_ready"
                        )
                        return get_groups_page_duckdb(
                            run_id, sort_key, page, page_size, filters
                        )
        except Exception as e:
            logger.warning(f"Failed to check data size for backend selection: {e}")

    # Final fallback: Use PyArrow
    logger.info(f"groups_perf: backend=pyarrow reason=final_fallback | run_id={run_id}")

    # Persist backend choice in session state
    _set_backend_choice(run_id, "pyarrow")

    logger.info(
        f"get_groups_page: PyArrow fallback selected | run_id={run_id} sort_key='{sort_key}' backend=pyarrow source=review_ready"
    )
    return get_groups_page_pyarrow(run_id, sort_key, page, page_size, filters)


def get_groups_page_pyarrow(
    run_id: str, sort_key: str, page: int, page_size: int, filters: Dict[str, Any]
) -> Tuple[List[Dict[str, Any]], int]:
    """
    Get a page of groups using PyArrow for server-side pagination.

    Args:
        run_id: Run ID to load data from
        sort_key: Sort key from dropdown
        page: Page number (1-based)
        page_size: Number of groups per page
        filters: Dictionary of active filters

    Returns:
        Tuple of (groups_data, total_count)
    """
    start_time = time.time()
    step_start = time.time()

    try:
        # Get artifact paths
        artifact_paths = get_artifact_paths(run_id)
        parquet_path = artifact_paths["review_ready_parquet"]

        if not os.path.exists(parquet_path):
            logger.warning(f"Parquet file not found: {parquet_path}")
            return [], 0

        # Log start
        logger.info(
            f'Groups page fetch start | run_id={run_id} sort="{sort_key}" page={page} page_size={page_size}'
        )

        # Check timeout periodically
        def check_timeout():
            elapsed = time.time() - start_time
            if elapsed > timeout_seconds:
                raise PageFetchTimeout(
                    f"Page fetch exceeded {timeout_seconds} second timeout after {elapsed:.1f}s"
                )

        # Load settings for timeout
        try:
            from .settings import get_settings
            settings = get_settings()
            timeout_seconds = settings.get("ui", {}).get("timeout_seconds", 30)
        except Exception:
            timeout_seconds = 30

        # Step 1: Load data with DuckDB or fallback to pandas
        step_start = time.time()
        if DUCKDB is None:
            # Fallback to pandas if DuckDB not available
            logger.warning("DuckDB not available, falling back to pandas for data loading")

            # Use pandas to read parquet directly
            import pandas as pd
            df = pd.read_parquet(parquet_path)

            # Project columns
            header_columns = [
                GROUP_ID,
                ACCOUNT_NAME,
                IS_PRIMARY,
                WEAKEST_EDGE_TO_PRIMARY,
                DISPOSITION,
            ]
            existing_columns = [col for col in header_columns if col in df.columns]

            # Select only existing columns
            projected_df = df[existing_columns]
            projected_table = projected_df  # Keep as pandas DataFrame for now
        else:
            try:
                conn = DUCKDB.connect(":memory:")

                # Read parquet and project columns
                header_columns = [
                    GROUP_ID,
                    ACCOUNT_NAME,
                    IS_PRIMARY,
                    WEAKEST_EDGE_TO_PRIMARY,
                    DISPOSITION,
                ]

                # Check which columns exist in the parquet file
                schema_query = "DESCRIBE SELECT * FROM read_parquet(?) LIMIT 0"
                schema_result = conn.execute(schema_query, [parquet_path]).df()
                available_columns = schema_result['column_name'].tolist()

                # Only include columns that exist
                existing_columns = [col for col in header_columns if col in available_columns]

                logger.info(
                    f"Column projection | run_id={run_id} available={len(available_columns)} projected={len(existing_columns)} columns={existing_columns}"
                )

                # Read data with projection
                columns_str = ", ".join(existing_columns)
                query = "SELECT " + columns_str + " FROM read_parquet(?)"
                projected_df = conn.execute(query, [parquet_path]).df()
                projected_table = projected_df  # Keep as pandas DataFrame

                conn.close()

            except Exception as e:
                logger.warning(f"DuckDB execution failed: {e}, falling back to pandas")

                # Fallback to pandas if DuckDB execution fails
                import pandas as pd
                df = pd.read_parquet(parquet_path)

                # Project columns
                header_columns = [
                    GROUP_ID,
                    ACCOUNT_NAME,
                    IS_PRIMARY,
                    WEAKEST_EDGE_TO_PRIMARY,
                    DISPOSITION,
                ]
                existing_columns = [col for col in header_columns if col in df.columns]

                # Select only existing columns
                projected_df = df[existing_columns]
                projected_table = projected_df  # Keep as pandas DataFrame for now

        projection_time = time.time() - step_start
        # Get row count (works for both pandas and PyArrow)
        row_count = projected_table.shape[0] if hasattr(projected_table, 'shape') else projected_table.num_rows

        logger.info(
            f"Projection complete | run_id={run_id} rows={row_count} elapsed={projection_time:.3f}s"
        )

        check_timeout()

        # Step 3: Apply filters
        step_start = time.time()
        filtered_table = apply_filters_duckdb(projected_table, filters)
        filter_time = time.time() - step_start

        # Get row counts (works for both pandas and PyArrow)
        before_count = projected_table.shape[0] if hasattr(projected_table, 'shape') else projected_table.num_rows
        after_count = filtered_table.shape[0] if hasattr(filtered_table, 'shape') else filtered_table.num_rows

        logger.info(
            f"Filters applied | run_id={run_id} before={before_count} after={after_count} elapsed={filter_time:.3f}s"
        )

        check_timeout()

        # Step 4: Compute group statistics
        step_start = time.time()
        groups_table = compute_group_stats_duckdb(filtered_table)
        stats_time = time.time() - step_start

        # Get group count (works for both pandas and PyArrow)
        group_count = groups_table.shape[0] if hasattr(groups_table, 'shape') else groups_table.num_rows

        logger.info(
            f"Group stats computed | run_id={run_id} groups={group_count} elapsed={stats_time:.3f}s"
        )

        # Auto-switch to DuckDB if group stats take too long
        try:
            from .settings import get_settings
            settings = get_settings()
            max_pyarrow_seconds = settings.get("ui", {}).get(
                "max_pyarrow_group_stats_seconds", 5
            )
        except Exception:
            max_pyarrow_seconds = 5

        if stats_time > max_pyarrow_seconds and DUCKDB_AVAILABLE:
            logger.info(
                f"Auto-switching groups backend to DuckDB | run_id={run_id} reason=pyarrow_groupby_slow elapsed={stats_time:.3f}s"
            )
            # Close current connection and switch to DuckDB
            return get_groups_page_duckdb(run_id, sort_key, page, page_size, filters)

        check_timeout()

        # Get total count (works for both pandas and PyArrow)
        total_groups = groups_table.shape[0] if hasattr(groups_table, 'shape') else groups_table.num_rows

        if total_groups == 0:
            elapsed = time.time() - start_time
            logger.info(
                f'Groups page loaded | run_id={run_id} rows=0 offset=0 sort="{sort_key}" elapsed={elapsed:.3f}'
            )
            return [], 0

        # Calculate pagination
        offset = (page - 1) * page_size
        limit = page_size

        # Step 5: Calculate pagination
        offset = (page - 1) * page_size
        limit = page_size

        # Step 6: Apply sorting and slicing
        if offset >= total_groups:
            page_data = []
        else:
            step_start = time.time()
            sort_keys = build_sort_expression(sort_key)

            # Sort the groups table (works for both pandas and PyArrow)
            if hasattr(groups_table, 'sort_values'):
                # pandas DataFrame - map our (field, 'ascending'/'descending') tuples
                field, direction = sort_keys[0]
                ascending = (direction == 'ascending')
                sorted_table = groups_table.sort_values(field, ascending=ascending)
            else:
                # PyArrow table
                sorted_table = groups_table.sort_by(sort_keys)

            sort_time = time.time() - step_start

            logger.info(
                f"Sorting applied | run_id={run_id} sort_keys={sort_keys} elapsed={sort_time:.3f}s"
            )

            check_timeout()

            # Step 7: Apply slice (LIMIT/OFFSET)
            step_start = time.time()
            if hasattr(sorted_table, 'iloc'):
                # pandas DataFrame
                page_table = sorted_table.iloc[offset:offset + limit]
            else:
                # PyArrow table
                page_table = sorted_table.slice(offset, limit)

            slice_time = time.time() - step_start

            # Get slice row count
            slice_rows = page_table.shape[0] if hasattr(page_table, 'shape') else page_table.num_rows

            logger.info(
                f"Slice applied | run_id={run_id} offset={offset} limit={limit} slice_rows={slice_rows} elapsed={slice_time:.3f}s"
            )

            # Step 8: Convert slice to list
            step_start = time.time()
            if hasattr(page_table, 'to_dict'):
                # pandas DataFrame
                page_data = page_table.to_dict('records')
            else:
                # PyArrow table
                page_data = page_table.to_pylist()

            pandas_time = time.time() - step_start

            logger.info(
                f"Data conversion | run_id={run_id} rows={len(page_data)} elapsed={pandas_time:.3f}s"
            )

        elapsed = time.time() - start_time
        logger.info(
            f'Groups page loaded | run_id={run_id} rows={len(page_data)} offset={offset} sort="{sort_key}" elapsed={elapsed:.3f} projected_cols={existing_columns}'
        )

        return page_data, total_groups

    except PageFetchTimeout:
        elapsed = time.time() - start_time
        logger.error(
            f"Groups page load timeout | run_id={run_id} elapsed={elapsed:.3f}"
        )
        raise
    except Exception as e:
        elapsed = time.time() - start_time
        logger.error(
            f'Groups page load failed | run_id={run_id} error="{str(e)}" elapsed={elapsed:.3f}'
        )
        raise


def get_groups_page_duckdb(
    run_id: str, sort_key: str, page: int, page_size: int, filters: Dict[str, Any]
) -> Tuple[List[Dict[str, Any]], int]:
    """
    Load groups page data using DuckDB for optimal performance.

    Args:
        run_id: Run ID to load data for
        page: Page number (1-based)
        page_size: Number of groups per page
        filters: Optional filters to apply
        sort_by: Field to sort by
        sort_order: Sort order (asc/desc)

    Returns:
        Tuple of (page_data, total_groups)
    """
    # Load settings
    try:
        from .settings import get_settings
        settings = get_settings()
    except Exception:
        settings = {}
    duckdb_threads = settings.get("ui", {}).get("duckdb_threads", 4)

    # Get artifact paths
    artifact_paths = get_artifact_paths(run_id)
    parquet_path = artifact_paths["review_ready_parquet"]

    if not os.path.exists(parquet_path):
        logger.warning(f"Parquet file not found: {parquet_path}")
        return [], 0

    # Start timing
    start_time = time.time()

    # Check timeout periodically
    def check_timeout():
        elapsed = time.time() - start_time
        timeout_seconds = settings.get("ui", {}).get("timeout_seconds", 30)
        if elapsed > timeout_seconds:
            raise PageFetchTimeout(
                f"Page fetch exceeded {timeout_seconds} second timeout after {elapsed:.1f}s"
            )

    # Log start
    logger.info(
        f'DuckDB groups page fetch start | run_id={run_id} sort="{sort_key}" page={page} page_size={page_size}'
    )

    # Step 1: DuckDB connection
    if DUCKDB is None:
        raise ImportError("DuckDB not available for groups page querying")

    step_start = time.time()
    conn = DUCKDB.connect(":memory:")
    conn.execute(f"PRAGMA threads = {duckdb_threads}")
    connect_time = time.time() - step_start

    logger.info(
        f"DuckDB connection | run_id={run_id} threads={duckdb_threads} elapsed={connect_time:.3f}s"
    )

    check_timeout()

    # Step 2: Build SQL query
    step_start = time.time()

    # Build WHERE clause for filters using parameters
    where_sql = []
    params = []

    if filters.get("dispositions"):
        in_sql, in_params = _in_clause(filters["dispositions"])
        where_sql.append(f"{DISPOSITION} {in_sql}")
        params.extend(in_params)

    if (min_es := filters.get("min_edge_strength", 0.0)) not in (None, 0.0):
        where_sql.append(f"{WEAKEST_EDGE_TO_PRIMARY} >= ?")
        params.append(float(min_es))

    where_clause = " AND ".join(where_sql) if where_sql else "1=1"

    # Build ORDER BY clause using centralized mapping
    order_by_clause = get_order_by(sort_key)
    # Note: get_order_by returns column names without table aliases, so we need to add them
    if "primary_name" in order_by_clause.lower():
        order_by_clause = order_by_clause.replace(PRIMARY_NAME, f"p.{PRIMARY_NAME}")
    else:
        order_by_clause = order_by_clause.replace(
            GROUP_SIZE, f"s.{GROUP_SIZE}"
        ).replace(MAX_SCORE, f"s.{MAX_SCORE}")

    logger.info(
        f"groups_page_duckdb | run_id={run_id} sort_key='{sort_key}' clause='{order_by_clause}' backend=duckdb global_sort=true"
    )

    # Calculate pagination
    offset = (page - 1) * page_size

    # Build SQL using string concatenation and parameters (safe - no user input)
    # Build the query with proper global sorting before pagination
    # This ensures ORDER BY is applied to the entire dataset before LIMIT/OFFSET
    # Result: Consistent sorting across all pages, not just within each page
    page_sql = (
        "WITH base AS ("
        "  SELECT " + ",".join([GROUP_ID, ACCOUNT_NAME, IS_PRIMARY, WEAKEST_EDGE_TO_PRIMARY, DISPOSITION]) +
        "  FROM read_parquet(?) WHERE " + where_clause +
        "), stats AS ("
        "  SELECT " + GROUP_ID + ", COUNT(*) AS " + GROUP_SIZE + ", MAX(" + WEAKEST_EDGE_TO_PRIMARY + ") AS " + MAX_SCORE +
        "  FROM base GROUP BY " + GROUP_ID +
        "), primary_names AS ("
        "  SELECT " + GROUP_ID + ", any_value(" + ACCOUNT_NAME + ") FILTER (WHERE " + IS_PRIMARY + ") AS " + PRIMARY_NAME +
        "  FROM base GROUP BY " + GROUP_ID +
        ") "
        "SELECT s." + GROUP_ID + ", s." + GROUP_SIZE + ", s." + MAX_SCORE + ", COALESCE(p." + PRIMARY_NAME + ", '') AS " + PRIMARY_NAME +
        " FROM stats s LEFT JOIN primary_names p USING (" + GROUP_ID + ") "
        " ORDER BY " + order_by_clause + ", s." + GROUP_ID + " ASC "
        " LIMIT ? OFFSET ?"
    )
    
    # Build parameters: parquet_path, filter_params, page_size, offset
    page_params = [parquet_path, *params, page_size, offset]

    query_build_time = time.time() - step_start

    logger.info(
        f'DuckDB query built | run_id={run_id} where_clause="{where_clause}" order_by="{order_by_clause}" elapsed={query_build_time:.3f}s'
    )

    check_timeout()

    # Step 3: Execute query
    step_start = time.time()
    result = conn.execute(page_sql, page_params)
    query_exec_time = time.time() - step_start

    logger.info(
        f"DuckDB query executed | run_id={run_id} elapsed={query_exec_time:.3f}s"
    )

    check_timeout()

    # Step 4: Convert to pandas
    step_start = time.time()
    df = result.df()
    pandas_time = time.time() - step_start

    logger.info(
        f"DuckDB pandas conversion | run_id={run_id} rows={len(df)} elapsed={pandas_time:.3f}s"
    )

    # Step 5: Convert to list of dicts
    page_data = df.to_dict("records")

    # Get total count using parameters
    count_sql = (
        "WITH base AS ("
        "  SELECT " + GROUP_ID +
        "  FROM read_parquet(?) WHERE " + where_clause +
        ") SELECT COUNT(DISTINCT " + GROUP_ID + ") as total_groups FROM base"
    )
    count_params = [parquet_path, *params]
    total_result = conn.execute(count_sql, count_params)
    total_groups = total_result.fetchone()[0]

    # Close connection
    conn.close()

    elapsed = time.time() - start_time
    logger.info(
        f'DuckDB groups page loaded | run_id={run_id} rows={len(page_data)} offset={offset} sort="{sort_key}" elapsed={elapsed:.3f}'
    )

    return page_data, total_groups


def get_groups_page_from_stats_duckdb(
    run_id: str,
    sort_key: str,
    page: int,
    page_size: int,
    filters: Dict[str, Any],
    group_stats_path: str,
) -> Tuple[List[Dict[str, Any]], int]:
    """
    Get a page of groups using DuckDB from group_stats.parquet.

    Args:
        run_id: The run ID
        sort_key: The sort key
        page: The page number
        page_size: The page size
        filters: The filters dictionary
        group_stats_path: Path to group_stats.parquet

    Returns:
        Tuple of (page_data, total_groups)
    """
    start_time = time.time()
    logger.info(
        f"get_groups_page_from_stats_duckdb called | run_id={run_id} sort_key='{sort_key}' page={page} page_size={page_size}"
    )

    try:
        # Step 1: Connect to DuckDB
        if DUCKDB is None:
            raise ImportError("DuckDB not available for group stats querying")

        step_start = time.time()
        conn = DUCKDB.connect(":memory:")
        conn_time = time.time() - step_start
        logger.info(
            f"DuckDB connection | run_id={run_id} threads=4 elapsed={conn_time:.3f}s"
        )

        # Step 2: Build query using parameters
        step_start = time.time()
        where_sql = []
        params = []

        if filters.get("dispositions"):
            in_sql, in_params = _in_clause(filters["dispositions"])
            where_sql.append(f"{DISPOSITION} {in_sql}")
            params.extend(in_params)

        if (min_es := filters.get("min_edge_strength", 0.0)) not in (None, 0.0):
            where_sql.append(f"{MAX_SCORE} >= ?")
            params.append(float(min_es))

        where_clause = " AND ".join(where_sql) if where_sql else "1=1"

        # Build ORDER BY clause using centralized mapping
        order_by = get_order_by(sort_key)

        logger.info(
            f"groups_page_from_stats_duckdb | run_id={run_id} sort_key='{sort_key}' clause='{order_by}' backend=duckdb global_sort=true"
        )

        # Build the query using string concatenation and parameters (safe - no user input)
        # This ensures ORDER BY is applied to the entire dataset before LIMIT/OFFSET
        # Result: Consistent sorting across all pages, not just within each page
        sql = (
            "SELECT " + ",".join([GROUP_ID, GROUP_SIZE, MAX_SCORE, PRIMARY_NAME, DISPOSITION]) +
            " FROM ("
            "  SELECT " + ",".join([GROUP_ID, GROUP_SIZE, MAX_SCORE, PRIMARY_NAME, DISPOSITION]) +
            "  FROM read_parquet(?) "
            "  WHERE " + where_clause +
            "  ORDER BY " + order_by + ", " + GROUP_ID + " ASC"
            " ) sorted_data "
            " LIMIT ? OFFSET ?"
        )
        
        # Build parameters: group_stats_path, page_size, offset
        sql_params = [group_stats_path, page_size, (page - 1) * page_size]

        logger.info(
            f"DuckDB query built | run_id={run_id} filters='{where_clause}' clause='{order_by}' elapsed={time.time() - step_start:.3f}s"
        )

        # Step 3: Execute query
        step_start = time.time()
        result = conn.execute(sql, sql_params)
        query_time = time.time() - step_start
        logger.info(
            f"DuckDB query executed | run_id={run_id} elapsed={query_time:.3f}s"
        )

        # Step 4: Convert to pandas
        step_start = time.time()
        df_result = result.df()
        pandas_time = time.time() - step_start
        logger.info(
            f"DuckDB pandas conversion | run_id={run_id} rows={len(df_result)} elapsed={pandas_time:.3f}s"
        )

        # Step 5: Get total count using parameters
        step_start = time.time()
        count_sql = (
            "SELECT COUNT(*) as total "
            "FROM read_parquet(?) "
            "WHERE " + where_clause
        )
        count_params = [group_stats_path, *params]
        count_result = conn.execute(count_sql, count_params)
        total_groups = count_result.fetchone()[0]
        count_time = time.time() - step_start
        logger.info(
            f"Total count query | run_id={run_id} total={total_groups} elapsed={count_time:.3f}s"
        )

        # Step 6: Convert to list format
        page_data = df_result.to_dict("records")

        # Close connection
        conn.close()

        elapsed = time.time() - start_time
        logger.info(
            f'Groups page from stats loaded | run_id={run_id} rows={len(page_data)} offset={(page - 1) * page_size} sort="{sort_key}" elapsed={elapsed:.3f}'
        )

        return page_data, total_groups

    except Exception as e:
        elapsed = time.time() - start_time
        logger.error(
            f'Groups page from stats failed | run_id={run_id} error="{str(e)}" elapsed={elapsed:.3f}'
        )
        raise


def get_total_groups_count(run_id: str, filters: Dict[str, Any]) -> int:
    """Get total count of groups."""
    try:
        artifact_paths = get_artifact_paths(run_id)
        parquet_path = artifact_paths["review_ready_parquet"]

        if not os.path.exists(parquet_path):
            return 0

        if DUCKDB is None:
            # Fallback to pandas
            import pandas as pd
            df = pd.read_parquet(parquet_path)
            filtered_df = apply_filters_duckdb(df, filters)
            return filtered_df[GROUP_ID].nunique()
        else:
            # Use DuckDB for faster counting
            conn = DUCKDB.connect(":memory:")
            
            # Build WHERE clause for filters using parameters
            where_sql = []
            params = []
            
            if filters.get("dispositions"):
                in_sql, in_params = _in_clause(filters["dispositions"])
                where_sql.append(f"{DISPOSITION} {in_sql}")
                params.extend(in_params)

            if (min_es := filters.get("min_edge_strength", 0.0)) not in (None, 0.0):
                where_sql.append(f"{WEAKEST_EDGE_TO_PRIMARY} >= ?")
                params.append(float(min_es))

            where_clause = " AND ".join(where_sql) if where_sql else "1=1"

            # Use parameters for safe execution
            count_sql = (
                "SELECT COUNT(DISTINCT " + GROUP_ID + ") as total_groups "
                "FROM read_parquet(?) "
                "WHERE " + where_clause
            )
            
            count_params = [parquet_path, *params]
            result = conn.execute(count_sql, count_params)
            total_groups = result.fetchone()[0]
            conn.close()
            
            return total_groups

    except Exception as e:
        logger.error(f"Failed to get total groups count: {e}")
        return 0
