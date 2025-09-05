"""
Group pagination utilities for ui_helpers refactor.

This module handles pagination logic for groups.
"""

import os
import time
import re
from typing import Dict, Any, List, Tuple

from .opt_deps import DUCKDB
from .schema_utils import (
    GROUP_ID, GROUP_SIZE, MAX_SCORE, PRIMARY_NAME, ACCOUNT_NAME, IS_PRIMARY,
    WEAKEST_EDGE_TO_PRIMARY, DISPOSITION
)
from .artifact_management import get_artifact_paths
from .filtering import get_order_by, build_sort_expression
from .filtering import apply_filters_duckdb
from .ui_session import session
from .metrics import record_groups_request, record_backend_choice, record_timeout, record_page_size_clamped
from .logging_utils import get_logger
from .sql_utils import in_clause as _in_clause

logger = get_logger(__name__)

# Exception for page fetch timeouts
class PageFetchTimeout(Exception):
    """Raised when page fetch exceeds timeout."""
    pass


def _build_where_clause(filters: Dict[str, Any], score_column: str) -> Tuple[str, List]:
    """
    Build WHERE clause and parameters for common filters.
    
    Args:
        filters: Filter dictionary
        score_column: Column name to use for min_edge_strength comparison
        
    Returns:
        Tuple of (where_clause, params)
    """
    where_sql = []
    params = []
    
    if filters.get("dispositions"):
        in_sql, in_params = _in_clause(filters["dispositions"])
        where_sql.append(f"{DISPOSITION} {in_sql}")
        params.extend(in_params)
    
    if (min_es := filters.get("min_edge_strength", 0.0)) not in (None, 0.0):
        where_sql.append(f"{score_column} >= ?")
        params.append(float(min_es))
    
    return (" AND ".join(where_sql) if where_sql else "1=1"), params


def _alias_order_by(order_by_clause: str) -> str:
    """Add table aliases to ORDER BY clause for DuckDB queries with word boundary safety."""
    if "primary_name" in order_by_clause.lower():
        # Use word boundaries to avoid replacing inside larger tokens
        return re.sub(r'\b' + re.escape(PRIMARY_NAME) + r'\b', f"p.{PRIMARY_NAME}", order_by_clause)
    
    # Use word boundaries for other fields
    result = re.sub(r'\b' + re.escape(GROUP_SIZE) + r'\b', f"s.{GROUP_SIZE}", order_by_clause)
    result = re.sub(r'\b' + re.escape(MAX_SCORE) + r'\b', f"s.{MAX_SCORE}", result)
    return result


def _safe_get_order_by(sort_key: str) -> str:
    """
    Safely get ORDER BY clause with defensive fallback for unknown sort keys.
    
    Args:
        sort_key: The sort key to resolve
        
    Returns:
        ORDER BY clause string
        
    Raises:
        ValueError: If sort_key is invalid and no fallback is available
    """
    try:
        return get_order_by(sort_key)
    except Exception as e:
        logger.warning(f"Failed to resolve sort_key '{sort_key}': {e}")
        # Fallback to a safe default
        try:
            return get_order_by("Group Size (Desc)")
        except Exception:
            # Last resort fallback
            logger.error("Failed to resolve fallback sort key, using hardcoded default")
            return f"{GROUP_SIZE} DESC, {GROUP_ID} ASC"


def _get_available_columns_for_pagination(parquet_path: str) -> List[str]:
    """Get available columns from parquet file for pagination queries."""
    try:
        import pyarrow.parquet as pq
        schema = pq.read_schema(parquet_path)
        return [field.name for field in schema]
    except Exception as e:
        logger.warning(f"Failed to read schema from {parquet_path}: {e}")
        # Fallback to basic columns
        return [GROUP_ID, ACCOUNT_NAME, DISPOSITION]


def _set_backend_choice(run_id: str, backend: str) -> None:
    """Set backend choice for a specific run."""
    try:
        session.set_backend_choice(run_id, backend)
    except Exception as e:
        logger.warning(f"Failed to set backend choice: {e}")


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
    start_time = time.time()
    logger.info(
        f"get_groups_page called | run_id={run_id} sort_key='{sort_key}' page={page} page_size={page_size}"
    )

    # Log the ORDER BY clause that will be used
    order_by = get_order_by(sort_key)
    logger.info(
        f"get_groups_page | run_id={run_id} sort_key='{sort_key}' order_by_requested='{order_by}'"
    )
    
    # Initialize metrics tracking
    backend_used = None
    source_used = None
    success = False

    # Load settings
    try:
        from .settings import get_settings
        settings = get_settings()
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

    # Check force flags first (highest priority)
    from .settings import get_ui_perf
    ui_perf = get_ui_perf(settings)
    force_pyarrow = ui_perf.get("force_pyarrow", False)
    force_duckdb = ui_perf.get("force_duckdb", False)
    
    # Handle force flag precedence explicitly
    if force_pyarrow and force_duckdb:
        logger.warning("Both force flags set; defaulting to DuckDB (precedence rule); disabling force_pyarrow")
        force_pyarrow = False
    
    if force_pyarrow:
        logger.info(f"Force PyArrow flag enabled | run_id={run_id}")
        _set_backend_choice(run_id, "pyarrow")
        record_backend_choice("forced", "pyarrow")
        try:
            result = get_groups_page_pyarrow(run_id, sort_key, page, page_size, filters)
            duration = time.time() - start_time
            record_groups_request("pyarrow", "review_ready", True, duration)
            return result
        except Exception as e:
            duration = time.time() - start_time
            record_groups_request("pyarrow", "review_ready", False, duration)
            raise
    
    if force_duckdb:
        logger.info(f"Force DuckDB flag enabled | run_id={run_id}")
        try:
            _set_backend_choice(run_id, "duckdb")
            record_backend_choice("forced", "duckdb")
            result = get_groups_page_duckdb(run_id, sort_key, page, page_size, filters)
            duration = time.time() - start_time
            record_groups_request("duckdb", "review_ready", True, duration)
            return result
        except Exception as e:
            duration = time.time() - start_time
            record_groups_request("duckdb", "review_ready", False, duration)
            logger.warning(f"Forced DuckDB failed: {e}")
            raise

    # Simplified backend selection logic to prevent fallback issues

    # Phase 1: Check for group_stats.parquet first (highest priority)
    artifact_paths = get_artifact_paths(run_id)
    group_stats_path = artifact_paths.get("group_stats_parquet")
    
    if group_stats_path and os.path.exists(group_stats_path):
        logger.info(f"Using group_stats.parquet for pagination | run_id={run_id}")
        try:
            _set_backend_choice(run_id, "duckdb")
            record_backend_choice("auto", "duckdb")
            result = get_groups_page_from_stats_duckdb(
                run_id, sort_key, page, page_size, filters
            )
            duration = time.time() - start_time
            record_groups_request("duckdb", "stats", True, duration)
            return result
        except Exception as e:
            duration = time.time() - start_time
            record_groups_request("duckdb", "stats", False, duration)
            logger.warning(f"group_stats.parquet failed, falling back: {e}")

    # Phase 2: Check if DuckDB is preferred over PyArrow
    if settings.get("ui_perf", {}).get("groups", {}).get("duckdb_prefer_over_pyarrow", False):
        logger.info(f"DuckDB preferred over PyArrow | run_id={run_id}")
        try:
            _set_backend_choice(run_id, "duckdb")
            record_backend_choice("auto", "duckdb")
            result = get_groups_page_duckdb(run_id, sort_key, page, page_size, filters)
            duration = time.time() - start_time
            record_groups_request("duckdb", "review_ready", True, duration)
            return result
        except Exception as e:
            duration = time.time() - start_time
            record_groups_request("duckdb", "review_ready", False, duration)
            logger.warning(f"DuckDB failed, falling back to PyArrow: {e}")

    # Phase 3: Use PyArrow (default fallback)
    logger.info(f"Using PyArrow for pagination | run_id={run_id}")
    _set_backend_choice(run_id, "pyarrow")
    record_backend_choice("auto", "pyarrow")
    try:
        result = get_groups_page_pyarrow(run_id, sort_key, page, page_size, filters)
        duration = time.time() - start_time
        record_groups_request("pyarrow", "review_ready", True, duration)
        return result
    except Exception as e:
        duration = time.time() - start_time
        record_groups_request("pyarrow", "review_ready", False, duration)
        raise


def get_groups_page_pyarrow(
    run_id: str, sort_key: str, page: int, page_size: int, filters: Dict[str, Any]
) -> Tuple[List[Dict[str, Any]], int]:
    """
    Get a page of groups using PyArrow backend.

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
        f"get_groups_page_pyarrow called | run_id={run_id} sort_key='{sort_key}' page={page} page_size={page_size}"
    )

    # Get artifact paths
    artifact_paths = get_artifact_paths(run_id)
    parquet_path = artifact_paths["review_ready_parquet"]

    if not os.path.exists(parquet_path):
        logger.warning(f"Parquet file not found: {parquet_path}")
        return [], 0

    # Load settings
    try:
        from .settings import get_settings
        settings = get_settings()
    except Exception:
        settings = {}

    # Check timeout with backward compatibility
    ui = settings.get("ui", {})
    max_pyarrow_seconds = (
        ui.get("max_pyarrow_groups_seconds")
        or ui.get("max_pyarrow_group_stats_seconds")
        or 5
    )
    start_time = time.time()

    def check_timeout():
        elapsed = time.time() - start_time
        if elapsed > max_pyarrow_seconds:
            raise PageFetchTimeout(
                f"PyArrow page fetch exceeded {max_pyarrow_seconds} second timeout after {elapsed:.1f}s"
            )

    # Log start
    logger.info(
        f'PyArrow groups page fetch start | run_id={run_id} sort="{sort_key}" page={page} page_size={page_size}'
    )

    # Step 1: Load parquet file
    step_start = time.time()
    try:
        import pyarrow.parquet as pq
        import pyarrow as pa
        # Project only needed columns to reduce IO and memory, with safety guard
        all_cols = pq.read_schema(parquet_path).names
        needed_columns = [GROUP_ID, GROUP_SIZE, MAX_SCORE, PRIMARY_NAME, DISPOSITION]
        present_columns = [c for c in needed_columns if c in all_cols]
        
        if len(present_columns) < len(needed_columns):
            missing = set(needed_columns) - set(present_columns)
            logger.warning(f"Stats columns missing in {parquet_path}: {missing}. Reading all columns.")
            table = pq.read_table(parquet_path)  # Fallback to all columns
        else:
            table = pq.read_table(parquet_path, columns=present_columns)
    except ImportError:
        logger.error("PyArrow not available for groups page querying")
        raise ImportError("PyArrow not available for groups page querying")
    except Exception as e:
        logger.error(f"Failed to read parquet file: {e}")
        raise

    load_time = time.time() - step_start
    logger.info(
        f"PyArrow parquet load | run_id={run_id} rows={table.num_rows} elapsed={load_time:.3f}s"
    )

    check_timeout()

    # Step 2: Apply filters
    step_start = time.time()
    from .filtering import apply_filters_pyarrow
    filtered_table = apply_filters_pyarrow(table, filters)
    filter_time = time.time() - step_start

    logger.info(
        f"PyArrow filters applied | run_id={run_id} rows={filtered_table.num_rows} elapsed={filter_time:.3f}s"
    )

    check_timeout()

    # Step 3: Build sort expression
    step_start = time.time()
    sort_keys = build_sort_expression(sort_key)
    sort_time = time.time() - step_start

    logger.info(
        f"PyArrow sort expression built | run_id={run_id} sort_keys={sort_keys} elapsed={sort_time:.3f}s"
    )

    check_timeout()

    # Step 4: Sort table
    step_start = time.time()
    try:
        # Map our (field, 'ascending'/'descending') tuples to PyArrow sort_by
        field, direction = sort_keys[0]
        ascending = direction == 'ascending'
        
        # Build sort keys once, then call sort_by once
        keys = [(field, ascending)]
        if GROUP_ID in filtered_table.column_names and field != GROUP_ID:
            keys.append((GROUP_ID, True))
        
        # Use explicit null placement for consistency with DuckDB NULLS LAST
        try:
            sorted_table = filtered_table.sort_by(keys, null_placement="at_end")
        except TypeError:
            # Fallback for older PyArrow versions
            sorted_table = filtered_table.sort_by(keys)
            
    except Exception as e:
        logger.error(f"Failed to sort table: {e}")
        # Fallback to pandas
        import pandas as pd
        df = filtered_table.to_pandas()
        # Fix the sorting logic with tie-breaker and explicit NULLs last
        if GROUP_ID in df.columns and field != GROUP_ID:
            df_sorted = df.sort_values([field, GROUP_ID], ascending=[ascending, True], na_position="last")
        else:
            df_sorted = df.sort_values(field, ascending=ascending, na_position="last")
        sorted_table = pa.Table.from_pandas(df_sorted)

    sort_exec_time = time.time() - step_start
    logger.info(
        f"PyArrow table sorted | run_id={run_id} elapsed={sort_exec_time:.3f}s"
    )

    check_timeout()

    # Step 5: Paginate
    step_start = time.time()
    
    # Clamp pagination inputs to avoid negative offsets and cap for performance
    page = max(1, int(page))
    requested_size = int(page_size)
    max_page_size = settings.get("ui", {}).get("max_page_size", 250)
    page_size = max(1, min(requested_size, max_page_size))
    if page_size != requested_size:  # Log when clamping occurs
        logger.info("Page size clamped from %s to %s (max_page_size limit)", requested_size, max_page_size)
        record_page_size_clamped()
    
    offset = (page - 1) * page_size
    end_offset = offset + page_size

    # Get total count before slicing
    total_groups = sorted_table.num_rows

    # Slice the table for the current page
    if offset >= total_groups:
        page_data = []
    else:
        page_table = sorted_table.slice(offset, end_offset - offset)
        page_data = page_table.to_pylist()  # list[dict]

    pagination_time = time.time() - step_start
    logger.info(
        f"PyArrow pagination | run_id={run_id} offset={offset} page_size={page_size} rows={len(page_data)} elapsed={pagination_time:.3f}s"
    )

    elapsed = time.time() - start_time
    logger.info(
        f'PyArrow groups page loaded | run_id={run_id} rows={len(page_data)} offset={offset} sort="{sort_key}" elapsed={elapsed:.3f}'
    )

    return page_data, total_groups


def get_groups_page_duckdb(
    run_id: str, sort_key: str, page: int, page_size: int, filters: Dict[str, Any]
) -> Tuple[List[Dict[str, Any]], int]:
    """
    Load groups page data using DuckDB for optimal performance.

    Args:
        run_id: Run ID to load data for
        sort_key: Field to sort by
        page: Page number (1-based)
        page_size: Number of groups per page
        filters: Optional filters to apply

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

    # Clamp pagination inputs to avoid negative offsets and cap for performance
    page = max(1, int(page))
    requested_size = int(page_size)
    max_page_size = settings.get("ui", {}).get("max_page_size", 250)
    page_size = max(1, min(requested_size, max_page_size))
    if page_size != requested_size:  # Log when clamping occurs
        logger.info("Page size clamped from %s to %s (max_page_size limit)", requested_size, max_page_size)
        record_page_size_clamped()
    
    # Log start
    logger.info(
        f'DuckDB groups page fetch start | run_id={run_id} sort="{sort_key}" page={page} page_size={page_size}'
    )

    # Step 1: DuckDB connection
    if DUCKDB is None:
        raise ImportError("DuckDB not available for groups page querying")

    conn = None
    try:
        conn = DUCKDB.connect(":memory:")
        duckdb_threads = int(duckdb_threads or 4)  # Ensure numeric
        duckdb_threads = min(duckdb_threads, 32)  # Double-enforce caps at call site
        conn.execute("PRAGMA threads=" + str(duckdb_threads))
        connect_time = time.time() - start_time

        logger.info(
            f"DuckDB connection | run_id={run_id} threads={duckdb_threads} elapsed={connect_time:.3f}s"
        )

        check_timeout()

        # Step 2: Build SQL query
        step_start = time.time()

        # Build WHERE clause using micro-DRY helper
        where_clause, params = _build_where_clause(filters, WEAKEST_EDGE_TO_PRIMARY)

        # Build ORDER BY clause using centralized mapping
        order_by_clause = _alias_order_by(_safe_get_order_by(sort_key))

        logger.info(
            f"groups_page_duckdb | run_id={run_id} sort_key='{sort_key}' order_by='{order_by_clause}' backend=duckdb global_sort=true"
        )

        # Calculate pagination
        offset = (page - 1) * page_size

        # Build SQL using string concatenation and parameters (safe - no user input)
        # Build the query with proper global sorting before pagination
        # Get available columns dynamically to avoid schema mismatches
        available_columns = _get_available_columns_for_pagination(parquet_path)
        
        # Build dynamic SQL based on available columns
        base_columns = [col for col in [GROUP_ID, ACCOUNT_NAME, IS_PRIMARY, WEAKEST_EDGE_TO_PRIMARY, DISPOSITION] if col in available_columns]
        if not base_columns:
            raise ValueError(f"No required columns found in {parquet_path}")
        
        # Build dynamic aggregation based on available columns
        stats_select = f"{GROUP_ID}, COUNT(*) AS {GROUP_SIZE}"
        if WEAKEST_EDGE_TO_PRIMARY in available_columns:
            stats_select += f", MAX({WEAKEST_EDGE_TO_PRIMARY}) AS {MAX_SCORE}"
        else:
            stats_select += f", 0.0 AS {MAX_SCORE}"
        
        # Build primary name selection based on available columns
        if IS_PRIMARY in available_columns and ACCOUNT_NAME in available_columns:
            primary_name_select = f"any_value({ACCOUNT_NAME}) FILTER (WHERE {IS_PRIMARY}) AS {PRIMARY_NAME}"
        elif ACCOUNT_NAME in available_columns:
            primary_name_select = f"any_value({ACCOUNT_NAME}) AS {PRIMARY_NAME}"
        else:
            primary_name_select = f"'' AS {PRIMARY_NAME}"
        
        # This ensures ORDER BY is applied to the entire dataset before LIMIT/OFFSET
        # Result: Consistent sorting across all pages, not just within each page
        page_sql = (
            "WITH base AS ("
            "  SELECT " + ",".join(base_columns) +
            "  FROM read_parquet(?) WHERE " + where_clause +
            "), stats AS ("
            "  SELECT " + stats_select +
            "  FROM base GROUP BY " + GROUP_ID +
            "), primary_names AS ("
            "  SELECT " + GROUP_ID + ", " + primary_name_select +
            "  FROM base GROUP BY " + GROUP_ID +
            ") "
            "SELECT s." + GROUP_ID + ", s." + GROUP_SIZE + ", s." + MAX_SCORE + ", COALESCE(p." + PRIMARY_NAME + ", '') AS " + PRIMARY_NAME +
            " FROM stats s LEFT JOIN primary_names p USING (" + GROUP_ID + ") "
            " ORDER BY " + order_by_clause + " NULLS LAST, s." + GROUP_ID + " ASC "
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

        elapsed = time.time() - start_time
        logger.info(
            f'DuckDB groups page loaded | run_id={run_id} rows={len(page_data)} offset={offset} sort="{sort_key}" elapsed={elapsed:.3f}'
        )

        return page_data, total_groups

    except Exception as e:
        logger.error(f"DuckDB query failed: {e}")
        raise
    finally:
        if conn:
            conn.close()


def get_groups_page_from_stats_duckdb(
    run_id: str, sort_key: str, page: int, page_size: int, filters: Dict[str, Any]
) -> Tuple[List[Dict[str, Any]], int]:
    """
    Get a page of groups using DuckDB from group_stats.parquet.

    Args:
        run_id: The run ID
        sort_key: The sort key
        page: The page number
        page_size: The page size
        filters: The filters dictionary

    Returns:
        Tuple of (page_data, total_groups)
    """
    start_time = time.time()
    logger.info(
        f"get_groups_page_from_stats_duckdb called | run_id={run_id} sort_key='{sort_key}' page={page} page_size={page_size}"
    )

    # Get artifact paths
    artifact_paths = get_artifact_paths(run_id)
    group_stats_path = artifact_paths.get("group_stats_parquet")

    if not group_stats_path or not os.path.exists(group_stats_path):
        logger.warning(f"group_stats.parquet not found: {group_stats_path}")
        raise FileNotFoundError(f"group_stats.parquet not found: {group_stats_path}")

    try:
        # Step 1: Connect to DuckDB
        if DUCKDB is None:
            raise ImportError("DuckDB not available for group stats querying")

        # Load settings for duckdb_threads
        try:
            from .settings import get_settings
            settings = get_settings()
        except Exception:
            settings = {}
        duckdb_threads = settings.get("ui", {}).get("duckdb_threads", 4)
        
        # Clamp pagination inputs to avoid negative offsets and cap for performance
        page = max(1, int(page))
        requested_size = int(page_size)
        max_page_size = settings.get("ui", {}).get("max_page_size", 250)
        page_size = max(1, min(requested_size, max_page_size))
        if page_size != requested_size:  # Log when clamping occurs
            logger.info("Page size clamped from %s to %s (max_page_size limit)", requested_size, max_page_size)

        conn = None
        try:
            conn = DUCKDB.connect(":memory:")
            duckdb_threads = int(duckdb_threads or 4)  # Ensure numeric
            duckdb_threads = min(duckdb_threads, 32)  # Double-enforce caps at call site
            conn.execute("PRAGMA threads=" + str(duckdb_threads))
            conn_time = time.time() - start_time
            logger.info(
                f"DuckDB connection | run_id={run_id} threads={duckdb_threads} elapsed={conn_time:.3f}s"
            )

            # Step 2: Build query using parameters
            step_start = time.time()
            
            # Build WHERE clause using micro-DRY helper
            where_clause, params = _build_where_clause(filters, MAX_SCORE)

            # Build ORDER BY clause - no aliasing needed for stats path (single table)
            order_by = _safe_get_order_by(sort_key)

            logger.info(
                f"groups_page_from_stats_duckdb | run_id={run_id} sort_key='{sort_key}' order_by_resolved='{order_by}' backend=duckdb global_sort=true"
            )

            # Get available columns dynamically to avoid schema mismatches
            available_columns = _get_available_columns_for_pagination(group_stats_path)
            stats_columns = [col for col in [GROUP_ID, GROUP_SIZE, MAX_SCORE, PRIMARY_NAME, DISPOSITION] if col in available_columns]
            
            if not stats_columns:
                raise ValueError(f"No required columns found in group_stats_parquet: {group_stats_path}")
            
            # Build the query with fallback for "Unknown" primary names
            # This joins back to review_ready_parquet to get fallback names for the current page
            review_ready_path = artifact_paths.get("review_ready_parquet")
            
            # Temporarily disable fallback query to fix immediate issues
            # TODO: Re-enable fallback query once DuckDB syntax issues are resolved
            if False:  # Disabled for now
                pass
            else:
                # Fallback to simple query if review_ready_parquet not available
                sql = (
                    "SELECT " + ",".join(stats_columns) +
                    " FROM ("
                    "  SELECT " + ",".join(stats_columns) +
                    "  FROM read_parquet(?) "
                    "  WHERE " + where_clause +
                    "  ORDER BY " + order_by + " NULLS LAST, " + GROUP_ID + " ASC"
                    " ) sorted_data "
                    " LIMIT ? OFFSET ?"
                )
                
                # Build parameters: group_stats_path, filter_params, page_size, offset
                sql_params = [group_stats_path, *params, page_size, (page - 1) * page_size]

            logger.info(
                f"DuckDB query built | run_id={run_id} filters='{where_clause}' order_by_resolved='{order_by}' elapsed={time.time() - step_start:.3f}s"
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

            elapsed = time.time() - start_time
            logger.info(
                f'Groups page from stats loaded | run_id={run_id} rows={len(page_data)} offset={(page - 1) * page_size} sort="{sort_key}" elapsed={elapsed:.3f}'
            )

            return page_data, total_groups

        finally:
            if conn:
                conn.close()

    except Exception as e:
        elapsed = time.time() - start_time
        logger.error(
            f'Groups page from stats failed | run_id={run_id} error="{str(e)}" elapsed={elapsed:.3f}'
        )
        raise


def get_total_groups_count(run_id: str, filters: Dict[str, Any]) -> int:
    """
    Get the total count of groups for a run with optional filters.

    Args:
        run_id: The run ID
        filters: Optional filters to apply

    Returns:
        Total number of groups
    """
    logger.info(f"get_total_groups_count called | run_id={run_id}")

    # Get artifact paths
    artifact_paths = get_artifact_paths(run_id)
    parquet_path = artifact_paths["review_ready_parquet"]

    if not os.path.exists(parquet_path):
        logger.warning(f"Parquet file not found: {parquet_path}")
        return 0

    # Load settings
    try:
        from .settings import get_settings
        settings = get_settings()
    except Exception:
        settings = {}

    # Check if we should use group_stats.parquet
    if settings.get("ui_perf", {}).get("groups", {}).get("use_stats_parquet", True):
        group_stats_path = artifact_paths.get("group_stats_parquet")
        if group_stats_path and os.path.exists(group_stats_path):
            try:
                return get_total_groups_count_from_stats_duckdb(run_id, filters)
            except Exception as e:
                logger.warning(f"group_stats.parquet failed, falling back: {e}")

    # Fallback to main parquet file
    try:
        return get_total_groups_count_from_main_parquet(run_id, filters)
    except Exception as e:
        logger.error(f"Failed to get total groups count: {e}")
        return 0


def get_total_groups_count_from_stats_duckdb(run_id: str, filters: Dict[str, Any]) -> int:
    """
    Get total groups count from group_stats.parquet using DuckDB.

    Args:
        run_id: The run ID
        filters: Optional filters to apply

    Returns:
        Total number of groups
    """
    logger.info(f"get_total_groups_count_from_stats_duckdb called | run_id={run_id}")

    # Get artifact paths
    artifact_paths = get_artifact_paths(run_id)
    group_stats_path = artifact_paths.get("group_stats_parquet")

    if not group_stats_path or not os.path.exists(group_stats_path):
        logger.warning(f"group_stats.parquet not found: {group_stats_path}")
        return 0

    try:
        # Load settings for duckdb_threads
        try:
            from .settings import get_settings
            settings = get_settings()
        except Exception:
            settings = {}
        duckdb_threads = settings.get("ui", {}).get("duckdb_threads", 4)

        # Connect to DuckDB
        if DUCKDB is None:
            raise ImportError("DuckDB not available for group stats count querying")

        conn = None
        try:
            conn = DUCKDB.connect(":memory:")
            duckdb_threads = int(duckdb_threads or 4)  # Ensure numeric
            duckdb_threads = min(duckdb_threads, 32)  # Double-enforce caps at call site
            conn.execute("PRAGMA threads=" + str(duckdb_threads))

            # Build WHERE clause using micro-DRY helper
            where_clause, params = _build_where_clause(filters, MAX_SCORE)

            # Build count query
            count_sql = (
                "SELECT COUNT(*) as total "
                "FROM read_parquet(?) "
                "WHERE " + where_clause
            )
            count_params = [group_stats_path, *params]

            # Execute query
            result = conn.execute(count_sql, count_params)
            total_groups = result.fetchone()[0]

            logger.info(
                f"Total groups count from stats | run_id={run_id} total={total_groups}"
            )

            return total_groups

        finally:
            if conn:
                conn.close()

    except Exception as e:
        logger.error(f"Failed to get total groups count from stats: {e}")
        return 0


def get_total_groups_count_from_main_parquet(run_id: str, filters: Dict[str, Any]) -> int:
    """
    Get total groups count from main parquet file using DuckDB.

    Args:
        run_id: The run ID
        filters: Optional filters to apply

    Returns:
        Total number of groups
    """
    logger.info(f"get_total_groups_count_from_main_parquet called | run_id={run_id}")

    # Get artifact paths
    artifact_paths = get_artifact_paths(run_id)
    parquet_path = artifact_paths["review_ready_parquet"]

    if not os.path.exists(parquet_path):
        logger.warning(f"Parquet file not found: {parquet_path}")
        return 0

    try:
        # Load settings for duckdb_threads
        try:
            from .settings import get_settings
            settings = get_settings()
        except Exception:
            settings = {}
        duckdb_threads = settings.get("ui", {}).get("duckdb_threads", 4)

        # Connect to DuckDB
        if DUCKDB is None:
            raise ImportError("DuckDB not available for main parquet count querying")

        conn = None
        try:
            conn = DUCKDB.connect(":memory:")
            duckdb_threads = int(duckdb_threads or 4)  # Ensure numeric
            duckdb_threads = min(duckdb_threads, 32)  # Double-enforce caps at call site
            conn.execute("PRAGMA threads=" + str(duckdb_threads))

            # Build WHERE clause using micro-DRY helper
            where_clause, params = _build_where_clause(filters, WEAKEST_EDGE_TO_PRIMARY)

            # Build count query
            count_sql = (
                "SELECT COUNT(DISTINCT " + GROUP_ID + ") as total "
                "FROM read_parquet(?) "
                "WHERE " + where_clause
            )
            count_params = [parquet_path, *params]

            # Execute query
            result = conn.execute(count_sql, count_params)
            total_groups = result.fetchone()[0]

            logger.info(
                f"Total groups count from main parquet | run_id={run_id} total={total_groups}"
            )

            return total_groups

        finally:
            if conn:
                conn.close()

    except Exception as e:
        logger.error(f"Failed to get total groups count from main parquet: {e}")
        return 0
