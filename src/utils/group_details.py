"""Group details utilities for ui_helpers refactor.

This module fetches detailed rows for a single group (members, edges, etc).

Backend Selection:
- DuckDB: Fast filtering + pagination for large datasets
- PyArrow: Memory-efficient processing with pandas fallback

Settings Keys:
- ui.duckdb_threads: Thread count for DuckDB (default: 4)
- ui.timeout_seconds: DuckDB timeout (default: 30s)
- ui.max_pyarrow_groups_seconds: PyArrow timeout (default: 5s)
- ui.max_pyarrow_group_stats_seconds: Legacy timeout key (backward compat)

Fallbacks:
- PyArrow → pandas for sorting failures
- Unknown sort columns → ACCOUNT_NAME ASC
- Missing backends → ImportError with clear message
"""

import os
import time
from typing import Any, Dict, List, Tuple

from .artifact_management import get_artifact_paths
from .filtering import get_order_by  # if you want user-facing sort options
from .logging_utils import get_logger
from .metrics import (
    record_backend_choice,
    record_details_request,
    record_page_size_clamped,
)
from .opt_deps import DUCKDB
from .schema_utils import (
    ACCOUNT_ID,
    ACCOUNT_NAME,
    CREATED_DATE,
    # add any extra detail columns you need here, e.g. DOMAIN, EMAIL, etc.
    DISPOSITION,
    GROUP_ID,
    IS_PRIMARY,
    SUFFIX_CLASS,
    WEAKEST_EDGE_TO_PRIMARY,
)
from .sql_utils import in_clause as _in_clause
from .ui_session import session

logger = get_logger(__name__)

# Column list constant for details queries
# Updated to match actual group_details.parquet schema from cleaning.py
DETAILS_COLUMNS = [
    GROUP_ID,
    ACCOUNT_ID,
    ACCOUNT_NAME,
    SUFFIX_CLASS,
    CREATED_DATE,
    DISPOSITION,
]

# Legacy columns that may be missing in some parquet files
LEGACY_COLUMNS = [IS_PRIMARY, WEAKEST_EDGE_TO_PRIMARY]

# Micro-DRY: Reusable SELECT clause
_DETAILS_SELECT = "SELECT " + ",".join(DETAILS_COLUMNS) + " FROM read_parquet(?) "


class DetailsFetchTimeout(Exception):
    pass


def _get_available_columns(parquet_path: str) -> List[str]:
    """Get available columns from parquet file, with fallback for missing columns."""
    try:
        import pyarrow.parquet as pq

        schema = pq.read_schema(parquet_path)
        available_columns = [field.name for field in schema]

        # Start with required columns that should always be present
        select_columns = []
        for col in DETAILS_COLUMNS:
            if col in available_columns:
                select_columns.append(col)
            else:
                logger.warning(f"Required column '{col}' not found in {parquet_path}")

        # Add legacy columns if available
        for col in LEGACY_COLUMNS:
            if col in available_columns:
                select_columns.append(col)
                logger.info(f"Found legacy column '{col}' in {parquet_path}")

        return select_columns
    except Exception as e:
        logger.warning(f"Failed to read schema from {parquet_path}: {e}")
        # Fallback to basic columns
        return [GROUP_ID, ACCOUNT_NAME, DISPOSITION]


def _build_dynamic_select(available_columns: List[str]) -> str:
    """Build SELECT clause based on available columns."""
    return "SELECT " + ",".join(available_columns) + " FROM read_parquet(?) "


def _set_backend_choice(run_id: str, backend: str) -> None:
    try:
        session.set_backend_choice(run_id, backend)
    except Exception as e:
        logger.warning(f"Failed to set backend choice: {e}")


def _build_where_clause(
    filters: Dict[str, Any],
    available_columns: List[str],
) -> Tuple[str, List]:
    """Build WHERE for per-row details (dispositions/min_edge_strength)."""
    where_sql, params = [], []
    if filters.get("dispositions"):
        in_sql, in_params = _in_clause(filters["dispositions"])
        where_sql.append(DISPOSITION + " " + in_sql)
        params.extend(in_params)
    if (min_es := filters.get("min_edge_strength", 0.0)) not in (None, 0.0):
        if WEAKEST_EDGE_TO_PRIMARY in available_columns:
            where_sql.append(WEAKEST_EDGE_TO_PRIMARY + " >= ?")
            params.append(float(min_es))
        else:
            # Safe no-op; log once per run_id in caller if useful
            pass
    return (" AND ".join(where_sql) if where_sql else "1=1"), params


def _parse_order_by(order_by: str) -> Tuple[str, bool]:
    """Parse ORDER BY clause into column and direction.

    Args:
        order_by: ORDER BY clause from get_order_by (controlled/whitelisted)

    Returns:
        Tuple of (column_name, ascending)

    Example:
        "weakest_edge_to_primary DESC" -> ("weakest_edge_to_primary", False)
        "account_name ASC" -> ("account_name", True)

    """
    try:
        parts = order_by.split()
        col, dir_ = parts[0], (parts[1].lower() if len(parts) > 1 else "asc")
        return col, dir_ != "desc"
    except Exception:
        # Fallback to safe defaults if parsing fails
        logger.warning(f"Failed to parse order_by '{order_by}', using defaults")
        return ACCOUNT_NAME, True


def get_group_details(
    run_id: str,
    group_id: str,
    sort_key: str,
    page: int,
    page_size: int,
    filters: Dict[str, Any],
) -> Tuple[List[Dict[str, Any]], int]:
    """Entry point selecting backend & returning (rows, total_rows) for a single group."""
    start_time = time.time()
    filters_signature = f"{len(filters)}_filters" if filters else "no_filters"

    # Reduce logging verbosity - only log for first page or errors
    if page == 1:
        logger.info(
            f"get_group_details | run_id={run_id} group_id={group_id} sort_key='{sort_key}' "
            f"page={page} page_size={page_size} filters={filters_signature}",
        )

    try:
        from .settings import get_settings

        settings = get_settings()
        # Validate critical config at startup
        from .settings import validate_settings

        warnings = validate_settings()
        if warnings:
            logger.warning(f"Config validation warnings: {warnings}")
    except Exception as e:
        logger.warning(f"Config validation failed: {e}")
        settings = {}

    artifact_paths = get_artifact_paths(run_id)
    details_path = artifact_paths.get("group_details_parquet")
    review_ready_path = artifact_paths.get("review_ready_parquet")

    # Prefer a dedicated details parquet if present; otherwise read from review_ready.
    source_path = (
        details_path
        if details_path and os.path.exists(details_path)
        else review_ready_path
    )
    if not source_path or not os.path.exists(source_path):
        logger.warning(f"Details parquet not found for run_id={run_id}")
        return [], 0

    # Get order_by early (comes from whitelist; safe to use in ORDER BY)
    order_by = get_order_by(
        sort_key,
        context="group_details",
    )  # Use group_details context for correct column mapping
    if page == 1:  # Only log for first page to reduce noise
        logger.info(f"get_group_details will use ORDER BY '{order_by}'")

    # Check force flags first (highest priority)
    from .settings import get_ui_perf

    ui_perf = get_ui_perf(settings)
    force_pyarrow = ui_perf.get("force_pyarrow", False)
    force_duckdb = ui_perf.get("force_duckdb", False)

    # Handle force flag precedence explicitly
    if force_pyarrow and force_duckdb:
        logger.warning(
            "Both force flags set; defaulting to DuckDB (precedence rule); disabling force_pyarrow",
        )
        force_pyarrow = False

    if force_pyarrow:
        logger.info(f"Force PyArrow flag enabled | run_id={run_id}")
        _set_backend_choice(run_id, "pyarrow")
        record_backend_choice("forced", "pyarrow")
        try:
            result, total = _get_group_details_pyarrow(
                source_path,
                group_id,
                order_by,
                page,
                page_size,
                filters,
                settings,
            )
            duration = time.time() - start_time
            record_details_request("pyarrow", True, duration)
            return result, total
        except Exception:
            duration = time.time() - start_time
            record_details_request("pyarrow", False, duration)
            raise

    if force_duckdb:
        logger.info(f"Force DuckDB flag enabled | run_id={run_id}")
        if DUCKDB is None:
            raise ImportError("DuckDB not available for forced backend selection")
        _set_backend_choice(run_id, "duckdb")
        record_backend_choice("forced", "duckdb")
        try:
            result, total = _get_group_details_duckdb(
                source_path,
                group_id,
                order_by,
                page,
                page_size,
                filters,
                settings,
            )
            duration = time.time() - start_time
            record_details_request("duckdb", True, duration)
            return result, total
        except Exception:
            duration = time.time() - start_time
            record_details_request("duckdb", False, duration)
            raise

    # Strategy: prefer DuckDB for details (joins, filters, sorts), but allow PyArrow fallback if configured.
    prefer_duckdb = (
        settings.get("ui_perf", {})
        .get("details", {})
        .get("duckdb_prefer_over_pyarrow", True)
    )
    allow_pyarrow_fallback = (
        settings.get("ui_perf", {})
        .get("details", {})
        .get("allow_pyarrow_fallback", False)
    )

    if prefer_duckdb and DUCKDB is not None:
        try:
            _set_backend_choice(run_id, "duckdb")
            result, total = _get_group_details_duckdb(
                source_path,
                group_id,
                order_by,
                page,
                page_size,
                filters,
                settings,
            )
            # Structured logging with metrics - only for first page to reduce noise
            duration_ms = int((time.time() - start_time) * 1000)
            if page == 1:
                logger.info(
                    f"get_group_details_complete | backend=duckdb duration_ms={duration_ms} "
                    f"rows={len(result)} total={total} page={page} page_size={page_size}",
                )
            return result, total
        except Exception as e:
            logger.warning(
                f"DuckDB details failed, fallback allowed? {allow_pyarrow_fallback} | {e}",
            )
            if not allow_pyarrow_fallback:
                raise

    _set_backend_choice(run_id, "pyarrow")
    result, total = _get_group_details_pyarrow(
        source_path,
        group_id,
        order_by,
        page,
        page_size,
        filters,
        settings,
    )
    # Structured logging with metrics - only for first page to reduce noise
    duration_ms = int((time.time() - start_time) * 1000)
    if page == 1:
        logger.info(
            f"get_group_details_complete | backend=pyarrow duration_ms={duration_ms} "
            f"rows={len(result)} total={total} page={page} page_size={page_size}",
        )
    return result, total


def _get_group_details_duckdb(
    parquet_path: str,
    group_id: str,
    order_by: str,
    page: int,
    page_size: int,
    filters: Dict[str, Any],
    settings: Dict[str, Any],
) -> Tuple[List[Dict[str, Any]], int]:
    """DuckDB backend for group details (fast filtering + pagination)."""
    if DUCKDB is None:
        raise ImportError("DuckDB not available for group details")

    duckdb_threads = settings.get("ui", {}).get("duckdb_threads", 4)
    timeout_seconds = settings.get("ui", {}).get("timeout_seconds", 30)

    start = time.time()

    def check_timeout() -> None:
        if time.time() - start > timeout_seconds:
            raise DetailsFetchTimeout(f"Exceeded {timeout_seconds}s")

    # Get available columns dynamically
    available_columns = _get_available_columns(parquet_path)
    dynamic_select = _build_dynamic_select(available_columns)

    where_clause, params = _build_where_clause(filters, available_columns)
    # Clamp pagination inputs to avoid negative offsets and cap for performance
    page = max(1, int(page))
    requested_size = int(page_size)
    max_page_size = settings.get("ui", {}).get("max_page_size", 250)
    page_size = max(1, min(requested_size, max_page_size))
    if page_size != requested_size:  # Log when clamping occurs
        logger.info(
            "Page size clamped from %s to %s (max_page_size limit)",
            requested_size,
            max_page_size,
        )
        record_page_size_clamped()
    offset = (page - 1) * page_size

    # Build SQL using dynamic column selection
    sql = (
        dynamic_select + "WHERE " + GROUP_ID + " = ? AND " + where_clause + " "
        "ORDER BY "
        + order_by
        + " NULLS LAST, "
        + ACCOUNT_NAME
        + " ASC "  # order_by from get_order_by whitelist, stable tie-breaker, NULLs last
        "LIMIT ? OFFSET ?"
    )
    params_page = [parquet_path, group_id, *params, page_size, offset]

    count_sql = (
        "SELECT COUNT(*) FROM read_parquet(?) "
        "WHERE " + GROUP_ID + " = ? AND " + where_clause
    )
    params_count = [parquet_path, group_id, *params]

    conn = None
    try:
        conn = DUCKDB.connect(":memory:")
        duckdb_threads = int(duckdb_threads or 4)  # Ensure numeric
        duckdb_threads = min(duckdb_threads, 32)  # Double-enforce caps at call site
        conn.execute("PRAGMA threads=" + str(duckdb_threads))
        check_timeout()

        res = conn.execute(sql, params_page)
        df = res.df()
        check_timeout()

        total = conn.execute(count_sql, params_count).fetchone()[0]
        check_timeout()  # Final timeout check before returning
        return df.to_dict("records"), int(total)
    finally:
        if conn:
            conn.close()


def _get_group_details_pyarrow(
    parquet_path: str,
    group_id: str,
    order_by: str,
    page: int,
    page_size: int,
    filters: Dict[str, Any],
    settings: Dict[str, Any],
) -> Tuple[List[Dict[str, Any]], int]:
    """PyArrow backend for group details with pandas fallback and deterministic sort."""
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    # Back-compat for timeout keys
    ui = settings.get("ui", {})
    max_pyarrow_seconds = (
        ui.get("max_pyarrow_groups_seconds")
        or ui.get("max_pyarrow_group_stats_seconds")
        or 5
    )
    start = time.time()

    def check_timeout() -> None:
        if time.time() - start > max_pyarrow_seconds:
            raise DetailsFetchTimeout(f"Exceeded {max_pyarrow_seconds}s")

    # Get available columns dynamically and project only needed columns
    available_columns = _get_available_columns(parquet_path)
    table = pq.read_table(parquet_path, columns=available_columns)
    check_timeout()

    # Hard filter group_id first (Arrow-native)
    table = table.filter(pc.equal(table[GROUP_ID], pa.scalar(group_id)))
    check_timeout()

    # Filter: apply other filters (dispositions, min_edge_strength)
    from .filtering import apply_filters_pyarrow

    filtered = apply_filters_pyarrow(table, filters, available_columns)
    check_timeout()

    # Early exit if no rows after filtering
    if filtered.num_rows == 0:
        return [], 0

    # Sort deterministically: primary order_by, tie-breaker by ACCOUNT_NAME
    # Parse the order_by clause safely
    col, ascending = _parse_order_by(order_by)

    # Guard against missing columns
    wanted_col = col
    if wanted_col not in filtered.column_names:
        col, ascending = ACCOUNT_NAME, True
        logger.warning(
            "Order column '%s' (from order_by=%s) not found; falling back to %s",
            wanted_col,
            order_by,
            ACCOUNT_NAME,
        )

    try:
        keys = [(col, ascending)]
        if ACCOUNT_NAME in filtered.column_names and col != ACCOUNT_NAME:
            keys.append((ACCOUNT_NAME, True))

        # Use explicit null placement for consistency with DuckDB NULLS LAST
        try:
            sorted_tbl = filtered.sort_by(keys, null_placement="at_end")
        except TypeError:
            # Fallback for older PyArrow versions
            sorted_tbl = filtered.sort_by(keys)

    except Exception:
        df = filtered.to_pandas()
        if ACCOUNT_NAME in df.columns:
            df = df.sort_values(
                [col, ACCOUNT_NAME],
                ascending=[ascending, True],
                na_position="last",
            )
        else:
            df = df.sort_values(col, ascending=ascending, na_position="last")
        sorted_tbl = pa.Table.from_pandas(df)

    # Clamp pagination inputs to avoid negative offsets and cap for performance
    page = max(1, int(page))
    requested_size = int(page_size)
    max_page_size = settings.get("ui", {}).get("max_page_size", 250)
    page_size = max(1, min(requested_size, max_page_size))
    if page_size != requested_size:  # Log when clamping occurs
        logger.info(
            "Page size clamped from %s to %s (max_page_size limit)",
            requested_size,
            max_page_size,
        )
        record_page_size_clamped()

    total = sorted_tbl.num_rows
    offset = (page - 1) * page_size
    page_tbl = sorted_tbl.slice(offset, page_size) if offset < total else pa.table({})
    return page_tbl.to_pylist(), total
