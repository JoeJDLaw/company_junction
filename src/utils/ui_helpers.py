"""
UI helper functions for Phase 1.17.1.

This module provides pure functions for run loading, stage status parsing,
and artifact path management for the Streamlit UI.
"""

import json
import os
import time
import hashlib
import yaml
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pyarrow as pa  # type: ignore
import pyarrow.parquet as pq  # type: ignore
import pyarrow.compute as pc  # type: ignore
import pyarrow.dataset as ds  # type: ignore

# Optional DuckDB import for fallback
try:
    import duckdb

    DUCKDB_AVAILABLE = True
except ImportError:
    DUCKDB_AVAILABLE = False

from src.utils.path_utils import (
    get_config_path,
    get_processed_dir,
    get_interim_dir,
    get_artifact_path,
)
from src.utils.schema_utils import (
    GROUP_ID,
    ACCOUNT_NAME,
    IS_PRIMARY,
    WEAKEST_EDGE_TO_PRIMARY,
    DISPOSITION,
    GROUP_SIZE,
    MAX_SCORE,
    PRIMARY_NAME,
    ACCOUNT_ID,
    SUFFIX_CLASS,
    CREATED_DATE,
    ALIAS_CROSS_REFS,
    ALIAS_CANDIDATES,
)

from src.utils.cache_utils import get_latest_run_id, load_run_index
from src.utils.logging_utils import get_logger

logger = get_logger(__name__)


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


def choose_backend(
    preferred: str,
    available: bool,
    file_size: int,
    config_flags: Dict[str, Any],
    reason_context: str = "",
) -> str:
    """
    Centralized backend choice function for UI data reads.

    Args:
        preferred: Preferred backend ("duckdb" or "pyarrow")
        available: Whether DuckDB is available
        file_size: Size of data file in rows
        config_flags: Configuration flags for backend selection
        reason_context: Additional context for logging

    Returns:
        Chosen backend: "duckdb" or "pyarrow"
    """
    # Extract relevant config flags
    use_duckdb_for_groups = config_flags.get("ui", {}).get(
        "use_duckdb_for_groups", False
    )
    duckdb_prefer_over_pyarrow = (
        config_flags.get("ui_perf", {})
        .get("groups", {})
        .get("duckdb_prefer_over_pyarrow", False)
    )
    rows_threshold = (
        config_flags.get("ui_perf", {})
        .get("groups", {})
        .get("rows_duckdb_threshold", 30000)
    )

    # Decision logic - explicit fallback conditions
    if preferred == "duckdb" and available and use_duckdb_for_groups:
        reason = "config_prefers_duckdb"
        chosen = "duckdb"
    elif (
        preferred == "duckdb"
        and available
        and duckdb_prefer_over_pyarrow
        and file_size > rows_threshold
    ):
        reason = f"threshold_exceeded rows={file_size} > {rows_threshold}"
        chosen = "duckdb"
    elif preferred == "duckdb" and available and duckdb_prefer_over_pyarrow:
        reason = "duckdb_preferred_over_pyarrow"
        chosen = "duckdb"
    else:
        reason = "fallback_to_pyarrow"
        chosen = "pyarrow"

    # Enhanced logging with more context
    logger.info(
        f"Backend choice: chosen={chosen} reason={reason} "
        f"preferred={preferred} available={available} "
        f"file_sz={file_size} config_flags={use_duckdb_for_groups},{duckdb_prefer_over_pyarrow} "
        f"context={reason_context}"
    )

    # Additional debug logging for fallback cases
    if chosen == "pyarrow" and preferred == "duckdb":
        if not available:
            logger.warning(
                f"DuckDB not available - falling back to PyArrow | context={reason_context}"
            )
        elif not use_duckdb_for_groups:
            logger.info(
                f"ui.use_duckdb_for_groups=false - using PyArrow | context={reason_context}"
            )
        elif not duckdb_prefer_over_pyarrow:
            logger.info(
                f"ui_perf.groups.duckdb_prefer_over_pyarrow=false - using PyArrow | context={reason_context}"
            )
        elif file_size <= rows_threshold:
            logger.info(
                f"File size {file_size} <= threshold {rows_threshold} - using PyArrow | context={reason_context}"
            )

    return chosen


def _ensure_session_state_backend(run_id: str) -> None:
    """
    Ensure the backend session state structure exists for a given run.

    Args:
        run_id: The run ID to set up session state for
    """
    import streamlit as st

    if "cj" not in st.session_state:
        st.session_state["cj"] = {}
    if "backend" not in st.session_state["cj"]:
        st.session_state["cj"]["backend"] = {}
    if "groups" not in st.session_state["cj"]["backend"]:
        st.session_state["cj"]["backend"]["groups"] = {}


def _set_backend_choice(run_id: str, backend: str) -> None:
    """
    Set the backend choice in session state for a given run.

    Args:
        run_id: The run ID
        backend: The chosen backend ("duckdb" or "pyarrow")
    """
    _ensure_session_state_backend(run_id)
    import streamlit as st

    st.session_state["cj"]["backend"]["groups"][run_id] = backend


class PageFetchTimeout(Exception):
    """Exception raised when page fetch exceeds timeout."""

    pass


class DetailsCache:
    """
    LRU cache for group details to avoid repeated DuckDB queries.

    Cache key: (run_id, parquet_fingerprint, group_id, backend)
    """

    def __init__(self, capacity: int = None):
        # Load capacity from config or use default
        if capacity is None:
            try:
                from src.utils.io_utils import load_settings

                settings = load_settings(str(get_config_path()))
                capacity = settings.get("ui", {}).get("cache_capacity", 16)
            except Exception:
                capacity = 16
        self.capacity = capacity
        self.cache = {}
        self.access_order = []

    def get(self, key: Tuple[str, str, str, str]) -> Optional[List[Dict[str, Any]]]:
        """Get item from cache, updating access order."""
        if key in self.cache:
            # Move to end (most recently used)
            self.access_order.remove(key)
            self.access_order.append(key)
            return self.cache[key]
        return None

    def put(self, key: Tuple[str, str, str, str], value: List[Dict[str, Any]]) -> None:
        """Put item in cache, evicting oldest if at capacity."""
        if key in self.cache:
            # Update existing item
            self.access_order.remove(key)
        elif len(self.cache) >= self.capacity:
            # Evict oldest item
            oldest_key = self.access_order.pop(0)
            del self.cache[oldest_key]

        self.cache[key] = value
        self.access_order.append(key)

    def clear(self) -> None:
        """Clear all cached items."""
        self.cache.clear()
        self.access_order.clear()

    def invalidate_run(self, run_id: str) -> None:
        """Invalidate all items for a specific run."""
        keys_to_remove = [key for key in self.cache.keys() if key[0] == run_id]
        for key in keys_to_remove:
            del self.cache[key]
            self.access_order.remove(key)


# Global details cache instance
_details_cache = DetailsCache()


def _is_non_empty(obj: Any) -> bool:
    """
    Safely check if an object is non-empty, handling pandas, numpy, and pyarrow objects.

    Args:
        obj: Object to check

    Returns:
        True if object is non-empty, False otherwise
    """
    if obj is None:
        return False
    try:
        import pandas as pd

        if isinstance(obj, (pd.Series, pd.DataFrame)):
            return not obj.empty
    except ImportError:
        pass
    except Exception:
        pass
    try:
        import numpy as np

        if isinstance(obj, np.ndarray):
            return obj.size > 0
    except ImportError:
        pass
    except Exception:
        pass
    try:
        import pyarrow as pa

        if isinstance(obj, pa.Table):
            return obj.num_rows > 0
        if isinstance(obj, (pa.Array, pa.ChunkedArray)):
            return len(obj) > 0
    except ImportError:
        pass
    except Exception:
        pass
    if hasattr(obj, "__len__"):
        try:
            return len(obj) > 0
        except Exception:
            return False
    return True


def list_runs() -> List[Dict[str, Any]]:
    """
    Get a sorted list of all runs with metadata, with duplicates removed.

    Returns:
        List of run dictionaries sorted by timestamp (newest first)
    """
    from src.utils.cache_utils import list_runs_deduplicated

    # Get deduplicated runs
    deduplicated_runs = list_runs_deduplicated()

    # Convert to the expected format
    runs = []
    for run_id, run_data in deduplicated_runs:
        runs.append(
            {
                "run_id": run_id,
                "timestamp": run_data.get("timestamp", ""),
                "status": run_data.get("status", "unknown"),
                "input_paths": run_data.get("input_paths", []),
                "config_paths": run_data.get("config_paths", []),
                "input_hash": run_data.get("input_hash", ""),
                "config_hash": run_data.get("config_hash", ""),
                "dag_version": run_data.get("dag_version", "1.0.0"),
            }
        )

    return runs


def get_run_metadata(run_id: str) -> Optional[Dict[str, Any]]:
    """
    Get detailed metadata for a specific run.

    Args:
        run_id: The run ID to get metadata for

    Returns:
        Run metadata dictionary or None if run not found
    """
    import logging

    logger = logging.getLogger(__name__)

    run_index = load_run_index()

    if run_id not in run_index:
        logger.warning(f"Run {run_id} not found in run_index")
        return None

    run_data = run_index[run_id]

    # Parse timestamp for display
    try:
        timestamp = datetime.fromisoformat(run_data.get("timestamp", ""))
        # Convert to local time if it's UTC
        if timestamp.tzinfo is None:
            # Assume UTC if no timezone info
            from datetime import timezone

            timestamp = timestamp.replace(tzinfo=timezone.utc)

        # Convert to local time
        local_timestamp = timestamp.astimezone()
        formatted_timestamp = local_timestamp.strftime("%Y-%m-%d %H:%M local")
    except (ValueError, TypeError) as e:
        logger.warning(f"Failed to parse timestamp: {e}")
        formatted_timestamp = run_data.get("timestamp", "Unknown")

    result = {
        "run_id": run_id,
        "timestamp": run_data.get("timestamp", ""),
        "formatted_timestamp": formatted_timestamp,
        "status": run_data.get("status", "unknown"),
        "input_paths": run_data.get("input_paths", []),
        "config_paths": run_data.get("config_paths", []),
        "input_hash": run_data.get("input_hash", ""),
        "config_hash": run_data.get("config_hash", ""),
        "dag_version": run_data.get("dag_version", "1.0.0"),
    }

    return result


def validate_run_artifacts(run_id: str) -> Dict[str, Any]:
    """
    Validate that a run has all required artifacts.

    Args:
        run_id: The run ID to validate

    Returns:
        Dictionary with validation results
    """
    validation: Dict[str, Any] = {
        "run_exists": False,
        "status": "unknown",
        "has_review_ready_csv": False,
        "has_review_ready_parquet": False,
        "has_pipeline_state": False,
        "has_review_meta": False,
        "missing_files": [],
        "errors": [],
    }

    # Check if run exists in index
    run_metadata = get_run_metadata(run_id)
    if not run_metadata:
        validation["errors"].append(f"Run {run_id} not found in run index")
        return validation

    validation["run_exists"] = True
    validation["status"] = run_metadata["status"]

    # Check required files
    processed_dir = get_processed_dir(run_id)
    interim_dir = get_interim_dir(run_id)

    # Check review_ready files
    csv_path = processed_dir / "review_ready.csv"
    parquet_path = processed_dir / "review_ready.parquet"

    if os.path.exists(csv_path):
        validation["has_review_ready_csv"] = True
    else:
        validation["missing_files"].append("review_ready.csv")

    if os.path.exists(parquet_path):
        validation["has_review_ready_parquet"] = True
    else:
        validation["missing_files"].append("review_ready.parquet")

    # Check pipeline state
    state_path = interim_dir / "pipeline_state.json"
    if os.path.exists(state_path):
        validation["has_pipeline_state"] = True
    else:
        validation["missing_files"].append("pipeline_state.json")

    # Check review meta
    meta_path = processed_dir / "review_meta.json"
    if os.path.exists(meta_path):
        validation["has_review_meta"] = True
    else:
        validation["missing_files"].append("review_meta.json")

    return validation


def load_stage_state(run_id: str) -> Optional[Dict[str, Any]]:
    """
    Load and parse MiniDAG stage state for a run.

    Args:
        run_id: The run ID to load state for

    Returns:
        Parsed stage state or None if not available
    """
    state_path = get_interim_dir(run_id) / "pipeline_state.json"

    if not os.path.exists(state_path):
        return None

    try:
        with open(state_path, "r") as f:
            state_data = json.load(f)

        if not isinstance(state_data, dict):
            return None

        stages = state_data.get("stages", {})
        metadata = state_data.get("metadata", {})

        # Parse stage information
        stage_info = []
        for stage_name, stage_data in stages.items():
            if isinstance(stage_data, dict):
                start_time = stage_data.get("start_time")
                end_time = stage_data.get("end_time")

                # Calculate duration
                duration = 0.0
                if (
                    start_time is not None
                    and end_time is not None
                    and start_time > 0
                    and end_time > 0
                ):
                    duration = end_time - start_time

                # Format timestamps
                start_str = ""
                end_str = ""
                if start_time is not None and start_time > 0:
                    start_str = datetime.fromtimestamp(start_time).strftime("%H:%M:%S")
                if end_time is not None and end_time > 0:
                    end_str = datetime.fromtimestamp(end_time).strftime("%H:%M:%S")

                stage_info.append(
                    {
                        "name": stage_name,
                        "status": stage_data.get("status", "unknown"),
                        "start_time": start_time,
                        "end_time": end_time,
                        "start_str": start_str,
                        "end_str": end_str,
                        "duration": duration,
                        "duration_str": f"{duration:.2f}s" if duration > 0 else "N/A",
                    }
                )

        return {
            "stages": stage_info,
            "metadata": metadata,
            "run_id": run_id,
        }

    except (json.JSONDecodeError, IOError) as e:
        logger.warning(f"Failed to load stage state for {run_id}: {e}")
        return None


def get_artifact_paths(run_id: str) -> Dict[str, str]:
    """
    Get artifact paths for a run.

    Args:
        run_id: The run ID

    Returns:
        Dictionary of artifact paths
    """
    # Check if run exists in interim or processed
    interim_dir = get_interim_dir(run_id)
    processed_dir = get_processed_dir(run_id)

    # Determine which directory exists and use that
    if os.path.exists(processed_dir):
        base_dir = str(processed_dir)
        interim_format = "parquet"
    elif os.path.exists(interim_dir):
        base_dir = str(interim_dir)
        interim_format = "parquet"
    else:
        # Fallback to processed directory
        base_dir = str(processed_dir)
        interim_format = "parquet"

    return {
        "review_ready_csv": f"{base_dir}/review_ready.csv",
        "review_ready_parquet": f"{base_dir}/review_ready.parquet",
        "review_meta": f"{base_dir}/review_meta.json",
        "pipeline_state": f"{base_dir}/pipeline_state.json",
        "candidate_pairs": f"{base_dir}/candidate_pairs.{interim_format}",
        "groups": f"{base_dir}/groups.{interim_format}",
        "survivorship": f"{base_dir}/survivorship.{interim_format}",
        "dispositions": f"{base_dir}/dispositions.{interim_format}",
        "alias_matches": f"{base_dir}/alias_matches.{interim_format}",
        "block_top_tokens": f"{base_dir}/block_top_tokens.csv",
        "group_stats_parquet": str(get_artifact_path(run_id, "group_stats.parquet")),
        "group_details_parquet": str(
            get_artifact_path(run_id, "group_details.parquet")
        ),
    }


def get_default_run_id() -> str:
    """
    Get the default run ID (latest successful run).

    Returns:
        Latest run ID or empty string if none available
    """
    latest_run_id = get_latest_run_id()
    if latest_run_id:
        return latest_run_id

    # Fallback: get the most recent complete run
    runs = list_runs()
    for run in runs:
        if run["status"] == "complete":
            return str(run["run_id"])

    return ""


def format_run_display_name(
    run_id: str, metadata: Optional[Dict[str, Any]] = None
) -> str:
    """
    Format a run ID for display in the UI.

    Args:
        run_id: The run ID
        metadata: Optional run metadata

    Returns:
        Formatted display name
    """

    if not metadata:
        metadata = get_run_metadata(run_id)

    if not metadata:
        return run_id

    # Extract input file name
    input_paths = metadata.get("input_paths", [])
    input_file = "Unknown"
    if input_paths:
        input_path = input_paths[0]
        input_file = str(Path(input_path).name)

        # Handle temporary files and unknown paths
        if (
            input_file.startswith("tmp")
            or "/tmp/" in input_path
            or "Unknown" in input_file
        ):
            # Use a more descriptive name for temporary files
            input_file = f"temp_file_{run_id[:8]}"
            logger.info(
                f"Display name fallback: using run_id prefix for temp file | run_id={run_id}"
            )

    # Format timestamp
    timestamp = metadata.get("formatted_timestamp", "Unknown")

    result = f"{input_file} ({timestamp})"
    return result


def get_run_status_icon(status: str) -> str:
    """
    Get status icon for display.

    Args:
        status: Run status string

    Returns:
        Status icon string
    """
    status_icons = {
        "complete": "✅",
        "running": "⏳",
        "failed": "❌",
        "interrupted": "⚠️",
        "unknown": "❓",
    }

    return str(status_icons.get(status, "❓"))


def get_stage_status_icon(status: str) -> str:
    """
    Get stage status icon for display.

    Args:
        status: Stage status string

    Returns:
        Status icon string
    """
    status_icons = {
        "completed": "✅",
        "running": "⏳",
        "failed": "❌",
        "interrupted": "⚠️",
        "pending": "⏸️",
        "unknown": "❓",
    }

    return str(status_icons.get(status, "❓"))


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
            from src.utils.io_utils import load_settings

            settings = load_settings(str(get_config_path()))
            timeout_seconds = settings.get("ui", {}).get("timeout_seconds", 30)
        except Exception:
            timeout_seconds = 30

        # Step 1: Dataset creation
        step_start = time.time()
        dataset = ds.dataset(parquet_path)
        dataset_time = time.time() - step_start
        logger.info(f"Dataset creation | run_id={run_id} elapsed={dataset_time:.3f}s")

        check_timeout()

        # Step 2: Column projection
        step_start = time.time()
        schema = dataset.schema
        available_columns = schema.names

        header_columns = [
            GROUP_ID,
            ACCOUNT_NAME,
            IS_PRIMARY,
            WEAKEST_EDGE_TO_PRIMARY,
            DISPOSITION,
        ]
        # Only include columns that exist
        existing_columns = [col for col in header_columns if col in available_columns]

        logger.info(
            f"Column projection | run_id={run_id} available={len(available_columns)} projected={len(existing_columns)} columns={existing_columns}"
        )

        # Use scanner for projection
        scanner = dataset.scanner(columns=existing_columns)
        projected_table = scanner.to_table()
        projection_time = time.time() - step_start

        logger.info(
            f"Projection complete | run_id={run_id} rows={projected_table.num_rows} elapsed={projection_time:.3f}s"
        )

        check_timeout()

        # Step 3: Apply filters
        step_start = time.time()
        filtered_table = apply_filters_pyarrow(projected_table, filters)
        filter_time = time.time() - step_start

        logger.info(
            f"Filters applied | run_id={run_id} before={projected_table.num_rows} after={filtered_table.num_rows} elapsed={filter_time:.3f}s"
        )

        check_timeout()

        # Step 4: Compute group statistics
        step_start = time.time()
        groups_table = compute_group_stats_pyarrow(filtered_table)
        stats_time = time.time() - step_start

        logger.info(
            f"Group stats computed | run_id={run_id} groups={groups_table.num_rows} elapsed={stats_time:.3f}s"
        )

        # Auto-switch to DuckDB if group stats take too long
        try:
            from src.utils.io_utils import load_settings

            settings = load_settings(str(get_config_path()))
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

        # Get total count
        total_groups = groups_table.num_rows

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

            # Sort the groups table
            sorted_table = groups_table.sort_by(sort_keys)
            sort_time = time.time() - step_start

            logger.info(
                f"Sorting applied | run_id={run_id} sort_keys={sort_keys} elapsed={sort_time:.3f}s"
            )

            check_timeout()

            # Step 7: Apply slice (LIMIT/OFFSET before pandas conversion)
            step_start = time.time()
            page_table = sorted_table.slice(offset, limit)
            slice_time = time.time() - step_start

            logger.info(
                f"Slice applied | run_id={run_id} offset={offset} limit={limit} slice_rows={page_table.num_rows} elapsed={slice_time:.3f}s"
            )

            # Step 8: Convert slice to pandas
            step_start = time.time()
            page_data = page_table.to_pylist()
            pandas_time = time.time() - step_start

            logger.info(
                f"Pandas conversion | run_id={run_id} rows={len(page_data)} elapsed={pandas_time:.3f}s"
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
        return [], 0


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
        f"get_groups_page | run_id={run_id} sort_key='{sort_key}' order_by='{order_by}'"
    )

    # Load settings
    try:
        from src.utils.io_utils import load_settings

        settings = load_settings(str(get_config_path()))
        # Settings loading is now logged by the load_settings function itself
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
                dataset = ds.dataset(review_path)
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


def apply_filters_pyarrow(table: pa.Table, filters: Dict[str, Any]) -> pa.Table:
    """
    Apply filters to PyArrow table.

    Args:
        table: PyArrow table to filter
        filters: Dictionary of filters to apply

    Returns:
        Filtered PyArrow table
    """
    filtered_table = table

    # Apply disposition filter
    if filters.get("dispositions") and DISPOSITION in table.column_names:
        dispositions = filters["dispositions"]
        mask = pc.is_in(pc.field(DISPOSITION), pa.array(dispositions))
        filtered_table = filtered_table.filter(mask)

    # Apply edge strength filter
    if filters.get("min_edge_strength", 0.0) > 0.0:
        if WEAKEST_EDGE_TO_PRIMARY in table.column_names:
            mask = pc.field(WEAKEST_EDGE_TO_PRIMARY) >= filters["min_edge_strength"]
            filtered_table = filtered_table.filter(mask)

    # Apply alias filter
    if filters.get("has_aliases", False):
        # Check for records with aliases
        if ALIAS_CROSS_REFS in table.column_names:
            mask = pc.string_length(pc.field(ALIAS_CROSS_REFS)) > 2  # More than "[]"
        elif ALIAS_CANDIDATES in table.column_names:
            mask = pc.string_length(pc.field(ALIAS_CANDIDATES)) > 2
        else:
            mask = pc.scalar(False)
        filtered_table = filtered_table.filter(mask)

    return filtered_table


def compute_group_stats_pyarrow(table: pa.Table) -> pa.Table:
    """
    Compute group statistics for sorting.

    Args:
        table: PyArrow table with group data

    Returns:
        PyArrow table with group statistics
    """
    # Convert to pandas for easier group operations
    df = table.to_pandas()

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
            }
        )

    return pa.Table.from_pylist(stats_data)


def get_group_details_duckdb(
    run_id: str, group_id: str, settings: Optional[Dict[str, Any]] = None
) -> List[Dict[str, Any]]:
    """
    Get group details using DuckDB for efficient per-group querying from group_details.parquet.

    Args:
        run_id: The run ID
        group_id: The specific group ID to fetch details for
        settings: Optional settings dict (loaded if not provided)

    Returns:
        List of dictionaries containing group details
    """
    start_time = time.time()
    step_start = time.time()

    try:
        # Get artifact paths
        artifact_paths = get_artifact_paths(run_id)
        details_path = artifact_paths.get("group_details_parquet")

        if not details_path or not os.path.exists(details_path):
            raise FileNotFoundError(
                f"group_details.parquet not found at {details_path}"
            )

        # Load settings if not provided
        if settings is None:
            try:
                # Try multiple possible paths for settings
                settings_paths = [
                    "config/settings.yaml",
                    "../config/settings.yaml",
                    "../../config/settings.yaml",
                ]
                settings = None

                for path in settings_paths:
                    try:
                        with open(path, "r") as f:
                            settings = yaml.safe_load(f)
                            logger.info(f"Loaded settings from {path}")
                            break
                    except Exception:
                        continue

                if settings is None:
                    raise Exception("Could not load settings from any path")

            except Exception as e:
                logger.error(f"Failed to load settings: {e}")
                # Fallback to default settings
                settings = {"ui": {"duckdb_threads": 4}}

        duckdb_threads = settings.get("ui", {}).get("duckdb_threads", 4)

        logger.info(
            f"Group details fetch start | run_id={run_id} group_id={group_id} backend=duckdb"
        )

        # Step 1: DuckDB connection
        conn = duckdb.connect(":memory:")
        conn.execute(f"PRAGMA threads = {duckdb_threads}")
        connect_time = time.time() - step_start
        logger.info(
            f"details_backend_selected: backend=duckdb reason=details_parquet_available elapsed={connect_time:.3f}"
        )

        # Step 2: Build and execute query
        step_start = time.time()
        sql = f"""
        SELECT {GROUP_ID}, {ACCOUNT_ID}, {ACCOUNT_NAME}, {SUFFIX_CLASS}, {CREATED_DATE}, {DISPOSITION}
        FROM read_parquet('{details_path}')
        WHERE {GROUP_ID} = '{group_id}'
        ORDER BY {ACCOUNT_NAME} ASC
        """

        try:
            result = conn.execute(sql)
            query_time = time.time() - step_start
            logger.info(
                f"details_query_exec: run_id={run_id} group_id={group_id} rows={result.rowcount} elapsed={query_time:.3f}"
            )
        except Exception as query_error:
            query_time = time.time() - step_start
            logger.error(
                f"details_query_error: run_id={run_id} group_id={group_id} exc_class={type(query_error).__name__} message='{str(query_error)}' sql='{sql}' elapsed={query_time:.3f}"
            )
            raise

        # Step 3: Convert to pandas
        step_start = time.time()
        df = result.df()
        pandas_time = time.time() - step_start
        logger.info(
            f"details_to_pandas: run_id={run_id} group_id={group_id} rows={len(df)} elapsed={pandas_time:.3f}"
        )

        details_data = df.to_dict("records")
        conn.close()

        elapsed = time.time() - start_time
        logger.info(
            f"Group details loaded | run_id={run_id} group_id={group_id} rows={len(details_data)} elapsed={elapsed:.3f} backend=duckdb"
        )

        return details_data

    except Exception as e:
        elapsed = time.time() - start_time
        logger.error(
            f'Group details load failed | run_id={run_id} group_id={group_id} error="{str(e)}" elapsed={elapsed:.3f} backend=duckdb'
        )
        raise


def _get_parquet_fingerprint(file_path: str) -> str:
    """
    Get a fingerprint for a parquet file based on mtime and size.

    Args:
        file_path: Path to the parquet file

    Returns:
        Fingerprint string
    """
    try:
        stat = os.stat(file_path)
        fingerprint = f"{stat.st_mtime}_{stat.st_size}"
        return fingerprint
    except OSError:
        return "unknown"


def get_group_details_pyarrow(run_id: str, group_id: str) -> List[Dict[str, Any]]:
    """
    Get group details using PyArrow (fallback implementation).

    Args:
        run_id: The run ID
        group_id: The group ID

    Returns:
        List of dictionaries containing group details
    """
    start_time = time.time()
    try:
        # Get artifact paths
        artifact_paths = get_artifact_paths(run_id)
        parquet_path = artifact_paths["review_ready_parquet"]

        if not os.path.exists(parquet_path):
            return []

        # Read parquet file and filter for group
        table = pq.read_table(parquet_path)
        group_mask = pc.equal(pc.field("group_id"), pc.scalar(group_id))
        group_table = table.filter(group_mask)

        if group_table.num_rows == 0:
            return []

        # Convert to pandas for easier processing
        group_df = group_table.to_pandas()

        # Project only the columns shown in details UI
        projected_cols = [
            ACCOUNT_NAME,
            ACCOUNT_ID,
            DISPOSITION,
            IS_PRIMARY,
            WEAKEST_EDGE_TO_PRIMARY,
            "suffix",  # Keep as string since not in schema_utils yet
        ]
        available_cols = [col for col in projected_cols if col in group_df.columns]
        group_df = group_df[available_cols]

        elapsed = time.time() - start_time
        logger.info(
            f"Group details loaded (PyArrow) | run_id={run_id} group_id={group_id} elapsed={elapsed:.3f}"
        )

        return group_df.to_dict("records")

    except Exception as e:
        elapsed = time.time() - start_time
        logger.error(
            f"Failed to load group details (PyArrow): {e} | run_id={run_id} group_id={group_id} elapsed={elapsed:.3f}"
        )
        return []


def get_group_details_lazy(run_id: str, group_id: str) -> List[Dict[str, Any]]:
    """
    Get group details using DuckDB-first approach with caching (Phase 1.23.1).

    Args:
        run_id: The run ID
        group_id: The group ID

    Returns:
        List of dictionaries containing group details
    """
    start_time = time.time()

    # Load settings
    try:
        from src.utils.io_utils import load_settings

        settings = load_settings(str(get_config_path()))
        # Settings loading is now logged by the load_settings function itself
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

    # Check if details parquet is enabled
    use_details_parquet = (
        settings.get("ui_perf", {}).get("details", {}).get("use_details_parquet", True)
    )
    allow_pyarrow_fallback = (
        settings.get("ui_perf", {})
        .get("details", {})
        .get("allow_pyarrow_fallback", False)
    )

    # Phase 1.23.1: Try DuckDB with group_details.parquet first
    if use_details_parquet and DUCKDB_AVAILABLE:
        try:
            # Check cache first
            artifact_paths = get_artifact_paths(run_id)
            details_path = artifact_paths.get("group_details_parquet")

            if details_path and os.path.exists(details_path):
                parquet_fingerprint = _get_parquet_fingerprint(details_path)
                cache_key = (run_id, parquet_fingerprint, group_id, "duckdb")

                # Try cache hit first
                cached_result = _details_cache.get(cache_key)
                if cached_result is not None:
                    elapsed = time.time() - start_time
                    logger.info(
                        f"Group details loaded | run_id={run_id} group_id={group_id} rows={len(cached_result)} elapsed={elapsed:.3f} backend=duckdb cache=hit"
                    )
                    return cached_result

                # Cache miss - load from DuckDB
                result = get_group_details_duckdb(run_id, group_id, settings)

                # Cache the result
                _details_cache.put(cache_key, result)

                elapsed = time.time() - start_time
                logger.info(
                    f"Group details loaded | run_id={run_id} group_id={group_id} rows={len(result)} elapsed={elapsed:.3f} backend=duckdb cache=miss"
                )
                return result

        except Exception as e:
            logger.warning(f"DuckDB details load failed, falling back to PyArrow: {e}")
            if allow_pyarrow_fallback:
                # Fallback to PyArrow only if explicitly enabled
                result = get_group_details_pyarrow(run_id, group_id)
                elapsed = time.time() - start_time
                logger.info(
                    f"Group details loaded | run_id={run_id} group_id={group_id} rows={len(result)} elapsed={elapsed:.3f} backend=pyarrow fallback=true"
                )
                return result
            else:
                # Re-raise the error since fallback is disabled
                raise

    # Fallback to PyArrow if details parquet is disabled or DuckDB unavailable
    if allow_pyarrow_fallback:
        result = get_group_details_pyarrow(run_id, group_id)
        elapsed = time.time() - start_time
        logger.info(
            f"Group details loaded | run_id={run_id} group_id={group_id} rows={len(result)} elapsed={elapsed:.3f} backend=pyarrow fallback=disabled"
        )
        return result
    else:
        raise RuntimeError(
            "DuckDB details loading failed and PyArrow fallback is disabled"
        )


def build_cache_key(
    run_id: str,
    sort_key: str,
    page: int,
    page_size: int,
    filters: Dict[str, Any],
    backend: str = "pyarrow",
    source: str = "review_ready",
) -> str:
    """
    Build a cache key for groups page data.

    Args:
        run_id: The run ID
        sort_key: The sort key
        page: The page number
        page_size: The page size
        filters: The filters dictionary
        backend: The backend used ("pyarrow" or "duckdb")
        source: The data source ("stats" or "review_ready")

    Returns:
        A string cache key
    """
    # Get parquet fingerprint based on source
    try:
        artifact_paths = get_artifact_paths(run_id)
        if source == "stats":
            parquet_path = artifact_paths.get("group_stats_parquet")
        else:
            parquet_path = artifact_paths["review_ready_parquet"]

        if parquet_path and os.path.exists(parquet_path):
            stat = os.stat(parquet_path)
            parquet_fingerprint = f"{int(stat.st_mtime)}_{stat.st_size}"
        else:
            parquet_fingerprint = "missing"
    except Exception:
        parquet_fingerprint = "unknown"

    # Create filters signature
    filters_signature = hashlib.md5(str(sorted(filters.items())).encode()).hexdigest()[
        :8
    ]

    # Build cache key components including source and backend
    key_components = [
        run_id,
        source,
        backend,
        parquet_fingerprint,
        sort_key,
        str(page),
        str(page_size),
        filters_signature,
    ]

    cache_key = hashlib.md5("|".join(key_components).encode()).hexdigest()
    logger.info(
        f'Cache key generated | run_id={run_id} key={cache_key[:8]}... source={source} backend={backend} fingerprint={parquet_fingerprint} page={page} size={page_size} sort="{sort_key}"'
    )

    return cache_key


def build_details_cache_key(run_id: str, group_id: str, backend: str = "duckdb") -> str:
    """
    Build a cache key for group details data.

    Args:
        run_id: The run ID
        group_id: The group ID
        backend: The backend used ("pyarrow" or "duckdb")

    Returns:
        A string cache key
    """
    # Get parquet fingerprint
    try:
        artifact_paths = get_artifact_paths(run_id)
        parquet_path = artifact_paths["review_ready_parquet"]
        stat = os.stat(parquet_path)
        parquet_fingerprint = f"{int(stat.st_mtime)}_{stat.st_size}"
    except Exception:
        parquet_fingerprint = "unknown"

    # Build cache key components
    key_components = [run_id, group_id, parquet_fingerprint, backend]

    cache_key = hashlib.md5("|".join(key_components).encode()).hexdigest()
    return cache_key


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
        step_start = time.time()
        conn = duckdb.connect(":memory:")
        conn_time = time.time() - step_start
        logger.info(
            f"DuckDB connection | run_id={run_id} threads=4 elapsed={conn_time:.3f}s"
        )

        # Step 2: Build query
        step_start = time.time()
        where_clauses = []

        if filters.get("dispositions"):
            dispositions = filters["dispositions"]
            dispositions_str = ", ".join([f"'{d}'" for d in dispositions])
            where_clauses.append(f"{DISPOSITION} IN ({dispositions_str})")

        if filters.get("min_edge_strength", 0.0) > 0.0:
            where_clauses.append(f"{MAX_SCORE} >= {filters['min_edge_strength']}")

        where_clause = " AND ".join(where_clauses) if where_clauses else "1=1"

        # Build ORDER BY clause using centralized mapping
        order_by = get_order_by(sort_key)

        logger.info(
            f"groups_page_from_stats_duckdb | run_id={run_id} sort_key='{sort_key}' order_by='{order_by}' backend=duckdb global_sort=true"
        )

        # Build the query with proper global sorting before pagination
        # This ensures ORDER BY is applied to the entire dataset before LIMIT/OFFSET
        # Result: Consistent sorting across all pages, not just within each page
        sql = f"""
        SELECT 
            {GROUP_ID},
            {GROUP_SIZE},
            {MAX_SCORE},
            {PRIMARY_NAME},
            {DISPOSITION}
        FROM (
            SELECT 
                {GROUP_ID},
                {GROUP_SIZE},
                {MAX_SCORE},
                {PRIMARY_NAME},
                {DISPOSITION}
            FROM read_parquet('{group_stats_path}')
            WHERE {where_clause}
            ORDER BY {order_by}
        ) sorted_data
        LIMIT {page_size} OFFSET {(page - 1) * page_size}
        """

        logger.info(
            f"DuckDB query built | run_id={run_id} where_clause='{where_clause}' order_by='{order_by}' elapsed={time.time() - step_start:.3f}s"
        )

        # Step 3: Execute query
        step_start = time.time()
        result = conn.execute(sql)
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

        # Step 5: Get total count for pagination
        step_start = time.time()
        count_sql = f"""
        SELECT COUNT(*) as total
        FROM read_parquet('{group_stats_path}')
        WHERE {where_clause}
        """
        count_result = conn.execute(count_sql)
        total_groups = count_result.fetchone()[0]
        count_time = time.time() - step_start
        logger.info(
            f"Total count query | run_id={run_id} total={total_groups} elapsed={count_time:.3f}s"
        )

        # Step 6: Convert to list format
        page_data = df_result.to_dict("records")

        # Phase 1.26.2: Debug logging for sorting results
        if page_data and len(page_data) > 0:
            first_few_names = [
                item.get(PRIMARY_NAME, "N/A")[:20] for item in page_data[:5]
            ]
            logger.info(
                f"Sorting results debug | run_id={run_id} sort_key='{sort_key}' "
                f"first_5_names={first_few_names}"
            )

            # Additional debug for Account Name sorting issues
            if "Account Name" in sort_key:
                logger.info(f"DEBUG: Account Name sort detected, sort_key='{sort_key}'")
                logger.info(f"DEBUG: order_by='{order_by}'")
                logger.info(f"DEBUG: where_clause='{where_clause}'")

                # Get a sample of names from the entire dataset to verify global sorting
                debug_sql = f"""
                SELECT {PRIMARY_NAME} as name
                FROM read_parquet('{group_stats_path}')
                WHERE {where_clause}
                ORDER BY {order_by}
                LIMIT 10
                """
                logger.info(f"DEBUG: debug_sql='{debug_sql}'")

                try:
                    debug_result = conn.execute(debug_sql)
                    debug_df = debug_result.df()
                    debug_names = [
                        name[:20] for name in debug_df["name"].tolist() if name
                    ]
                    logger.info(
                        f"Global sorting debug | run_id={run_id} sort_key='{sort_key}' "
                        f"first_10_names_global={debug_names}"
                    )

                    # Also check for NULL/empty values and extreme values
                    null_check_sql = f"""
                    SELECT 
                        COUNT(*) as total_rows,
                        COUNT({PRIMARY_NAME}) as non_null_names,
                        COUNT(CASE WHEN {PRIMARY_NAME} IS NULL OR {PRIMARY_NAME} = '' THEN 1 END) as null_or_empty_names,
                        MIN({PRIMARY_NAME}) as min_name,
                        MAX({PRIMARY_NAME}) as max_name
                    FROM read_parquet('{group_stats_path}')
                    WHERE {where_clause}
                    """
                    null_result = conn.execute(null_check_sql)
                    null_df = null_result.df()
                    logger.info(
                        f"Data quality check | run_id={run_id} sort_key='{sort_key}' "
                        f"total={null_df.iloc[0]['total_rows']} "
                        f"non_null={null_df.iloc[0]['non_null_names']} "
                        f"null_empty={null_df.iloc[0]['null_or_empty_names']} "
                        f"min_name='{null_df.iloc[0]['min_name']}' "
                        f"max_name='{null_df.iloc[0]['max_name']}'"
                    )

                    # Additional debugging for whitespace and character analysis
                    whitespace_check_sql = f"""
                    SELECT 
                        {PRIMARY_NAME} as name,
                        LENGTH({PRIMARY_NAME}) as name_length,
                        LENGTH(TRIM({PRIMARY_NAME})) as trimmed_length,
                        ASCII(SUBSTRING({PRIMARY_NAME}, 1, 1)) as first_char_ascii,
                        SUBSTRING({PRIMARY_NAME}, 1, 1) as first_char
                    FROM read_parquet('{group_stats_path}')
                    WHERE {where_clause}
                    ORDER BY {order_by}
                    LIMIT 5
                    """
                    try:
                        whitespace_result = conn.execute(whitespace_check_sql)
                        whitespace_df = whitespace_result.df()
                        for _, row in whitespace_df.iterrows():
                            name = row["name"]
                            logger.info(
                                f"Character analysis | run_id={run_id} sort_key='{sort_key}' "
                                f"name='{name}' length={row['name_length']} "
                                f"trimmed_length={row['trimmed_length']} "
                                f"first_char='{row['first_char']}' "
                                f"first_ascii={row['first_char_ascii']}"
                            )
                    except Exception as e:
                        logger.warning(f"Whitespace check failed: {e}")

                    # Check the actual min and max names for character analysis
                    min_max_check_sql = f"""
                    SELECT 
                        {PRIMARY_NAME} as name,
                        LENGTH({PRIMARY_NAME}) as name_length,
                        LENGTH(TRIM({PRIMARY_NAME})) as trimmed_length,
                        ASCII(SUBSTRING({PRIMARY_NAME}, 1, 1)) as first_char_ascii,
                        SUBSTRING({PRIMARY_NAME}, 1, 1) as first_char,
                        HEX({PRIMARY_NAME}) as name_hex
                    FROM read_parquet('{group_stats_path}')
                    WHERE {where_clause}
                    AND ({PRIMARY_NAME} = (SELECT MIN({PRIMARY_NAME}) FROM read_parquet('{group_stats_path}') WHERE {where_clause})
                         OR {PRIMARY_NAME} = (SELECT MAX({PRIMARY_NAME}) FROM read_parquet('{group_stats_path}') WHERE {where_clause}))
                    ORDER BY {PRIMARY_NAME}
                    """
                    try:
                        min_max_result = conn.execute(min_max_check_sql)
                        min_max_df = min_max_result.df()
                        for _, row in min_max_df.iterrows():
                            name = row["name"]
                            logger.info(
                                f"Min/Max analysis | run_id={run_id} sort_key='{sort_key}' "
                                f"name='{name}' length={row['name_length']} "
                                f"trimmed_length={row['trimmed_length']} "
                                f"first_char='{row['first_char']}' "
                                f"first_ascii={row['first_char_ascii']} "
                                f"hex='{row['name_hex'][:50]}...'"
                            )
                    except Exception as e:
                        logger.warning(f"Min/Max analysis failed: {e}")
                except Exception as e:
                    logger.warning(f"Global sorting debug failed: {e}")
                    logger.warning(
                        f"DEBUG: Exception details: {type(e).__name__}: {str(e)}"
                    )

        # Close connection
        conn.close()

        elapsed = time.time() - start_time
        logger.info(
            f"DuckDB groups page loaded from stats | run_id={run_id} rows={len(page_data)} offset={(page - 1) * page_size} sort='{sort_key}' elapsed={elapsed:.3f}s"
        )

        # Phase 1.22.1: Structured logging for performance tracking
        logger.info(
            f"groups_perf: backend=duckdb reason=stats_parquet_available cold_load_s={elapsed:.3f} groups={total_groups} used_stats_parquet=true"
        )

        return page_data, total_groups

    except Exception as e:
        elapsed = time.time() - start_time
        logger.error(
            f"Groups page load from stats failed | run_id={run_id} error='{str(e)}' elapsed={elapsed:.3f}s"
        )
        return [], 0


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
        from src.utils.io_utils import load_settings

        settings = load_settings(str(get_config_path()))
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
    step_start = time.time()
    conn = duckdb.connect(":memory:")
    conn.execute(f"PRAGMA threads = {duckdb_threads}")
    connect_time = time.time() - step_start

    logger.info(
        f"DuckDB connection | run_id={run_id} threads={duckdb_threads} elapsed={connect_time:.3f}s"
    )

    check_timeout()

    # Step 2: Build SQL query
    step_start = time.time()

    # Build WHERE clause for filters
    where_conditions = []
    if filters.get("dispositions"):
        dispositions = filters["dispositions"]
        disp_list = "', '".join(dispositions)
        where_conditions.append(f"{DISPOSITION} IN ('{disp_list}')")

    if filters.get("min_edge_strength", 0.0) > 0.0:
        where_conditions.append(
            f"{WEAKEST_EDGE_TO_PRIMARY} >= {filters['min_edge_strength']}"
        )

    where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"

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
        f"groups_page_duckdb | run_id={run_id} sort_key='{sort_key}' order_by='{order_by_clause}' backend=duckdb global_sort=true"
    )

    # Calculate pagination
    offset = (page - 1) * page_size

    # Build SQL
    # Build the query with proper global sorting before pagination
    # This ensures ORDER BY is applied to the entire dataset before LIMIT/OFFSET
    # Result: Consistent sorting across all pages, not just within each page
    sql = f"""
    SELECT
      s.{GROUP_ID},
      s.{GROUP_SIZE},
      s.{MAX_SCORE},
      COALESCE(p.{PRIMARY_NAME}, '') AS {PRIMARY_NAME}
    FROM (
      WITH base AS (
        SELECT
          {GROUP_ID},
          {ACCOUNT_NAME},
          {IS_PRIMARY},
          {WEAKEST_EDGE_TO_PRIMARY},
          {DISPOSITION}
        FROM read_parquet('{parquet_path}')
        WHERE {where_clause}
      ),
      stats AS (
        SELECT 
          {GROUP_ID}, 
          COUNT(*) AS {GROUP_SIZE}, 
          MAX({WEAKEST_EDGE_TO_PRIMARY}) AS {MAX_SCORE}
        FROM base
        GROUP BY {GROUP_ID}
      ),
      primary_names AS (
        SELECT
          {GROUP_ID},
          any_value({ACCOUNT_NAME}) FILTER (WHERE {IS_PRIMARY}) AS {PRIMARY_NAME}
        FROM base
        GROUP BY {GROUP_ID}
      )
      SELECT
        s.{GROUP_ID},
        s.{GROUP_SIZE},
        s.{MAX_SCORE},
        COALESCE(p.{PRIMARY_NAME}, '') AS {PRIMARY_NAME}
      FROM stats s
      LEFT JOIN primary_names p USING ({GROUP_ID})
      ORDER BY {order_by_clause}, s.{GROUP_ID} ASC
    ) sorted_data
    LIMIT {page_size}
    OFFSET {offset};
    """

    query_build_time = time.time() - step_start

    logger.info(
        f'DuckDB query built | run_id={run_id} where_clause="{where_clause}" order_by="{order_by_clause}" elapsed={query_build_time:.3f}s'
    )

    check_timeout()

    # Step 3: Execute query
    step_start = time.time()
    result = conn.execute(sql)
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

    # Get total count
    count_sql = f"""
    WITH base AS (
      SELECT {GROUP_ID}
      FROM read_parquet('{parquet_path}')
      WHERE {where_clause}
    )
    SELECT COUNT(DISTINCT {GROUP_ID}) as total_groups
    FROM base;
    """
    total_result = conn.execute(count_sql)
    total_groups = total_result.fetchone()[0]

    # Close connection
    conn.close()

    elapsed = time.time() - start_time
    logger.info(
        f'DuckDB groups page loaded | run_id={run_id} rows={len(page_data)} offset={offset} sort="{sort_key}" elapsed={elapsed:.3f} projected_cols=["group_id", "group_size", "max_score", "primary_name"]'
    )

    return page_data, total_groups


def get_total_groups_count(run_id: str, filters: Dict[str, Any]) -> int:
    """
    Get total count of groups after applying filters.

    Args:
        run_id: Run ID to load data from
        filters: Dictionary of active filters

    Returns:
        Total number of groups
    """
    try:
        # Get artifact paths
        artifact_paths = get_artifact_paths(run_id)
        parquet_path = artifact_paths["review_ready_parquet"]

        if not os.path.exists(parquet_path):
            return 0

        # Use dataset scanning for efficient counting
        dataset = ds.dataset(parquet_path)

        # Project only group_id column for counting
        scanner = dataset.scanner(columns=[GROUP_ID])
        projected_table = scanner.to_table()

        # Apply filters
        filtered_table = apply_filters_pyarrow(projected_table, filters)

        # Get unique group count efficiently
        # Note: This is still expensive but much better than loading all data
        unique_groups = filtered_table.column(GROUP_ID).unique()
        return int(len(unique_groups))

    except Exception as e:
        logger.error(f"Failed to get total groups count: {e}")
        return 0
