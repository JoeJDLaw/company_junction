"""Cleanup API for Streamlit integration.

This module provides a typed API wrapper for the pipeline cleanup functionality,
avoiding shelling out from Streamlit and providing clean interfaces for the UI.
"""

import os
import shutil
from typing import Any, Optional

from src.utils.cache_utils import (
    get_latest_run_id,
    load_run_index,
    save_run_index,
)
from src.utils.logging_utils import get_logger
from src.utils.path_utils import get_interim_dir, get_processed_dir

logger = get_logger(__name__)


class RunInfo:
    """Information about a pipeline run."""

    def __init__(
        self,
        run_id: str,
        run_type: str,
        status: str,
        timestamp: str,
        input_paths: list[str],
        config_paths: list[str],
    ) -> None:
        self.run_id = run_id
        self.run_type = run_type
        self.status = status
        self.timestamp = timestamp
        self.input_paths = input_paths
        self.config_paths = config_paths

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "run_id": self.run_id,
            "run_type": self.run_type,
            "status": self.status,
            "timestamp": self.timestamp,
            "input_paths": self.input_paths,
            "config_paths": self.config_paths,
        }


class PreviewInfo:
    """Preview information for deletion operations."""

    def __init__(
        self,
        runs_to_delete: list[RunInfo],
        runs_not_found: list[str],
        runs_inflight: list[str],
        total_bytes: int,
        latest_affected: bool,
    ) -> None:
        self.runs_to_delete = runs_to_delete
        self.runs_not_found = runs_not_found
        self.runs_inflight = runs_inflight
        self.total_bytes = total_bytes
        self.latest_affected = latest_affected

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "runs_to_delete": [run.to_dict() for run in self.runs_to_delete],
            "runs_not_found": self.runs_not_found,
            "runs_inflight": self.runs_inflight,
            "total_bytes": self.total_bytes,
            "latest_affected": self.latest_affected,
        }


class DeleteResult:
    """Result of a deletion operation."""

    def __init__(
        self,
        deleted: list[str],
        not_found: list[str],
        inflight_blocked: list[str],
        errors: list[str],
        total_bytes_freed: int,
        latest_reassigned: bool,
        new_latest: Optional[str],
    ) -> None:
        self.deleted = deleted
        self.not_found = not_found
        self.inflight_blocked = inflight_blocked
        self.errors = errors
        self.total_bytes_freed = total_bytes_freed
        self.latest_reassigned = latest_reassigned
        self.new_latest = new_latest

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "deleted": self.deleted,
            "not_found": self.not_found,
            "inflight_blocked": self.inflight_blocked,
            "errors": self.errors,
            "total_bytes_freed": self.total_bytes_freed,
            "latest_reassigned": self.latest_reassigned,
            "new_latest": self.new_latest,
        }


def list_runs() -> list[RunInfo]:
    """Get a list of all runs with metadata.

    Returns:
        List of RunInfo objects sorted by timestamp (newest first)
    """
    run_index = load_run_index()
    latest_run_id = get_latest_run_id()

    runs = []
    for run_id, run_data in run_index.items():
        # Handle legacy runs without run_type
        run_type = run_data.get("run_type", "dev")
        if "run_type" not in run_data:
            logger.warning(f"Legacy run {run_id} missing run_type, treating as 'dev'")

        runs.append(
            RunInfo(
                run_id=run_id,
                run_type=run_type,
                status=run_data.get("status", "unknown"),
                timestamp=run_data.get("timestamp", ""),
                input_paths=run_data.get("input_paths", []),
                config_paths=run_data.get("config_paths", []),
            )
        )

    # Sort by timestamp (newest first)
    runs.sort(key=lambda x: x.timestamp, reverse=True)
    return runs


def _calculate_directory_size(directory: str) -> int:
    """Calculate total size of a directory in bytes."""
    total_size = 0
    if os.path.exists(directory):
        for root, _dirs, files in os.walk(directory):
            for file in files:
                file_path = os.path.join(root, file)
                try:
                    total_size += os.path.getsize(file_path)
                except OSError:
                    pass
    return total_size


def preview_delete(run_ids: list[str]) -> PreviewInfo:
    """Preview deletion of runs and return what would be removed.

    Args:
        run_ids: List of run IDs to preview deletion for

    Returns:
        PreviewInfo with deletion preview information
    """
    run_index = load_run_index()
    latest_run_id = get_latest_run_id()

    runs_to_delete = []
    runs_not_found = []
    runs_inflight = []
    total_bytes = 0
    latest_affected = False

    for run_id in run_ids:
        if run_id not in run_index:
            runs_not_found.append(run_id)
            continue

        run_data = run_index[run_id]
        status = run_data.get("status", "unknown")

        if status == "running":
            runs_inflight.append(run_id)
            continue

        # Check if this run is the latest
        if run_id == latest_run_id:
            latest_affected = True

        # Calculate directory sizes
        interim_dir = str(get_interim_dir(run_id))
        processed_dir = str(get_processed_dir(run_id))

        run_bytes = _calculate_directory_size(interim_dir) + _calculate_directory_size(
            processed_dir
        )

        # Handle legacy runs without run_type
        run_type = run_data.get("run_type", "dev")
        if "run_type" not in run_data:
            logger.warning(f"Legacy run {run_id} missing run_type, treating as 'dev'")

        runs_to_delete.append(
            RunInfo(
                run_id=run_id,
                run_type=run_type,
                status=status,
                timestamp=run_data.get("timestamp", ""),
                input_paths=run_data.get("input_paths", []),
                config_paths=run_data.get("config_paths", []),
            )
        )
        total_bytes += run_bytes

    return PreviewInfo(
        runs_to_delete=runs_to_delete,
        runs_not_found=runs_not_found,
        runs_inflight=runs_inflight,
        total_bytes=total_bytes,
        latest_affected=latest_affected,
    )


def delete_runs(run_ids: list[str]) -> DeleteResult:
    """Delete runs and their artifacts.

    Args:
        run_ids: List of run IDs to delete

    Returns:
        DeleteResult with deletion results
    """
    run_index = load_run_index()
    latest_run_id = get_latest_run_id()

    deleted = []
    not_found = []
    inflight_blocked = []
    errors = []
    total_bytes_freed = 0
    latest_reassigned = False
    new_latest = None

    # Check for running runs first
    for run_id in run_ids:
        if run_id in run_index and run_index[run_id].get("status") == "running":
            inflight_blocked.append(run_id)

    if inflight_blocked:
        logger.warning(f"Blocking deletion of running runs: {inflight_blocked}")
        return DeleteResult(
            deleted=[],
            not_found=[],
            inflight_blocked=inflight_blocked,
            errors=[],
            total_bytes_freed=0,
            latest_reassigned=False,
            new_latest=None,
        )

    # Perform deletions
    for run_id in run_ids:
        if run_id not in run_index:
            not_found.append(run_id)
            continue

        # Remove from index
        del run_index[run_id]

        # Remove cache directories
        interim_dir = str(get_interim_dir(run_id))
        processed_dir = str(get_processed_dir(run_id))

        run_bytes_freed = 0
        for directory in [interim_dir, processed_dir]:
            if os.path.exists(directory):
                try:
                    # Calculate size before deletion
                    run_bytes_freed += _calculate_directory_size(directory)

                    shutil.rmtree(directory)
                    logger.info(f"Deleted run directory: {directory}")
                except OSError as e:
                    error_msg = f"Failed to delete {directory}: {e}"
                    logger.error(error_msg)
                    errors.append(error_msg)

        deleted.append(run_id)
        total_bytes_freed += run_bytes_freed

    # Save updated index
    save_run_index(run_index)

    # Recompute latest pointer if needed
    if latest_run_id and latest_run_id in run_ids:
        new_latest = _recompute_latest_pointer()
        latest_reassigned = True

    return DeleteResult(
        deleted=deleted,
        not_found=not_found,
        inflight_blocked=inflight_blocked,
        errors=errors,
        total_bytes_freed=total_bytes_freed,
        latest_reassigned=latest_reassigned,
        new_latest=new_latest,
    )


def _recompute_latest_pointer() -> Optional[str]:
    """Recompute and update the latest pointer to the newest completed run.

    Returns:
        The new latest run ID, or None if no completed runs exist
    """
    run_index = load_run_index()

    # Find the newest completed run
    completed_runs = [
        (run_id, run_data)
        for run_id, run_data in run_index.items()
        if run_data.get("status") == "complete"
    ]

    if not completed_runs:
        # No completed runs, remove latest pointer
        _remove_latest_pointer()
        return None

    # Sort by timestamp and get the newest
    completed_runs.sort(key=lambda x: x[1]["timestamp"], reverse=True)
    new_latest = completed_runs[0][0]

    # Update latest pointer
    _create_latest_pointer(new_latest)
    logger.info(f"Recomputed latest pointer to: {new_latest}")

    return new_latest


def _create_latest_pointer(run_id: str) -> None:
    """Create latest pointer to the most recent successful run."""
    latest_json = str(get_processed_dir("latest") / "latest.json")

    # Create JSON pointer as backup (always create this) - use atomic write
    try:
        # Write to temp file first, then atomically replace
        temp_json = f"{latest_json}.tmp"
        with open(temp_json, "w") as f:
            import json
            from datetime import datetime

            json.dump({"run_id": run_id, "timestamp": datetime.now().isoformat()}, f)
        os.replace(temp_json, latest_json)
        logger.info(f"Created latest JSON pointer: {latest_json}")
    except OSError as e:
        logger.error(f"Failed to create latest JSON pointer: {e}")
        # Clean up temp file if it exists
        if os.path.exists(temp_json):
            try:
                os.remove(temp_json)
            except OSError:
                pass


def _remove_latest_pointer() -> None:
    """Remove the latest pointer (JSON)."""
    latest_json = str(get_processed_dir("latest") / "latest.json")

    # Remove JSON pointer
    if os.path.exists(latest_json):
        try:
            os.remove(latest_json)
            logger.info(f"Removed latest JSON pointer: {latest_json}")
        except OSError as e:
            logger.warning(f"Failed to remove JSON pointer: {e}")
