"""Cleanup API for Streamlit integration.

This module provides a typed API wrapper for the pipeline cleanup functionality,
delegating to cache_utils to ensure safety and consistency.
"""

from typing import Any, Optional

from src.utils.cache_utils import (
    delete_runs as _delete_core,
)
from src.utils.cache_utils import (
    load_run_index,
)
from src.utils.cache_utils import (
    preview_delete_runs as _preview_core,
)
from src.utils.logging_utils import get_logger

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


def preview_delete(run_ids: list[str]) -> PreviewInfo:
    """Preview deletion of runs and return what would be removed.

    Args:
        run_ids: List of run IDs to preview deletion for

    Returns:
        PreviewInfo with deletion preview information
    """
    # Delegate to cache_utils for safety and consistency
    raw = _preview_core(run_ids)
    run_index = load_run_index()

    # Convert cache_utils format to our typed DTOs
    runs_to_delete = []
    for run_data in raw["runs_to_delete"]:
        run_id = run_data["run_id"]
        run_meta = run_index.get(run_id, {})

        # Handle legacy runs without run_type
        run_type = run_meta.get("run_type", "dev")
        if "run_type" not in run_meta and run_id in run_index:
            logger.warning(f"Legacy run {run_id} missing run_type, treating as 'dev'")

        runs_to_delete.append(
            RunInfo(
                run_id=run_id,
                run_type=run_type,
                status=run_data["status"],
                timestamp=run_meta.get("timestamp", ""),
                input_paths=run_meta.get("input_paths", []),
                config_paths=run_meta.get("config_paths", []),
            )
        )

    return PreviewInfo(
        runs_to_delete=runs_to_delete,
        runs_not_found=raw["runs_not_found"],
        runs_inflight=raw["runs_inflight"],
        total_bytes=raw["total_bytes"],
        latest_affected=raw["latest_affected"],
    )


def delete_runs(run_ids: list[str]) -> DeleteResult:
    """Delete runs and their artifacts.

    Args:
        run_ids: List of run IDs to delete

    Returns:
        DeleteResult with deletion results
    """
    # Delegate to cache_utils for safety and consistency
    raw = _delete_core(run_ids)

    return DeleteResult(
        deleted=raw["deleted"],
        not_found=raw["not_found"],
        inflight_blocked=raw["inflight_blocked"],
        errors=raw["errors"],
        total_bytes_freed=raw["total_bytes_freed"],
        latest_reassigned=raw["latest_reassigned"],
        new_latest=raw["new_latest"],
    )
