"""Run management utilities for ui_helpers refactor.

This module handles run lifecycle management.
"""

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from src.utils.cache_utils import (
    get_latest_run_id,
    list_runs_deduplicated,
    load_run_index,
)
from src.utils.logging_utils import get_logger
from src.utils.path_utils import get_interim_dir, get_processed_dir

logger = get_logger(__name__)


def list_runs() -> List[Dict[str, Any]]:
    """Get a sorted list of all runs with metadata, with duplicates removed.

    Returns:
        List of run dictionaries sorted by timestamp (newest first)

    """
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
            },
        )

    return runs


def get_run_metadata(run_id: str) -> Optional[Dict[str, Any]]:
    """Get detailed metadata for a specific run.

    Args:
        run_id: The run ID to get metadata for

    Returns:
        Run metadata dictionary or None if run not found

    """
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
    """Validate that a run has all required artifacts.

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


def get_default_run_id() -> str:
    """Get the default run ID (latest successful run).

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
    run_id: str,
    metadata: Optional[Dict[str, Any]] = None,
) -> str:
    """Format a run ID for display in the UI.

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

    return f"{input_file} ({run_id[:8]})"


def load_stage_state(run_id: str) -> Optional[Dict[str, Any]]:
    """Load and parse MiniDAG stage state for a run.

    Args:
        run_id: The run ID to load state for

    Returns:
        Parsed stage state or None if not available

    """
    state_path = get_interim_dir(run_id) / "pipeline_state.json"

    if not os.path.exists(state_path):
        return None

    try:
        with open(state_path) as f:
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
                    start_str = datetime.fromtimestamp(float(start_time)).strftime(
                        "%H:%M:%S",
                    )
                if end_time is not None and end_time > 0:
                    end_str = datetime.fromtimestamp(float(end_time)).strftime(
                        "%H:%M:%S",
                    )

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
                    },
                )

        return {
            "stages": stage_info,
            "metadata": metadata,
            "run_id": run_id,
            "state_path": str(state_path),
        }

    except (OSError, json.JSONDecodeError) as e:
        logger.warning(f"Failed to load stage state for {run_id}: {e}")
        return None
