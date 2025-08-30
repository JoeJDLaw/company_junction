"""
UI helper functions for Phase 1.17.1.

This module provides pure functions for run loading, stage status parsing,
and artifact path management for the Streamlit UI.
"""

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from src.utils.cache_utils import get_latest_run_id, load_run_index
from src.utils.logging_utils import get_logger

logger = get_logger(__name__)


def list_runs() -> List[Dict[str, Any]]:
    """
    Get a sorted list of all runs with metadata.

    Returns:
        List of run dictionaries sorted by timestamp (newest first)
    """
    run_index = load_run_index()
    runs = []

    for run_id, run_data in run_index.items():
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

    # Sort by timestamp (newest first)
    runs.sort(key=lambda x: x["timestamp"], reverse=True)

    return runs


def get_run_metadata(run_id: str) -> Optional[Dict[str, Any]]:
    """
    Get detailed metadata for a specific run.

    Args:
        run_id: The run ID to get metadata for

    Returns:
        Run metadata dictionary or None if run not found
    """
    run_index = load_run_index()

    if run_id not in run_index:
        return None

    run_data = run_index[run_id]

    # Parse timestamp for display
    try:
        timestamp = datetime.fromisoformat(run_data.get("timestamp", ""))
        formatted_timestamp = timestamp.strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError):
        formatted_timestamp = run_data.get("timestamp", "Unknown")

    return {
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
    processed_dir = f"data/processed/{run_id}"
    interim_dir = f"data/interim/{run_id}"

    # Check review_ready files
    csv_path = f"{processed_dir}/review_ready.csv"
    parquet_path = f"{processed_dir}/review_ready.parquet"

    if os.path.exists(csv_path):
        validation["has_review_ready_csv"] = True
    else:
        validation["missing_files"].append("review_ready.csv")

    if os.path.exists(parquet_path):
        validation["has_review_ready_parquet"] = True
    else:
        validation["missing_files"].append("review_ready.parquet")

    # Check pipeline state
    state_path = f"{interim_dir}/pipeline_state.json"
    if os.path.exists(state_path):
        validation["has_pipeline_state"] = True
    else:
        validation["missing_files"].append("pipeline_state.json")

    # Check review meta
    meta_path = f"{processed_dir}/review_meta.json"
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
    state_path = f"data/interim/{run_id}/pipeline_state.json"

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
                start_time = stage_data.get("start_time", 0)
                end_time = stage_data.get("end_time", 0)

                # Calculate duration
                duration = 0.0
                if start_time > 0 and end_time > 0:
                    duration = end_time - start_time

                # Format timestamps
                start_str = ""
                end_str = ""
                if start_time > 0:
                    start_str = datetime.fromtimestamp(start_time).strftime("%H:%M:%S")
                if end_time > 0:
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
    Get paths to artifacts for a run.

    Args:
        run_id: The run ID to get paths for

    Returns:
        Dictionary mapping artifact names to file paths
    """
    processed_dir = f"data/processed/{run_id}"
    interim_dir = f"data/interim/{run_id}"

    return {
        "review_ready_csv": f"{processed_dir}/review_ready.csv",
        "review_ready_parquet": f"{processed_dir}/review_ready.parquet",
        "review_meta": f"{processed_dir}/review_meta.json",
        "pipeline_state": f"{interim_dir}/pipeline_state.json",
        "candidate_pairs": f"{interim_dir}/candidate_pairs.parquet",
        "groups": f"{interim_dir}/groups.parquet",
        "survivorship": f"{interim_dir}/survivorship.parquet",
        "dispositions": f"{interim_dir}/dispositions.parquet",
        "alias_matches": f"{interim_dir}/alias_matches.parquet",
        "block_top_tokens": f"{interim_dir}/block_top_tokens.csv",
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
        input_file = str(Path(input_paths[0]).name)

    # Format timestamp
    timestamp = metadata.get("formatted_timestamp", "Unknown")

    return f"{input_file} ({timestamp})"


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
        "pending": "⏸️",
        "unknown": "❓",
    }

    return str(status_icons.get(status, "❓"))
