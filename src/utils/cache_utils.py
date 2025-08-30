"""
Cache utilities for versioned run management.

This module handles run ID generation, cache directory management,
run index operations, and latest symlink handling for the pipeline.
"""

import hashlib
import json
import os
import shutil
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from src.utils.logging_utils import get_logger

logger = get_logger(__name__)

# Default values
DEFAULT_KEEP_RUNS = 10
RUN_INDEX_PATH = "data/run_index.json"


def compute_file_hash(file_path: str) -> str:
    """Compute SHA256 hash of a file's contents."""
    hash_sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_sha256.update(chunk)
    return hash_sha256.hexdigest()


def generate_run_id(input_paths: List[str], config_paths: List[str]) -> str:
    """
    Generate a unique run ID based on input and config file hashes.

    Format: {input_hash[:8]}_{config_hash[:8]}_{YYYYMMDDHHMMSS}
    """
    # Compute combined hash of all input files
    input_hash = hashlib.sha256()
    for input_path in sorted(input_paths):
        if os.path.exists(input_path):
            input_hash.update(compute_file_hash(input_path).encode())

    # Compute combined hash of all config files
    config_hash = hashlib.sha256()
    for config_path in sorted(config_paths):
        if os.path.exists(config_path):
            config_hash.update(compute_file_hash(config_path).encode())

    # Generate timestamp
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

    run_id = f"{input_hash.hexdigest()[:8]}_{config_hash.hexdigest()[:8]}_{timestamp}"
    logger.info(f"Generated run_id: {run_id}")
    return run_id


def get_cache_directories(run_id: str) -> Tuple[str, str]:
    """Get interim and processed cache directories for a run."""
    interim_dir = f"data/interim/{run_id}"
    processed_dir = f"data/processed/{run_id}"
    return interim_dir, processed_dir


def create_cache_directories(run_id: str) -> Tuple[str, str]:
    """Create cache directories for a run and return their paths."""
    interim_dir, processed_dir = get_cache_directories(run_id)

    os.makedirs(interim_dir, exist_ok=True)
    os.makedirs(processed_dir, exist_ok=True)

    logger.info(f"Created cache directories: {interim_dir}, {processed_dir}")
    return interim_dir, processed_dir


def load_run_index() -> Dict[str, Any]:
    """Load the run index from JSON file."""
    if not os.path.exists(RUN_INDEX_PATH):
        return {}

    try:
        with open(RUN_INDEX_PATH, "r") as f:
            data = json.load(f)
            return data if isinstance(data, dict) else {}
    except (json.JSONDecodeError, IOError) as e:
        logger.warning(f"Failed to load run index: {e}")
        return {}


def save_run_index(run_index: Dict[str, Any]) -> None:
    """Save the run index to JSON file."""
    os.makedirs(os.path.dirname(RUN_INDEX_PATH), exist_ok=True)

    try:
        with open(RUN_INDEX_PATH, "w") as f:
            json.dump(run_index, f, indent=2)
    except IOError as e:
        logger.error(f"Failed to save run index: {e}")


def add_run_to_index(
    run_id: str,
    input_paths: List[str],
    config_paths: List[str],
    status: str = "running",
) -> None:
    """Add a new run to the index."""
    run_index = load_run_index()

    # Compute hashes
    input_hash = hashlib.sha256()
    for input_path in sorted(input_paths):
        if os.path.exists(input_path):
            input_hash.update(compute_file_hash(input_path).encode())

    config_hash = hashlib.sha256()
    for config_path in sorted(config_paths):
        if os.path.exists(config_path):
            config_hash.update(compute_file_hash(config_path).encode())

    run_index[run_id] = {
        "timestamp": datetime.now().isoformat(),
        "input_paths": input_paths,
        "input_hash": input_hash.hexdigest(),
        "config_paths": config_paths,
        "config_hash": config_hash.hexdigest(),
        "status": status,
        "dag_version": "1.0.0",
    }

    save_run_index(run_index)
    logger.info(f"Added run {run_id} to index with status: {status}")


def update_run_status(run_id: str, status: str) -> None:
    """Update the status of a run in the index."""
    run_index = load_run_index()

    if run_id in run_index:
        run_index[run_id]["status"] = status
        save_run_index(run_index)
        logger.info(f"Updated run {run_id} status to: {status}")
    else:
        logger.warning(f"Run {run_id} not found in index")


def create_latest_pointer(run_id: str) -> None:
    """Create latest pointer to the most recent successful run."""
    latest_symlink = "data/processed/latest"
    latest_json = "data/processed/latest.json"

    # Create symlink (may fail on some filesystems)
    try:
        if os.path.exists(latest_symlink):
            os.remove(latest_symlink)
        os.symlink(run_id, latest_symlink)
        logger.info(f"Created latest symlink: {latest_symlink} -> {run_id}")
    except OSError as e:
        logger.warning(f"Failed to create symlink: {e}")

    # Create JSON pointer as backup
    try:
        with open(latest_json, "w") as f:
            json.dump({"run_id": run_id, "timestamp": datetime.now().isoformat()}, f)
        logger.info(f"Created latest JSON pointer: {latest_json}")
    except IOError as e:
        logger.error(f"Failed to create latest JSON pointer: {e}")


def get_latest_run_id() -> Optional[str]:
    """Get the run ID of the latest successful run."""
    latest_symlink = "data/processed/latest"
    latest_json = "data/processed/latest.json"

    # Try symlink first
    if os.path.islink(latest_symlink):
        try:
            return os.readlink(latest_symlink)
        except OSError:
            pass

    # Fallback to JSON
    if os.path.exists(latest_json):
        try:
            with open(latest_json, "r") as f:
                data = json.load(f)
                if isinstance(data, dict):
                    run_id = data.get("run_id")
                    if isinstance(run_id, str):
                        return run_id
        except (json.JSONDecodeError, IOError):
            pass

    return None


def prune_old_runs(keep_runs: int = DEFAULT_KEEP_RUNS) -> None:
    """Prune old completed runs, keeping only the most recent N."""
    run_index = load_run_index()

    # Get completed runs sorted by timestamp
    completed_runs = [
        (run_id, run_data)
        for run_id, run_data in run_index.items()
        if run_data.get("status") == "complete"
    ]
    completed_runs.sort(key=lambda x: x[1]["timestamp"], reverse=True)

    # Keep the most recent N completed runs
    runs_to_keep = completed_runs[:keep_runs]
    runs_to_delete = completed_runs[keep_runs:]

    for run_id, _ in runs_to_delete:
        # Remove from index
        del run_index[run_id]

        # Remove cache directories
        interim_dir, processed_dir = get_cache_directories(run_id)
        for directory in [interim_dir, processed_dir]:
            if os.path.exists(directory):
                try:
                    shutil.rmtree(directory)
                    logger.info(f"Removed old run directory: {directory}")
                except OSError as e:
                    logger.warning(f"Failed to remove directory {directory}: {e}")

    save_run_index(run_index)
    logger.info(f"Pruned {len(runs_to_delete)} old runs, kept {len(runs_to_keep)}")


def cleanup_failed_runs() -> None:
    """Clean up directories for failed runs."""
    run_index = load_run_index()

    for run_id, run_data in list(run_index.items()):
        if run_data.get("status") == "failed":
            # Remove from index
            del run_index[run_id]

            # Remove cache directories
            interim_dir, processed_dir = get_cache_directories(run_id)
            for directory in [interim_dir, processed_dir]:
                if os.path.exists(directory):
                    try:
                        shutil.rmtree(directory)
                        logger.info(f"Cleaned up failed run directory: {directory}")
                    except OSError as e:
                        logger.warning(f"Failed to remove directory {directory}: {e}")

    save_run_index(run_index)
