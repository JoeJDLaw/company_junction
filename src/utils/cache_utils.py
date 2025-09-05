"""Cache utilities for versioned run management.

This module handles run ID generation, cache directory management,
run index operations, and latest symlink handling for the pipeline.
"""

import hashlib
import json
import os
import shutil
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from src.utils.logging_utils import get_logger
from src.utils.path_utils import get_interim_dir, get_processed_dir


# Phase 1 destructive operations fuse
def _get_destructive_fuse() -> bool:
    """Get the destructive fuse setting from environment."""
    return os.environ.get("PHASE1_DESTRUCTIVE_FUSE", "false").lower() == "true"


PHASE_1_DESTRUCTIVE_FUSE = _get_destructive_fuse()

logger = get_logger(__name__)

# Default values
DEFAULT_KEEP_RUNS = 10
RUN_INDEX_PATH = str(get_processed_dir("index") / "run_index.json")


def compute_file_hash(file_path: str) -> str:
    """Compute SHA256 hash of a file's contents."""
    hash_sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_sha256.update(chunk)
    return hash_sha256.hexdigest()


def generate_run_id(input_paths: List[str], config_paths: List[str]) -> str:
    """Generate a unique run ID based on input and config file hashes.

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
    interim_dir = str(get_interim_dir(run_id))
    processed_dir = str(get_processed_dir(run_id))
    return interim_dir, processed_dir


def create_cache_directories(run_id: str) -> Tuple[str, str]:
    """Create cache directories for a run and return their paths."""
    if not run_id:
        logger.error("Missing run_id; refusing to write to non-scoped processed path")
        sys.exit(2)

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
        with open(RUN_INDEX_PATH) as f:
            data = json.load(f)
            return data if isinstance(data, dict) else {}
    except (OSError, json.JSONDecodeError) as e:
        logger.warning(f"Failed to load run index: {e}")
        return {}


def save_run_index(run_index: Dict[str, Any]) -> None:
    """Save the run index to JSON file."""
    os.makedirs(os.path.dirname(RUN_INDEX_PATH), exist_ok=True)

    try:
        with open(RUN_INDEX_PATH, "w") as f:
            json.dump(run_index, f, indent=2)
    except OSError as e:
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
    if not _get_destructive_fuse():
        logger.warning(
            "Latest pointer creation disabled: Phase 1 destructive fuse not enabled",
        )
        return

    latest_symlink = str(get_processed_dir("latest"))
    latest_json = str(get_processed_dir("latest") / "latest.json")

    # Create symlink (may fail on some filesystems)
    try:
        if os.path.islink(latest_symlink) or os.path.exists(latest_symlink):
            os.remove(latest_symlink)
        # Use relative path for symlink
        os.symlink(f"{run_id}", latest_symlink)
        logger.info(f"Created latest symlink: {latest_symlink} -> {run_id}")
    except OSError as e:
        logger.warning(f"Failed to create symlink: {e}")

    # Create JSON pointer as backup (always create this) - use atomic write
    try:
        # Write to temp file first, then atomically replace
        temp_json = f"{latest_json}.tmp"
        with open(temp_json, "w") as f:
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


def get_latest_run_id() -> Optional[str]:
    """Get the run ID of the latest successful run."""
    latest_symlink = str(get_processed_dir("latest"))
    latest_json = str(get_processed_dir("latest") / "latest.json")

    # Try JSON first (more reliable)
    if os.path.exists(latest_json):
        try:
            with open(latest_json) as f:
                data = json.load(f)
                if isinstance(data, dict):
                    run_id = data.get("run_id")
                    if isinstance(run_id, str):
                        return run_id
        except (OSError, json.JSONDecodeError):
            pass

    # Fallback to symlink
    if os.path.islink(latest_symlink):
        try:
            return os.readlink(latest_symlink)
        except OSError:
            pass

    return None


def prune_old_runs(keep_runs: int = DEFAULT_KEEP_RUNS) -> None:
    """Prune old completed runs, keeping only the most recent N."""
    if not _get_destructive_fuse():
        logger.warning("Prune old runs disabled: Phase 1 destructive fuse not enabled")
        return

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
    if not _get_destructive_fuse():
        logger.warning(
            "Cleanup failed runs disabled: Phase 1 destructive fuse not enabled",
        )
        return

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


def preview_delete_runs(run_ids: List[str]) -> Dict[str, Any]:
    """Preview deletion of runs and return what would be removed.

    Args:
        run_ids: List of run IDs to preview deletion for

    Returns:
        Dict with deletion preview information

    """
    run_index = load_run_index()
    preview: Dict[str, Any] = {
        "runs_to_delete": [],
        "runs_not_found": [],
        "runs_inflight": [],
        "total_bytes": 0,
        "latest_affected": False,
        "latest_run_id": get_latest_run_id(),
    }

    for run_id in run_ids:
        if run_id not in run_index:
            preview["runs_not_found"].append(run_id)
            continue

        run_data = run_index[run_id]
        status = run_data.get("status", "unknown")

        if status == "running":
            preview["runs_inflight"].append(run_id)
            continue

        # Check if this run is the latest
        if run_id == preview["latest_run_id"]:
            preview["latest_affected"] = True

        # Calculate directory sizes
        interim_dir, processed_dir = get_cache_directories(run_id)
        run_bytes = 0
        run_files = []

        for directory in [interim_dir, processed_dir]:
            if os.path.exists(directory):
                for root, dirs, files in os.walk(directory):
                    for file in files:
                        file_path = os.path.join(root, file)
                        try:
                            file_size = os.path.getsize(file_path)
                            run_bytes += file_size
                            run_files.append(file_path)
                        except OSError:
                            pass

        preview["runs_to_delete"].append(
            {
                "run_id": run_id,
                "status": status,
                "bytes": run_bytes,
                "files": run_files,
            },
        )
        preview["total_bytes"] += run_bytes

    return preview


def delete_runs(run_ids: List[str]) -> Dict[str, Any]:
    """Delete runs and their artifacts.

    Args:
        run_ids: List of run IDs to delete

    Returns:
        Dict with deletion results

    """
    if not _get_destructive_fuse():
        logger.warning("Delete runs disabled: Phase 1 destructive fuse not enabled")
        return {
            "deleted": [],
            "not_found": [],
            "inflight_blocked": [],
            "errors": ["Delete runs disabled: Phase 1 destructive fuse not enabled"],
            "total_bytes_freed": 0,
            "latest_reassigned": False,
            "new_latest": None,
        }

    run_index = load_run_index()
    results: Dict[str, Any] = {
        "deleted": [],
        "not_found": [],
        "inflight_blocked": [],
        "errors": [],
        "total_bytes_freed": 0,
        "latest_reassigned": False,
        "new_latest": None,
    }

    # Check for truly inflight runs (with process validation)
    truly_inflight = []
    for run_id in run_ids:
        if run_id in run_index and run_index[run_id].get("status") == "running":
            # Check if there's actually a process running for this run
            if is_run_truly_inflight(run_id):
                truly_inflight.append(run_id)

    if truly_inflight:
        results["inflight_blocked"] = truly_inflight
        logger.warning(f"Blocking deletion of truly inflight runs: {truly_inflight}")
        return results  # Block all deletions if any run is truly inflight

    # Perform deletions
    for run_id in run_ids:
        if run_id not in run_index:
            results["not_found"].append(run_id)
            continue

        # Remove from index
        del run_index[run_id]

        # Remove cache directories
        interim_dir, processed_dir = get_cache_directories(run_id)
        run_bytes_freed = 0

        for directory in [interim_dir, processed_dir]:
            if os.path.exists(directory):
                try:
                    # Calculate size before deletion
                    for root, dirs, files in os.walk(directory):
                        for file in files:
                            try:
                                run_bytes_freed += os.path.getsize(
                                    os.path.join(root, file),
                                )
                            except OSError:
                                pass

                    shutil.rmtree(directory)
                    logger.info(f"Deleted run directory: {directory}")
                except OSError as e:
                    error_msg = f"Failed to delete {directory}: {e}"
                    logger.error(error_msg)
                    results["errors"].append(error_msg)

        results["deleted"].append(run_id)
        results["total_bytes_freed"] += run_bytes_freed

    # Save updated index
    save_run_index(run_index)

    # Recompute latest pointer if needed
    current_latest = get_latest_run_id()
    if current_latest and current_latest in run_ids:
        new_latest = recompute_latest_pointer()
        results["latest_reassigned"] = True
        results["new_latest"] = new_latest
    elif current_latest and current_latest in results["deleted"]:
        # If the current latest was deleted, recompute
        new_latest = recompute_latest_pointer()
        results["latest_reassigned"] = True
        results["new_latest"] = new_latest

    # Log deletion audit
    log_deletion_audit(run_ids, results["total_bytes_freed"])

    return results


def recompute_latest_pointer() -> Optional[str]:
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
        remove_latest_pointer()
        return None

    # Sort by timestamp and get the newest
    completed_runs.sort(key=lambda x: x[1]["timestamp"], reverse=True)
    new_latest = completed_runs[0][0]

    # Update latest pointer
    create_latest_pointer(new_latest)
    logger.info(f"Recomputed latest pointer to: {new_latest}")

    return new_latest


def remove_latest_pointer() -> None:
    """Remove the latest pointer (symlink and JSON)."""
    if not _get_destructive_fuse():
        logger.warning(
            "Latest pointer removal disabled: Phase 1 destructive fuse not enabled",
        )
        return

    latest_symlink = str(get_processed_dir("latest"))
    latest_json = str(get_processed_dir("latest") / "latest.json")

    # Remove symlink
    if os.path.islink(latest_symlink):
        try:
            os.remove(latest_symlink)
            logger.info(f"Removed latest symlink: {latest_symlink}")
        except OSError as e:
            logger.warning(f"Failed to remove symlink: {e}")

    # Remove JSON pointer
    if os.path.exists(latest_json):
        try:
            os.remove(latest_json)
            logger.info(f"Removed latest JSON pointer: {latest_json}")
        except OSError as e:
            logger.warning(f"Failed to remove JSON pointer: {e}")


def log_deletion_audit(run_ids: List[str], bytes_freed: int) -> None:
    """Log deletion audit information to file."""
    audit_log_path = str(get_processed_dir("audit") / "run_deletions.log")

    audit_entry = {
        "timestamp": datetime.now().isoformat(),
        "run_ids": run_ids,
        "bytes_freed": bytes_freed,
    }

    try:
        os.makedirs(os.path.dirname(audit_log_path), exist_ok=True)
        with open(audit_log_path, "a") as f:
            f.write(json.dumps(audit_entry) + "\n")
        logger.info(f"Logged deletion audit: {len(run_ids)} runs, {bytes_freed} bytes")
    except OSError as e:
        logger.error(f"Failed to log deletion audit: {e}")


def list_runs_sorted() -> List[Tuple[str, Dict[str, Any]]]:
    """Get list of runs sorted by timestamp (newest first)."""
    run_index = load_run_index()

    runs = [(run_id, run_data) for run_id, run_data in run_index.items()]
    runs.sort(key=lambda x: x[1]["timestamp"], reverse=True)

    return runs


def list_runs_deduplicated() -> List[Tuple[str, Dict[str, Any]]]:
    """Get list of runs sorted by timestamp (newest first) with duplicates removed.

    Duplicates are runs with identical input_hash and config_hash. Only the most recent
    run for each unique combination is kept.

    Returns:
        List of deduplicated runs sorted by timestamp (newest first)

    """
    run_index = load_run_index()

    # Group runs by input_hash + config_hash
    hash_groups: Dict[str, List[Tuple[str, Dict[str, Any]]]] = {}

    for run_id, run_data in run_index.items():
        input_hash = run_data.get("input_hash", "")
        config_hash = run_data.get("config_hash", "")
        hash_key = f"{input_hash}_{config_hash}"

        if hash_key not in hash_groups:
            hash_groups[hash_key] = []
        hash_groups[hash_key].append((run_id, run_data))

    # For each group, keep only the most recent run
    deduplicated_runs = []
    total_duplicates = 0

    for hash_key, group_runs in hash_groups.items():
        if len(group_runs) > 1:
            # Sort by timestamp (newest first) and keep only the first (most recent)
            group_runs.sort(key=lambda x: x[1]["timestamp"], reverse=True)
            deduplicated_runs.append(group_runs[0])
            total_duplicates += len(group_runs) - 1
            logger.info(
                f"Run deduplication: kept {group_runs[0][0]} from group of {len(group_runs)} runs",
            )
        else:
            deduplicated_runs.append(group_runs[0])

    # Sort all deduplicated runs by timestamp (newest first)
    deduplicated_runs.sort(key=lambda x: x[1]["timestamp"], reverse=True)

    if total_duplicates > 0:
        logger.info(
            f"Run deduplication: removed {total_duplicates} duplicates from {len(deduplicated_runs) + total_duplicates} total runs",
        )

    return deduplicated_runs


def is_run_truly_inflight(run_id: str) -> bool:
    """Check if a run is truly inflight by looking for active processes.

    Args:
        run_id: The run ID to check

    Returns:
        True if there's an active process for this run, False otherwise

    """
    try:
        import psutil

        # Look for python processes running cleaning.py with this run_id
        for proc in psutil.process_iter(["pid", "name", "cmdline"]):
            try:
                if proc.info["name"] == "python" and proc.info["cmdline"]:
                    cmdline = " ".join(proc.info["cmdline"])
                    if "cleaning.py" in cmdline and run_id in cmdline:
                        logger.info(
                            f"Found active process {proc.info['pid']} for run {run_id}",
                        )
                        return True
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue

        logger.info(f"No active process found for run {run_id}")
        return False

    except ImportError:
        # If psutil is not available, be conservative and assume it's inflight
        logger.warning("psutil not available, assuming run is inflight")
        return True


def get_next_latest_run() -> Optional[str]:
    """Get the next latest run ID after the current latest."""
    current_latest = get_latest_run_id()
    if not current_latest:
        return None

    runs = list_runs_sorted()

    # Find the next completed run after current latest
    for run_id, run_data in runs:
        if run_data.get("status") == "complete" and run_id != current_latest:
            return run_id

    return None
