"""Path utilities for the company junction pipeline."""

import json
from datetime import datetime
from pathlib import Path
from typing import Optional


def get_project_root() -> Path:
    """Get the project root directory.

    Returns:
        Path to the project root

    """
    return Path(__file__).parent.parent.parent


def ensure_directory_exists(directory_path: str) -> None:
    """Create directory if it doesn't exist.

    Args:
        directory_path: Path to the directory to create

    """
    Path(directory_path).mkdir(parents=True, exist_ok=True)


def get_data_paths() -> dict:
    """Get standard data directory paths.

    Returns:
        Dictionary containing paths to raw, interim, and processed data directories

    """
    project_root = get_project_root()
    return {
        "raw": project_root / "data" / "raw",
        "interim": project_root / "data" / "interim",
        "processed": project_root / "data" / "processed",
    }


def get_config_path(filename: str = "settings.yaml") -> Path:
    """Get the path to a config file.

    Args:
        filename: Name of the config file (default: settings.yaml)

    Returns:
        Path to the config file, relative to project root

    """
    # Try to find the project root by looking for config directory
    current = Path.cwd()

    # Look for config directory in current and parent directories
    for parent in [current] + list(current.parents):
        config_dir = parent / "config"
        if config_dir.exists() and (config_dir / filename).exists():
            # Return relative path from project root
            return Path("config") / filename

    # Fallback: assume we're in project root
    return Path("config") / filename


def get_processed_dir(run_id: Optional[str], output_dir: Optional[str] = None) -> Path:
    """Get the processed data directory for a run.

    Args:
        run_id: The run ID or special directory name (e.g., "latest", "audit", "index")
        output_dir: Optional output directory override (defaults to data/processed)

    Returns:
        Path to the processed directory, relative to project root

    Raises:
        ValueError: If run_id is empty or None

    """
    if not run_id:
        raise ValueError("run_id cannot be empty")
    if run_id is None:
        raise ValueError("run_id cannot be None")

    if output_dir:
        return Path(output_dir) / run_id
    else:
        return Path("data") / "processed" / run_id


def get_interim_dir(run_id: str, output_dir: Optional[str] = None) -> Path:
    """Get the interim data directory for a run.

    Args:
        run_id: The run ID
        output_dir: Optional output directory override (defaults to data/interim)

    Returns:
        Path to the interim directory, relative to project root

    Raises:
        ValueError: If run_id is empty or None

    """
    if not run_id:
        raise ValueError("run_id cannot be empty")
    if run_id is None:
        raise ValueError("run_id cannot be None")

    if output_dir:
        return Path(output_dir) / run_id
    else:
        return Path("data") / "interim" / run_id


def get_artifact_path(run_id: str, artifact: str, output_dir: Optional[str] = None) -> Path:
    """Get the path to an artifact for a run.

    Args:
        run_id: The run ID
        artifact: Name of the artifact file
        output_dir: Optional output directory override

    Returns:
        Path to the artifact, relative to project root

    """
    # Check processed directory first, then interim
    processed_path = get_processed_dir(run_id, output_dir) / artifact
    interim_path = get_interim_dir(run_id, output_dir) / artifact

    # Return the path that exists, or processed as default
    if processed_path.exists():
        return processed_path
    if interim_path.exists():
        return interim_path
    # Default to processed directory
    return processed_path


def get_latest_run_id() -> Optional[str]:
    """Get the latest run ID from the latest symlink or latest.json metadata.

    Returns:
        The latest run ID, or None if no latest run exists

    """
    # Try to read from latest.json first (more reliable)
    latest_json = Path("data/processed/latest.json")
    if latest_json.exists():
        try:
            with open(latest_json) as f:
                data = json.load(f)
                run_id = data.get("run_id")
                if run_id is not None:  # Allow null/None values
                    return str(run_id)
        except (OSError, json.JSONDecodeError):
            pass  # Fall back to symlink

    # Fall back to symlink
    latest_symlink = Path("data/processed/latest")
    if latest_symlink.exists() and latest_symlink.is_symlink():
        try:
            target = latest_symlink.resolve()
            if target.exists():
                return target.name
        except (OSError, RuntimeError):
            pass

    return None


def read_latest_run_id() -> Optional[str]:
    """Read the latest run ID from latest.json metadata file.

    Returns:
        The latest run ID, or None if no latest run exists or file is invalid

    """
    latest_json = Path("data/processed/latest.json")
    if not latest_json.exists():
        return None

    try:
        with open(latest_json) as f:
            data = json.load(f)
            run_id = data.get("run_id")  # Can be None for empty state
            return str(run_id) if run_id is not None else None
    except (OSError, json.JSONDecodeError):
        return None


def write_latest_pointer(run_id: Optional[str]) -> None:
    """Write the latest run pointer to latest.json and optionally create/update symlink.

    Args:
        run_id: The run ID to set as latest, or None for empty state

    """
    latest_json = Path("data/processed/latest.json")
    latest_symlink = Path("data/processed/latest")

    # Ensure processed directory exists
    latest_json.parent.mkdir(parents=True, exist_ok=True)

    # Write metadata file
    metadata = {
        "run_id": run_id,
        "updated_at": datetime.now().isoformat(),
        "empty_state": run_id is None,
    }

    with open(latest_json, "w") as f:
        json.dump(metadata, f, indent=2)

    # Handle symlink based on run_id
    if run_id is None:
        # Remove symlink for empty state
        if latest_symlink.exists():
            try:
                if latest_symlink.is_symlink():
                    latest_symlink.unlink()
                elif latest_symlink.is_dir():
                    latest_symlink.rmdir()  # In case it's a directory
                else:
                    latest_symlink.unlink()  # In case it's a file
            except (OSError, PermissionError):
                # Ignore errors when removing symlink
                pass
    else:
        # Create/update symlink to point to the run
        target_dir = get_processed_dir(run_id)
        if target_dir.exists():
            try:
                if latest_symlink.exists():
                    if latest_symlink.is_symlink():
                        latest_symlink.unlink()
                    elif latest_symlink.is_dir():
                        latest_symlink.rmdir()
                    else:
                        latest_symlink.unlink()
                latest_symlink.symlink_to(target_dir)
            except (OSError, PermissionError) as e:
                # Log error but don't fail
                import logging

                logging.getLogger(__name__).warning(f"Failed to create symlink: {e}")
