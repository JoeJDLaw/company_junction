"""
Path utilities for the company junction pipeline.
"""

from pathlib import Path


def get_project_root() -> Path:
    """
    Get the project root directory.

    Returns:
        Path to the project root
    """
    return Path(__file__).parent.parent.parent


def ensure_directory_exists(directory_path: str) -> None:
    """
    Create directory if it doesn't exist.

    Args:
        directory_path: Path to the directory to create
    """
    Path(directory_path).mkdir(parents=True, exist_ok=True)


def get_data_paths() -> dict:
    """
    Get standard data directory paths.

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


def get_processed_dir(run_id: str) -> Path:
    """Get the processed data directory for a run.

    Args:
        run_id: The run ID

    Returns:
        Path to the processed directory, relative to project root
    """
    return Path("data") / "processed" / run_id


def get_interim_dir(run_id: str) -> Path:
    """Get the interim data directory for a run.

    Args:
        run_id: The run ID

    Returns:
        Path to the interim directory, relative to project root
    """
    return Path("data") / "interim" / run_id


def get_artifact_path(run_id: str, artifact: str) -> Path:
    """Get the path to an artifact for a run.

    Args:
        run_id: The run ID
        artifact: Name of the artifact file

    Returns:
        Path to the artifact, relative to project root
    """
    # Check processed directory first, then interim
    processed_path = get_processed_dir(run_id) / artifact
    interim_path = get_interim_dir(run_id) / artifact

    # Return the path that exists, or processed as default
    if processed_path.exists():
        return processed_path
    elif interim_path.exists():
        return interim_path
    else:
        # Default to processed directory
        return processed_path
