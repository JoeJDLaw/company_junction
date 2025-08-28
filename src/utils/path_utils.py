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
