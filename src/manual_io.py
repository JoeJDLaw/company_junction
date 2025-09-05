"""Centralized manual I/O operations for Phase 1.9.

This module provides the single source of truth for reading and writing
manual blacklist and disposition override files.
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, Set

logger = logging.getLogger(__name__)


def ensure_manual_directory() -> Path:
    """Ensure the manual data directory exists."""
    manual_dir = Path("data/manual")
    manual_dir.mkdir(parents=True, exist_ok=True)
    return manual_dir


def _atomic_write_json(data: Any, file_path: Path) -> bool:
    """Write JSON data atomically using a temporary file.

    Args:
        data: Data to write
        file_path: Target file path

    Returns:
        True if successful, False otherwise

    """
    try:
        # Create temporary file in same directory
        temp_file = file_path.with_suffix(".tmp")

        # Write to temporary file
        with open(temp_file, "w") as f:
            json.dump(data, f, indent=2)

        # Atomic rename
        temp_file.replace(file_path)
        return True

    except Exception as e:
        logger.error(f"Error writing to {file_path}: {e}")
        # Clean up temp file if it exists
        if temp_file.exists():
            try:
                temp_file.unlink()
            except (OSError, FileNotFoundError):
                pass
        return False


def load_manual_blacklist(path: str = "data/manual/manual_blacklist.json") -> Set[str]:
    """Load manual blacklist terms from JSON file.

    Args:
        path: Path to the blacklist file

    Returns:
        Set of blacklist terms, empty set if file doesn't exist or is malformed

    """
    file_path = Path(path)

    if not file_path.exists():
        return set()

    try:
        with open(file_path) as f:
            data = json.load(f)
            terms = data.get("terms", [])
            return set(terms)
    except Exception as e:
        logger.warning(f"Could not load manual blacklist from {path}: {e}")
        return set()


def save_manual_blacklist(
    terms: Set[str], path: str = "data/manual/manual_blacklist.json",
) -> bool:
    """Save manual blacklist terms to JSON file.

    Args:
        terms: Set of blacklist terms
        path: Path to the blacklist file

    Returns:
        True if successful, False otherwise

    """
    ensure_manual_directory()
    file_path = Path(path)

    data = {"terms": sorted(list(terms)), "last_updated": datetime.now().isoformat()}

    return _atomic_write_json(data, file_path)


def load_manual_overrides(
    path: str = "data/manual/manual_dispositions.json",
) -> Dict[str, Dict[str, Any]]:
    """Load manual disposition overrides from JSON file.

    Args:
        path: Path to the overrides file

    Returns:
        Dictionary mapping record_id to override data, empty dict if file doesn't exist

    """
    file_path = Path(path)

    if not file_path.exists():
        return {}

    try:
        with open(file_path) as f:
            overrides = json.load(f)

        # Convert list to dict if needed (backward compatibility)
        if isinstance(overrides, list):
            override_dict: Dict[str, Dict[str, Any]] = {}
            for override in overrides:
                if isinstance(override, dict):
                    record_id = override.get("record_id")
                    if record_id:
                        override_dict[record_id] = override
            return override_dict

        # Ensure it's a dict
        if isinstance(overrides, dict):
            return overrides
        logger.warning(
            f"Invalid overrides format in {path}: expected dict or list, got {type(overrides)}",
        )
        return {}

    except Exception as e:
        logger.warning(f"Could not load manual overrides from {path}: {e}")
        return {}


def save_manual_overrides(
    overrides: Dict[str, Dict[str, Any]],
    path: str = "data/manual/manual_dispositions.json",
) -> bool:
    """Save manual disposition overrides to JSON file.

    Args:
        overrides: Dictionary mapping record_id to override data
        path: Path to the overrides file

    Returns:
        True if successful, False otherwise

    """
    ensure_manual_directory()
    file_path = Path(path)

    # Convert to list format for backward compatibility
    overrides_list = list(overrides.values())

    return _atomic_write_json(overrides_list, file_path)


def upsert_manual_override(
    record_id: str,
    override: str,
    reason: str = "",
    reviewer: str = "streamlit_user",
    path: str = "data/manual/manual_dispositions.json",
) -> bool:
    """Add or update a manual disposition override.

    Args:
        record_id: Unique record identifier
        override: Disposition override (Keep/Delete/Update/Verify)
        reason: Reason for override
        reviewer: Name of reviewer
        path: Path to the overrides file

    Returns:
        True if successful, False otherwise

    """
    overrides = load_manual_overrides(path)

    # Add or update override
    overrides[record_id] = {
        "record_id": record_id,
        "override": override,
        "reason": reason,
        "reviewer": reviewer,
        "ts": datetime.now().isoformat(),
    }

    return save_manual_overrides(overrides, path)


def remove_manual_override(
    record_id: str, path: str = "data/manual/manual_dispositions.json",
) -> bool:
    """Remove a manual disposition override.

    Args:
        record_id: Unique record identifier
        path: Path to the overrides file

    Returns:
        True if successful, False otherwise

    """
    overrides = load_manual_overrides(path)

    if record_id in overrides:
        del overrides[record_id]
        return save_manual_overrides(overrides, path)

    return True  # Already removed


def get_manual_override(
    record_id: str, path: str = "data/manual/manual_dispositions.json",
) -> Optional[str]:
    """Get manual override for a specific record.

    Args:
        record_id: Unique record identifier
        path: Path to the overrides file

    Returns:
        Override disposition if found, None otherwise

    """
    overrides = load_manual_overrides(path)
    override_data = overrides.get(record_id)
    return override_data.get("override") if override_data else None
