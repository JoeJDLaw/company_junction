"""Hash utilities for the company junction pipeline.

This module provides stable content-only hashing functions for input validation
and resume functionality.
"""

import hashlib
import json
from pathlib import Path
from typing import Any, Union


def stable_content_hash(
    content: Union[str, bytes, Path],
    *,
    normalize_newlines: bool = True,
    strip_trailing_ws: bool = True,
) -> str:
    """Generate a stable content hash that ignores file metadata.

    Args:
        content: Content to hash (string, bytes, or file path)
        normalize_newlines: Normalize newlines to \n
        strip_trailing_ws: Strip trailing whitespace and blank lines

    Returns:
        SHA256 hash of the normalized content

    Raises:
        FileNotFoundError: If content is a path that doesn't exist
        ValueError: If content is empty or invalid

    """
    if isinstance(content, Path):
        if not content.exists():
            raise FileNotFoundError(f"File not found: {content}")
        with open(content, "rb") as f:
            content_bytes = f.read()
    elif isinstance(content, str):
        content_bytes = content.encode("utf-8")
    elif isinstance(content, bytes):
        content_bytes = content
    else:
        raise ValueError(f"Unsupported content type: {type(content)}")

    if not content_bytes:
        raise ValueError("Content cannot be empty")

    # Normalize newlines to \n
    if normalize_newlines:
        content_bytes = content_bytes.replace(b"\r\n", b"\n").replace(b"\r", b"\n")

    # Strip trailing whitespace and blank lines
    if strip_trailing_ws:
        lines = content_bytes.split(b"\n")
        # Strip trailing blank lines
        while lines and not lines[-1].strip():
            lines.pop()
        # Strip trailing whitespace from last line
        if lines:
            lines[-1] = lines[-1].rstrip()
        content_bytes = b"\n".join(lines)

    # Generate SHA256 hash
    return hashlib.sha256(content_bytes).hexdigest()


def stable_schema_hash(schema_obj: dict) -> str:
    """Generate a stable hash for schema objects.

    Args:
        schema_obj: Schema dictionary to hash

    Returns:
        SHA256 hash of the normalized schema

    """
    # Sort keys and use compact JSON for determinism
    normalized = json.dumps(schema_obj, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def stable_file_hash(
    file_path: Union[str, Path],
    *,
    normalize_newlines: bool = True,
    strip_trailing_ws: bool = True,
) -> str:
    """Generate a stable hash for a file, ignoring metadata.

    Args:
        file_path: Path to the file
        normalize_newlines: Normalize newlines to \n
        strip_trailing_ws: Strip trailing whitespace and blank lines

    Returns:
        SHA256 hash of the file content

    Raises:
        FileNotFoundError: If file doesn't exist

    """
    return stable_content_hash(
        Path(file_path),
        normalize_newlines=normalize_newlines,
        strip_trailing_ws=strip_trailing_ws,
    )


# Backward compatibility wrapper
def compute_file_hash(file_path: Union[str, Path]) -> str:
    """Backward compatibility wrapper for existing code.

    Args:
        file_path: Path to the file

    Returns:
        SHA256 hash of the file content

    """
    return stable_file_hash(file_path)


# =============================================================================
# LEGACY FUNCTIONS - Required for backward compatibility
# =============================================================================


def config_hash(cfg_dict: dict) -> str:
    """Compute a deterministic hash of the configuration.

    Args:
        cfg_dict: Configuration dictionary

    Returns:
        8-character hex hash

    """
    # Sort keys and use consistent separators for deterministic hashing
    config_str = json.dumps(cfg_dict, separators=(",", ":"), sort_keys=True)
    hash_obj = hashlib.sha1(config_str.encode())
    return hash_obj.hexdigest()[:8]


def stable_group_id(member_ids: list[str], cfg_dict: dict, n: int = 10) -> str:
    """Generate a stable, deterministic group ID.

    Args:
        member_ids: List of member account IDs
        cfg_dict: Configuration dictionary
        n: Length of the group ID (default 10)

    Returns:
        Stable group ID as hex string

    """
    payload = {
        "members": sorted(map(str, member_ids)),
        "config_hash": config_hash(cfg_dict),
    }

    # Use consistent JSON serialization
    payload_str = json.dumps(payload, separators=(",", ":"), sort_keys=True)
    hash_obj = hashlib.sha1(payload_str.encode())
    return hash_obj.hexdigest()[:n]


def _compute_config_hash(config_dict: dict[str, Any]) -> str:
    """Compute a deterministic hash of the configuration.

    Args:
        config_dict: Configuration dictionary

    Returns:
        8-character hex hash

    """
    # Sort keys and use consistent separators for deterministic hashing
    config_str = json.dumps(config_dict, separators=(",", ":"), sort_keys=True)
    hash_obj = hashlib.sha1(config_str.encode())
    return hash_obj.hexdigest()[:8]
