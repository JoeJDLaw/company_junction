"""
Hash utilities for the company junction pipeline.
"""

import json
import hashlib
from typing import Dict, List, Any


def config_hash(cfg_dict: Dict) -> str:
    """
    Compute a deterministic hash of the configuration.

    Args:
        cfg_dict: Configuration dictionary

    Returns:
        8-character hex hash
    """
    # Sort keys and use consistent separators for deterministic hashing
    config_str = json.dumps(cfg_dict, separators=(",", ":"), sort_keys=True)
    hash_obj = hashlib.sha1(config_str.encode())
    return hash_obj.hexdigest()[:8]


def stable_group_id(member_ids: List[str], cfg_dict: Dict, n: int = 10) -> str:
    """
    Generate a stable, deterministic group ID.

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


def _compute_config_hash(config_dict: Dict[str, Any]) -> str:
    """
    Compute a deterministic hash of the configuration.

    Args:
        config_dict: Configuration dictionary

    Returns:
        8-character hex hash
    """
    # Sort keys and use consistent separators for deterministic hashing
    config_str = json.dumps(config_dict, separators=(",", ":"), sort_keys=True)
    hash_obj = hashlib.sha1(config_str.encode())
    return hash_obj.hexdigest()[:8]
