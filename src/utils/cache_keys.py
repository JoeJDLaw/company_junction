"""Cache key management for ui_helpers refactor.

This module provides centralized cache key generation with versioning.
"""

import hashlib
import os
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Optional

from src.utils.artifact_management import get_artifact_paths
from src.utils.logging_utils import get_logger

logger = get_logger(__name__)


class CacheKeyVersion(Enum):
    """Cache key version enumeration."""

    V1 = "CJCK1"  # Initial version
    V2 = "CJCK2"  # Future: document changes that require version bump


@dataclass(frozen=True)
class CacheKey:
    """Cache key specification."""

    version: CacheKeyVersion
    components: tuple[Any, ...]

    def compute(self) -> str:
        """Compute cache key from components.

        Returns:
            Versioned cache key string with CJCK prefix

        """
        import json

        # Serialize components to stable string representation
        def serialize_component(comp: Any) -> str:
            if isinstance(comp, dict):
                return json.dumps(comp, sort_keys=True)
            if isinstance(comp, tuple):
                return json.dumps(list(comp), sort_keys=True)
            return str(comp)

        # Create stable string representation
        components_str = "|".join(serialize_component(comp) for comp in self.components)

        # Hash with SHA-256 and prefix with version
        import hashlib

        hash_value = hashlib.sha256(components_str.encode()).hexdigest()

        return f"{self.version.value}:{hash_value}"

    @classmethod
    def validate(cls, key: str) -> Optional[str]:
        """Validate cache key version.

        Args:
            key: Cache key string to validate

        Returns:
            Warning message if validation fails, None if valid

        """
        if not key:
            return "Cache key is empty"

        # Check if key starts with known version token
        for version in CacheKeyVersion:
            if key.startswith(f"{version.value}:"):
                return None  # Valid version

        # Unknown or missing version token
        return f"Unknown cache key version or missing version token: {key[:20]}..."


def fingerprint(path: Optional[str]) -> str:
    """Generate fingerprint for a file path.

    Args:
        path: File path to fingerprint

    Returns:
        Fingerprint string: "mtime_size" or "missing"/"unknown"

    """
    if not path or path is None:
        return "missing"

    # Check if file exists first (matching legacy behavior)
    if not os.path.exists(path):
        return "missing"

    try:
        stat = os.stat(path)
        return f"{int(stat.st_mtime)}_{stat.st_size}"
    except OSError:
        return "unknown"


def build_cache_key(
    run_id: str,
    sort_key: str,
    page: int,
    page_size: int,
    filters: Dict[str, Any],
    backend: str = "pyarrow",
    source: str = "review_ready",
) -> str:
    """Build a cache key for groups page data.

    Args:
        run_id: The run ID
        sort_key: The sort key
        page: The page number
        page_size: The page size
        filters: The filters dictionary
        backend: The backend used ("pyarrow" or "duckdb")
        source: The data source ("stats" or "review_ready")

    Returns:
        A string cache key

    """
    # Get parquet fingerprint based on source
    try:
        artifact_paths = get_artifact_paths(run_id)
        if source == "stats":
            parquet_path = artifact_paths.get("group_stats_parquet")
        else:
            parquet_path = artifact_paths["review_ready_parquet"]

        # Use fingerprint function internally while keeping output identical
        parquet_fingerprint = (
            fingerprint(parquet_path) if parquet_path is not None else "unknown"
        )
    except Exception:
        parquet_fingerprint = "unknown"

    # Create filters signature
    filters_signature = hashlib.md5(str(sorted(filters.items())).encode()).hexdigest()[
        :8
    ]

    # Build cache key components including source and backend
    key_components = [
        run_id,
        source,
        backend,
        parquet_fingerprint,
        sort_key,
        str(page),
        str(page_size),
        filters_signature,
    ]

    cache_key = hashlib.md5("|".join(key_components).encode()).hexdigest()
    logger.info(
        f'Cache key generated | run_id={run_id} key={cache_key[:8]}... source={source} backend={backend} fingerprint={parquet_fingerprint} page={page} size={page_size} sort="{sort_key}"',
    )

    return cache_key


def build_details_cache_key(run_id: str, group_id: str, backend: str = "duckdb") -> str:
    """Build a cache key for group details data.

    Args:
        run_id: The run ID
        group_id: The group ID
        backend: The backend used ("pyarrow" or "duckdb")

    Returns:
        A string cache key

    """
    # Get parquet fingerprint
    try:
        artifact_paths = get_artifact_paths(run_id)
        parquet_path = artifact_paths["review_ready_parquet"]
        parquet_fingerprint = fingerprint(parquet_path)
    except Exception:
        parquet_fingerprint = "unknown"

    # Build cache key components
    key_components = [run_id, group_id, parquet_fingerprint, backend]

    cache_key = hashlib.md5("|".join(key_components).encode()).hexdigest()
    return cache_key
