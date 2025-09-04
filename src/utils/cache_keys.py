"""
Cache key management for ui_helpers refactor.

This module provides centralized cache key generation with versioning.
"""

import os
import hashlib
from typing import Any, Dict, Optional
from dataclasses import dataclass
from enum import Enum
from src.utils.artifact_management import get_artifact_paths
from src.utils.logging_utils import get_logger

logger = get_logger(__name__)

class CacheKeyVersion(Enum):
    """Cache key version enumeration."""
    V1 = "CJCK1"  # Initial version
    # V2 = "CJCK2"  # Future: document changes that require version bump

@dataclass(frozen=True)
class CacheKey:
    """Cache key specification."""
    version: CacheKeyVersion
    components: tuple[Any, ...]
    
    def compute(self) -> str:
        """Generate stable hash from components."""
        # TODO: Implement actual hash computation
        pass
    
    @classmethod
    def validate(cls, key: str) -> Optional[str]:
        """Returns warning if key version mismatch."""
        # TODO: Implement actual validation
        pass

def fingerprint(path: str) -> str:
    """Stable mtime+size fingerprint."""
    # TODO: Implement actual fingerprint logic
    pass

def build_cache_key(
    run_id: str,
    sort_key: str,
    page: int,
    page_size: int,
    filters: Dict[str, Any],
    backend: str = "pyarrow",
    source: str = "review_ready",
) -> str:
    """
    Build a cache key for groups page data.

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

        if parquet_path and os.path.exists(parquet_path):
            stat = os.stat(parquet_path)
            parquet_fingerprint = f"{int(stat.st_mtime)}_{stat.st_size}"
        else:
            parquet_fingerprint = "missing"
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
        f'Cache key generated | run_id={run_id} key={cache_key[:8]}... source={source} backend={backend} fingerprint={parquet_fingerprint} page={page} size={page_size} sort="{sort_key}"'
    )

    return cache_key


def build_details_cache_key(run_id: str, group_id: str, backend: str = "duckdb") -> str:
    """
    Build a cache key for group details data.

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
        stat = os.stat(parquet_path)
        parquet_fingerprint = f"{int(stat.st_mtime)}_{stat.st_size}"
    except Exception:
        parquet_fingerprint = "unknown"

    # Build cache key components
    key_components = [run_id, group_id, parquet_fingerprint, backend]

    cache_key = hashlib.md5("|".join(key_components).encode()).hexdigest()
    return cache_key
