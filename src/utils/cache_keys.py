"""
Cache key management for ui_helpers refactor.

This module provides centralized cache key generation with versioning.
"""

import hashlib
from typing import Any, Dict, Optional
from dataclasses import dataclass
from enum import Enum

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

# TODO: Move build_cache_key functions here
def build_cache_key(
    run_id: str,
    sort_key: str,
    page: int,
    page_size: int,
    filters: Dict[str, Any],
    backend: str = "pyarrow",
    source: str = "review_ready",
) -> str:
    """Build a cache key for groups page data."""
    # TODO: Implement actual logic
    pass

def build_details_cache_key(run_id: str, group_id: str, backend: str = "duckdb") -> str:
    """Build a cache key for group details data."""
    # TODO: Implement actual logic
    pass
