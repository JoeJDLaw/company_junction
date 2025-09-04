"""
Settings management for ui_helpers refactor.

This module provides centralized access to application settings
with caching and validation.
"""

from functools import lru_cache
from typing import Dict, Any, List

# TODO: Implement get_settings with LRU caching
@lru_cache(maxsize=1)
def get_settings() -> Dict[str, Any]:
    """Get application settings with caching."""
    # TODO: Implement actual settings loading from config files
    # For now, return default settings that match legacy behavior
    
    return {
        "ui": {
            "timeout_seconds": 30,
            "duckdb_threads": 4,
            "use_duckdb_for_groups": False,
            "max_pyarrow_group_stats_seconds": 5
        },
        "ui_perf": {
            "groups": {
                "use_stats_parquet": True,
                "duckdb_prefer_over_pyarrow": False,
                "rows_duckdb_threshold": 30000
            },
            "details": {
                "use_details_parquet": True,
                "allow_pyarrow_fallback": False
            }
        }
    }

def get_ui_perf() -> Dict[str, Any]:
    """Helper to get ui.perf section with defaults."""
    # TODO: Implement actual ui.perf logic
    pass

def validate_settings() -> List[str]:
    """Returns list of validation warnings."""
    # TODO: Implement actual validation
    pass
