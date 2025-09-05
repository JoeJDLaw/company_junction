"""
Settings management for ui_helpers refactor.

This module provides centralized access to application settings
with caching and validation.
"""

import os
from functools import lru_cache
from typing import Dict, Any, List

__all__ = ["get_settings", "get_ui_perf", "validate_settings"]

# TODO: Implement get_settings with LRU caching
@lru_cache(maxsize=1)
def get_settings() -> Dict[str, Any]:
    """Get application settings with caching."""
    # TODO: Implement actual settings loading from config files
    # For now, return default settings that match legacy behavior
    
    # Check environment variables for force flags
    force_pyarrow = os.environ.get("CJ_FORCE_PYARROW", "0") == "1"
    force_duckdb = os.environ.get("CJ_FORCE_DUCKDB", "0") == "1"
    
    return {
        "ui": {
            "timeout_seconds": 30,
            "duckdb_threads": 4,
            "use_duckdb_for_groups": False,
            "max_pyarrow_group_stats_seconds": 5,
            "max_page_size": 250,  # Safer default for production
            "admin_mode": True,  # Enable admin features like run deletion
            "enable_run_deletion": True,  # Enable run deletion functionality
                            "similarity_threshold_default": 100  # Default threshold (100 = exact duplicates only)
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
            },
            "force_pyarrow": force_pyarrow,
            "force_duckdb": force_duckdb
        }
    }

def get_ui_perf(settings: Dict[str, Any] | None = None) -> Dict[str, Any]:
    """Returns ui_perf settings with defaults and force flag precedence.
    
    Precedence order: config > env > default
    
    Args:
        settings: Settings dict to use. If None, uses get_settings().
        
    Returns:
        Dict with ui_perf configuration including force flags.
    """
    if settings is None:
        settings = get_settings()
    
    ui_perf = settings.get("ui_perf", {})
    
    # Back-compat env vars (for emergency overrides)
    env_force_pyarrow = os.environ.get("CJ_FORCE_PYARROW", "0") == "1"
    env_force_duckdb = os.environ.get("CJ_FORCE_DUCKDB", "0") == "1"
    
    # Precedence: config > env > default
    cfg = {
        "force_pyarrow": ui_perf.get("force_pyarrow", env_force_pyarrow),
        "force_duckdb": ui_perf.get("force_duckdb", env_force_duckdb),
        "groups": {
            **{"use_stats_parquet": True, "duckdb_prefer_over_pyarrow": False}, 
            **ui_perf.get("groups", {})
        },
        "details": {
            **{"use_details_parquet": True, "allow_pyarrow_fallback": False}, 
            **ui_perf.get("details", {})
        },
    }
    
    return cfg

def validate_settings(settings: Dict[str, Any] | None = None) -> List[str]:
    """Returns list of validation warnings.
    
    Args:
        settings: Settings dict to validate. If None, uses get_settings().
        
    Returns:
        List of validation warning messages.
    """
    warnings = []
    if settings is None:
        settings = get_settings()
    ui = settings.get("ui", {})
    
    # Validate page size limits
    max_page_size = ui.get("max_page_size", 250)
    if not isinstance(max_page_size, int) or max_page_size < 1 or max_page_size > 10000:
        warnings.append(f"ui.max_page_size must be int 1-10000, got {max_page_size}")
    
    # Validate thread count
    duckdb_threads = ui.get("duckdb_threads", 4)
    if not isinstance(duckdb_threads, int) or duckdb_threads < 1 or duckdb_threads > 32:
        warnings.append(f"ui.duckdb_threads must be int 1-32, got {duckdb_threads}")
    
    # Validate timeouts
    timeout_seconds = ui.get("timeout_seconds", 30)
    if not isinstance(timeout_seconds, (int, float)) or timeout_seconds < 1 or timeout_seconds > 300:
        warnings.append(f"ui.timeout_seconds must be number 1-300, got {timeout_seconds}")
    
    max_pyarrow_seconds = ui.get("max_pyarrow_groups_seconds", 5)
    if not isinstance(max_pyarrow_seconds, (int, float)) or max_pyarrow_seconds < 1 or max_pyarrow_seconds > 60:
        warnings.append(f"ui.max_pyarrow_groups_seconds must be number 1-60, got {max_pyarrow_seconds}")
    
    return warnings
