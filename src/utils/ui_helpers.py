"""
UI helper functions for Phase 1.17.1.

This module provides pure functions for run loading, stage status parsing,
and artifact path management for the Streamlit UI.

DEPRECATION NOTICE: This module is being decomposed into smaller, focused modules.
Use the new modules directly for better maintainability.
"""

import os
import warnings
from typing import Any, Dict, List, Optional, Tuple

# Default: PendingDeprecationWarning to nudge devs
if not os.getenv("CJ_UI_HELPERS_NO_WARN"):
    warnings.warn(
        "ui_helpers is pending deprecation; use new modules directly",
        PendingDeprecationWarning
    )

# Strong deprecation behind flag
if os.getenv("CJ_UI_HELPERS_DEPRECATE"):
    warnings.warn(
        "ui_helpers is deprecated; use new modules",
        DeprecationWarning
    )

# TODO: Re-export functions from new modules as they are implemented
# from .artifact_management import get_artifact_paths
# from .run_management import list_runs, get_run_metadata
# from .filtering import apply_filters_pyarrow, apply_filters_duckdb
# from .cache_keys import build_cache_key, build_details_cache_key
# from .group_stats import compute_group_stats_duckdb
# from .group_pagination import get_groups_page, get_groups_page_pyarrow
# from .group_pagination import get_groups_page_duckdb, get_groups_page_from_stats_duckdb

# TODO: Update __all__ as functions are moved
__all__ = [
    # "get_artifact_paths",
    # "list_runs",
    # "get_run_metadata",
    # "apply_filters_pyarrow",
    # "apply_filters_duckdb",
    # "build_cache_key",
    # "build_details_cache_key",
    # "compute_group_stats_duckdb",
    # "get_groups_page",
    # "get_groups_page_pyarrow",
    # "get_groups_page_duckdb",
    # "get_groups_page_from_stats_duckdb",
]

# TODO: Remove this placeholder when functions are moved
def _placeholder_function():
    """Placeholder function - will be removed when actual functions are moved."""
    raise NotImplementedError(
        "This function has not been moved to the new module structure yet. "
        "Check the refactor plan for current status."
    )

# TODO: Replace these with actual re-exports
get_artifact_paths = _placeholder_function
list_runs = _placeholder_function
get_run_metadata = _placeholder_function
apply_filters_pyarrow = _placeholder_function
apply_filters_duckdb = _placeholder_function
build_cache_key = _placeholder_function
build_details_cache_key = _placeholder_function
compute_group_stats_duckdb = _placeholder_function
get_groups_page = _placeholder_function
get_groups_page_pyarrow = _placeholder_function
get_groups_page_duckdb = _placeholder_function
get_groups_page_from_stats_duckdb = _placeholder_function
