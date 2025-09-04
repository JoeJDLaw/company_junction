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

# DEPRECATION WARNING: This module will be removed in the next release
if not os.getenv("CJ_UI_HELPERS_NO_WARN"):
    warnings.warn(
        "src.utils.ui_helpers is deprecated. Import from:\n"
        "  - src.utils.group_pagination (get_groups_page, get_total_groups_count)\n"
        "  - src.utils.group_details (get_group_details)\n"
        "  - src.utils.filtering (get_order_by, build_sort_expression, apply_filters_*)\n"
        "  - src.utils.artifact_management (get_artifact_paths)\n"
        "  - src.utils.ui_session (session, get_backend_choice, set_backend_choice)\n"
        "  - src.utils.cache_keys (build_cache_key, build_details_cache_key)\n"
        "This module will be removed in the next release.",
        DeprecationWarning,
        stacklevel=2,
    )

# Track deprecated imports for monitoring
try:
    from .metrics import record_backend_choice
    record_backend_choice("deprecated_import", "ui_helpers")
except ImportError:
    # Metrics not available, continue silently
    pass

# Strong deprecation behind flag
if os.getenv("CJ_UI_HELPERS_DEPRECATE"):
    warnings.warn(
        "ui_helpers is deprecated; use new modules",
        DeprecationWarning
    )

# Re-export functions from new modules as they are implemented
from .artifact_management import get_artifact_paths
from .run_management import list_runs, get_run_metadata, validate_run_artifacts, get_default_run_id, format_run_display_name, load_stage_state
from .filtering import apply_filters_pyarrow, apply_filters_duckdb, get_order_by, build_sort_expression
from .cache_keys import build_cache_key, build_details_cache_key
from .group_stats import compute_group_stats_duckdb
from .group_pagination import get_groups_page, get_groups_page_pyarrow
from .group_pagination import get_groups_page_duckdb, get_groups_page_from_stats_duckdb
from .group_details import get_group_details

# Update __all__ as functions are moved
__all__ = [
    "get_artifact_paths",
    "list_runs",
    "get_run_metadata",
    "validate_run_artifacts",
    "get_default_run_id",
    "format_run_display_name",
    "load_stage_state",
    "apply_filters_pyarrow",
    "apply_filters_duckdb",
    "get_order_by",
    "build_sort_expression",
    "build_cache_key",
    "build_details_cache_key",
    "compute_group_stats_duckdb",
    "get_groups_page",
    "get_groups_page_pyarrow",
    "get_groups_page_duckdb",
    "get_groups_page_from_stats_duckdb",
    "get_group_details",
]

# TODO: Remove this placeholder when functions are moved
def _placeholder_function():
    """Placeholder function - will be removed when actual functions are moved."""
    raise NotImplementedError(
        "This function has not been moved to the new module structure yet. "
        "Check the refactor plan for current status."
    )

# Replace these with actual re-exports
# get_artifact_paths is now imported from artifact_management
# list_runs, get_run_metadata, validate_run_artifacts, get_default_run_id, format_run_display_name, load_stage_state are now imported from run_management
# apply_filters_pyarrow, apply_filters_duckdb, get_order_by, build_sort_expression are now imported from filtering
# build_cache_key, build_details_cache_key are now imported from cache_keys
# compute_group_stats_duckdb is now imported from group_stats
# get_groups_page, get_groups_page_pyarrow, get_groups_page_duckdb, get_groups_page_from_stats_duckdb are now imported from group_pagination
