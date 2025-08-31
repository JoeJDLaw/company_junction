"""
Controls component for Phase 1.18.1 refactor.

This module handles pagination controls, sorting, and filter controls.
"""

import streamlit as st
from typing import Any, Dict

from src.utils.state_utils import (
    get_page_state,
    set_page_state,
    get_filters_state,
    set_filters_state,
    get_backend_state,
    set_backend_state,
)
from src.utils.sort_utils import validate_sort_key
from src.utils.ui_helpers import build_cache_key


def render_controls(
    selected_run_id: str, settings: Dict[str, Any], filters: Dict[str, Any]
) -> tuple[Dict[str, Any], str, int, int]:
    """
    Render pagination and filter controls.

    Args:
        selected_run_id: The selected run ID
        settings: Application settings
        filters: Current filters dictionary

    Returns:
        Tuple of (updated_filters, sort_by, page, page_size)
    """
    # Get current state
    page_state = get_page_state(st.session_state)
    filters_state = get_filters_state(st.session_state)
    backend_state = get_backend_state(st.session_state)

    # Force DuckDB backend when flag is enabled
    use_duckdb = settings.get("ui", {}).get("use_duckdb_for_groups", False)
    if use_duckdb:
        backend_state.groups[selected_run_id] = "duckdb"
        set_backend_state(st.session_state, backend_state)

    # Sorting controls (preserve existing labels and behavior)
    st.sidebar.write("**Sorting**")
    sort_by = st.sidebar.selectbox(
        "Sort Groups By",
        [
            "Group Size (Desc)",
            "Group Size (Asc)",
            "Max Score (Desc)",
            "Max Score (Asc)",
            "Account Name (Asc)",
            "Account Name (Desc)",
        ],
        index=0,
    )

    # Validate sort key
    if not validate_sort_key(sort_by):
        sort_by = "Group Size (Desc)"

    # Pagination controls
    page_size_options = settings.get("ui", {}).get(
        "page_size_options", [50, 100, 200, 500]
    )
    default_page_size = settings.get("ui", {}).get("page_size_default", 50)

    page_size = st.sidebar.selectbox(
        "Page Size", page_size_options, index=page_size_options.index(default_page_size)
    )

    # Update page state
    page_state.size = page_size
    set_page_state(st.session_state, page_state)

    # Check if filters changed to reset page
    backend = backend_state.groups.get(selected_run_id, "pyarrow")
    current_filters_key = build_cache_key(
        selected_run_id, sort_by, 1, page_size, filters, backend
    )

    if filters_state.signature != current_filters_key:
        page_state.number = 1
        set_page_state(st.session_state, page_state)
        filters_state.signature = current_filters_key
        set_filters_state(st.session_state, filters_state)

    return filters, sort_by, page_state.number, page_size
