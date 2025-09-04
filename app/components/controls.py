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
) -> tuple[Dict[str, Any], str, int, int, int]:
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
    prefer_duck = settings.get("ui_perf", {}).get("groups", {}).get("duckdb_prefer_over_pyarrow", False)
    if prefer_duck:
        backend_state.groups[selected_run_id] = "duckdb"
        set_backend_state(st.session_state, backend_state)

    # Sorting controls (preserve existing labels and behavior)
    st.sidebar.write("**Sorting**")

    # Store previous sort key to detect changes
    sort_key_key = f"previous_sort_{selected_run_id}"
    previous_sort = st.session_state.get(sort_key_key, "Group Size (Desc)")

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

    # Check if sort order changed and force refresh
    if sort_by != previous_sort:
        st.session_state[sort_key_key] = sort_by
        # Reset to page 1 when sort order changes
        page_state.number = 1
        set_page_state(st.session_state, page_state)
        # Force a rerun to apply the new sorting
        st.rerun()

    # Validate sort key
    if not validate_sort_key(sort_by):
        sort_by = "Group Size (Desc)"

    # Phase 1.35.2: Similarity Threshold Stepper
    if settings.get("ui", {}).get("similarity_slider", {}).get("enable", True):
        st.sidebar.write("**Similarity Threshold**")
        
        # Get current similarity threshold from session state
        similarity_key = f"similarity_threshold_{selected_run_id}"
        default_threshold = settings.get("ui", {}).get("similarity_slider", {}).get("default_bucket", "100")
        current_threshold = st.session_state.get(similarity_key, int(default_threshold))
        
        # Create stepper control (plus/minus buttons)
        col1, col2, col3 = st.sidebar.columns([1, 2, 1])
        
        with col1:
            if st.button("-", key=f"decrease_{selected_run_id}"):
                min_threshold = settings.get("ui", {}).get("similarity_slider", {}).get("min", 90)
                if current_threshold > min_threshold:
                    current_threshold -= 1
                    st.session_state[similarity_key] = current_threshold
                    st.rerun()
        
        with col2:
            st.write(f"**{current_threshold}%**")
            st.caption("Edge Strength")
        
        with col3:
            if st.button("+", key=f"increase_{selected_run_id}"):
                max_threshold = settings.get("ui", {}).get("similarity_slider", {}).get("max", 100)
                if current_threshold < max_threshold:
                    current_threshold += 1
                    st.session_state[similarity_key] = current_threshold
                    st.rerun()
        
        # Store current threshold in session state
        st.session_state[similarity_key] = current_threshold
        
        # Add threshold to filters for export parity
        filters["similarity_threshold"] = current_threshold

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

    # Get similarity threshold for return
    similarity_threshold = filters.get("similarity_threshold", int(default_threshold))
    
    return filters, sort_by, page_state.number, page_size, similarity_threshold
