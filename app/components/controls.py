"""Controls component for Phase 1.18.1 refactor.

This module handles pagination controls, sorting, and filter controls.
"""

from typing import Any

import streamlit as st

from src.utils.cache_keys import build_cache_key
from src.utils.sort_utils import validate_sort_key
from src.utils.state_utils import (
    get_backend_state,
    get_filters_state,
    get_page_state,
    set_backend_state,
    set_filters_state,
    set_page_state,
)


def render_controls(
    selected_run_id: str,
    settings: dict[str, Any],
    filters: dict[str, Any],
) -> tuple[dict[str, Any], str, int, int, int]:
    """Render pagination and filter controls.

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
    prefer_duck = (
        settings.get("ui_perf", {})
        .get("groups", {})
        .get("duckdb_prefer_over_pyarrow", False)
    )
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
            "Similarity Score (Desc)",
            "Similarity Score (Asc)",
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

    # Phase 1.35.2: Similarity Threshold Slider (improved from simplified version)
    if settings.get("ui", {}).get("similarity_slider", {}).get("enable", True):
        st.sidebar.write("**Similarity Threshold**")

        # Get current similarity threshold from session state
        similarity_key = f"similarity_threshold_{selected_run_id}"
        default_threshold = settings.get("ui", {}).get(
            "similarity_threshold_default",
            100,
        )  # Start with 100% (exact duplicates)
        current_threshold = st.session_state.get(similarity_key, default_threshold)

        # Use slider with clear similarity terminology
        threshold = st.sidebar.slider(
            "Minimum Similarity Score",
            min_value=0.0,
            max_value=100.0,
            value=float(current_threshold),
            step=5.0,
            format="%.0f%%",
            help="Show only groups with similarity scores above this threshold. 100% = exact duplicates, 0% = completely different names.",
            key=f"similarity_slider_{selected_run_id}",
        )

        # Show current filter value directly under slider
        st.sidebar.caption(f"Current Similarity Filter: {int(threshold)}%")

        # Check if threshold changed and update state
        if threshold != current_threshold:
            st.session_state[similarity_key] = threshold
            # Reset to page 1 when threshold changes
            page_state.number = 1
            set_page_state(st.session_state, page_state)
            st.rerun()

        # Add threshold to filters for export parity
        filters["similarity_threshold"] = threshold
        # Normalize to min_edge_strength for backend compatibility
        filters["min_edge_strength"] = float(threshold)

        # Update the Min Edge Strength input to match the similarity threshold
        st.session_state[f"min_edge_strength_{selected_run_id}"] = float(threshold)

    # Pagination controls
    page_size_options = settings.get("ui", {}).get(
        "page_size_options",
        [50, 100, 200, 500],
    )
    default_page_size = settings.get("ui", {}).get("page_size_default", 50)

    page_size = st.sidebar.selectbox(
        "Page Size",
        page_size_options,
        index=page_size_options.index(default_page_size),
    )

    # Update page state
    page_state.size = page_size
    set_page_state(st.session_state, page_state)

    # Check if filters changed to reset page
    backend = backend_state.groups.get(selected_run_id, "pyarrow")
    current_filters_key = build_cache_key(
        selected_run_id,
        sort_by,
        1,
        page_size,
        filters,
        backend,
    )

    if filters_state.signature != current_filters_key:
        page_state.number = 1
        set_page_state(st.session_state, page_state)
        filters_state.signature = current_filters_key
        set_filters_state(st.session_state, filters_state)

    # Get similarity threshold for return
    similarity_threshold = filters.get("similarity_threshold", int(default_threshold))

    return filters, sort_by, page_state.number, page_size, similarity_threshold
