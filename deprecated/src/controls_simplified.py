"""Simplified controls component demonstrating the new unified state management.

This is a demonstration of how the controls component would look with the
simplified session state management approach.
"""

from typing import Any

import streamlit as st

from src.utils.cache_keys import build_cache_key
from src.utils.simple_state import (
    get_app_state,
    get_backend_for_run,
    reset_page_to_one,
    set_app_state,
    set_backend_for_run,
    update_page_size,
)


def render_controls_simplified(
    selected_run_id: str,
    settings: dict[str, Any],
    filters: dict[str, Any],
) -> tuple[dict[str, Any], str, int, int, int]:
    """Render pagination and filter controls using simplified state management.

    Args:
        selected_run_id: The selected run ID
        settings: Application settings
        filters: Current filters dictionary

    Returns:
        Tuple of (updated_filters, sort_by, page, page_size, similarity_threshold)

    """
    # Get unified application state
    app_state = get_app_state(st.session_state)

    # Force DuckDB backend when flag is enabled
    prefer_duck = (
        settings.get("ui_perf", {})
        .get("groups", {})
        .get("duckdb_prefer_over_pyarrow", False)
    )
    if prefer_duck:
        set_backend_for_run(app_state, selected_run_id, "duckdb")

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
        app_state.previous_sort_key = sort_by
        reset_page_to_one(app_state)  # Reset to page 1 when sort changes
        st.rerun()

    # Similarity threshold controls
    st.sidebar.write("**Similarity Threshold**")

    # Get default threshold from settings
    default_threshold = settings.get("ui", {}).get("similarity_threshold_default", 100)

    # Use app state for threshold persistence
    similarity_key = f"similarity_threshold_{selected_run_id}"
    current_threshold = app_state.similarity_threshold

    # Threshold slider
    threshold = st.sidebar.slider(
        "Min Edge Strength",
        min_value=0.0,
        max_value=100.0,
        value=float(current_threshold),
        step=5.0,
        format="%.0f%%",
        help="Minimum edge strength for displaying groups. Higher values show only stronger matches.",
    )

    # Check if threshold changed
    if threshold != current_threshold:
        app_state.similarity_threshold = threshold
        st.session_state[similarity_key] = threshold

        # Add threshold to filters for export parity
        filters["similarity_threshold"] = threshold

        # Reset to page 1 when threshold changes
        reset_page_to_one(app_state)

        # Force refresh to apply new threshold
        st.rerun()

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

    # Update page size if changed
    if page_size != app_state.page_size:
        update_page_size(app_state, page_size)

    # Check if filters changed to reset page
    backend = get_backend_for_run(app_state, selected_run_id)
    current_filters_key = build_cache_key(
        selected_run_id,
        sort_by,
        1,
        page_size,
        filters,
        backend,
    )

    if app_state.filter_signature != current_filters_key:
        reset_page_to_one(app_state)
        app_state.filter_signature = current_filters_key

    # Save state
    set_app_state(st.session_state, app_state)

    # Get similarity threshold for return
    similarity_threshold = filters.get("similarity_threshold", int(default_threshold))

    return filters, sort_by, app_state.page_number, page_size, similarity_threshold


# Comparison: Before vs After
"""
BEFORE (Current approach):
- 4 separate state objects (page_state, filters_state, backend_state, etc.)
- Multiple imports from state_utils
- Scattered state updates
- Complex state management logic

AFTER (Simplified approach):
- Single app_state object
- Single import from simple_state
- Centralized state updates
- Cleaner, more readable code
- Convenience functions for common operations
"""
