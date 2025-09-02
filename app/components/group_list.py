"""
Group list component for Phase 1.18.1 refactor.

This module handles the paginated display of groups with navigation controls.
"""

import streamlit as st
from typing import Any, Dict, List, Tuple

from src.utils.state_utils import get_page_state, set_page_state, get_backend_state
from src.utils.fragment_utils import fragment
from src.utils.ui_helpers import (
    get_groups_page,
    get_total_groups_count,
    build_cache_key,
    PageFetchTimeout,
)


def render_group_list(
    selected_run_id: str,
    sort_by: str,
    page: int,
    page_size: int,
    filters: Dict[str, Any],
) -> Tuple[List[Dict[str, Any]], int, int]:
    """
    Render the paginated group list with fragments.

    Args:
        selected_run_id: The selected run ID
        sort_by: The sort key
        page: The current page number
        page_size: The page size
        filters: The filters dictionary

    Returns:
        Tuple of (page_groups, total_groups, max_page)
    """
    # Get backend state
    backend_state = get_backend_state(st.session_state)
    # backend = backend_state.groups.get(selected_run_id, "pyarrow")  # Not used in current implementation

    # Get total groups count
    total_groups = get_total_groups_count(selected_run_id, filters)
    max_page = max(1, (total_groups + page_size - 1) // page_size)

    # Ensure page is within bounds
    page_state = get_page_state(st.session_state)
    if page_state.number > max_page:
        page_state.number = max_page
        set_page_state(st.session_state, page_state)
    elif page_state.number < 1:
        page_state.number = 1
        set_page_state(st.session_state, page_state)

    # Build cache key for this specific page request (for logging/debugging)
    _ = build_cache_key(
        selected_run_id,
        sort_by,
        page_state.number,
        page_size,
        filters,
        backend_state.groups.get(selected_run_id, "pyarrow"),
    )

    try:
        page_groups, actual_total = get_groups_page(
            selected_run_id, sort_by, page_state.number, page_size, filters
        )

        # Update total if different (should be the same, but handle edge cases)
        if actual_total != total_groups:
            total_groups = actual_total
            max_page = max(1, (total_groups + page_size - 1) // page_size)

    except PageFetchTimeout:
        # Handle timeout specifically
        st.error("⚠️ Page load timed out after 30 seconds. The dataset is very large.")

        # Offer user options
        col1, col2 = st.columns(2)
        with col1:
            if st.button("🔄 Try Again", key=f"try_again_{selected_run_id}"):
                st.rerun()
        with col2:
            if st.button(
                "📉 Reduce Page Size to 50", key=f"reduce_page_size_{selected_run_id}"
            ):
                page_state.size = 50
                set_page_state(st.session_state, page_state)
                st.rerun()

        st.info(
            "💡 **Tip:** For large datasets, try reducing the page size or applying filters to reduce the data load."
        )
        return [], 0, 1

    except Exception as e:
        # Fallback to current behavior if pagination fails
        st.warning(f"Pagination failed, falling back to current behavior: {e}")
        st.error(
            f'Groups pagination fallback | run_id={selected_run_id} error="{str(e)}"'
        )
        return [], 0, 1

    # Page navigation
    col1, col2, col3 = st.columns(3)
    with col1:
        if (
            st.button("Prev", key=f"prev_page_{selected_run_id}")
            and page_state.number > 1
        ):
            page_state.number -= 1
            set_page_state(st.session_state, page_state)
    with col2:
        st.write(f"Page {page_state.number} / {max_page}")
    with col3:
        if (
            st.button("Next", key=f"next_page_{selected_run_id}")
            and page_state.number < max_page
        ):
            page_state.number += 1
            set_page_state(st.session_state, page_state)

    # Show pagination caption
    start_idx = (page_state.number - 1) * page_size
    end_idx = min(start_idx + page_size, total_groups)
    st.caption(f"Showing {start_idx + 1}–{end_idx} of {total_groups} groups")

    # Phase 1.26.2: Cache management moved to maintenance sidebar for better organization
    # Cache clearing is now handled in the maintenance component with helpful tooltips

    # Phase 1.22.1: Show performance indicator when using fast path
    try:
        from src.utils.ui_helpers import get_artifact_paths
        import os

        artifact_paths = get_artifact_paths(selected_run_id)
        group_stats_path = artifact_paths.get("group_stats_parquet")

        if group_stats_path and os.path.exists(group_stats_path):
            st.success(
                "⚡ **Fast stats mode**: Using pre-computed group statistics for instant loading"
            )
        else:
            st.info("📊 **Standard mode**: Computing group statistics on-demand")
    except Exception:
        pass  # Don't break the UI if this fails

    return page_groups, total_groups, max_page


@fragment
def render_group_list_fragment(
    selected_run_id: str,
    sort_by: str,
    page: int,
    page_size: int,
    filters: Dict[str, Any],
) -> None:
    """
    Render the group list within a fragment to prevent page-wide blocking.

    Args:
        selected_run_id: The selected run ID
        sort_by: The sort key
        page: The current page number
        page_size: The page size
        filters: The filters dictionary
    """
    # Duplicate render guard for the list
    key = f"group_list_rendered:{selected_run_id}:{page}:{sort_by}"
    if not st.session_state.get(key):
        st.session_state[key] = True

    # Wrap groups list in fragment to prevent page-wide blocking
    page_groups, total_groups, max_page = render_group_list(
        selected_run_id, sort_by, page, page_size, filters
    )

    # Group by group_id and display each group
    for group_info in page_groups:
        group_id = group_info["group_id"]
        group_size = group_info["group_size"]
        primary_name = group_info["primary_name"]

        # Extract additional fields for better decision making
        max_score = group_info.get("max_score", 0.0)
        disposition = group_info.get("disposition", "Unknown")

        # Create a more informative expander title with key fields
        expander_title = f"Group {group_id}: {primary_name} ({group_size} records)"
        if max_score > 0:
            expander_title += f" | Score: {max_score:.3f}"
        if disposition and disposition != "Unknown":
            expander_title += f" | {disposition}"

        with st.expander(expander_title):
            # Display key group information for quick review
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Group Size", group_size)
            with col2:
                if max_score > 0:
                    st.metric("Max Score", f"{max_score:.3f}")
                else:
                    st.metric("Max Score", "N/A")
            with col3:
                if disposition and disposition != "Unknown":
                    # Color-code dispositions for quick visual identification
                    if disposition == "Keep":
                        st.success(f"✅ {disposition}")
                    elif disposition == "Update":
                        st.warning(f"⚠️ {disposition}")
                    elif disposition == "Delete":
                        st.error(f"🗑️ {disposition}")
                    elif disposition == "Verify":
                        st.info(f"🔍 {disposition}")
                    else:
                        st.write(f"📋 {disposition}")
                else:
                    st.write("📋 No disposition")

            # This will be handled by the group_details component
            st.write("Group details will be loaded here...")
