"""Group list component for Phase 1.18.1 refactor.

This module handles the paginated display of groups with navigation controls.
"""

from typing import Any

import streamlit as st

from src.utils.cache_keys import build_cache_key
from src.utils.fragment_utils import fragment
from src.utils.group_pagination import (
    PageFetchTimeout,
    get_groups_page,
    get_total_groups_count,
)
from src.utils.state_utils import get_backend_state, get_page_state, set_page_state


def render_group_list(
    selected_run_id: str,
    sort_by: str,
    page: int,
    page_size: int,
    filters: dict[str, Any],
) -> tuple[list[dict[str, Any]], int, int]:
    """Render the paginated group list with fragments.

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
            selected_run_id,
            sort_by,
            page_state.number,
            page_size,
            filters,
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
                "📉 Reduce Page Size to 50",
                key=f"reduce_page_size_{selected_run_id}",
            ):
                page_state.size = 50
                set_page_state(st.session_state, page_state)
                st.rerun()

        st.info(
            "💡 **Tip:** For large datasets, try reducing the page size or applying filters to reduce the data load.",
        )
        return [], 0, 1

    except Exception as e:
        # Fallback to current behavior if pagination fails
        st.warning(f"Pagination failed, falling back to current behavior: {e}")
        st.error(
            f'Groups pagination fallback | run_id={selected_run_id} error="{e!s}"',
        )
        return [], 0, 1

    # Page navigation
    col1, col2, col3 = st.columns(3)
    with col1:
        if (
            st.button("◀ Prev", key=f"prev_page_{selected_run_id}")
            and page_state.number > 1
        ):
            page_state.number -= 1
            set_page_state(st.session_state, page_state)
    with col2:
        st.write(f"Page {page_state.number} / {max_page}")
    with col3:
        if (
            st.button("Next ▶", key=f"next_page_{selected_run_id}")
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
        import os

        from src.utils.artifact_management import get_artifact_paths

        artifact_paths = get_artifact_paths(selected_run_id)
        group_stats_path = artifact_paths.get("group_stats_parquet")

        if group_stats_path and os.path.exists(group_stats_path):
            # Show active threshold in the banner
            active_threshold = int(filters.get("min_edge_strength", 0) or 0)
            st.success(
                f"⚡ **Fast stats mode**: Using pre-computed group statistics for instant loading · "
                f"Similarity ≥ {active_threshold:.0f}%",
                icon="⚡",
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
    filters: dict[str, Any],
) -> None:
    """Render the group list within a fragment to prevent page-wide blocking.

    Args:
        selected_run_id: The selected run ID
        sort_by: The sort key
        page: The current page number
        page_size: The page size
        filters: The filters dictionary

    """
    # Add CSS for larger, more readable expander headers
    st.markdown(
        """
        <style>
        .streamlit-expanderHeader {
            font-size: 1.6rem !important;
            font-weight: 600 !important;
            line-height: 1.6 !important;
            padding: 1.25rem 1.5rem !important;
            margin-bottom: 1rem !important;
        }
        .streamlit-expanderContent {
            padding: 1.5rem !important;
        }
        /* Make the expander header text more readable */
        .streamlit-expanderHeader p {
            font-size: 1.6rem !important;
            font-weight: 600 !important;
            margin: 0 !important;
            line-height: 1.6 !important;
        }
        /* Ensure the markdown container text is large enough */
        .st-emotion-cache-gx6i9d p {
            font-size: 1.6rem !important;
            font-weight: 600 !important;
            line-height: 1.6 !important;
        }
        /* Remove horizontal dividers between groups */
        .stDivider {
            display: none !important;
        }
        /* Add more spacing between expanders instead */
        .streamlit-expander {
            margin-bottom: 1.5rem !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    # Duplicate render guard for the list
    key = f"group_list_rendered:{selected_run_id}:{page}:{sort_by}"
    if not st.session_state.get(key):
        st.session_state[key] = True

    # Wrap groups list in fragment to prevent page-wide blocking
    page_groups, total_groups, max_page = render_group_list(
        selected_run_id,
        sort_by,
        page,
        page_size,
        filters,
    )

    # Group by group_id and display each group
    for i, group_info in enumerate(page_groups):
        group_id = group_info["group_id"]
        group_size = group_info["group_size"]
        primary_name = group_info["primary_name"]

        # Extract additional fields for better decision making
        max_score = group_info.get("max_score", 0.0)
        disposition = group_info.get("disposition", "Unknown")

        # Create a more informative expander title with key fields
        primary_name_display = primary_name or "Unknown"

        # Dynamic font scaling based on similarity score (0.9rem to 1.6rem)
        similarity_score = float(max_score or 0)
        font_size = (
            0.9 + (similarity_score / 100.0) * 0.7
        )  # 0.9rem at 0%, 1.6rem at 100%
        font_size = max(0.9, min(1.6, font_size))  # Clamp between 0.9 and 1.6

        # Create expander title with dynamic font scaling
        expander_title = (
            f"Group {group_id} · {primary_name_display} · {group_size} records"
        )
        if max_score is not None and max_score > 0:
            # Show max score but clarify it's the highest edge score in the group
            expander_title += f" · Max Edge {int(round(max_score))}%"
        if disposition and disposition != "Unknown":
            expander_title += f" · {disposition}"

        with st.expander(expander_title, expanded=False):
            # Add dynamic font scaling for the primary name inside the expander
            st.markdown(
                f"<div style='font-size:{font_size:.1f}rem; font-weight:700; margin-bottom:0.5rem;'>{primary_name_display}</div>",
                unsafe_allow_html=True,
            )
            # Show active threshold for this group
            active_threshold = int(filters.get("min_edge_strength", 0) or 0)
            st.caption(
                f"Showing groups with Similarity Score ≥ {active_threshold:.0f}%",
            )

            # Display key group information for quick review
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Group Size", group_size)
            with col2:
                if max_score > 0:
                    # Show max score with better context
                    st.metric("Max Edge Score", f"{max_score:.1f}%", help="Highest similarity score between any two records in this group")
                    # Add a note about score distribution if we have more info
                    if group_size > 2:
                        st.caption("(scores vary within group)")
                else:
                    st.metric("Max Edge Score", "N/A")
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

            # Render group details
            from app.components.group_details import render_group_details

            render_group_details(
                selected_run_id,
                group_id,
                group_size,
                primary_name,
                expander_title,
                create_expander=False,
            )

        # Visual separators removed - using CSS margin instead for cleaner spacing
