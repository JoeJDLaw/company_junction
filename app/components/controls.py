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

    # View Mode Selection (Edge Groups vs Similarity Clusters)
    st.sidebar.write("**View Mode**")
    
    # Get current view mode from session state
    view_mode_key = f"view_mode_{selected_run_id}"
    current_view_mode = st.session_state.get(view_mode_key, "Similarity Clusters")
    
    view_mode = st.sidebar.selectbox(
        "Grouping Method",
        ["Edge Groups", "Similarity Clusters"],
        index=1 if current_view_mode == "Similarity Clusters" else 0,
        help="Edge Groups: Traditional edge-gated grouping. Similarity Clusters: Clustering-based grouping with configurable similarity thresholds.",
        key=f"view_mode_select_{selected_run_id}",
    )
    
    # Check if view mode changed and update state
    if view_mode != current_view_mode:
        st.session_state[view_mode_key] = view_mode
        # Reset to page 1 when view mode changes
        page_state.number = 1
        set_page_state(st.session_state, page_state)
        st.rerun()
    
    # Add view mode to filters
    filters["view_mode"] = view_mode

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
    
    # Clustering-specific controls (only show for Similarity Clusters view)
    if view_mode == "Similarity Clusters":
        st.sidebar.write("**Clustering Settings**")
        
        # Clustering policy
        policy_key = f"clustering_policy_{selected_run_id}"
        current_policy = st.session_state.get(policy_key, "complete")
        
        clustering_policy = st.sidebar.selectbox(
            "Clustering Policy",
            ["complete", "single"],
            index=0 if current_policy == "complete" else 1,
            format_func=lambda x: "Complete-linkage (strict)" if x == "complete" else "Single-linkage (looser)",
            help="Complete-linkage: All pairs in a cluster must meet the similarity threshold. Single-linkage: Any connection path above threshold is sufficient.",
            key=f"clustering_policy_{selected_run_id}",
        )
        
        # Check if policy changed and update state
        if clustering_policy != current_policy:
            st.session_state[policy_key] = clustering_policy
            # Reset to page 1 when policy changes
            page_state.number = 1
            set_page_state(st.session_state, page_state)
            st.rerun()
        
        # Min cluster size
        min_cluster_size_key = f"min_cluster_size_{selected_run_id}"
        current_min_size = st.session_state.get(min_cluster_size_key, 2)
        
        min_cluster_size = st.sidebar.number_input(
            "Min Cluster Size",
            min_value=1,
            max_value=10,
            value=current_min_size,
            step=1,
            help="Minimum number of records required to form a cluster. Smaller clusters become outliers.",
            key=f"min_cluster_size_{selected_run_id}",
        )
        
        # Check if min cluster size changed and update state
        if min_cluster_size != current_min_size:
            st.session_state[min_cluster_size_key] = min_cluster_size
            # Reset to page 1 when min cluster size changes
            page_state.number = 1
            set_page_state(st.session_state, page_state)
            st.rerun()
        
        # Add clustering parameters to filters
        filters["clustering_policy"] = clustering_policy
        filters["min_cluster_size"] = min_cluster_size

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
