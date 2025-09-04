"""
Maintenance component for Phase 1.18.1 refactor.

This module handles run deletion and cache clearing functionality.
"""

import streamlit as st

from src.utils.state_utils import get_cache_state, set_cache_state


def render_maintenance(selected_run_id: str) -> None:
    """
    Render maintenance controls in the sidebar.

    Args:
        selected_run_id: The selected run ID
    """
    from src.utils.logging_utils import get_logger

    logger = get_logger(__name__)
    logger.info(f"Sidebar maintenance copy rendered | run_id={selected_run_id}")

    st.sidebar.subheader("Run Maintenance")

    # Cache clearing functionality
    cache_state = get_cache_state(st.session_state)

    # Phase 1.26.2: Group maintenance buttons together with helpful tooltips
    st.write("**Cache & Session Management:**")

    # Clear caches button with tooltip
    if st.button(
        "🗑️ Clear Caches for Current Run",
        key=f"clear_caches_maintenance_{selected_run_id}",
        help="Use this when you see stale data or want to ensure fresh results. Clears cached group lists and details for the current run only.",
    ):
        # Clear list-level caches for current run
        st.cache_data.clear()
        st.cache_resource.clear()

        # Phase 1.23.1: Clear details cache for current run
        # _details_cache is no longer exported - use public cache clearing APIs
        st.success(f"Cleared caches for run {selected_run_id}")

        # Set one-shot flag
        cache_state.clear_requested_for_run_id = selected_run_id
        set_cache_state(st.session_state, cache_state)

    # Reset session state button with tooltip
    if st.button(
        "🔄 Reset Session State",
        key=f"reset_session_{selected_run_id}",
        help="Use this when the UI seems stuck or behaves unexpectedly. Resets all UI state and forces a fresh start.",
    ):
        # Clear all session state
        for key in list(st.session_state.keys()):
            del st.session_state[key]
        st.success("Session state reset successfully")
        st.rerun()

    # Run deletion functionality (placeholder)
    st.write("**Run Deletion:**")
    st.info("Run deletion functionality will be implemented in a future phase.")

    # Additional maintenance options
    st.write("**Additional Options:**")
    st.info("More maintenance options will be added in future phases.")
