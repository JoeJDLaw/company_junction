"""
Maintenance component for Phase 1.18.1 refactor.

This module handles run deletion and cache clearing functionality.
"""

import streamlit as st

from src.utils.state_utils import get_cache_state, set_cache_state


def render_maintenance(selected_run_id: str) -> None:
    """
    Render maintenance controls.

    Args:
        selected_run_id: The selected run ID
    """
    st.subheader("Run Maintenance")

    # Cache clearing functionality
    cache_state = get_cache_state(st.session_state)

    if st.button(
        "🗑️ Clear Caches for Current Run",
        key=f"clear_caches_maintenance_{selected_run_id}",
    ):
        # Clear list-level caches for current run
        st.cache_data.clear()
        st.cache_resource.clear()
        st.success(f"Cleared caches for run {selected_run_id}")

        # Set one-shot flag
        cache_state.clear_requested_for_run_id = selected_run_id
        set_cache_state(st.session_state, cache_state)

    # Run deletion functionality (placeholder)
    st.write("**Run Deletion:**")
    st.info("Run deletion functionality will be implemented in a future phase.")

    # Additional maintenance options
    st.write("**Additional Options:**")
    if st.button("🔄 Reset Session State", key=f"reset_session_{selected_run_id}"):
        # Clear all session state
        for key in list(st.session_state.keys()):
            del st.session_state[key]
        st.success("Session state reset successfully")
        st.rerun()
