"""Maintenance component for Phase 1.18.1 refactor.

This module handles run deletion and cache clearing functionality.
"""

from typing import Any

import streamlit as st

from src.utils.state_utils import get_cache_state, set_cache_state


# Constants for UI text (ease i18n/styling changes)
class MaintenanceConstants:
    """Constants for maintenance component UI text."""

    # Section headers
    CACHE_SESSION_HEADER = "Cache & Session"
    RUN_DELETION_HEADER = "Run Deletion"

    # Button labels
    CLEAR_CACHES_BUTTON = "🧹 Clear Run Cache"
    RESET_SESSION_BUTTON = "♻️ Reset Session"
    PREVIEW_DELETION_BUTTON = "👁️ Preview"
    DELETE_SELECTED_BUTTON = "🗑️ Delete"
    CANCEL_DELETION_BUTTON = "✖️ Cancel"

    # Help text
    CLEAR_CACHES_HELP = "Use this when you see stale data or want to ensure fresh results. Clears cached group lists and details for the current run only."
    RESET_SESSION_HELP = "Use this when the UI seems stuck or behaves unexpectedly. Resets all UI state and forces a fresh start."
    SELECT_RUNS_HELP = (
        "Select one or more runs to delete. Running runs are excluded for safety."
    )

    # Status icons
    COMPLETE_ICON = "✅"
    FAILED_ICON = "❌"
    RUNNING_ICON = "🔄"

    # Messages
    CACHE_CLEARED_MSG = "Cleared caches for run {run_id}"
    SESSION_RESET_MSG = "Session state reset successfully"
    NO_RUNS_MSG = "No runs available for deletion."
    NO_DELETABLE_RUNS_MSG = "No deletable runs found (all runs are currently running)."
    DELETION_CANCELLED_MSG = "Deletion cancelled."

    # Feature flag keys
    ENABLE_RUN_DELETION_KEY = "ui.enable_run_deletion"
    ADMIN_MODE_KEY = "ui.admin_mode"


def render_maintenance(selected_run_id: str) -> None:
    """Render maintenance controls in the sidebar.

    Args:
        selected_run_id: The selected run ID

    """
    from src.utils.logging_utils import get_logger
    from src.utils.settings import get_settings

    logger = get_logger(__name__)
    logger.info(f"Sidebar maintenance copy rendered | run_id={selected_run_id}")

    # Use sidebar panel for all rendering
    panel = st.sidebar

    # Cache-busting comment to force browser refresh
    # v2.0 - All maintenance controls properly wrapped in expander

    with panel.expander("⚙️ Advanced: Maintenance", expanded=False):
        # Keep feature flags + cache_state as-is
        settings = get_settings()
        enable_run_deletion = settings.get("ui", {}).get("enable_run_deletion", True)
        admin_mode = settings.get("ui", {}).get("admin_mode", False)
        cache_state = get_cache_state(st.session_state)

        # Replace panel.write(...) headers with compact captions
        panel.caption(MaintenanceConstants.CACHE_SESSION_HEADER)

        # Put the two main buttons on one row
        c1, c2 = panel.columns(2)
        with c1:
            if panel.button(
                MaintenanceConstants.CLEAR_CACHES_BUTTON,
                key=f"clear_caches_maintenance_{selected_run_id}",
                help=MaintenanceConstants.CLEAR_CACHES_HELP,
                use_container_width=True,
            ):
                st.cache_data.clear()
                st.cache_resource.clear()
                panel.success(
                    MaintenanceConstants.CACHE_CLEARED_MSG.format(
                        run_id=selected_run_id,
                    ),
                )
                cache_state.clear_requested_for_run_id = selected_run_id
                set_cache_state(st.session_state, cache_state)

        with c2:
            if panel.button(
                MaintenanceConstants.RESET_SESSION_BUTTON,
                key=f"reset_session_{selected_run_id}",
                help=MaintenanceConstants.RESET_SESSION_HELP,
                use_container_width=True,
            ):
                for key in list(st.session_state.keys()):
                    del st.session_state[key]
                panel.success(MaintenanceConstants.SESSION_RESET_MSG)
                st.rerun()

        # Additional Options row (3 columns)
        panel.caption("Additional Options")
        oc1, oc2, oc3 = panel.columns(3)

        with oc1:
            if panel.button(
                "🔄 Refresh",
                key=f"force_refresh_{selected_run_id}",
                help="Force a complete page refresh to reload all data",
                use_container_width=True,
            ):
                st.rerun()

        with oc2:
            if panel.button(
                "🧽 Clear All",
                key=f"clear_all_caches_{selected_run_id}",
                help="Clear all caches (not just current run)",
                use_container_width=True,
            ):
                st.cache_data.clear()
                st.cache_resource.clear()
                panel.success("All caches cleared successfully")

        with oc3:
            try:
                # Prefer popover if available
                with panel.popover("📤 Export"):
                    import json

                    session_data = dict(st.session_state)
                    panel.download_button(
                        label="Download JSON",
                        data=json.dumps(session_data, indent=2, default=str),
                        file_name=f"session_state_{selected_run_id}.json",
                        mime="application/json",
                        key=f"download_session_{selected_run_id}",
                    )
            except Exception:
                # Fallback to inline download
                if panel.button(
                    "📤 Export",
                    key=f"export_session_{selected_run_id}",
                    help="Export current session state for debugging",
                    use_container_width=True,
                ):
                    import json

                    session_data = dict(st.session_state)
                    panel.download_button(
                        label="Download JSON",
                        data=json.dumps(session_data, indent=2, default=str),
                        file_name=f"session_state_{selected_run_id}.json",
                        mime="application/json",
                        key=f"download_session_fallback_{selected_run_id}",
                    )

        # Run Deletion (feature-gated) in nested expander
        if enable_run_deletion and admin_mode:
            with panel.expander(
                f"🗑️ {MaintenanceConstants.RUN_DELETION_HEADER}", expanded=False,
            ):
                from src.utils.run_management import format_run_display_name, list_runs

                runs = list_runs()
                if not runs:
                    panel.info("No runs available for deletion.")
                else:
                    deletable_runs = [r for r in runs if r["status"] != "running"]
                    if not deletable_runs:
                        panel.info("No deletable runs (all running).")
                    else:
                        run_options, display_to_id = [], {}
                        for r in deletable_runs:
                            icon = "✅" if r["status"] == "complete" else "❌"
                            disp = f"{icon} {format_run_display_name(r['run_id'], r)}"
                            run_options.append(disp)
                            display_to_id[disp] = r["run_id"]

                        selected_runs = panel.multiselect(
                            "Choose runs",
                            run_options,
                            help=MaintenanceConstants.SELECT_RUNS_HELP,
                        )

                        # Tiny action row
                        ac1, ac2, ac3 = panel.columns(3)
                        if selected_runs:
                            ids = [display_to_id[s] for s in selected_runs]
                            with ac1:
                                if panel.button(
                                    MaintenanceConstants.PREVIEW_DELETION_BUTTON,
                                    key=f"preview_{selected_run_id}",
                                    use_container_width=True,
                                ):
                                    from src.utils.cache_utils import (
                                        preview_delete_runs,
                                    )

                                    try:
                                        preview = preview_delete_runs(ids)
                                        st.session_state[
                                            f"deletion_preview_{selected_run_id}"
                                        ] = preview
                                        st.session_state[
                                            f"selected_run_ids_{selected_run_id}"
                                        ] = ids
                                        panel.success("Preview ready below")
                                    except Exception as e:
                                        panel.error(f"Preview failed: {e}")

                            with ac2:
                                if panel.button(
                                    MaintenanceConstants.DELETE_SELECTED_BUTTON,
                                    key=f"delete_{selected_run_id}",
                                    use_container_width=True,
                                ):
                                    from src.utils.cache_utils import delete_runs

                                    try:
                                        results = delete_runs(ids)
                                        if results.get("deleted"):
                                            panel.success(
                                                f"Deleted {len(results['deleted'])} runs",
                                            )
                                        if results.get("errors"):
                                            panel.error("Errors occurred; see logs.")
                                        st.session_state.pop(
                                            f"deletion_preview_{selected_run_id}", None,
                                        )
                                        st.session_state.pop(
                                            f"selected_run_ids_{selected_run_id}", None,
                                        )
                                        st.rerun()
                                    except Exception as e:
                                        panel.error(f"Delete failed: {e}")

                            with ac3:
                                if panel.button(
                                    MaintenanceConstants.CANCEL_DELETION_BUTTON,
                                    key=f"cancel_{selected_run_id}",
                                    use_container_width=True,
                                ):
                                    st.session_state.pop(
                                        f"deletion_preview_{selected_run_id}", None,
                                    )
                                    st.session_state.pop(
                                        f"selected_run_ids_{selected_run_id}", None,
                                    )
                                    panel.info("Cancelled.")

                        preview_data: dict[str, Any] | None = st.session_state.get(
                            f"deletion_preview_{selected_run_id}",
                        )
                        if preview_data:
                            panel.caption("Preview")
                            if preview_data.get("runs_to_delete"):
                                panel.write(
                                    f"- Runs: {len(preview_data['runs_to_delete'])}",
                                )
                            if preview_data.get("total_bytes", 0) > 0:
                                mb = preview_data["total_bytes"] / (1024 * 1024)
                                panel.write(f"- Size: {mb:.1f} MB")

        elif not enable_run_deletion:
            panel.caption("Run deletion is disabled in current configuration.")
        elif not admin_mode:
            panel.caption("Run deletion is only available in admin mode.")
