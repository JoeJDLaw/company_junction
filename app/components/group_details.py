"""
Group details component for Phase 1.18.1 refactor.

This module handles the detailed information display for individual groups.
"""

import streamlit as st
import pandas as pd
from typing import Any

from src.utils.state_utils import (
    get_details_state,
    set_details_state,
    get_aliases_state,
    set_aliases_state,
)
from src.utils.fragment_utils import fragment
from src.utils.ui_helpers import get_group_details_lazy, _is_non_empty
from src.utils.schema_utils import (
    ACCOUNT_ID,
    ACCOUNT_NAME,
    DISPOSITION,
    IS_PRIMARY,
    WEAKEST_EDGE_TO_PRIMARY,
    SUFFIX_CLASS,
)


def render_group_details(
    selected_run_id: str,
    group_id: str,
    group_size: int,
    primary_name: str,
    title: str = None,
) -> None:
    """
    Render group details within an expander.

    Args:
        selected_run_id: The selected run ID
        group_id: The group ID
        group_size: The group size
        primary_name: The primary name
        title: Optional custom title for the expander (if None, uses default format)
    """
    # Get state
    details_state = get_details_state(st.session_state)
    aliases_state = get_aliases_state(st.session_state)

    # Create keys for lazy loading
    details_key = (selected_run_id, group_id)
    details_loaded = details_state.loaded.get(details_key, False)

    # Use custom title if provided, otherwise use default format
    expander_title = (
        title if title else f"Group {group_id}: {primary_name} ({group_size} records)"
    )

    with st.expander(expander_title):
        # Phase 1.26.2: Auto-load details when expander is open
        details_state.requested[details_key] = True
        set_details_state(st.session_state, details_state)

        # Check if details were requested and load them
        if details_state.requested.get(details_key, False) and not details_loaded:
            try:
                # Load the full group details
                group_details = get_group_details_lazy(selected_run_id, group_id)
                details_state.data[details_key] = group_details
                details_state.loaded[details_key] = True
                set_details_state(st.session_state, details_state)

                # Phase 1.26.2: Render the details table immediately after loading
                try:
                    # Convert to DataFrame for rendering
                    group_data = pd.DataFrame(group_details)

                    # Display group info
                    _render_group_info(group_data, group_id)

                    # Display group table
                    _render_group_table(group_data)

                    # Only show aliases if full group details are loaded
                    _render_alias_cross_links(
                        selected_run_id, group_id, group_data, aliases_state
                    )
                except Exception as render_error:
                    st.error(f"Failed to render group details: {render_error}")
                    st.write(
                        "Raw data:", group_details[:3]
                    )  # Show first 3 records for debugging
            except Exception as e:
                # Phase 1.23.1: Enhanced error handling for DuckDB failures
                error_msg = str(e)

                if (
                    "DuckDB details loading failed and PyArrow fallback is disabled"
                    in error_msg
                ):
                    # Show friendly error for DuckDB failures
                    st.error("🚨 DuckDB query failed — see logs")

                    # Show diagnostic information
                    with st.expander("🔍 Error Details", expanded=False):
                        st.write(f"**Group ID:** {group_id}")
                        st.write(f"**Run ID:** {selected_run_id}")

                        # Check if group_details.parquet exists
                        try:
                            from src.utils.ui_helpers import get_artifact_paths

                            artifact_paths = get_artifact_paths(selected_run_id)
                            details_path = artifact_paths.get("group_details_parquet")

                            if details_path:
                                import os

                                if os.path.exists(details_path):
                                    st.write(
                                        f"**Details file:** ✅ Exists at `{details_path}`"
                                    )

                                    # Check schema
                                    try:
                                        df = pd.read_parquet(details_path)
                                        st.write(f"**Schema:** {list(df.columns)}")
                                        st.write(f"**Rows:** {len(df)}")
                                        st.write(
                                            f"**Group ID sample:** {df['group_id'].head(3).tolist()}"
                                        )
                                    except Exception as schema_error:
                                        st.write(
                                            f"**Schema check failed:** {schema_error}"
                                        )
                                else:
                                    st.write(
                                        f"**Details file:** ❌ Missing at `{details_path}`"
                                    )
                            else:
                                st.write(
                                    "**Details file:** ❌ Path not found in artifacts"
                                )
                        except Exception as path_error:
                            st.write(f"**Path check failed:** {path_error}")

                        st.write(f"**Error:** {error_msg}")
                        st.write(
                            "**Expected columns:** `group_id`, `account_id`, `account_name`, `suffix_class`, `created_date`, `Disposition`"
                        )
                else:
                    # Generic error
                    st.error(f"Failed to load group details: {error_msg}")

                details_state.data[details_key] = {"error": error_msg}
                details_state.loaded[details_key] = True
                set_details_state(st.session_state, details_state)

        # Render the details table if data is available
        # Note: This is now handled in the async loading section above
        # to avoid duplicate rendering and ensure immediate display


def _render_group_info(group_data: pd.DataFrame, group_id: str) -> None:
    """Render group information badges."""
    col1, col2, col3 = st.columns([2, 1, 1])

    with col1:
        # Group badges
        if "suffix_class" in group_data.columns:
            suffix_classes = group_data["suffix_class"].unique()
            if len(suffix_classes) > 1:
                st.error("⚠️ Suffix Mismatch")
            elif len(suffix_classes) == 1 and suffix_classes[0] == "NONE":
                st.info("📋 No suffix variations")
            else:
                st.success(f"✅ {suffix_classes[0]}")

        # Blacklist hits
        if ACCOUNT_NAME in group_data.columns:
            blacklist_hits = (
                group_data[ACCOUNT_NAME]
                .str.lower()
                .str.contains(
                    "|".join(
                        [
                            "pnc is not sure",
                            "unsure",
                            "unknown",
                            "1099",
                            "none",
                            "n/a",
                            "test",
                        ]
                    )
                )
                .sum()
            )

            if blacklist_hits > 0:
                st.warning(f"⚠️ {blacklist_hits} blacklist hits")

    with col2:
        # Primary selection info
        if IS_PRIMARY in group_data.columns:
            primary_count = group_data[IS_PRIMARY].sum()
            st.write(f"**Primary Records:** {primary_count}")

    with col3:
        # Manual override info (placeholder)
        st.write("**Manual Override:** Not implemented yet")


def _render_group_table(group_data: pd.DataFrame) -> None:
    """Render the group data table."""
    st.write("**Records:**")

    # Enhanced display columns with more useful information
    display_cols = [
        ACCOUNT_NAME,
        ACCOUNT_ID,
        "relationship",
        DISPOSITION,
        IS_PRIMARY,
        WEAKEST_EDGE_TO_PRIMARY,
        SUFFIX_CLASS,
        "created_date",
        "group_join_reason",
        "shared_tokens_count",
        "applied_penalties",
        "survivorship_reason",
    ]
    display_cols = [col for col in display_cols if col in group_data.columns]

    if display_cols:
        # Configure column display for better readability
        column_config = {
            ACCOUNT_NAME: st.column_config.TextColumn(
                "Account Name", width="large", help="Company name", max_chars=None
            ),
            ACCOUNT_ID: st.column_config.TextColumn("Account ID", width="medium"),
            "relationship": st.column_config.TextColumn("Relationship", width="medium"),
            DISPOSITION: st.column_config.SelectboxColumn(
                "Disposition",
                width="small",
                options=["Keep", "Update", "Delete", "Verify"],
            ),
            IS_PRIMARY: st.column_config.CheckboxColumn("Primary", width="small"),
            WEAKEST_EDGE_TO_PRIMARY: st.column_config.NumberColumn(
                "Edge Score", width="small", format="%.1f"
            ),
            SUFFIX_CLASS: st.column_config.TextColumn("Suffix", width="small"),
            "created_date": st.column_config.DateColumn("Created Date", width="small"),
            "group_join_reason": st.column_config.TextColumn("Join Reason", width="medium"),
            "shared_tokens_count": st.column_config.NumberColumn("Shared Tokens", width="small"),
            "applied_penalties": st.column_config.TextColumn("Penalties", width="medium"),
            "survivorship_reason": st.column_config.TextColumn("Survivorship", width="medium"),
        }

        st.dataframe(
            group_data[display_cols],
            width="stretch",
            column_config=column_config,
            hide_index=True,
        )
    else:
        st.warning("No displayable columns found in group data")


# Explain metadata function removed - all relevant fields now displayed in main table


@fragment
def _render_alias_cross_links(
    selected_run_id: str, group_id: str, group_data: pd.DataFrame, aliases_state: Any
) -> None:
    """Render alias cross-links expander."""
    aliases_key = (selected_run_id, group_id)
    aliases_loaded = aliases_state.requested.get(aliases_key, False)

    # Check if group has aliases (lightweight check)
    has_aliases = False
    if "alias_cross_refs" in group_data.columns:
        has_aliases = any(
            _is_non_empty(record.get("alias_cross_refs"))
            for _, record in group_data.iterrows()
        )

    if has_aliases:
        st.write("**Alias Cross-links:**")
        with st.expander("View cross-links", expanded=False):
            if aliases_loaded:
                # Display cached alias cross-refs
                cross_refs_list = aliases_state.data.get(aliases_key, [])
                if cross_refs_list:
                    st.write(f"📎 {len(cross_refs_list)} cross-links")
                    for ref in cross_refs_list:
                        st.write(
                            f"• {ref.get('alias', '')} → Group {ref.get('group_id', '')} "
                            f"(score: {ref.get('score', '')}, source: {ref.get('source', '')})"
                        )
                else:
                    st.info("No cross-links available")
            else:
                if st.button("Load cross-links", key=f"btn_alias_{group_id}"):
                    # Load alias cross-refs on demand
                    cross_refs_list = []
                    for _, record in group_data.iterrows():
                        cross_refs = record.get("alias_cross_refs")
                        if _is_non_empty(cross_refs):
                            try:
                                import json

                                refs = json.loads(str(cross_refs))
                                if isinstance(refs, list):
                                    cross_refs_list.extend(refs)
                            except (json.JSONDecodeError, TypeError):
                                continue

                    aliases_state.data[aliases_key] = cross_refs_list
                    aliases_state.requested[aliases_key] = True
                    set_aliases_state(st.session_state, aliases_state)
                    # No st.rerun() - let the fragment handle the update
                else:
                    st.caption("Cross-links load on demand.")


@fragment
def render_group_details_fragment(
    selected_run_id: str, group_id: str, group_size: int, primary_name: str
) -> None:
    """
    Render group details within a fragment to prevent page-wide blocking.

    Args:
        selected_run_id: The selected run ID
        group_id: The group ID
        group_size: The group size
        primary_name: The primary name
    """
    render_group_details(selected_run_id, group_id, group_size, primary_name)
