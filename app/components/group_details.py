"""
Group details component for Phase 1.18.1 refactor.

This module handles lazy expanders for group details and cross-links.
"""

import streamlit as st
import pandas as pd
from typing import Any

from src.utils.state_utils import (
    get_details_state,
    set_details_state,
    get_explain_state,
    set_explain_state,
    get_aliases_state,
    set_aliases_state,
)
from src.utils.ui_helpers import get_group_details_lazy, _is_non_empty


def render_group_details(
    selected_run_id: str, group_id: str, group_size: int, primary_name: str
) -> None:
    """
    Render group details within an expander.

    Args:
        selected_run_id: The selected run ID
        group_id: The group ID
        group_size: The group size
        primary_name: The primary name
    """
    # Get state
    details_state = get_details_state(st.session_state)
    explain_state = get_explain_state(st.session_state)
    aliases_state = get_aliases_state(st.session_state)

    # Create keys for lazy loading
    details_key = (selected_run_id, group_id)
    details_loaded = details_state.loaded.get(details_key, False)

    with st.expander(f"Group {group_id}: {primary_name} ({group_size} records)"):
        # Check if full group details are loaded
        if details_loaded:
            # Full details are loaded, display everything
            group_details = details_state.data.get(details_key, {})
            if isinstance(group_details, dict) and "error" in group_details:
                st.error(f"Failed to load group {group_id}: {group_details['error']}")
                return

            # Convert to list if it's not already
            if isinstance(group_details, list):
                group_data = pd.DataFrame(group_details)
            else:
                st.error(f"Invalid group details format for {group_id}")
                return

            # Display group info
            _render_group_info(group_data, group_id)

            # Display group table
            _render_group_table(group_data)

            # Only show explain metadata and aliases if full group details are loaded
            _render_explain_metadata(
                selected_run_id, group_id, group_data, explain_state
            )
            _render_alias_cross_links(
                selected_run_id, group_id, group_data, aliases_state
            )

        else:
            # Full details not loaded yet, show load button
            if st.button("Load Group Details", key=f"load_group_{group_id}"):
                # Load the full group details
                try:
                    group_details = get_group_details_lazy(selected_run_id, group_id)
                    details_state.data[details_key] = group_details
                    details_state.loaded[details_key] = True
                    set_details_state(st.session_state, details_state)
                    st.rerun()
                except Exception as e:
                    st.error(f"Failed to load group details: {e}")
            else:
                st.caption("Click 'Load Group Details' to view records and metadata.")
                st.caption(f"Group {group_id} has {group_size} records.")


def _render_group_info(group_data: pd.DataFrame, group_id: str) -> None:
    """Render group information badges."""
    col1, col2, col3 = st.columns([2, 1, 1])

    with col1:
        # Group badges
        if "suffix_class" in group_data.columns:
            suffix_classes = group_data["suffix_class"].unique()
            if len(suffix_classes) > 1:
                st.error("⚠️ Suffix Mismatch")
            else:
                st.success(f"✅ {suffix_classes[0]}")

        # Blacklist hits
        if "account_name" in group_data.columns:
            blacklist_hits = (
                group_data["account_name"]
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
        if "is_primary" in group_data.columns:
            primary_count = group_data["is_primary"].sum()
            st.write(f"**Primary Records:** {primary_count}")

    with col3:
        # Manual override info (placeholder)
        st.write("**Manual Override:** Not implemented yet")


def _render_group_table(group_data: pd.DataFrame) -> None:
    """Render the group data table."""
    st.write("**Records:**")

    display_cols = [
        "account_name",
        "account_id",
        "relationship",
        "Disposition",
        "is_primary",
        "weakest_edge_to_primary",
        "suffix_class",
    ]
    display_cols = [col for col in display_cols if col in group_data.columns]

    if display_cols:
        # Configure column display for better readability
        column_config = {
            "account_name": st.column_config.TextColumn(
                "Account Name", width="large", help="Company name", max_chars=None
            ),
            "account_id": st.column_config.TextColumn("Account ID", width="medium"),
            "relationship": st.column_config.TextColumn("Relationship", width="medium"),
            "Disposition": st.column_config.SelectboxColumn(
                "Disposition",
                width="small",
                options=["Keep", "Update", "Delete", "Verify"],
            ),
            "is_primary": st.column_config.CheckboxColumn("Primary", width="small"),
            "weakest_edge_to_primary": st.column_config.NumberColumn(
                "Edge Score", width="small", format="%.1f"
            ),
            "suffix_class": st.column_config.TextColumn("Suffix", width="small"),
        }

        st.dataframe(
            group_data[display_cols],
            width="stretch",
            column_config=column_config,
            hide_index=True,
        )
    else:
        st.warning("No displayable columns found in group data")


def _render_explain_metadata(
    selected_run_id: str, group_id: str, group_data: pd.DataFrame, explain_state: Any
) -> None:
    """Render explain metadata expander."""
    explain_key = (selected_run_id, group_id)
    explain_loaded = explain_state.requested.get(explain_key, False)

    with st.expander("🔍 Explain Metadata", expanded=False):
        if explain_loaded:
            # Display cached explain metadata
            explain_data = explain_state.data.get(explain_key, pd.DataFrame())
            if not explain_data.empty:
                st.dataframe(
                    explain_data,
                    width="stretch",
                    hide_index=True,
                    column_config={
                        "account_name": st.column_config.TextColumn(
                            "Account Name", width="large"
                        ),
                        "group_join_reason": st.column_config.TextColumn(
                            "Join Reason", width="medium"
                        ),
                        "weakest_edge_to_primary": st.column_config.NumberColumn(
                            "Weakest Edge", width="small", format="%.1f"
                        ),
                        "shared_tokens_count": st.column_config.NumberColumn(
                            "Shared Tokens", width="small"
                        ),
                        "applied_penalties": st.column_config.TextColumn(
                            "Penalties", width="medium"
                        ),
                        "survivorship_reason": st.column_config.TextColumn(
                            "Survivorship", width="medium"
                        ),
                    },
                )
            else:
                st.info("No explain metadata available for this group")
        else:
            if st.button("Load details", key=f"btn_explain_{group_id}"):
                # Load explain metadata on demand
                explain_cols = []
                for col in [
                    "group_join_reason",
                    "weakest_edge_to_primary",
                    "shared_tokens_count",
                    "applied_penalties",
                    "survivorship_reason",
                ]:
                    if col in group_data.columns:
                        explain_cols.append(col)

                if explain_cols:
                    explain_data = group_data[["account_name"] + explain_cols].copy()
                    explain_state.data[explain_key] = explain_data
                else:
                    explain_state.data[explain_key] = pd.DataFrame()

                explain_state.requested[explain_key] = True
                set_explain_state(st.session_state, explain_state)
                st.rerun()
            else:
                st.caption("Details load on demand.")


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
                    st.rerun()
                else:
                    st.caption("Cross-links load on demand.")


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
    with st.experimental_fragment():
        render_group_details(selected_run_id, group_id, group_size, primary_name)
