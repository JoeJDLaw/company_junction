"""Group details component for Phase 1.18.1 refactor.

This module handles the detailed information display for individual groups.
"""

from typing import Any

import pandas as pd
import streamlit as st

from src.utils.fragment_utils import fragment
from src.utils.group_details import get_group_details
from src.utils.schema_utils import (
    ACCOUNT_ID,
    ACCOUNT_NAME,
    DISPOSITION,
    IS_PRIMARY,
    SUFFIX_CLASS,
    WEAKEST_EDGE_TO_PRIMARY,
)
from src.utils.state_utils import (
    get_aliases_state,
    get_details_state,
    set_aliases_state,
    set_details_state,
)


def render_group_details(
    selected_run_id: str,
    group_id: str,
    group_size: int,
    primary_name: str,
    title: str | None = None,
    create_expander: bool = True,
) -> None:
    """Render group details within an expander.

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

    # Conditionally create expander based on create_expander parameter
    if create_expander:
        with st.expander(expander_title):
            _render_group_details_content(
                selected_run_id,
                group_id,
                group_size,
                primary_name,
                details_key,
                details_loaded,
                details_state,
                aliases_state,
            )
    else:
        # Render content directly without expander
        _render_group_details_content(
            selected_run_id,
            group_id,
            group_size,
            primary_name,
            details_key,
            details_loaded,
            details_state,
            aliases_state,
        )


def _render_group_details_content(
    selected_run_id: str,
    group_id: str,
    group_size: int,
    primary_name: str,
    details_key: tuple,
    details_loaded: bool,
    details_state: Any,
    aliases_state: Any,
) -> None:
    # Phase 1.26.2: Auto-load details when expander is open
    details_state.requested[details_key] = True
    set_details_state(st.session_state, details_state)

    # Check if details were requested and load them
    if details_state.requested.get(details_key, False) and not details_loaded:
        try:
            # Load the full group details
            group_details_result = get_group_details(
                selected_run_id,
                group_id,
                "Account Name (Asc)",
                1,
                100,
                {},
            )
            group_details, total_count = group_details_result  # Unpack the tuple
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
                    selected_run_id,
                    group_id,
                    group_data,
                    aliases_state,
                )
            except Exception as render_error:
                st.error(f"Failed to render group details: {render_error}")
                st.write(
                    "Raw data:",
                    (
                        group_details[:3]
                        if isinstance(group_details, list)
                        else group_details
                    ),
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
                        from src.utils.artifact_management import get_artifact_paths

                        artifact_paths = get_artifact_paths(selected_run_id)
                        details_path = artifact_paths.get("group_details_parquet")

                        if details_path:
                            import os

                            if os.path.exists(details_path):
                                st.write(
                                    f"**Details file:** ✅ Exists at `{details_path}`",
                                )

                                # Check schema
                                try:
                                    df = pd.read_parquet(details_path)
                                    st.write(f"**Schema:** {list(df.columns)}")
                                    st.write(f"**Rows:** {len(df)}")
                                    st.write(
                                        f"**Group ID sample:** {df['group_id'].head(3).tolist()}",
                                    )
                                except Exception as schema_error:
                                    st.write(f"**Schema check failed:** {schema_error}")
                            else:
                                st.write(
                                    f"**Details file:** ❌ Missing at `{details_path}`",
                                )
                        else:
                            st.write("**Details file:** ❌ Path not found in artifacts")
                    except Exception as path_error:
                        st.write(f"**Path check failed:** {path_error}")

                    st.write(f"**Error:** {error_msg}")
                    st.write(
                        "**Expected columns:** `group_id`, `account_id`, `account_name`, `suffix_class`, `created_date`, `disposition`",
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
    """Render compact group information badges."""
    col1, col2 = st.columns([3, 1])

    with col1:
        # Compact group badges in a single row
        badges = []

        # Suffix status
        if "suffix_class" in group_data.columns:
            suffix_classes = group_data["suffix_class"].unique()
            if len(suffix_classes) > 1:
                badges.append("⚠️ Suffix Mismatch")
            elif len(suffix_classes) == 1 and suffix_classes[0] == "NONE":
                badges.append("📋 No suffix variations")
            else:
                badges.append(f"✅ {suffix_classes[0]}")

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
                        ],
                    ),
                )
                .sum()
            )
            if blacklist_hits > 0:
                badges.append(f"⚠️ {blacklist_hits} blacklist hits")

        # Display badges in a compact row
        if badges:
            st.write(" | ".join(badges))

    with col2:
        # Primary count (compact)
        if IS_PRIMARY in group_data.columns:
            primary_count = group_data[IS_PRIMARY].sum()
            st.caption(f"Primary: {primary_count}")


def _render_group_table(group_data: pd.DataFrame) -> None:
    """Render the group data table in compact format."""
    # Show essential columns first, with option to expand
    essential_cols = [
        ACCOUNT_NAME,
        ACCOUNT_ID,
        DISPOSITION,
        SUFFIX_CLASS,
    ]
    essential_cols = [col for col in essential_cols if col in group_data.columns]

    # Additional columns for expanded view
    additional_cols = [
        "relationship",
        IS_PRIMARY,
        WEAKEST_EDGE_TO_PRIMARY,
        "created_date",
        "group_join_reason",
        "shared_tokens_count",
        "applied_penalties",
        "survivorship_reason",
    ]
    additional_cols = [col for col in additional_cols if col in group_data.columns]

    # Show compact view by default
    display_cols = essential_cols

    if display_cols:
        # Configure column display for better readability (essential columns only)
        column_config = {
            ACCOUNT_NAME: st.column_config.TextColumn(
                "Account Name",
                width="large",
                help="Company name",
                max_chars=None,
            ),
            ACCOUNT_ID: st.column_config.TextColumn("Account ID", width="medium"),
            DISPOSITION: st.column_config.SelectboxColumn(
                DISPOSITION,
                width="small",
                options=["Keep", "Update", "Delete", "Verify"],
            ),
            SUFFIX_CLASS: st.column_config.TextColumn("Suffix", width="small"),
        }

        # Show essential columns in compact table
        st.dataframe(
            group_data[display_cols],
            width="stretch",
            column_config=column_config,
            hide_index=True,
        )

        # Show additional columns in expander if available
        if additional_cols:
            with st.expander("🔍 Additional Details", expanded=False):
                additional_config = {
                    "relationship": st.column_config.TextColumn(
                        "Relationship",
                        width="medium",
                    ),
                    IS_PRIMARY: st.column_config.CheckboxColumn(
                        "Primary",
                        width="small",
                    ),
                    WEAKEST_EDGE_TO_PRIMARY: st.column_config.NumberColumn(
                        "Edge Score",
                        width="small",
                        format="%.1f",
                    ),
                    "created_date": st.column_config.DateColumn(
                        "Created Date",
                        width="small",
                    ),
                    "group_join_reason": st.column_config.TextColumn(
                        "Join Reason",
                        width="medium",
                    ),
                    "shared_tokens_count": st.column_config.NumberColumn(
                        "Shared Tokens",
                        width="small",
                    ),
                    "applied_penalties": st.column_config.TextColumn(
                        "Penalties",
                        width="medium",
                    ),
                    "survivorship_reason": st.column_config.TextColumn(
                        "Survivorship",
                        width="medium",
                    ),
                }

                st.dataframe(
                    group_data[additional_cols],
                    width="stretch",
                    column_config=additional_config,
                    hide_index=True,
                )
    else:
        st.warning("No displayable columns found in group data")


# Explain metadata function removed - all relevant fields now displayed in main table


@fragment
def _render_alias_cross_links(
    selected_run_id: str,
    group_id: str,
    group_data: pd.DataFrame,
    aliases_state: Any,
) -> None:
    """Render alias cross-links expander."""
    aliases_key = (selected_run_id, group_id)
    aliases_loaded = aliases_state.requested.get(aliases_key, False)

    # Check if alias_cross_refs column exists
    if "alias_cross_refs" not in group_data.columns:
        st.caption(
            "🔗 No alias data available for this run (column `alias_cross_refs` missing).",
        )
        return

    # Check if group has aliases (lightweight check)
    has_aliases = any(
        record.get("alias_cross_refs") for _, record in group_data.iterrows()
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
                            f"(score: {ref.get('score', '')}, source: {ref.get('source', '')})",
                        )
                else:
                    st.info("No cross-links available")
            elif st.button("Load cross-links", key=f"btn_alias_{group_id}"):
                # Load alias cross-refs on demand
                cross_refs_list = []
                for _, record in group_data.iterrows():
                    cross_refs = record.get("alias_cross_refs")
                    if cross_refs:
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
    else:
        # Column exists but group has no aliases
        st.caption("🔗 No cross-links found for this group.")


@fragment
def render_group_details_fragment(
    selected_run_id: str,
    group_id: str,
    group_size: int,
    primary_name: str,
) -> None:
    """Render group details within a fragment to prevent page-wide blocking.

    Args:
        selected_run_id: The selected run ID
        group_id: The group ID
        group_size: The group size
        primary_name: The primary name

    """
    render_group_details(selected_run_id, group_id, group_size, primary_name)
