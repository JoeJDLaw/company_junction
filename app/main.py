"""Streamlit GUI for the Company Junction deduplication pipeline.

This app provides an interactive interface for:
- Loading review-ready data from pipeline output
- Filtering and reviewing duplicate groups
- Manual disposition overrides and blacklist management
- Exporting filtered results

## Refactor Information
This file was refactored in Phase 1.18.1 (2025-08-31) to improve modularity.
The original ~1600-line file is preserved at: deprecated/2025-08-31_legacy_files/main.py

## How to Run
```bash
python run_streamlit.py
```

## Expected Data Files
- **Primary**: `data/processed/{run_id}/review_ready.parquet` (preferred for native types)
- **Fallback**: `data/processed/{run_id}/review_ready.csv` (if Parquet not available)

## Features
- **Review Interface**: Browse duplicate groups with disposition assignments
- **Filtering**: By disposition, group size, score, suffix mismatches, aliases
- **Sorting**: By group size, score, or account name
- **Manual Overrides**: Group-level disposition changes with JSON persistence
- **Blacklist Management**: View and edit pattern-based deletion rules with word-boundary matching
- **Pipeline Launcher**: Generate and copy pipeline commands for easy execution
- **Export**: Download manual overrides and blacklist for audit

## Manual Data Files
- **Overrides**: `data/manual/manual_dispositions.json`
- **Blacklist**: `data/manual/manual_blacklist.json`
- These files are git-ignored and persist across app sessions.
- **Audit**: `data/processed/review_meta.json` contains run metadata and statistics.
"""

from typing import Any, Dict, List, Optional, cast

import pandas as pd
import streamlit as st
import yaml

from app.components import (
    render_controls,
    render_export,
    render_maintenance,
)
from src.utils.logging_utils import get_logger, setup_logging
from src.utils.state_utils import migrate_legacy_keys


def load_settings() -> Dict[str, Any]:
    """Load application settings from config file."""
    try:
        with open("config/settings.yaml") as f:
            settings = yaml.safe_load(f)
        return settings or {}
    except Exception as e:
        st.error(f"Failed to load settings: {e}")
        return {}


@st.cache_data(show_spinner=False)
def load_review_data(run_id: str) -> Optional[pd.DataFrame]:
    """Load review-ready data from a specific run with enhanced error handling."""
    try:
        with st.spinner(f"Loading review data from run {run_id}..."):
            # Get artifact paths for the run
            from src.utils.artifact_management import get_artifact_paths
            from src.utils.run_management import validate_run_artifacts

            # Validate run artifacts
            validation = validate_run_artifacts(run_id)

            if not validation["run_exists"]:
                st.error(f"Run {run_id} not found in run index")
                return None

            if validation["status"] == "failed":
                st.error(f"Run {run_id} failed during execution")
                return None

            # Try to load from parquet first, then CSV
            artifact_paths = get_artifact_paths(run_id)

            # Try parquet
            if validation["has_review_ready_parquet"]:
                try:
                    df = pd.read_parquet(artifact_paths["review_ready_parquet"])
                    st.success(f"Loaded {len(df)} records from run {run_id}")
                    return df
                except Exception as e:
                    st.warning(f"Parquet load failed: {e}, trying CSV")

            # Try CSV
            if validation["has_review_ready_csv"]:
                try:
                    df = pd.read_csv(artifact_paths["review_ready_csv"])
                    if "alias_cross_refs" in df.columns:
                        df["alias_cross_refs"] = df["alias_cross_refs"].apply(
                            parse_alias_cross_refs,
                        )
                    st.success(f"Loaded {len(df)} records from run {run_id}")
                    return df
                except Exception as e:
                    st.error(f"CSV load failed: {e}")
                    return None

            st.error("No valid data files found")
            return None

    except Exception as e:
        st.error(f"Failed to load review data: {e}")
        return None


def parse_alias_cross_refs(cross_refs_str: str) -> List[Dict[str, Any]]:
    """Parse alias cross-references from string format."""
    if pd.isna(cross_refs_str) or cross_refs_str == "":
        return []

    try:
        import json

        return cast("list[dict[str, Any]]", json.loads(cross_refs_str))
    except (json.JSONDecodeError, TypeError):
        return []


def main() -> None:
    """Main application entry point."""
    # Setup logging
    setup_logging()
    logger = get_logger(__name__)

    # Track missing column warnings to avoid duplicates
    missing_msgs = set()

    def warn_once(msg: str) -> None:
        if msg not in missing_msgs:
            st.warning(msg)
            missing_msgs.add(msg)

    # Page configuration
    st.set_page_config(
        page_title="Company Junction Review",
        page_icon="🏢",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    # Header
    st.title("🏢 Company Junction Review")
    st.caption("Review and manage duplicate company groups detected by the pipeline.")

    # Load settings
    settings = load_settings()

    # Migrate legacy session state keys
    migrate_legacy_keys(st.session_state)

    # Run selection
    st.sidebar.header("Run Selection")

    # Get available runs
    from src.utils.run_management import format_run_display_name, list_runs

    try:
        runs = list_runs()
    except Exception as e:
        st.error(f"Failed to list runs: {e}")
        render_maintenance("list_runs_error")
        return

    # Always render sidebar structure first
    logger.info("Sidebar controls rendering...")

    # Filter controls (always render)
    st.sidebar.header("Export Filters")

    # Disposition filter
    dispositions = ["Keep", "Update", "Delete", "Verify"]
    selected_dispositions = st.sidebar.multiselect(
        "Disposition", dispositions, default=dispositions,
    )

    # Group size filter (currently affects Export only)
    min_group_size = st.sidebar.number_input(
        "Min Group Size (Export only)", min_value=1, value=1, step=1,
    )

    # (Similarity is controlled in controls.py; no shadow control here)

    # Additional filters
    show_suffix_mismatch = st.sidebar.checkbox(
        "Show Suffix Mismatch Only (Export only)", value=False,
    )
    has_aliases = st.sidebar.checkbox("Has Aliases Only (Export only)", value=False)

    # Build filters dictionary
    filters = {
        "dispositions": selected_dispositions if selected_dispositions else None,
        "min_group_size": min_group_size,
        "show_suffix_mismatch": show_suffix_mismatch,
        "has_aliases": has_aliases,
        # NOTE: similarity threshold is injected below after render_controls()
    }

    # Handle no runs case (empty state)
    if not runs:
        st.info("🏗️ **No runs found**")
        st.markdown(
            """
        The pipeline hasn't been run yet, or all runs have been cleaned up.

        **To get started:**
        1. Run the deduplication pipeline to create your first review
        2. Or restore runs from backup if they were accidentally deleted

        **Available actions:**
        - Use the maintenance panel to manage pipeline runs
        - Check the cleanup tool for run management options
        """,
        )

        # Still render maintenance in sidebar for pipeline management
        render_maintenance("no_runs")
        return

    # Filter to show complete runs first, then others
    complete_runs = [run for run in runs if run["status"] == "complete"]
    other_runs = [run for run in runs if run["status"] != "complete"]
    sorted_runs = complete_runs + other_runs

    # Generate display names for runs
    run_options = []
    for run in sorted_runs:
        display_name = format_run_display_name(run["run_id"], run)
        status_icon = (
            "✅"
            if run["status"] == "complete"
            else "⏳" if run["status"] == "running" else "❌"
        )
        run_options.append(f"{status_icon} {display_name}")

    # Default to first complete run, or first run if no complete runs
    default_index = 0
    if complete_runs:
        default_index = 0  # First complete run
    elif other_runs:
        default_index = 0  # First other run

    selected_run_display = st.sidebar.selectbox(
        "Select Run", run_options, index=default_index,
    )

    # Find selected run ID
    selected_run_id = None
    selected_run = None
    for i, run in enumerate(sorted_runs):
        display_name = format_run_display_name(run["run_id"], run)
        status_icon = (
            "✅"
            if run["status"] == "complete"
            else "⏳" if run["status"] == "running" else "❌"
        )
        full_display_name = f"{status_icon} {display_name}"
        if full_display_name == selected_run_display:
            selected_run_id = run["run_id"]
            selected_run = run
            break

    if not selected_run_id:
        st.error("Invalid run selection")
        # Still render maintenance in sidebar
        render_maintenance("invalid_run")
        return

    # (Similarity is controlled in controls.py; no shadow control here)

    # (Maintenance is rendered in sidebar by render_maintenance function)

    # Show run status for non-complete runs
    if selected_run is not None and selected_run["status"] == "running":
        st.info(f"⏳ Run {selected_run_id} is still processing...")
        st.progress(0.5)  # Placeholder progress
        return
    if selected_run is not None and selected_run["status"] == "failed":
        st.error(f"❌ Run {selected_run_id} failed during execution")
        return
    if selected_run is not None and selected_run["status"] != "complete":
        st.warning(
            f"⚠️ Run {selected_run_id} has status: {selected_run['status'] if selected_run else 'unknown'}",
        )
        return

    # Load review data
    df = load_review_data(selected_run_id)
    if df is None:
        return

    # Apply filters
    filtered_df = df.copy()

    if selected_dispositions and "disposition" in filtered_df.columns:
        filtered_df = filtered_df[
            filtered_df["disposition"].isin(selected_dispositions)
        ]
    elif selected_dispositions and "disposition" not in filtered_df.columns:
        warn_once(
            "Export filter skipped: 'disposition' column not present in this run.",
        )

    if min_group_size > 1 and "group_id" in filtered_df.columns:
        group_sizes = filtered_df.groupby("group_id").size()
        large_groups = group_sizes[group_sizes >= min_group_size].index
        filtered_df = filtered_df[filtered_df["group_id"].isin(large_groups)]
    elif min_group_size > 1 and "group_id" not in filtered_df.columns:
        warn_once("Export filter skipped: 'group_id' column not present in this run.")

    # (Similarity filter for export is applied after render_controls to keep parity)

    if (
        show_suffix_mismatch
        and "group_id" in filtered_df.columns
        and "suffix_class" in filtered_df.columns
    ):
        # Filter for groups with suffix mismatches
        group_suffixes = filtered_df.groupby("group_id")["suffix_class"].nunique()
        mismatch_groups = group_suffixes[group_suffixes > 1].index
        filtered_df = filtered_df[filtered_df["group_id"].isin(mismatch_groups)]
    elif show_suffix_mismatch and "suffix_class" not in filtered_df.columns:
        warn_once(
            "Export filter skipped: 'suffix_class' column not present in this run.",
        )

    if (
        has_aliases
        and "alias_cross_refs" in filtered_df.columns
        and "group_id" in filtered_df.columns
    ):
        has_alias_groups = filtered_df[
            filtered_df["alias_cross_refs"].notna()
            & (filtered_df["alias_cross_refs"] != "")
        ]["group_id"].unique()
        filtered_df = filtered_df[filtered_df["group_id"].isin(has_alias_groups)]
    elif has_aliases and "alias_cross_refs" not in filtered_df.columns:
        warn_once(
            "Export filter skipped: 'alias_cross_refs' column not present in this run.",
        )

    # Render controls first to get similarity_threshold
    filters, sort_by, page, page_size, similarity_threshold = render_controls(
        selected_run_id, settings, filters,
    )

    # Render maintenance in sidebar
    render_maintenance(selected_run_id)

    # Similarity filter caption is now shown in controls.py directly under the slider

    # Phase 1.35.2: Apply similarity threshold filtering (export parity)
    if similarity_threshold and similarity_threshold > 0:
        if "weakest_edge_to_primary" in filtered_df.columns:
            filtered_df = filtered_df[
                filtered_df["weakest_edge_to_primary"] >= float(similarity_threshold)
            ]
            st.info(
                f"📊 Filtered to groups with Similarity ≥ {int(similarity_threshold)}%",
            )
        else:
            warn_once(
                "Similarity export filter skipped: 'weakest_edge_to_primary' column not present in this run.",
            )

    # Display groups
    st.subheader("Duplicate Groups")

    # Show active disposition context
    if selected_dispositions:
        disposition_bullets = " • ".join(selected_dispositions)
        st.caption(f"**Showing dispositions:** • {disposition_bullets}")

    # Render group list with expanders
    from app.components.group_list import render_group_list_fragment

    render_group_list_fragment(selected_run_id, sort_by, page, page_size, filters)

    # (Group details are rendered inside each row's expander by the group list component.)

    # Add visual separator before export section
    st.divider()

    # Render export
    render_export(filtered_df, similarity_threshold)


if __name__ == "__main__":
    main()
