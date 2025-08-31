"""
Streamlit GUI for the Company Junction deduplication pipeline.

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
streamlit run app/main.py
```

## Expected Data Files
- **Primary**: `data/processed/review_ready.parquet` (preferred for native types)
- **Fallback**: `data/processed/review_ready.csv` (if Parquet not available)

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

import streamlit as st
import pandas as pd
import yaml
from typing import Any, Dict, List, Optional

from src.utils.logging_utils import setup_logging
from src.utils.state_utils import migrate_legacy_keys
from app.components import (
    render_controls,
    render_group_list,
    render_group_details,
    render_maintenance,
    render_export,
)


def load_settings() -> Dict[str, Any]:
    """Load application settings from config file."""
    try:
        with open("config/settings.yaml", "r") as f:
            settings = yaml.safe_load(f)
        return settings or {}
    except Exception as e:
        st.error(f"Failed to load settings: {e}")
        return {}


def load_review_data(run_id: str) -> Optional[pd.DataFrame]:
    """Load review-ready data from a specific run with enhanced error handling."""
    try:
        with st.spinner(f"Loading review data from run {run_id}..."):
            # Get artifact paths for the run
            from src.utils.ui_helpers import get_artifact_paths, validate_run_artifacts

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
                            parse_alias_cross_refs
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

        if isinstance(cross_refs_str, str):
            return json.loads(cross_refs_str)
        return cross_refs_str
    except (json.JSONDecodeError, TypeError):
        return []


def main():
    """Main application entry point."""
    # Setup logging
    setup_logging()

    # Page configuration
    st.set_page_config(
        page_title="Company Junction Review",
        page_icon="🏢",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    # Header
    st.title("🏢 Company Junction Review")
    st.markdown(
        "Review and manage duplicate company groups from the deduplication pipeline."
    )

    # Load settings
    settings = load_settings()

    # Migrate legacy session state keys
    migrate_legacy_keys(st.session_state)

    # Run selection
    st.sidebar.header("Run Selection")

    # Get available runs
    from src.utils.ui_helpers import list_runs, format_run_display_name

    runs = list_runs()

    if not runs:
        st.error("No runs found. Please run the pipeline first.")
        return

    # Filter to show complete runs first, then others
    complete_runs = [run for run in runs if run["status"] == "complete"]
    other_runs = [run for run in runs if run["status"] != "complete"]
    sorted_runs = complete_runs + other_runs

    # Generate display names for runs
    run_options = []
    for run in sorted_runs:
        display_name = format_run_display_name(run["run_id"], run)
        status_icon = "✅" if run["status"] == "complete" else "⏳" if run["status"] == "running" else "❌"
        run_options.append(f"{status_icon} {display_name}")

    # Default to first complete run, or first run if no complete runs
    default_index = 0
    if complete_runs:
        default_index = 0  # First complete run
    elif other_runs:
        default_index = 0  # First other run

    selected_run_display = st.sidebar.selectbox("Select Run", run_options, index=default_index)

    # Find selected run ID
    selected_run_id = None
    selected_run = None
    for i, run in enumerate(sorted_runs):
        display_name = format_run_display_name(run["run_id"], run)
        status_icon = "✅" if run["status"] == "complete" else "⏳" if run["status"] == "running" else "❌"
        full_display_name = f"{status_icon} {display_name}"
        if full_display_name == selected_run_display:
            selected_run_id = run["run_id"]
            selected_run = run
            break

    if not selected_run_id:
        st.error("Invalid run selection")
        return

    # Show run status
    if selected_run["status"] == "running":
        st.info(f"⏳ Run {selected_run_id} is still processing...")
        st.progress(0.5)  # Placeholder progress
        return
    elif selected_run["status"] == "failed":
        st.error(f"❌ Run {selected_run_id} failed during execution")
        return
    elif selected_run["status"] != "complete":
        st.warning(f"⚠️ Run {selected_run_id} has status: {selected_run['status']}")
        return

    # Load review data
    df = load_review_data(selected_run_id)
    if df is None:
        return

    # Filter controls
    st.sidebar.header("Filters")

    # Disposition filter
    dispositions = ["Keep", "Update", "Delete", "Verify"]
    selected_dispositions = st.sidebar.multiselect(
        "Disposition", dispositions, default=dispositions
    )

    # Group size filter
    min_group_size = st.sidebar.number_input(
        "Min Group Size", min_value=1, value=1, step=1
    )

    # Score filter
    min_edge_strength = st.sidebar.number_input(
        "Min Edge Strength", min_value=0.0, value=0.0, step=0.1, format="%.1f"
    )

    # Additional filters
    show_suffix_mismatch = st.sidebar.checkbox("Show Suffix Mismatch Only", value=False)
    has_aliases = st.sidebar.checkbox("Has Aliases Only", value=False)

    # Apply filters
    filtered_df = df.copy()

    if selected_dispositions:
        filtered_df = filtered_df[
            filtered_df["Disposition"].isin(selected_dispositions)
        ]

    if min_group_size > 1:
        group_sizes = filtered_df.groupby("group_id").size()
        large_groups = group_sizes[group_sizes >= min_group_size].index
        filtered_df = filtered_df[filtered_df["group_id"].isin(large_groups)]

    if min_edge_strength > 0:
        filtered_df = filtered_df[
            filtered_df["weakest_edge_to_primary"] >= min_edge_strength
        ]

    if show_suffix_mismatch:
        # Filter for groups with suffix mismatches
        group_suffixes = filtered_df.groupby("group_id")["suffix_class"].nunique()
        mismatch_groups = group_suffixes[group_suffixes > 1].index
        filtered_df = filtered_df[filtered_df["group_id"].isin(mismatch_groups)]

    if has_aliases:
        # Filter for groups with aliases
        has_alias_groups = filtered_df[
            filtered_df["alias_cross_refs"].notna()
            & (filtered_df["alias_cross_refs"] != "")
        ]["group_id"].unique()
        filtered_df = filtered_df[filtered_df["group_id"].isin(has_alias_groups)]

    # Build filters dictionary
    filters = {
        "dispositions": selected_dispositions if selected_dispositions else None,
        "min_group_size": min_group_size,
        "show_suffix_mismatch": show_suffix_mismatch,
        "has_aliases": has_aliases,
        "min_edge_strength": min_edge_strength,
    }

    # Render controls
    filters, sort_by, page, page_size = render_controls(
        selected_run_id, settings, filters
    )

    # Display groups
    st.subheader("Duplicate Groups")

    # Render group list
    page_groups, total_groups, max_page = render_group_list(
        selected_run_id, sort_by, page, page_size, filters
    )

    # Render group details for each group
    for group_info in page_groups:
        group_id = group_info["group_id"]
        group_size = group_info["group_size"]
        primary_name = group_info["primary_name"]

        render_group_details(selected_run_id, group_id, group_size, primary_name)

    # Render maintenance
    render_maintenance(selected_run_id)

    # Render export
    render_export(filtered_df)


if __name__ == "__main__":
    main()
