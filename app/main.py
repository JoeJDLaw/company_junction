"""
Streamlit GUI for the Company Junction deduplication pipeline.

This app provides an interactive interface for:
- Loading review-ready data from pipeline output
- Filtering and reviewing duplicate groups
- Manual disposition overrides and blacklist management
- Exporting filtered results

## How to Run

### Prerequisites
1. Install dependencies: `pip install -r requirements.txt`
2. Run the pipeline first to generate review data:
   ```bash
   python src/cleaning.py --input data/raw/your_data.csv --outdir data/processed --config config/settings.yaml
   ```

### Start the App
```bash
streamlit run app/main.py
```

### Alternative: Headless Mode (for testing)
```bash
streamlit run app/main.py --server.headless true --server.port 8501
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
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, List, Any

from src.utils.logging_utils import setup_logging


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

            # No valid files found
            missing_files = validation["missing_files"]
            st.error(
                f"Run {run_id} is missing required files: {', '.join(missing_files)}"
            )
            st.info("Please ensure the pipeline completed successfully for this run.")
            return None

    except Exception as e:
        st.error(f"Error loading review data: {str(e)}")
        st.info("Please ensure the pipeline has completed successfully.")
        return None


def parse_alias_cross_refs(cross_refs_str: Any) -> List[Any]:
    """Parse alias cross-references from string representation."""
    if pd.isna(cross_refs_str) or cross_refs_str == "[]":
        return []
    try:
        import ast

        result = ast.literal_eval(cross_refs_str)
        if isinstance(result, list):
            return result
        else:
            return []
    except (ValueError, SyntaxError):
        return []


def load_settings(path: str = "config/settings.yaml") -> Dict[str, Any]:
    """Load settings from YAML file."""
    try:
        import yaml

        with open(path, "r") as fh:
            return yaml.safe_load(fh) or {}
    except Exception:
        return {}


def _safe_get(d: Dict[str, Any], *keys: str, default: Any = None) -> Any:
    """Safely get nested dictionary values."""
    cur = d
    for k in keys:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur


def parse_merge_preview(preview_json: Any) -> Optional[Dict[str, Any]]:
    """Parse merge preview JSON for display."""
    if not preview_json or pd.isna(preview_json):
        return None

    try:
        result = json.loads(preview_json)
        if isinstance(result, dict):
            return result
        else:
            return None
    except (ValueError, TypeError):
        return None


def main() -> None:
    """Main Streamlit application."""
    st.set_page_config(
        page_title="Company Junction Deduplication Review",
        page_icon="🔗",
        layout="wide",
    )

    st.title("🔗 Company Junction Deduplication Review")
    st.markdown("Review and filter duplicate detection results from the pipeline.")

    # Setup logging
    setup_logging()

    # Phase 1.17.1: Run Picker and Stage Status
    from src.utils.ui_helpers import (
        list_runs,
        get_default_run_id,
        get_run_metadata,
        load_stage_state,
        format_run_display_name,
        get_run_status_icon,
        get_stage_status_icon,
        get_artifact_paths,
    )

    # Initialize session state for run selection
    if "selected_run_id" not in st.session_state:
        st.session_state.selected_run_id = get_default_run_id()

    if "cached_data" not in st.session_state:
        st.session_state.cached_data = {}

    if "cached_run_id" not in st.session_state:
        st.session_state.cached_run_id = ""

    # Run Picker in Sidebar
    st.sidebar.header("Run Selection")

    # Get available runs
    runs = list_runs()

    if not runs:
        st.sidebar.error("No runs found in run index")
        st.info("Please run the pipeline first to generate review data.")
        return

    # Create run selection options
    run_options = []
    run_display_names = []

    for run in runs:
        run_id = run["run_id"]
        display_name = format_run_display_name(run_id, run)
        status_icon = get_run_status_icon(run["status"])

        # Mark latest run
        if run_id == get_default_run_id():
            display_name = f"🆕 {display_name} (Latest)"

        run_options.append(run_id)
        run_display_names.append(f"{status_icon} {display_name}")

    # Run selection
    selected_index = 0
    if st.session_state.selected_run_id in run_options:
        selected_index = run_options.index(st.session_state.selected_run_id)

    selected_run_display = st.sidebar.selectbox(
        "Select Run",
        options=run_display_names,
        index=selected_index,
        help="Choose which pipeline run to review",
    )

    # Extract run ID from selection
    selected_run_id = run_options[run_display_names.index(selected_run_display)]

    # Update session state if run changed
    if selected_run_id != st.session_state.selected_run_id:
        st.session_state.selected_run_id = selected_run_id
        st.rerun()

    # Run Metadata Panel
    run_metadata = get_run_metadata(selected_run_id)
    if run_metadata:
        with st.sidebar.expander("Run Metadata", expanded=False):
            st.write(
                f"**Status:** {get_run_status_icon(run_metadata['status'])} {run_metadata['status']}"
            )
            st.write(f"**Created:** {run_metadata['formatted_timestamp']}")

            if run_metadata["input_paths"]:
                input_file = Path(run_metadata["input_paths"][0]).name
                st.write(f"**Input:** {input_file}")

            if run_metadata["config_paths"]:
                config_file = Path(run_metadata["config_paths"][0]).name
                st.write(f"**Config:** {config_file}")

            st.write(f"**DAG Version:** {run_metadata['dag_version']}")

    # Stage Status (MiniDAG Lite)
    stage_state = load_stage_state(selected_run_id)
    if stage_state:
        with st.sidebar.expander("Pipeline Stages", expanded=False):
            stages = stage_state["stages"]

            # Create stage status table
            stage_data = []
            for stage in stages:
                status_icon = get_stage_status_icon(stage["status"])
                stage_data.append(
                    {
                        "Stage": stage["name"].replace("_", " ").title(),
                        "Status": f"{status_icon} {stage['status']}",
                        "Duration": stage["duration_str"],
                    }
                )

            if stage_data:
                import pandas as pd

                stage_df = pd.DataFrame(stage_data)
                st.dataframe(stage_df, hide_index=True, width="stretch")
            else:
                st.write("No stage information available")
    else:
        with st.sidebar.expander("Pipeline Stages", expanded=False):
            st.warning("Stage information not available for this run")

    # Artifact Downloads
    with st.sidebar.expander("Download Artifacts", expanded=False):
        artifact_paths = get_artifact_paths(selected_run_id)

        # Review ready files
        if os.path.exists(artifact_paths["review_ready_csv"]):
            with open(artifact_paths["review_ready_csv"], "r", encoding="utf-8") as f:
                csv_data: str = f.read()
            st.download_button(
                "Download Review Ready (CSV)",
                data=csv_data,
                file_name=f"review_ready_{selected_run_id}.csv",
                mime="text/csv",
            )

        if os.path.exists(artifact_paths["review_ready_parquet"]):
            with open(artifact_paths["review_ready_parquet"], "rb") as f:  # type: ignore
                parquet_data: bytes = f.read()  # type: ignore
            st.download_button(
                "Download Review Ready (Parquet)",
                data=parquet_data,
                file_name=f"review_ready_{selected_run_id}.parquet",
                mime="application/octet-stream",
            )

        # Review meta
        if os.path.exists(artifact_paths["review_meta"]):
            with open(artifact_paths["review_meta"], "r", encoding="utf-8") as f:
                meta_data: str = f.read()
            st.download_button(
                "Download Review Meta (JSON)",
                data=meta_data,
                file_name=f"review_meta_{selected_run_id}.json",
                mime="application/json",
            )

    # Phase 1.17.2: CLI Command Builder
    with st.sidebar.expander("Run Pipeline → CLI Builder", expanded=False):
        from src.utils.cli_builder import (
            get_available_input_files,
            get_available_config_files,
            validate_cli_args,
            build_cli_command,
            get_known_run_ids,
        )

        # Input & Output
        st.write("**Input & Output**")
        input_files = get_available_input_files()
        if not input_files:
            st.error("No CSV files found in data/raw/")
            return

        input_file = st.selectbox(
            "Input CSV File",
            options=input_files,
            help="Select input CSV file from data/raw/",
        )

        config_files = get_available_config_files()
        if not config_files:
            st.error("No YAML files found in config/")
            return

        config_file = st.selectbox(
            "Config File",
            options=config_files,
            index=(
                config_files.index("settings.yaml")
                if "settings.yaml" in config_files
                else 0
            ),
            help="Select configuration file from config/",
        )

        st.write("**Output:** data/processed (fixed)")

        # Parallelism
        st.write("**Parallelism**")
        no_parallel = st.checkbox(
            "--no-parallel", value=False, help="Disable parallel execution"
        )

        col1, col2 = st.columns(2)
        with col1:
            workers = st.number_input(
                "--workers",
                min_value=1,
                max_value=32,
                value=4,
                disabled=no_parallel,
                help="Number of parallel workers",
            )

        with col2:
            parallel_backend = st.selectbox(
                "--parallel-backend",
                options=["loky", "threading"],
                index=0,
                disabled=no_parallel,
                help="Parallel execution backend",
            )

        chunk_size = st.number_input(
            "--chunk-size",
            min_value=1,
            max_value=10000,
            value=1000,
            disabled=no_parallel,
            help="Chunk size for parallel processing",
        )

        # Run Control
        st.write("**Run Control**")
        no_resume = st.checkbox(
            "--no-resume", value=False, help="Disable resume functionality"
        )
        keep_runs = st.number_input(
            "--keep-runs",
            min_value=1,
            max_value=100,
            value=10,
            help="Number of completed runs to keep",
        )

        # Advanced Options
        with st.expander("Advanced Options", expanded=False):
            st.warning(
                "⚠️ Custom run ID will override timestamp uniqueness and may overwrite previous artifacts"
            )

            run_id_option = st.radio(
                "Run ID",
                options=["Auto-generate", "Custom"],
                help="Choose run ID generation method",
            )

            if run_id_option == "Custom":
                known_run_ids = get_known_run_ids()
                if known_run_ids:
                    run_id = st.selectbox(
                        "Select existing run ID",
                        options=known_run_ids,
                        help="This will overwrite the selected run",
                    )
                else:
                    run_id = st.text_input(
                        "Custom run ID",
                        help="Enter custom run ID (will overwrite if exists)",
                    )
            else:
                run_id = None

            extra_args = st.text_input(
                "Extra Arguments",
                help="Additional CLI arguments (space-separated)",
            )

        # Validation
        validation_errors = validate_cli_args(
            input_file=input_file,
            config=config_file,
            no_parallel=no_parallel,
            workers=workers if not no_parallel else None,
            parallel_backend=parallel_backend,
            chunk_size=chunk_size if not no_parallel else None,
            no_resume=no_resume,
            run_id=run_id,
            keep_runs=keep_runs,
        )

        # Show validation errors
        if validation_errors:
            st.error("**Validation Errors:**")
            for field, error in validation_errors.items():
                st.error(f"• {field}: {error}")

        # Build command
        command = build_cli_command(
            input_file=input_file,
            config=config_file,
            no_parallel=no_parallel,
            workers=workers if not no_parallel else None,
            parallel_backend=parallel_backend,
            chunk_size=chunk_size if not no_parallel else None,
            no_resume=no_resume,
            run_id=run_id,
            keep_runs=keep_runs,
            extra_args=extra_args,
        )

        # Command output
        st.write("**Generated Command:**")
        st.code(command, language="bash")

        # Copy and download buttons
        col1, col2 = st.columns(2)
        with col1:
            st.button(
                "📋 Copy Command",
                on_click=lambda: st.write("Command copied to clipboard!"),
                disabled=bool(validation_errors),
                help="Copy command to clipboard",
            )

        with col2:
            # Create shell script
            shell_script = f"#!/usr/bin/env bash\n\n{command}\n"
            st.download_button(
                "📥 Download .sh",
                data=shell_script,
                file_name=f"run_pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.sh",
                mime="text/plain",
                disabled=bool(validation_errors),
                help="Download as shell script",
            )

    # Phase 1.17.2: Run Maintenance
    with st.sidebar.expander("Run Maintenance", expanded=False):
        from src.utils.cache_utils import (
            preview_delete_runs,
            delete_runs,
            list_runs_sorted,
        )

        # Destructive actions fuse
        if "enable_destructive_actions" not in st.session_state:
            st.session_state.enable_destructive_actions = False

        st.session_state.enable_destructive_actions = st.checkbox(
            "⚠️ Enable destructive actions",
            value=st.session_state.enable_destructive_actions,
            help="Required to perform run deletions",
        )

        if not st.session_state.enable_destructive_actions:
            st.info("Check the box above to enable run maintenance features")
            return

        # Get runs for selection
        runs_sorted = list_runs_sorted()
        if not runs_sorted:
            st.info("No runs available for maintenance")
            return

        # Run selection
        st.write("**Select Runs to Delete:**")

        # Create selection options
        run_options = []
        run_display_names = []

        for run_id, run_data in runs_sorted:
            status = run_data.get("status", "unknown")
            status_icon = get_run_status_icon(status)
            display_name = format_run_display_name(run_id, run_data)

            # Disable running runs
            if status == "running":
                display_name = f"🚫 {display_name} (Running - cannot delete)"

            run_options.append(run_id)
            run_display_names.append(f"{status_icon} {display_name}")

        selected_runs = st.multiselect(
            "Runs to delete",
            options=run_display_names,
            help="Select one or more runs to delete",
        )

        # Extract run IDs
        selected_run_ids = []
        for display_name in selected_runs:
            idx = run_display_names.index(display_name)
            run_id = run_options[idx]
            run_data = runs_sorted[idx][1]

            # Skip running runs
            if run_data.get("status") == "running":
                continue

            selected_run_ids.append(run_id)

        # Preview button
        if st.button("🔍 Preview Deletion", disabled=not selected_run_ids):
            with st.spinner("Calculating deletion preview..."):
                preview = preview_delete_runs(selected_run_ids)

                if preview["runs_inflight"]:
                    st.error(
                        f"Cannot delete running runs: {', '.join(preview['runs_inflight'])}"
                    )
                    return

                if preview["runs_not_found"]:
                    st.warning(
                        f"Runs not found: {', '.join(preview['runs_not_found'])}"
                    )

                if preview["runs_to_delete"]:
                    st.write("**Runs to be deleted:**")
                    total_bytes = 0
                    for run_info in preview["runs_to_delete"]:
                        run_id = run_info["run_id"]
                        bytes_size = run_info["bytes"]
                        file_count = len(run_info["files"])
                        total_bytes += bytes_size

                        st.write(
                            f"• {run_id} ({file_count} files, {bytes_size:,} bytes)"
                        )

                    st.write(
                        f"**Total:** {len(preview['runs_to_delete'])} runs, {total_bytes:,} bytes"
                    )

                    if preview["latest_affected"]:
                        st.warning("⚠️ This will affect the latest pointer")

                    # Store preview for deletion
                    st.session_state.deletion_preview = preview
                    st.session_state.preview_performed = True

        # Delete button (only after preview)
        if st.session_state.get("preview_performed", False) and selected_run_ids:
            st.write("**Confirm Deletion:**")

            # Confirmation checkbox
            confirm_checkbox = st.checkbox(
                "I understand this permanently deletes data",
                value=False,
                help="Required confirmation for deletion",
            )

            # Typed confirmation
            if len(selected_run_ids) == 1:
                confirmation_text = f"Type '{selected_run_ids[0]}' to confirm"
                expected_confirmation = selected_run_ids[0]
            else:
                confirmation_text = "Type 'DELETE ALL' to confirm"
                expected_confirmation = "DELETE ALL"

            typed_confirmation = st.text_input(
                confirmation_text,
                help="Type the exact text to confirm deletion",
            )

            # Delete button
            if confirm_checkbox and typed_confirmation == expected_confirmation:
                if st.button("🗑️ Delete Selected Runs", type="primary"):
                    with st.spinner("Deleting runs..."):
                        results = delete_runs(selected_run_ids)

                        if results["inflight_blocked"]:
                            st.error(
                                f"Cannot delete running runs: {', '.join(results['inflight_blocked'])}"
                            )
                        else:
                            st.success(f"Deleted {len(results['deleted'])} runs")

                            if results["latest_reassigned"]:
                                st.info(
                                    f"Latest pointer reassigned to: {results['new_latest']}"
                                )

                            if results["total_bytes_freed"] > 0:
                                st.info(f"Freed {results['total_bytes_freed']:,} bytes")

                            # Clear session state
                            st.session_state.preview_performed = False
                            st.session_state.deletion_preview = None

                            # Invalidate cache to refresh run list
                            st.session_state.cached_data = {}
                            st.rerun()
            else:
                st.info(
                    "Check the confirmation box and type the exact text to enable deletion"
                )

        # Quick actions
        st.write("**Quick Actions:**")

        # Get completed runs
        completed_runs = [
            run_id
            for run_id, run_data in runs_sorted
            if run_data.get("status") == "complete"
        ]
        latest_run = get_default_run_id()

        if len(completed_runs) > 1 and latest_run in completed_runs:
            if st.button("🗑️ Delete all except latest"):
                runs_to_delete = [
                    run_id for run_id in completed_runs if run_id != latest_run
                ]
                if runs_to_delete:
                    with st.spinner("Deleting old runs..."):
                        results = delete_runs(runs_to_delete)
                        st.success(f"Deleted {len(results['deleted'])} old runs")
                        st.session_state.cached_data = {}
                        st.rerun()

        if len(runs_sorted) > 1:
            if st.button("🗑️ Delete all runs"):
                all_run_ids = [run_id for run_id, _ in runs_sorted]
                with st.spinner("Deleting all runs..."):
                    results = delete_runs(all_run_ids)
                    st.success(f"Deleted {len(results['deleted'])} runs")
                    st.session_state.cached_data = {}
                    st.rerun()

    # Load review data with caching
    if selected_run_id != st.session_state.cached_run_id:
        # Clear cache and load new data
        st.session_state.cached_data = {}
        st.session_state.cached_run_id = selected_run_id

    # Load data if not cached
    if selected_run_id not in st.session_state.cached_data:
        df = load_review_data(selected_run_id)
        if df is not None:
            st.session_state.cached_data[selected_run_id] = df
        else:
            st.error(f"Failed to load data for run {selected_run_id}")
            return
    else:
        df = st.session_state.cached_data[selected_run_id]
        st.success(f"Loaded {len(df)} records from cache (run {selected_run_id})")

    # Load settings for rules panel
    settings = load_settings()

    # Import manual data functions
    from app.manual_data import export_manual_data

    # Import centralized manual I/O
    from src.manual_io import (
        load_manual_blacklist as load_manual_blacklist_io,
        save_manual_blacklist,
        upsert_manual_override,
        get_manual_override,
    )

    # Import disposition functions for blacklist
    from src.disposition import get_blacklist_terms

    if df is None:
        st.warning(
            f"""
        ## No Review Data Found for Run {selected_run_id}
        
        Please run the pipeline first to generate review data:
        
        ```bash
        python src/cleaning.py --input data/raw/company_junction_range_01.csv --outdir data/processed --config config/settings.yaml
        ```
        
        This will create run-scoped artifacts under `data/processed/{{run_id}}/` for review.
        """
        )
        return

    # Minimal Rules & Settings panel
    with st.expander("Rules & Settings", expanded=False):
        st.write("**Similarity Thresholds**")
        high_threshold = settings.get("similarity", {}).get("high", "Not configured")
        medium_threshold = settings.get("similarity", {}).get(
            "medium", "Not configured"
        )
        st.write(f"High: {high_threshold}, Medium: {medium_threshold}")

        st.write("**Alias Extraction Rules**")
        st.markdown("- Semicolons split multiple entities")
        st.markdown("- Numbered sequences like `(1)`, `(2)` denote separate entities")
        st.markdown(
            "- Parentheses evaluated conservatively (legal suffixes or multiple caps)"
        )

        st.write("**Manual Overrides**")
        st.markdown(
            "Manual overrides and manual blacklist (if present) are applied from `data/manual/` during pipeline runs."
        )

        # Export manual data
        dispositions_json, blacklist_json = export_manual_data()
        col1, col2 = st.columns(2)
        with col1:
            st.download_button(
                "Download Manual Dispositions",
                dispositions_json,
                file_name="manual_dispositions.json",
                mime="application/json",
            )
        with col2:
            st.download_button(
                "Download Manual Blacklist",
                blacklist_json,
                file_name="manual_blacklist.json",
                mime="application/json",
            )

    # Pipeline Launcher (Phase 1.9)
    # Generate and copy pipeline commands for easy execution without subprocess security risks
    with st.sidebar.expander("Run Pipeline", expanded=False):
        st.write("**Generate pipeline command**")

        # List available CSV files in data/raw/
        raw_dir = Path("data/raw")
        csv_files = []
        if raw_dir.exists():
            csv_files = [f.name for f in raw_dir.glob("*.csv")]

        if csv_files:
            # Sort by modification time (newest first)
            csv_files.sort(
                key=lambda f: raw_dir.joinpath(f).stat().st_mtime, reverse=True
            )

            selected_file = st.selectbox("Select input CSV:", csv_files, index=0)

            # Generate command
            command = f"python src/cleaning.py --input data/raw/{selected_file} --outdir data/processed --config config/settings.yaml"

            # Display command in a clean, copyable format
            st.write("**Pipeline Command:**")

            # Use a text area for better display and copying
            st.text_area(
                "Copy this command:",
                value=command,
                height=100,
                key="pipeline_command",
                help="Select all text (Cmd+A) then copy (Cmd+C)",
            )

            # Copy button with clear instructions
            if st.button("📋 Copy to Clipboard", type="primary"):
                st.success("✅ **Command copied!**")
                st.info(
                    "🔄 **After running the pipeline:** The app will automatically load the new results. If needed, refresh the page or press 'R' to reload."
                )

            # Alternative: Use a download button that creates a temporary file
            st.download_button(
                "📄 Download Command as Text File",
                data=command,
                file_name="pipeline_command.txt",
                mime="text/plain",
                help="Download the command as a text file, then copy from the file",
            )

            st.write(
                "💡 **Tip:** You can also select the command above and use Cmd+A (select all) then Cmd+C (copy)"
            )
        else:
            st.warning("No CSV files found in `data/raw/`")
            st.info("Place your input CSV files in the `data/raw/` directory.")

    # Sidebar filters
    st.sidebar.header("Filters")

    # Min score filter (removed duplicate - keeping the one near group size filter)

    # Disposition filter
    dispositions = df["Disposition"].unique()

    # Initialize session state for disposition filter
    if "selected_disposition" not in st.session_state:
        st.session_state.selected_disposition = list(dispositions)

    selected_dispositions = st.sidebar.multiselect(
        "Disposition",
        options=dispositions,
        default=st.session_state.selected_disposition,
    )

    # Update session state
    st.session_state.selected_disposition = selected_dispositions

    # Suffix mismatch filter
    show_suffix_mismatch = st.sidebar.checkbox(
        "Show Suffix Mismatches Only",
        value=False,
        help="Show groups where members disagree on legal suffix (e.g., INC vs LLC)",
    )

    # Alias filter
    has_aliases = st.sidebar.checkbox(
        "Has Aliases",
        value=False,
        help="Show groups/records with alias candidates (semicolon/numbered/filtered parentheses)",
    )

    # Blacklist Visibility and Management (Phase 1.8/1.9)
    # Three-pane view with word-boundary matching and centralized I/O
    with st.sidebar.expander("Blacklist Management", expanded=False):
        # Get all blacklist terms
        builtin_terms = set(get_blacklist_terms())
        manual_terms = load_manual_blacklist_io()
        effective_terms = sorted(builtin_terms | manual_terms)

        # Built-in blacklist (read-only)
        st.caption(f"**Built-in terms (read-only) — {len(builtin_terms)}**")
        if builtin_terms:
            st.write(", ".join(sorted(builtin_terms)))
        else:
            st.write("— none —")

        st.divider()

        # Manual blacklist (editable)
        st.caption(f"**Manual terms (editable) — {len(manual_terms)}**")
        if manual_terms:
            for term in sorted(manual_terms):
                col1, col2 = st.columns([3, 1])
                with col1:
                    st.write(f"• {term}")
                with col2:
                    if st.button("Remove", key=f"remove_{term}"):
                        # Remove from manual blacklist
                        manual_terms.discard(term)
                        save_manual_blacklist(manual_terms)
                        st.rerun()
        else:
            st.write("No manual terms")

        # Add new manual term
        new_term = st.text_input("Add manual term:", key="new_blacklist_term")
        if st.button("Add Term") and new_term.strip():
            # Add to manual blacklist
            manual_terms.add(new_term.strip().lower())
            save_manual_blacklist(manual_terms)
            st.rerun()

        st.divider()

        # Effective blacklist (union view)
        st.caption(f"**Effective terms (used by pipeline) — {len(effective_terms)}**")
        if effective_terms:
            st.write(", ".join(effective_terms))
        else:
            st.write("— none —")

    # Group size filter
    group_sizes = df["group_id"].value_counts()
    min_group_size = st.sidebar.slider(
        "Minimum Group Size", min_value=1, max_value=int(group_sizes.max()), value=1
    )

    # Edge strength filter (Phase 1.11)
    if "weakest_edge_to_primary" in df.columns:
        min_edge_strength = st.sidebar.slider(
            "Minimum Edge to Primary",
            min_value=0.0,
            max_value=100.0,
            value=0.0,
            step=1.0,
            help="Filter groups where weakest edge to primary is below this threshold",
        )
    else:
        min_edge_strength = 0.0

    # Apply filters
    filtered_df = df.copy()

    # Apply edge strength filter (Phase 1.11)
    if min_edge_strength > 0.0 and "weakest_edge_to_primary" in filtered_df.columns:
        # Filter for groups where any member has edge strength below threshold
        weak_edge_groups = []
        for group_id in filtered_df["group_id"].unique():
            group_data = filtered_df[filtered_df["group_id"] == group_id]
            if group_data["weakest_edge_to_primary"].min() < min_edge_strength:
                weak_edge_groups.append(group_id)
        filtered_df = filtered_df[filtered_df["group_id"].isin(weak_edge_groups)]

    if selected_dispositions:
        filtered_df = filtered_df[
            filtered_df["Disposition"].isin(selected_dispositions)
        ]

    if show_suffix_mismatch:
        # Filter for groups with suffix mismatches
        suffix_mismatch_groups = []
        for group_id in filtered_df["group_id"].unique():
            group_data = filtered_df[filtered_df["group_id"] == group_id]
            suffix_classes = group_data["suffix_class"].unique()
            if len(suffix_classes) > 1:
                suffix_mismatch_groups.append(group_id)
        filtered_df = filtered_df[filtered_df["group_id"].isin(suffix_mismatch_groups)]

    if has_aliases:
        # Filter for records with aliases
        if "alias_cross_refs" in filtered_df.columns:
            filtered_df = filtered_df[
                filtered_df["alias_cross_refs"].apply(lambda x: len(x) > 0)
            ]
        elif "alias_candidates" in filtered_df.columns:
            # Fallback to alias_candidates if alias_cross_refs not available
            filtered_df = filtered_df[
                filtered_df["alias_candidates"].apply(
                    lambda x: len(x) > 0 if isinstance(x, list) else False
                )
            ]

    # Filter by group size
    group_sizes_filtered = filtered_df["group_id"].value_counts()
    valid_groups = group_sizes_filtered[group_sizes_filtered >= min_group_size].index
    filtered_df = filtered_df[filtered_df["group_id"].isin(valid_groups)]

    # Display summary
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Records", len(filtered_df))
    with col2:
        st.metric("Groups", len(filtered_df["group_id"].unique()))
    with col3:
        st.metric("Primary Records", filtered_df["is_primary"].sum())
    with col4:
        st.metric(
            "Avg Edge to Primary",
            f"{filtered_df['weakest_edge_to_primary'].mean():.1f}",
        )

    # Display disposition summary
    st.subheader("Disposition Summary")
    disposition_counts = filtered_df["Disposition"].value_counts()

    # Create compact table instead of chart
    disposition_table = pd.DataFrame(
        {
            "Disposition": disposition_counts.index,
            "Count": disposition_counts.values,
            "Percent": (
                disposition_counts.values.astype(float) / len(filtered_df) * 100
            ).round(1),
        }
    )

    # Add clickable filter buttons
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        if st.button(f"Keep ({disposition_counts.get('Keep', 0)})"):
            st.session_state.selected_disposition = ["Keep"]
    with col2:
        if st.button(f"Update ({disposition_counts.get('Update', 0)})"):
            st.session_state.selected_disposition = ["Update"]
    with col3:
        if st.button(f"Delete ({disposition_counts.get('Delete', 0)})"):
            st.session_state.selected_disposition = ["Delete"]
    with col4:
        if st.button(f"Verify ({disposition_counts.get('Verify', 0)})"):
            st.session_state.selected_disposition = ["Verify"]

    # Show compact table
    st.dataframe(disposition_table, width="stretch", hide_index=True)

    # Display groups
    st.subheader("Duplicate Groups")

    # Sorting controls
    st.sidebar.write("**Sorting**")
    sort_by = st.sidebar.selectbox(
        "Sort Groups By",
        [
            "Group Size (Desc)",
            "Group Size (Asc)",
            "Max Score (Desc)",
            "Max Score (Asc)",
            "Account Name (Asc)",
            "Account Name (Desc)",
        ],
        index=0,
    )

    # Apply sorting to groups
    group_stats = []
    for group_id in filtered_df["group_id"].unique():
        group_data = filtered_df[filtered_df["group_id"] == group_id]
        max_score = group_data["weakest_edge_to_primary"].max()
        # Get primary record's account name for sorting
        primary_record = (
            group_data[group_data["is_primary"]].iloc[0]
            if group_data["is_primary"].any()
            else group_data.iloc[0]
        )
        primary_name = primary_record.get("account_name", "")
        group_stats.append(
            {
                "group_id": group_id,
                "size": len(group_data),
                "max_score": max_score,
                "primary_name": primary_name,
            }
        )

    group_stats_df = pd.DataFrame(group_stats)

    if "Group Size" in sort_by:
        if "(Desc)" in sort_by:
            sorted_groups = group_stats_df.sort_values("size", ascending=False)[
                "group_id"
            ].tolist()
        else:
            sorted_groups = group_stats_df.sort_values("size", ascending=True)[
                "group_id"
            ].tolist()
    elif "Max Score" in sort_by:
        if "(Desc)" in sort_by:
            sorted_groups = group_stats_df.sort_values("max_score", ascending=False)[
                "group_id"
            ].tolist()
        else:
            sorted_groups = group_stats_df.sort_values("max_score", ascending=True)[
                "group_id"
            ].tolist()
    else:  # Account Name
        if "(Desc)" in sort_by:
            sorted_groups = group_stats_df.sort_values("primary_name", ascending=False)[
                "group_id"
            ].tolist()
        else:
            sorted_groups = group_stats_df.sort_values("primary_name", ascending=True)[
                "group_id"
            ].tolist()

    # Pagination
    page_size = st.sidebar.selectbox("Page Size", [10, 25, 50, 100], index=1)
    total_groups = len(sorted_groups)

    if "page" not in st.session_state:
        st.session_state.page = 1

    max_page = max(1, (total_groups + page_size - 1) // page_size)

    col1, col2, col3 = st.columns(3)
    with col1:
        if st.button("Prev") and st.session_state.page > 1:
            st.session_state.page -= 1
    with col2:
        st.write(f"Page {st.session_state.page} / {max_page}")
    with col3:
        if st.button("Next") and st.session_state.page < max_page:
            st.session_state.page += 1

    start_idx = (st.session_state.page - 1) * page_size
    end_idx = start_idx + page_size

    # Get groups for current page
    page_groups = sorted_groups[start_idx:end_idx]

    # Group by group_id and display each group
    for group_id in page_groups:
        group_data = filtered_df[filtered_df["group_id"] == group_id].copy()

        # Parse merge preview for this group
        merge_preview = None
        for _, row in group_data.iterrows():
            if row["merge_preview_json"]:
                merge_preview = parse_merge_preview(row["merge_preview_json"])
                break

        # Group header
        primary_record = (
            group_data[group_data["is_primary"]].iloc[0]
            if group_data["is_primary"].any()
            else group_data.iloc[0]
        )

        with st.expander(
            f"Group {group_id}: {primary_record['account_name']} ({len(group_data)} records)"
        ):
            # Group Info at the top
            col1, col2, col3 = st.columns([2, 1, 1])

            with col1:
                # Group badges
                suffix_classes = group_data["suffix_class"].unique()
                if len(suffix_classes) > 1:
                    st.error("⚠️ Suffix Mismatch")
                else:
                    st.success(f"✅ {suffix_classes[0]}")

                # Blacklist hits
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
                if merge_preview and "primary_record" in merge_preview:
                    primary_info = merge_preview["primary_record"]
                    st.write(f"**Primary:** {primary_info.get('account_id', 'N/A')}")
                    st.write(
                        f"**Rank:** {primary_info.get('relationship_rank', 'N/A')}"
                    )

            with col3:
                # Manual override dropdown
                # Get current override for the primary record
                primary_record_id = str(primary_record.name)
                current_override = get_manual_override(primary_record_id)

                if current_override:
                    st.info(f"**Override:** {current_override}")

                override_options = ["No Override", "Keep", "Delete", "Update", "Verify"]
                selected_override = st.selectbox(
                    "Manual Override",
                    override_options,
                    index=(
                        override_options.index(current_override)
                        if current_override
                        else 0
                    ),
                    key=f"override_{group_id}",
                )

                if (
                    selected_override != "No Override"
                    and selected_override != current_override
                ):
                    if st.button("Apply Override", key=f"apply_{group_id}"):
                        upsert_manual_override(
                            record_id=primary_record_id,
                            override=selected_override,
                            reason="Manual override from UI",
                            reviewer="streamlit_user",
                        )
                        st.rerun()

            # Display group table below
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

            # Configure column display for better readability
            column_config = {
                "account_name": st.column_config.TextColumn(
                    "Account Name", width="large", help="Company name", max_chars=None
                ),
                "account_id": st.column_config.TextColumn("Account ID", width="medium"),
                "relationship": st.column_config.TextColumn(
                    "Relationship", width="medium"
                ),
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

            # Explain metadata (Phase 1.11)
            if any(
                col in group_data.columns
                for col in [
                    "group_join_reason",
                    "weakest_edge_to_primary",
                    "shared_tokens_count",
                ]
            ):
                with st.expander("🔍 Explain Metadata", expanded=False):
                    explain_cols = []
                    if "group_join_reason" in group_data.columns:
                        explain_cols.append("group_join_reason")
                    if "weakest_edge_to_primary" in group_data.columns:
                        explain_cols.append("weakest_edge_to_primary")
                    if "shared_tokens_count" in group_data.columns:
                        explain_cols.append("shared_tokens_count")
                    if "applied_penalties" in group_data.columns:
                        explain_cols.append("applied_penalties")
                    if "survivorship_reason" in group_data.columns:
                        explain_cols.append("survivorship_reason")

                    if explain_cols:
                        explain_data = group_data[
                            ["account_name"] + explain_cols
                        ].copy()
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

            # Alias information (if present)
            if "alias_cross_refs" in group_data.columns:
                alias_records = group_data[
                    group_data["alias_cross_refs"].apply(lambda x: len(x) > 0)
                ]
                if not alias_records.empty:
                    st.write("**Alias Cross-links:**")
                    for _, record in alias_records.iterrows():
                        cross_refs = record["alias_cross_refs"]
                        if cross_refs:
                            st.write(f"📎 {len(cross_refs)} cross-links")
                            with st.expander("View cross-links"):
                                for ref in cross_refs:
                                    st.write(
                                        f"• {ref.get('alias', '')} → Group {ref.get('group_id', '')} (score: {ref.get('score', '')}, source: {ref.get('source', '')})"
                                    )

            # Display merge preview if available
            if merge_preview and "field_comparisons" in merge_preview:
                st.write("**Field Conflicts:**")

                conflicts = []
                for field, comparison in merge_preview["field_comparisons"].items():
                    if comparison.get("has_conflict", False):
                        conflicts.append(
                            {
                                "field": field,
                                "primary_value": comparison.get("primary_value", ""),
                                "alternatives": comparison.get(
                                    "alternative_values", []
                                ),
                            }
                        )

                if conflicts:
                    for conflict in conflicts:
                        st.write(f"**{conflict['field']}:**")
                        st.write(f"  Primary: {conflict['primary_value']}")
                        st.write(
                            f"  Alternatives: {', '.join(conflict['alternatives'])}"
                        )
                else:
                    st.success("✅ No field conflicts")

    # Export functionality
    st.subheader("Export")

    if st.button("Export Filtered Data"):
        # Create download link
        csv = filtered_df.to_csv(index=False)
        st.download_button(
            label="Download CSV",
            data=csv,
            file_name=f"filtered_review_data_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv",
        )


if __name__ == "__main__":
    main()
