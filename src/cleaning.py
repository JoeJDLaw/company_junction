"""
Data cleaning module for Salesforce export processing.

This module handles:
- CSV/Excel file loading
- Duplicate detection based on name matching
- Field merging logic
- Data validation and cleaning
- CLI orchestration for end-to-end pipeline
"""

import pandas as pd
import argparse
import sys
import logging
import json
import subprocess
from pathlib import Path
from typing import Dict, Any, Optional
import os
from datetime import datetime

# Import local modules
from src.normalize import normalize_dataframe, excel_serial_to_datetime
from src.similarity import pair_scores, save_candidate_pairs, get_stop_tokens
from src.grouping import create_groups_with_edge_gating
from src.utils.mini_dag import MiniDAG
from src.utils.perf_utils import (
    time_stage,
    track_memory_peak,
    log_performance_summary,
)
from src.utils.cache_utils import (
    generate_run_id,
    create_cache_directories,
    add_run_to_index,
    update_run_status,
    create_latest_pointer,
    prune_old_runs,
)
from src.utils.parallel_utils import create_parallel_executor
from src.utils.resource_monitor import log_resource_summary

# Import ID utilities for Salesforce ID canonicalization
try:
    from src.utils.id_utils import normalize_sfid_series
except ImportError:
    from src.utils.id_utils import normalize_sfid_series
from src.survivorship import (
    select_primary_records,
    generate_merge_preview,
    save_survivorship_results,
)
from src.disposition import apply_dispositions, save_dispositions
from src.alias_matching import (
    compute_alias_matches,
    create_alias_cross_refs,
    save_alias_matches,
)

try:
    from src.utils.io_utils import (
        load_settings,
        load_relationship_ranks,
        read_csv_stable,
    )
    from src.utils.logging_utils import setup_logging
    from src.utils.path_utils import ensure_directory_exists
    from src.utils.perf_utils import log_perf
    from src.utils.dtypes import optimize_dataframe_memory
except ImportError:
    from src.utils.io_utils import (
        load_settings,
        load_relationship_ranks,
        read_csv_stable,
    )
from src.utils.logging_utils import setup_logging
from src.utils.path_utils import ensure_directory_exists
from src.utils.perf_utils import log_perf
from src.utils.dtypes import optimize_dataframe_memory
from src.performance import (
    PerformanceTracker,
    save_performance_summary,
    compute_group_size_histogram,
    compute_block_top_tokens,
)

logger = logging.getLogger(__name__)


def _assert_pairs_cover_accounts(
    pairs: pd.DataFrame, accounts: pd.DataFrame, id_col: str = "account_id"
) -> None:
    """
    Assert that all pair IDs exist in the accounts DataFrame.

    Args:
        pairs: DataFrame with id_a and id_b columns
        accounts: DataFrame with account IDs
        id_col: Name of the ID column in accounts DataFrame
    """
    acc_ids = accounts[id_col].astype("string").fillna("").str.strip()
    a_missing = ~pairs["id_a"].isin(acc_ids)
    b_missing = ~pairs["id_b"].isin(acc_ids)

    a_n = int(a_missing.sum())
    b_n = int(b_missing.sum())
    if a_n or b_n:
        raise KeyError(
            "Referential integrity failure: "
            f"{a_n} id_a and {b_n} id_b values not present in accounts[{id_col}]. "
            f"sample id_a={pairs.loc[a_missing, 'id_a'].head(5).tolist()} "
            f"id_b={pairs.loc[b_missing, 'id_b'].head(5).tolist()}"
        )


def _create_audit_snapshot(
    settings: Dict[str, Any], alias_stats: Dict[str, Any], output_dir: str
) -> None:
    """
    Create an audit snapshot with run metadata.

    Args:
        settings: Configuration settings
        alias_stats: Alias matching statistics
        output_dir: Output directory path
    """
    try:
        # Get git commit (best effort)
        git_commit = None
        try:
            result = subprocess.run(
                ["git", "rev-parse", "--short", "HEAD"],
                capture_output=True,
                text=True,
                cwd=Path(__file__).parent.parent,
            )
            if result.returncode == 0:
                git_commit = result.stdout.strip()
        except Exception:
            pass

        # Get effective blacklist count
        from src.disposition import get_blacklist_terms
        from src.manual_io import load_manual_blacklist

        builtin_count = len(get_blacklist_terms())
        manual_terms = load_manual_blacklist()
        manual_count = len(manual_terms)
        effective_count = builtin_count + manual_count

        # Get manual overrides count
        from src.manual_io import load_manual_overrides

        overrides = load_manual_overrides()
        override_count = len(overrides)

        # Create audit data
        audit_data = {
            "run_ts": datetime.now().isoformat(),
            "thresholds": settings.get("similarity", {}),
            "effective_blacklist_count": effective_count,
            "builtin_blacklist_count": builtin_count,
            "manual_blacklist_count": manual_count,
            "manual_overrides_applied": override_count,
            "alias_stats": alias_stats or {},
            "git_commit": git_commit,
        }

        # Write audit file
        audit_path = os.path.join(output_dir, "review_meta.json")
        with open(audit_path, "w") as f:
            json.dump(audit_data, f, indent=2)

        logger.info(f"Audit snapshot written to: {audit_path}")

    except Exception as e:
        logger.warning(f"Failed to create audit snapshot: {e}")


def _create_performance_summary_enhanced(
    perf_tracker: PerformanceTracker,
    df_norm: pd.DataFrame,
    pairs_df: pd.DataFrame,
    df_groups: pd.DataFrame,
    df_final: pd.DataFrame,
    output_dir: str,
    run_id: Optional[str] = None,
) -> None:
    """
    Create comprehensive performance summary with the required schema.

    Args:
        perf_tracker: Performance tracker instance
        df_norm: Normalized accounts DataFrame
        pairs_df: Candidate pairs DataFrame
        df_groups: Groups DataFrame
        df_final: Final review-ready DataFrame
        output_dir: Output directory path
    """
    try:
        # Calculate dataset statistics
        dataset_stats = {"rows_in": len(df_norm), "rows_cleaned": len(df_final)}

        # Calculate candidate statistics
        candidate_stats = {
            "pairs_total": len(pairs_df) if not pairs_df.empty else 0,
            "pairs_scored": len(pairs_df) if not pairs_df.empty else 0,
            "pairs_ge_medium": (
                len(pairs_df[pairs_df["score"] >= 84]) if not pairs_df.empty else 0
            ),
            "pairs_ge_high": (
                len(pairs_df[pairs_df["score"] >= 92]) if not pairs_df.empty else 0
            ),
        }

        # Calculate group statistics
        group_stats = {
            "count": df_groups["group_id"].nunique() if not df_groups.empty else 0,
            "size_histogram": compute_group_size_histogram(df_groups),
            "max_group_size": (
                df_groups["group_size"].max() if not df_groups.empty else 0
            ),
        }

        # Calculate block statistics
        block_stats = {
            "top_tokens": (
                compute_block_top_tokens(pairs_df) if not pairs_df.empty else []
            )
        }

        # Generate comprehensive summary
        summary = perf_tracker.generate_summary(
            dataset_stats, candidate_stats, group_stats, block_stats
        )

        # Add run_id to summary if provided
        if run_id:
            summary["run_id"] = run_id

        # Save performance summary
        perf_summary_path = os.path.join(output_dir, "perf_summary.json")
        save_performance_summary(summary, perf_summary_path)

        # Also save a copy to the legacy location for backward compatibility
        legacy_perf_path = "data/processed/perf_summary.json"
        try:
            save_performance_summary(summary, legacy_perf_path)
            logger.info(
                f"Performance summary also saved to legacy location: {legacy_perf_path}"
            )
        except Exception as e:
            logger.warning(f"Failed to save legacy performance summary: {e}")

        logger.info(f"Enhanced performance summary written to: {perf_summary_path}")

    except Exception as e:
        logger.warning(f"Failed to create enhanced performance summary: {e}")
        raise


def load_salesforce_data(file_path: str) -> pd.DataFrame:
    """
    Load Salesforce export data from CSV or Excel file.

    Args:
        file_path: Path to the Salesforce export file

    Returns:
        DataFrame containing the Salesforce data
    """
    if file_path.endswith(".csv"):
        # Use stable CSV reader to avoid dtype warnings
        return read_csv_stable(file_path)
    elif file_path.endswith((".xlsx", ".xls")):
        return pd.read_excel(file_path)
    else:
        raise ValueError(f"Unsupported file format: {file_path}")


def validate_required_columns(df: pd.DataFrame) -> bool:
    """
    Validate that required columns are present.

    Args:
        df: DataFrame to validate

    Returns:
        True if validation passes

    Raises:
        ValueError: If required columns are missing
    """
    required_columns = ["Account ID", "Account Name", "Relationship", "Created Date"]

    # Check for Account Name or fallback to Employer Name
    name_columns = ["Account Name", "Employer Name"]
    has_name_column = any(col in df.columns for col in name_columns)

    if not has_name_column:
        raise ValueError(f"Missing required name column. Need one of: {name_columns}")

    missing_columns = []
    for col in required_columns:
        if col not in df.columns:
            missing_columns.append(col)

    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")

    return True


def run_pipeline(
    input_path: str,
    output_dir: str,
    config_path: str,
    enable_progress: bool = False,
    resume_from: Optional[str] = None,
    no_resume: bool = False,
    force: bool = False,
    state_path: str = "data/interim/pipeline_state.json",
    workers: Optional[int] = None,
    no_parallel: bool = False,
    chunk_size: int = 1000,
    parallel_backend: str = "loky",
    run_id: Optional[str] = None,
    keep_runs: int = 10,
) -> None:
    """
    Run the complete deduplication pipeline.

    Args:
        input_path: Path to input CSV file
        output_dir: Directory for output files
        config_path: Path to configuration file
    """
    logger.info("Starting Company Junction deduplication pipeline")

    # Phase 1.16: Generate run ID and setup cache directories
    if run_id is None:
        run_id = generate_run_id([input_path], [config_path])
    logger.info(f"Using run_id: {run_id}")

    # Create cache directories for this run
    interim_dir, processed_dir = create_cache_directories(run_id)

    # Add run to index
    add_run_to_index(run_id, [input_path], [config_path], "running")

    # Prune old runs
    prune_old_runs(keep_runs)

    # Log resource summary
    log_resource_summary()

    # Load configuration
    settings = load_settings(config_path)
    relationship_ranks = load_relationship_ranks("config/relationship_ranks.csv")

    # Setup logging
    setup_logging(settings.get("logging", {}).get("level", "INFO"))

    # Initialize performance tracker
    perf_tracker = PerformanceTracker()
    perf_tracker.start_run(settings)

    # Initialize MiniDAG for stage tracking with run-scoped state file
    run_scoped_state_path = f"{interim_dir}/pipeline_state.json"
    dag = MiniDAG(Path(run_scoped_state_path), run_id)

    # Register pipeline stages
    stages = [
        "normalization",
        "filtering",
        "candidate_generation",
        "grouping",
        "survivorship",
        "disposition",
        "alias_matching",
        "final_output",
    ]

    for stage in stages:
        dag.register(stage)

    # Phase 1.16: Initialize parallel executor
    parallel_executor = create_parallel_executor(
        workers=workers,
        backend=parallel_backend,
        chunk_size=chunk_size,
        small_input_threshold=settings.get("parallelism", {}).get(
            "small_input_threshold", 10000
        ),
        disable_parallel=no_parallel,
    )

    # Ensure output directories exist
    ensure_directory_exists(output_dir)
    ensure_directory_exists("data/interim")

    # Smart auto-resume logic with enhanced logging
    if no_resume:
        logger.info(
            "Auto-resume decision: --no-resume specified - forcing full run | reason=NO_RESUME_FLAG"
        )
        resume_from = None
    elif resume_from is None:
        # Auto-detect resume point
        auto_resume_stage = dag.get_smart_resume_stage()
        if auto_resume_stage:
            # Validate input hash invariance
            input_path_obj = Path(input_path)
            config_path_obj = Path(config_path)

            if dag._validate_input_invariance(input_path_obj, config_path_obj):
                resume_from = auto_resume_stage
                logger.info(
                    f"Auto-resume decision: resume_from='{resume_from}' | last_completed='{dag.get_last_completed_stage()}' | input_hash=PASS | reason=SMART_DETECT"
                )
            else:
                logger.info(
                    "Auto-resume decision: input_hash=FAIL - forcing full run due to input/config changes | reason=HASH_MISMATCH"
                )
                resume_from = None
        else:
            logger.info(
                "Auto-resume decision: no previous run found - starting fresh | reason=NO_PREVIOUS_RUN"
            )
            resume_from = None
    else:
        # Manual resume-from specified - validate input hash
        input_path_obj = Path(input_path)
        config_path_obj = Path(config_path)

        if not dag._validate_input_invariance(input_path_obj, config_path_obj):
            if force:  # Use force flag instead of enable_progress
                logger.warning(
                    "Input hash mismatch detected but --force specified - proceeding with resume | reason=FORCE_OVERRIDE"
                )
            else:
                logger.error(
                    "Input hash mismatch detected. Use --force to override or run without --resume-from | reason=HASH_MISMATCH_NO_FORCE"
                )
                sys.exit(1)
        else:
            logger.info(
                f"Manual resume decision: resume_from='{resume_from}' | input_hash=PASS | reason=MANUAL_OVERRIDE"
            )

    # Update state metadata with current run
    cmdline = f"python src/cleaning.py --input {input_path} --outdir {output_dir} --config {config_path}"
    dag._update_state_metadata(Path(input_path), Path(config_path), cmdline)

    # Add stop flag to parallel executor for graceful interruption
    import threading

    stop_flag = threading.Event()
    parallel_executor.stop_flag = stop_flag

    try:
        # Step 1: Load and validate data
        logger.info(f"Loading data from {input_path}")
        df = load_salesforce_data(input_path)

        # If resuming from a later stage, load intermediate data
        if resume_from and resume_from != "normalization":
            logger.info(f"Resuming from stage: {resume_from}")
            interim_format = settings.get("io", {}).get("interim_format", "parquet")

            # Load normalized data
            normalized_path = f"data/interim/accounts_normalized.{interim_format}"
            if Path(normalized_path).exists():
                logger.info(f"Loading normalized data from {normalized_path}")
                if interim_format == "parquet":
                    df_norm = pd.read_parquet(normalized_path)
                else:
                    df_norm = pd.read_csv(normalized_path)
                logger.info(f"Loaded {len(df_norm)} normalized records")
            else:
                raise FileNotFoundError(
                    f"Required intermediate file not found: {normalized_path}"
                )

            # Load candidate pairs if needed
            if resume_from in [
                "grouping",
                "survivorship",
                "disposition",
                "alias_matching",
                "final_output",
            ]:
                pairs_path = f"data/interim/candidate_pairs.{interim_format}"
                if Path(pairs_path).exists():
                    logger.info(f"Loading candidate pairs from {pairs_path}")
                    pairs_df = pd.read_parquet(pairs_path)
                    logger.info(f"Loaded {len(pairs_df)} candidate pairs")
                else:
                    raise FileNotFoundError(
                        f"Required intermediate file not found: {pairs_path}"
                    )

            # Load groups if needed
            if resume_from in [
                "survivorship",
                "disposition",
                "alias_matching",
                "final_output",
            ]:
                groups_path = f"data/interim/groups.{interim_format}"
                if Path(groups_path).exists():
                    logger.info(f"Loading groups from {groups_path}")
                    df_groups = pd.read_parquet(groups_path)
                    logger.info(f"Loaded {len(df_groups)} group records")
                else:
                    raise FileNotFoundError(
                        f"Required intermediate file not found: {groups_path}"
                    )

            # Load survivorship results if needed
            if resume_from in ["disposition", "alias_matching", "final_output"]:
                survivorship_path = f"data/interim/survivorship.{interim_format}"
                if Path(survivorship_path).exists():
                    logger.info(
                        f"Loading survivorship results from {survivorship_path}"
                    )
                    df_primary = pd.read_parquet(survivorship_path)
                    logger.info(f"Loaded {len(df_primary)} survivorship records")
                else:
                    raise FileNotFoundError(
                        f"Required intermediate file not found: {survivorship_path}"
                    )

            # Load dispositions if needed
            if resume_from in ["alias_matching", "final_output"]:
                dispositions_path = f"data/interim/dispositions.{interim_format}"
                if Path(dispositions_path).exists():
                    logger.info(f"Loading dispositions from {dispositions_path}")
                    df_dispositions = pd.read_parquet(dispositions_path)
                    logger.info(f"Loaded {len(df_dispositions)} disposition records")
                else:
                    raise FileNotFoundError(
                        f"Required intermediate file not found: {dispositions_path}"
                    )

            # Load alias matches if needed
            if resume_from == "final_output":
                alias_matches_path = f"data/interim/alias_matches.{interim_format}"
                if Path(alias_matches_path).exists():
                    logger.info(f"Loading alias matches from {alias_matches_path}")
                    df_alias_matches = pd.read_parquet(alias_matches_path)
                    logger.info(f"Loaded {len(df_alias_matches)} alias matches")
                else:
                    raise FileNotFoundError(
                        f"Required intermediate file not found: {alias_matches_path}"
                    )

        # Validate required columns
        validate_required_columns(df)

        # Standardize column names (Salesforce format → internal format)
        SALESFORCE_TO_INTERNAL = {
            "Account ID": "account_id",
            "Account Name": "account_name",
            "Created Date": "created_date",
            "Relationship": "relationship",
        }

        df = df.rename(columns=SALESFORCE_TO_INTERNAL)

        # Preserve original account_id as account_id_src for audit trail
        df["account_id_src"] = df["account_id"].astype("string").fillna("").str.strip()

        # Canonicalize Salesforce IDs to 18-character form
        logger.info("Canonicalizing Salesforce IDs to 18-character form")
        df["account_id"] = normalize_sfid_series(df["account_id_src"])

        # Remove duplicate account_id records (keep first occurrence)
        initial_count = len(df)
        df = df.drop_duplicates(subset=["account_id"], keep="first")
        if len(df) < initial_count:
            logger.warning(
                f"Removed {initial_count - len(df)} duplicate account_id records after canonicalization"
            )

        # Handle Excel serial dates
        if "created_date" in df.columns:
            df["created_date"] = df["created_date"].apply(excel_serial_to_datetime)

        logger.info(f"Loaded {len(df)} records")

        # Step 2: Normalize data
        if not resume_from or resume_from == "normalization":
            logger.info("[stage:start] normalization")
            dag.start("normalization")

            logger.info("Normalizing company names")
            name_column = "account_name"  # Use standardized column name
            with log_perf("normalization"):
                df_norm = normalize_dataframe(df, name_column)

                # Add name_core_tokens column for edge-gating
                import json

                def create_tokens(x: Any) -> str:
                    if pd.notna(x):
                        x_str = str(x)
                        if x_str.strip():
                            return json.dumps(x_str.split())
                    return "[]"

                df_norm["name_core_tokens"] = df_norm["name_core"].apply(create_tokens)

                perf_tracker.record_timing(
                    "clean_normalize", 0.0
                )  # Will be updated by log_perf

            dag.complete("normalization")
            logger.info("[stage:end] normalization")
        elif resume_from and resume_from != "normalization":
            logger.info("Skipping normalization stage (resuming from {resume_from})")

        # Step 2.5: Filter out problematic records for similarity analysis
        if not resume_from or resume_from == "filtering":
            logger.info("[stage:start] filtering")
            dag.start("filtering")

            logger.info("Filtering data for similarity analysis")
            initial_count = len(df_norm)

        # Filter out records with empty or problematic name_core
        df_norm = df_norm[df_norm["name_core"].str.strip() != ""].copy()

        # Enhanced problematic patterns (case-insensitive, whole-token match)
        problematic_patterns = [
            r"^\d+$",  # Numeric only (e.g., "123", "999")
            r"^[A-Za-z]$",  # Single character (e.g., "a", "x")
            r"^(test|sample|temp|unknown|n/?a|none|tbd)$",  # Common placeholders
            r"^1099$",  # Tax form references
        ]

        import re

        mask = (
            df_norm["name_core"]
            .str.lower()
            .str.strip()
            .apply(
                lambda x: not any(
                    re.match(pattern, x) for pattern in problematic_patterns
                )
            )
        )
        df_norm = df_norm[mask].copy()

        filtered_count = len(df_norm)
        logger.info(
            f"Filtered {initial_count - filtered_count} problematic records, {filtered_count} remaining"
        )

        if filtered_count == 0:
            raise ValueError(
                "No valid company names found after filtering. Check your data quality."
            )

        # Save filtered data
        interim_format = settings.get("io", {}).get("interim_format", "parquet")
        filtered_path = f"{interim_dir}/accounts_filtered.{interim_format}"
        if interim_format == "parquet":
            df_norm.to_parquet(filtered_path, index=False)
        else:
            df_norm.to_csv(filtered_path, index=False)
        logger.info(f"Saved filtered data to {filtered_path}")

        dag.complete("filtering")
        logger.info("[stage:end] filtering")

        # Step 3: Generate candidate pairs
        if not resume_from or resume_from == "candidate_generation":
            logger.info("[stage:start] candidate_generation")
            dag.start("candidate_generation")

            logger.info("Generating candidate pairs")
            with time_stage("candidate_generation", logger):
                with track_memory_peak("candidate_generation", logger):
                    pairs_df = pair_scores(
                        df_norm,
                        settings,
                        enable_progress,
                        parallel_executor,
                        interim_dir,
                    )
                    perf_tracker.record_timing("blocking", 0.0)  # Blocking phase
                    perf_tracker.record_timing("scoring", 0.0)  # Scoring phase

        # Apply memory optimization to pairs
        pairs_df = optimize_dataframe_memory(pairs_df, "candidate_pairs")

        # Standardize candidate pair IDs to match account IDs
        pairs_df = pairs_df.copy()
        pairs_df["id_a"] = pairs_df["id_a"].astype("string").fillna("").str.strip()
        pairs_df["id_b"] = pairs_df["id_b"].astype("string").fillna("").str.strip()

        # Verify referential integrity
        _assert_pairs_cover_accounts(pairs_df, df_norm, id_col="account_id")

        # Save candidate pairs
        pairs_path = f"{interim_dir}/candidate_pairs.{interim_format}"
        save_candidate_pairs(pairs_df, pairs_path)

        dag.complete("candidate_generation")
        logger.info("[stage:end] candidate_generation")

        # Step 4: Build groups with edge-gating
        if not resume_from or resume_from == "grouping":
            logger.info("[stage:start] grouping")
            dag.start("grouping")

            logger.info("Building duplicate groups with edge-gating")

        # Get stop tokens for edge-gating
        stop_tokens = get_stop_tokens()
        logger.info(f"Stop tokens: {stop_tokens}")

        with time_stage("grouping", logger):
            with track_memory_peak("grouping", logger):
                logger.info("About to call create_groups_with_edge_gating")
                df_groups = create_groups_with_edge_gating(
                    df_norm, pairs_df, settings, stop_tokens, enable_progress
                )
                logger.info(
                    f"create_groups_with_edge_gating returned: {type(df_groups)}"
                )
                if df_groups is not None:
                    logger.info(f"df_groups shape: {df_groups.shape}")
                perf_tracker.record_timing(
                    "grouping", 0.0
                )  # Will be updated by log_perf

        # Apply memory optimization to groups
        df_groups = optimize_dataframe_memory(df_groups, "groups")

        # Save groups
        groups_path = f"{interim_dir}/groups.{interim_format}"
        df_groups.to_parquet(groups_path, index=False)
        logger.info(f"Saved groups to {groups_path}")

        dag.complete("grouping")
        logger.info("[stage:end] grouping")

        # Step 5: Select primary records
        if not resume_from or resume_from == "survivorship":
            logger.info("[stage:start] survivorship")
            dag.start("survivorship")

            logger.info("Selecting primary records")
        logger.info(f"df_groups shape: {df_groups.shape}")
        logger.info(f"df_groups columns: {list(df_groups.columns)}")
        logger.info(f"df_groups sample: {df_groups.head(2).to_dict()}")

        with time_stage("survivorship", logger):
            with track_memory_peak("survivorship", logger):
                df_primary = select_primary_records(
                    df_groups, relationship_ranks, settings, enable_progress
                )
                perf_tracker.record_timing(
                    "survivorship", 0.0
                )  # Will be updated by log_perf

        # Generate merge preview
        df_primary = generate_merge_preview(df_primary)

        # Save survivorship results
        survivorship_path = f"{interim_dir}/survivorship.{interim_format}"
        save_survivorship_results(df_primary, survivorship_path)

        dag.complete("survivorship")
        logger.info("[stage:end] survivorship")

        # Step 6: Apply dispositions
        if not resume_from or resume_from == "disposition":
            logger.info("[stage:start] disposition")
            dag.start("disposition")

            logger.info("Applying disposition classification")
        with time_stage("disposition", logger):
            with track_memory_peak("disposition", logger):
                df_dispositions = apply_dispositions(df_primary, settings)
                perf_tracker.record_timing(
                    "disposition", 0.0
                )  # Will be updated by log_perf

        # Save dispositions
        dispositions_path = f"{interim_dir}/dispositions.{interim_format}"
        save_dispositions(df_dispositions, dispositions_path)

        dag.complete("disposition")
        logger.info("[stage:end] disposition")

        # Step 7: Compute alias matches and cross-references
        if not resume_from or resume_from == "alias_matching":
            logger.info("[stage:start] alias_matching")
            dag.start("alias_matching")

            logger.info("Computing alias matches and cross-references")
        alias_matches_path = f"{interim_dir}/alias_matches.{interim_format}"
        with log_perf("alias_matching"):
            result = compute_alias_matches(df_norm, df_groups, settings)

        df_alias_matches, alias_stats = result

        save_alias_matches(df_alias_matches, alias_matches_path)

        dag.complete("alias_matching")
        logger.info("[stage:end] alias_matching")

        # Add alias cross-references to dispositions
        df_dispositions = create_alias_cross_refs(df_dispositions, df_alias_matches)

        # Step 8: Create final review-ready output with explain metadata
        if not resume_from or resume_from == "final_output":
            logger.info("[stage:start] final_output")
            dag.start("final_output")

            logger.info("Creating review-ready output with explain metadata")

        # Add explain metadata to final output
        df_final = df_dispositions.copy()

        # Ensure all explain fields are present
        explain_fields = [
            "group_join_reason",
            "weakest_edge_to_primary",
            "shared_tokens_count",
            "applied_penalties",
            "survivorship_reason",
        ]

        for field in explain_fields:
            if field not in df_final.columns:
                df_final[field] = (
                    "" if field in ["applied_penalties", "survivorship_reason"] else 0.0
                )

        # Apply memory optimization to final output
        df_final = optimize_dataframe_memory(df_final, "review_ready")

        review_path = os.path.join(processed_dir, "review_ready.csv")
        df_final.to_csv(review_path, index=False)

        # Also write Parquet version for UI
        try:
            parquet_path = os.path.join(processed_dir, "review_ready.parquet")
            df_final.to_parquet(parquet_path, index=False)
            logger.info(f"Also wrote Parquet review file: {parquet_path}")
        except Exception as e:
            logger.warning(f"Parquet write failed: {e}")

        logger.info(f"Pipeline completed successfully. Review file: {review_path}")

        # Log alias performance stats
        if alias_stats:
            logger.info(
                f"Alias pairs generated: {alias_stats.get('pairs_generated', 0)} (capped blocks: {alias_stats.get('capped_blocks', 0)})"
            )
            logger.info(
                f"Alias matches accepted (score ≥ high & suffix match): {alias_stats.get('accepted_matches', 0)}"
            )
            logger.info(
                f"Alias matching completed in {alias_stats.get('elapsed_time', 0):.2f}s"
            )

        # Print summary
        disposition_counts = df_final["Disposition"].value_counts()
        logger.info(f"Disposition summary: {disposition_counts.to_dict()}")

        group_count = len(df_final["group_id"].unique())
        logger.info(f"Total groups: {group_count}")

        # End performance tracking
        perf_tracker.end_run()
        perf_tracker.record_timing("export_ui", 0.0)  # Export phase

        # Log performance summary
        log_performance_summary(logger)

        # Create audit snapshot
        _create_audit_snapshot(settings, alias_stats, processed_dir)

        # Create comprehensive performance summary
        try:
            _create_performance_summary_enhanced(
                perf_tracker,
                df_norm,
                pairs_df,
                df_groups,
                df_final,
                processed_dir,
                run_id,
            )
        except Exception as e:
            logger.warning(f"Failed to create enhanced performance summary: {e}")

        # Phase 1.16: Update run status and create latest pointer
        update_run_status(run_id, "complete")
        create_latest_pointer(run_id)
        logger.info(f"Pipeline completed successfully with run_id: {run_id}")

        dag.complete("final_output")
        logger.info("[stage:end] final_output")

    except KeyboardInterrupt:
        # Handle graceful interruption
        active_stage = dag.get_current_stage() or "unknown"
        logger.warning(
            f"Run interrupted by user | run_id={run_id}, stage={active_stage}, saved_state=interrupted"
        )

        # Mark the pipeline as interrupted
        dag.mark_interrupted(active_stage)

        # Update run status to interrupted
        update_run_status(run_id, "interrupted")

        # Exit with standard interrupt code
        sys.exit(130)
    except Exception:
        logger.exception("Pipeline failed with exception:")
        raise


def main() -> None:
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Company Junction Deduplication Pipeline"
    )
    parser.add_argument("--input", required=True, help="Input CSV file path")
    parser.add_argument("--outdir", required=True, help="Output directory path")
    parser.add_argument(
        "--config", default="config/settings.yaml", help="Configuration file path"
    )
    parser.add_argument(
        "--progress", action="store_true", help="Enable tqdm progress bars"
    )
    parser.add_argument("--resume-from", help="Resume pipeline from specific stage")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force resume even if input/config hash mismatch",
    )
    parser.add_argument(
        "--no-resume",
        action="store_true",
        help="Force a full pipeline run, ignoring resume",
    )
    parser.add_argument(
        "--state-path",
        default="data/interim/pipeline_state.json",
        help="Path to pipeline state file",
    )
    # Phase 1.16: Parallel execution and caching arguments
    parser.add_argument(
        "--workers",
        type=int,
        help="Number of parallel workers (None for auto-detection)",
    )
    parser.add_argument(
        "--no-parallel",
        action="store_true",
        help="Force sequential execution (disables parallel processing)",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=1000,
        help="Chunk size for parallel processing",
    )
    parser.add_argument(
        "--parallel-backend",
        choices=["loky", "threading"],
        default="loky",
        help="Backend for parallel execution (loky=processes, threading=threads)",
    )
    parser.add_argument(
        "--run-id",
        help="Custom run ID (auto-generated if not specified)",
    )
    parser.add_argument(
        "--keep-runs",
        type=int,
        default=10,
        help="Number of completed runs to keep (default: 10)",
    )

    args = parser.parse_args()

    # Validate input file exists
    if not os.path.exists(args.input):
        logger.error(f"Input file not found: {args.input}")
        sys.exit(1)

    # Run pipeline with interrupt handling
    try:
        run_pipeline(
            args.input,
            args.outdir,
            args.config,
            args.progress,
            args.resume_from,
            args.force,
            args.no_resume,
            args.state_path,
            args.workers,
            args.no_parallel,
            args.chunk_size,
            args.parallel_backend,
            args.run_id,
            args.keep_runs,
        )
    except KeyboardInterrupt:
        logger.warning("Pipeline interrupted by user (Ctrl+C)")
        sys.exit(130)  # Standard exit code for interrupt
    except Exception:
        sys.exit(87)  # Exit with code 87 after full traceback has been printed


if __name__ == "__main__":
    main()
