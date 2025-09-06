"""Data cleaning module for Salesforce export processing.

This module handles:
- CSV/Excel file loading
- Duplicate detection based on name matching
- Field merging logic
- Data validation and cleaning
- CLI orchestration for end-to-end pipeline
"""

import argparse
import hashlib
import json
import logging
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd

from src.alias_matching import (
    compute_alias_matches,
    create_alias_cross_refs,
    save_alias_matches,
)
from src.disposition import apply_dispositions, save_dispositions
from src.grouping import create_groups_with_edge_gating

# Import local modules
from src.normalize import excel_serial_to_datetime, normalize_dataframe
from src.performance import (
    PerformanceTracker,
    compute_group_size_histogram,
    save_performance_summary,
)
from src.similarity import get_stop_tokens, pair_scores, save_candidate_pairs
from src.survivorship import (
    generate_merge_preview,
    save_survivorship_results,
    select_primary_records,
)
from src.utils.cache_utils import (
    PHASE_1_DESTRUCTIVE_FUSE,
    add_run_to_index,
    create_cache_directories,
    create_latest_pointer,
    generate_run_id,
    prune_old_runs,
    update_run_status,
)

# Performance logging removed - using built-in logging instead
from src.utils.dtypes import optimize_dataframe_memory

# Import ID utilities for Salesforce ID canonicalization
from src.utils.id_utils import normalize_sfid_series
from src.utils.io_utils import (
    load_relationship_ranks,
    load_settings,
    read_csv_stable,
)
from src.utils.logging_utils import setup_logging
from src.utils.mini_dag import MiniDAG
from src.utils.parallel_utils import create_parallel_executor
from src.utils.path_utils import (
    ensure_directory_exists,
    get_artifact_path,
    get_config_path,
    get_interim_dir,
    get_processed_dir,
)

# Performance utilities
from src.utils.perf_utils import time_stage, track_memory_peak
from src.utils.resource_monitor import log_resource_summary
from src.utils.schema_utils import (
    ACCOUNT_ID,
    ACCOUNT_NAME,
    CREATED_DATE,
    DISPOSITION,
    GROUP_ID,
    GROUP_SIZE,
    IS_PRIMARY,
    MAX_SCORE,
    PRIMARY_NAME,
    SUFFIX_CLASS,
    WEAKEST_EDGE_TO_PRIMARY,
    apply_canonical_rename,
)

logger = logging.getLogger(__name__)


def _assert_pairs_cover_accounts(
    pairs: pd.DataFrame,
    accounts: pd.DataFrame,
    id_col: str = ACCOUNT_ID,
) -> None:
    """Assert that all pair IDs exist in the accounts DataFrame.

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
            f"id_b={pairs.loc[b_missing, 'id_b'].head(5).tolist()}",
        )


def _create_audit_snapshot(
    settings: Dict[str, Any],
    alias_stats: Dict[str, Any],
    output_dir: str,
) -> None:
    """Create an audit snapshot with run metadata.

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
                check=False,
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
    """Create comprehensive performance summary with the required schema.

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
                int(df_groups["group_size"].max()) if not df_groups.empty else 0
            ),
        }

        # Calculate block statistics
        if "block_key" in df_norm.columns:
            # Create simple block statistics from block_key column
            block_counts = df_norm["block_key"].value_counts().head(10)
            block_stats = {
                "top_tokens": [
                    {"token": str(token), "count": int(count), "cap": None}
                    for token, count in block_counts.items()
                    if token is not None and str(token).strip() != ""
                ],
            }
        else:
            block_stats = {"top_tokens": []}

        # Generate comprehensive summary
        summary = perf_tracker.generate_summary(
            dataset_stats,
            candidate_stats,
            group_stats,
            block_stats,
        )

        # Add run_id to summary if provided
        if run_id:
            summary["run_id"] = run_id

        # Save performance summary to primary location
        perf_summary_path = os.path.join(output_dir, "perf_summary.json")
        save_performance_summary(summary, perf_summary_path)
        logger.info(f"Performance summary saved to: {perf_summary_path}")

        # Also save a copy to the legacy location for backward compatibility
        legacy_perf_path = str(get_processed_dir("legacy") / "perf_summary.json")
        try:
            # Ensure legacy directory exists
            legacy_dir = get_processed_dir("legacy")
            legacy_dir.mkdir(parents=True, exist_ok=True)

            save_performance_summary(summary, legacy_perf_path)
            logger.info(f"Also saved to legacy location: {legacy_perf_path}")
        except Exception as e:
            logger.warning(f"Failed to save legacy performance summary: {e}")

    except Exception as e:
        logger.warning(f"Failed to create enhanced performance summary: {e}")
        raise


def load_salesforce_data(file_path: str) -> pd.DataFrame:
    """Load Salesforce export data from CSV or Excel file.

    Args:
        file_path: Path to the Salesforce export file

    Returns:
        DataFrame containing the Salesforce data

    """
    if file_path.endswith(".csv"):
        # Use stable CSV reader to avoid dtype warnings
        return read_csv_stable(file_path)
    if file_path.endswith((".xlsx", ".xls")):
        return pd.read_excel(file_path)
    raise ValueError(f"Unsupported file format: {file_path}")


def validate_required_columns(df: pd.DataFrame) -> bool:
    """Validate that required columns are present.

    Args:
        df: DataFrame to validate

    Returns:
        True if validation passes

    Raises:
        ValueError: If required columns are missing

    """
    # Use canonical column names since DataFrame has been renamed
    # Only check for columns that are actually mapped and renamed
    required_columns = [ACCOUNT_ID, ACCOUNT_NAME, CREATED_DATE]

    # Check for Account Name (required)
    if ACCOUNT_NAME not in df.columns:
        raise ValueError(f"Missing required name column: {ACCOUNT_NAME}")

    # Check for other required columns
    missing_columns = []
    for col in required_columns:
        if col not in df.columns:
            missing_columns.append(col)

    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")

    return True


def _compute_group_stats_pandas(df_primary: pd.DataFrame) -> pd.DataFrame:
    """Compute group statistics using pandas (legacy method).

    Args:
        df_primary: DataFrame with primary records

    Returns:
        DataFrame with group statistics

    """
    # Create group stats DataFrame with the exact schema specified
    group_stats = []
    for group_id in df_primary[GROUP_ID].unique():
        group_data = df_primary[df_primary[GROUP_ID] == group_id]

        # Get primary record (is_primary = True)
        primary_record = group_data[group_data[IS_PRIMARY]].iloc[0]

        # Calculate max_score within the group (or 0.0 if not applicable)
        max_score = (
            group_data[WEAKEST_EDGE_TO_PRIMARY].max()
            if WEAKEST_EDGE_TO_PRIMARY in group_data.columns
            else 0.0
        )

        group_stats.append(
            {
                GROUP_ID: group_id,
                GROUP_SIZE: len(group_data),
                MAX_SCORE: max_score,
                PRIMARY_NAME: primary_record.get(ACCOUNT_NAME, ""),
                DISPOSITION: primary_record.get(
                    DISPOSITION,
                    "Update",
                ),  # Default to Update if not set yet
            },
        )

    df_group_stats = pd.DataFrame(group_stats)

    # Ensure deterministic ordering and stable dtypes
    df_group_stats = df_group_stats.sort_values(GROUP_ID, kind="mergesort").reset_index(
        drop=True,
    )

    # Ensure correct dtypes with explicit casting
    df_group_stats = df_group_stats.astype(
        {
            GROUP_ID: "string",
            GROUP_SIZE: "int32",
            MAX_SCORE: "float32",
            PRIMARY_NAME: "string",
            DISPOSITION: "string",  # Use string instead of category for consistency
        },
    )

    return df_group_stats


def run_pipeline(
    input_path: str,
    output_dir: str,
    config_path: str,
    enable_progress: bool = False,
    resume_from: Optional[str] = None,
    no_resume: bool = False,
    force: bool = False,
    state_path: str = str(get_interim_dir("default") / "pipeline_state.json"),
    workers: Optional[int] = None,
    no_parallel: bool = False,
    chunk_size: int = 1000,
    parallel_backend: str = "loky",
    run_id: Optional[str] = None,
    keep_runs: int = 10,
    col_overrides: Optional[Dict[str, str]] = None,
    profile: bool = False,
) -> None:
    """Run the complete deduplication pipeline.

    Args:
        input_path: Path to input CSV file
        output_dir: Directory for output files
        config_path: Path to configuration file
        enable_progress: Enable progress logging
        resume_from: Stage to resume from
        no_resume: Disable resume functionality
        force: Force execution despite warnings
        state_path: Path to pipeline state file
        workers: Number of parallel workers
        no_parallel: Disable parallel execution
        chunk_size: Chunk size for parallel processing
        parallel_backend: Backend for parallel execution
        run_id: Custom run ID
        keep_runs: Number of runs to keep
        col_overrides: Column overrides for schema resolution
        profile: Enable performance profiling for pipeline stages

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

    # Prune old runs (gated by Phase 1 fuse)
    if PHASE_1_DESTRUCTIVE_FUSE:
        prune_old_runs(keep_runs)
    else:
        logger.info("Skipping run pruning: Phase 1 destructive fuse not enabled")

    # Log resource summary
    log_resource_summary()

    # Load configuration
    settings = load_settings(config_path)
    relationship_ranks = load_relationship_ranks(
        str(get_config_path("relationship_ranks.csv")),
    )

    # Add CLI worker count to settings if provided
    if workers is not None:
        if "parallelism" not in settings:
            settings["parallelism"] = {}
        settings["parallelism"]["workers"] = workers
        logger.info(f"Using CLI worker count: {workers}")

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
        "exact_equals",  # Phase 1.35.2: Added exact equals stage
        "candidate_generation",
        "grouping",
        "survivorship",
        DISPOSITION,
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
            "small_input_threshold",
            10000,
        ),
        disable_parallel=no_parallel,
    )

    # Add effective_workers to settings for alias matching
    settings["effective_workers"] = workers

    # Ensure output directories exist
    ensure_directory_exists(output_dir)
    ensure_directory_exists(str(get_interim_dir("default")))

    # Smart auto-resume logic with enhanced logging
    if no_resume:
        logger.info(
            "Auto-resume decision: --no-resume specified - forcing full run | reason=NO_RESUME_FLAG",
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
                    f"Auto-resume decision: resume_from='{resume_from}' | last_completed='{dag.get_last_completed_stage()}' | input_hash=PASS | reason=SMART_DETECT",
                )
            else:
                logger.info(
                    "Auto-resume decision: input_hash=FAIL - forcing full run due to input/config changes | reason=HASH_MISMATCH",
                )
                resume_from = None
        else:
            logger.info(
                "Auto-resume decision: no previous run found - starting fresh | reason=NO_PREVIOUS_RUN",
            )
            resume_from = None
    else:
        # Manual resume-from specified - validate input hash
        input_path_obj = Path(input_path)
        config_path_obj = Path(config_path)

        if not dag._validate_input_invariance(input_path_obj, config_path_obj):
            if force:  # Use force flag instead of enable_progress
                logger.warning(
                    "Input hash mismatch detected but --force specified - proceeding with resume | reason=FORCE_OVERRIDE",
                )
            else:
                logger.error(
                    "Input hash mismatch detected. Use --force to override or run without --resume-from | reason=HASH_MISMATCH_NO_FORCE",
                )
                sys.exit(1)
        else:
            logger.info(
                f"Manual resume decision: resume_from='{resume_from}' | input_hash=PASS | reason=MANUAL_OVERRIDE",
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

            # Load filtered data (pipeline produces filtered, not normalized)
            normalized_path = str(
                get_interim_dir(run_id) / f"accounts_filtered.{interim_format}",
            )
            if Path(normalized_path).exists():
                logger.info(f"Loading normalized data from {normalized_path}")
                if interim_format == "parquet":
                    df_norm = pd.read_parquet(normalized_path)
                else:
                    df_norm = pd.read_csv(normalized_path)
                logger.info(f"Loaded {len(df_norm)} normalized records")
            else:
                raise FileNotFoundError(
                    f"Required intermediate file not found: {normalized_path}",
                )

            # Load candidate pairs if needed
            if resume_from in [
                "grouping",
                "survivorship",
                DISPOSITION,
                "alias_matching",
                "final_output",
            ]:
                pairs_path = str(
                    get_interim_dir(run_id) / f"candidate_pairs.{interim_format}",
                )
                if Path(pairs_path).exists():
                    logger.info(f"Loading candidate pairs from {pairs_path}")
                    pairs_df = pd.read_parquet(pairs_path)
                    logger.info(f"Loaded {len(pairs_df)} candidate pairs")
                else:
                    raise FileNotFoundError(
                        f"Required intermediate file not found: {pairs_path}",
                    )

            # Load groups if needed
            if resume_from in [
                "survivorship",
                DISPOSITION,
                "alias_matching",
                "final_output",
            ]:
                groups_path = str(get_interim_dir(run_id) / f"groups.{interim_format}")
                if Path(groups_path).exists():
                    logger.info(f"Loading groups from {groups_path}")
                    df_groups = pd.read_parquet(groups_path)
                    logger.info(f"Loaded {len(df_groups)} group records")
                else:
                    raise FileNotFoundError(
                        f"Required intermediate file not found: {groups_path}",
                    )

            # Load survivorship results if needed
            if resume_from in [DISPOSITION, "alias_matching", "final_output"]:
                survivorship_path = str(
                    get_interim_dir(run_id) / f"survivorship.{interim_format}",
                )
                if Path(survivorship_path).exists():
                    logger.info(
                        f"Loading survivorship results from {survivorship_path}",
                    )
                    df_primary = pd.read_parquet(survivorship_path)
                    logger.info(f"Loaded {len(df_primary)} survivorship records")
                else:
                    raise FileNotFoundError(
                        f"Required intermediate file not found: {survivorship_path}",
                    )

            # Load dispositions if needed
            if resume_from in ["alias_matching", "final_output"]:
                dispositions_path = str(
                    get_interim_dir(run_id) / f"dispositions.{interim_format}",
                )
                if Path(dispositions_path).exists():
                    logger.info(f"Loading dispositions from {dispositions_path}")
                    df_dispositions = pd.read_parquet(dispositions_path)
                    logger.info(f"Loaded {len(df_dispositions)} disposition records")
                else:
                    raise FileNotFoundError(
                        f"Required intermediate file not found: {dispositions_path}",
                    )

            # Load alias matches if needed
            if resume_from == "final_output":
                alias_matches_path = str(
                    get_interim_dir(run_id) / f"alias_matches.{interim_format}",
                )
                if Path(alias_matches_path).exists():
                    logger.info(f"Loading alias matches from {alias_matches_path}")
                    df_alias_matches = pd.read_parquet(alias_matches_path)
                    logger.info(f"Loaded {len(df_alias_matches)} alias matches")
                else:
                    raise FileNotFoundError(
                        f"Required intermediate file not found: {alias_matches_path}",
                    )

        # Phase 1.26.1: Dynamic schema resolution
        from src.utils.schema_utils import resolve_schema, save_schema_mapping

        # Resolve schema mapping from DataFrame headers
        input_filename = Path(input_path).name
        schema_mapping = resolve_schema(
            df,
            settings,
            cli_overrides=col_overrides,
            input_filename=input_filename,
        )

        # Save schema mapping for observability and reproducibility
        save_schema_mapping(schema_mapping, run_id)

        # Log schema resolution results
        logger.info(
            f"schema_resolved | run_id={run_id} "
            f"required_ok=true "
            f"account_name=\"{schema_mapping.get(ACCOUNT_NAME, 'NOT_FOUND')}\" "
            f"mapped={schema_mapping} "
            f"heuristics_used={'heuristic' in str(schema_mapping)}",
        )

        # Apply canonical rename using helper function
        # This renames columns from ACTUAL -> CANONICAL before any canonical constants are used
        df = apply_canonical_rename(df, dict(schema_mapping))

        # Validate required columns after renaming
        validate_required_columns(df)

        # Preserve original account_id as account_id_src for audit trail
        df["account_id_src"] = df[ACCOUNT_ID].astype("string").fillna("").str.strip()

        # Canonicalize Salesforce IDs to 18-character form
        logger.info("Canonicalizing Salesforce IDs to 18-character form")
        df[ACCOUNT_ID] = normalize_sfid_series(df["account_id_src"])

        # Remove duplicate account_id records (keep first occurrence)
        initial_count = len(df)
        df = df.drop_duplicates(subset=[ACCOUNT_ID], keep="first")
        if len(df) < initial_count:
            logger.warning(
                f"Removed {initial_count - len(df)} duplicate {ACCOUNT_ID} records after canonicalization",
            )

        # Handle Excel serial dates
        if CREATED_DATE in df.columns:
            df[CREATED_DATE] = df[CREATED_DATE].apply(excel_serial_to_datetime)

        logger.info(f"Loaded {len(df)} records")

        # Step 2: Normalize data
        if not resume_from or resume_from == "normalization":
            logger.info("[stage:start] normalization")
            dag.start("normalization")

            logger.info("Normalizing company names")
            name_column = ACCOUNT_NAME  # Use standardized column name
            # Performance tracking removed - using built-in logging instead
            df_norm = normalize_dataframe(df, name_column)

            # Add name_core_tokens column for edge-gating
            def create_tokens(x: Any) -> str:
                if pd.notna(x):
                    x_str = str(x)
                    if x_str.strip():
                        return json.dumps(x_str.split())
                return "[]"

            df_norm["name_core_tokens"] = df_norm["name_core"].apply(create_tokens)

            perf_tracker.record_timing(
                "clean_normalize",
                0.0,
            )  # Will be updated by log_perf

            dag.complete("normalization")
            logger.info("[stage:end] normalization")
        elif resume_from and resume_from != "normalization":
            logger.info(f"Skipping normalization stage (resuming from {resume_from})")

        # Step 2.5: Filter out problematic records for similarity analysis
        if not resume_from or resume_from == "filtering":
            logger.info("[stage:start] filtering")
            dag.start("filtering")

            logger.info("Filtering data for similarity analysis")
            initial_count = len(df_norm)

            # Phase 1.35.2: Enhanced filtering with audit trail
            filtered_out_records = []
            reasons = {}

            # Filter out records with empty or problematic name_core
            empty_mask = df_norm["name_core"].str.strip() != ""
            empty_count = (~empty_mask).sum()
            if empty_count > 0:
                reasons["empty_name_core"] = empty_count
                filtered_out_records.extend(
                    df_norm[~empty_mask][["account_id", "account_name"]]
                    .assign(reason="empty_name_core")
                    .to_dict("records"),
                )
            df_norm = df_norm[empty_mask].copy()

            # Enhanced problematic patterns (case-insensitive, whole-token match)
            problematic_patterns = [
                r"^\d+$",  # Numeric only (e.g., "123", "999")
                r"^[A-Za-z]$",  # Single character (e.g., "a", "x")
                r"^(test|sample|temp|unknown|n/?a|none|tbd)$",  # Common placeholders
                r"^1099$",  # Tax form references
            ]

            import re

            pattern_mask = (
                df_norm["name_core"]
                .str.lower()
                .str.strip()
                .apply(
                    lambda x: not any(
                        re.match(pattern, x) for pattern in problematic_patterns
                    ),
                )
            )
            pattern_count = (~pattern_mask).sum()
            if pattern_count > 0:
                reasons["noise_string"] = pattern_count
                filtered_out_records.extend(
                    df_norm[~pattern_mask][["account_id", "account_name"]]
                    .assign(reason="noise_string")
                    .to_dict("records"),
                )
            df_norm = df_norm[pattern_mask].copy()

            # Phase 1.35.2: Write filtered-out audit artifact (no-overwrite policy)
            if (
                settings.get("filtering", {}).get("write_filtered_out", True)
                and filtered_out_records
            ):
                filtered_out_df = pd.DataFrame(filtered_out_records)

                # No-overwrite policy: check if file exists and create suffixed variant
                filtered_out_path = f"{interim_dir}/accounts_filtered_out.parquet"
                if Path(filtered_out_path).exists():
                    # Create suffixed variant
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    filtered_out_path = (
                        f"{interim_dir}/accounts_filtered_out_{timestamp}.parquet"
                    )
                    logger.info(
                        f"filtering | existing_file_present | fallback_path={filtered_out_path} | reason=no_overwrite_policy",
                    )

                filtered_out_df.to_parquet(filtered_out_path, index=False)
                logger.info(
                    f"filtering | written=accounts_filtered_out.parquet | records={len(filtered_out_records)} | path={filtered_out_path}",
                )

            filtered_count = len(df_norm)
            total_filtered = initial_count - filtered_count

            # Log reason breakdown with standardized format
            logger.info(
                f"filtering | backend=pandas | records_removed={total_filtered} | "
                f"records_remaining={filtered_count} | reasons={reasons}",
            )

            if filtered_count == 0:
                raise ValueError(
                    "No valid company names found after filtering. Check your data quality.",
                )

            # Save filtered data (existing logic)
            interim_format = settings.get("io", {}).get("interim_format", "parquet")
            filtered_path = f"{interim_dir}/accounts_filtered.{interim_format}"
            if interim_format == "parquet":
                df_norm.to_parquet(filtered_path, index=False)
            else:
                df_norm.to_csv(filtered_path, index=False)
            logger.info(f"Saved filtered data to {filtered_path}")

            dag.complete("filtering")
            logger.info("[stage:end] filtering")

        # Step 2.6: Phase 1.35.2 - Exact Equals Phase-0 (pre-normalization)
        if not resume_from or resume_from == "exact_equals":
            logger.info("[stage:start] exact_equals")
            dag.start("exact_equals")

            # Import exact equals utilities
            from src.utils.exact_equals import (
                create_unique_normalized,
                find_exact_equals_groups,
                write_exact_equals_artifacts,
            )

            if (
                settings.get("pipeline", {})
                .get("exact_equals_first_pass", {})
                .get("enable", True)
            ):
                logger.info(
                    "Phase 1.35.2: Running Exact-Equals Phase-0 before normalization",
                )

                # Find exact equals groups
                name_column = ACCOUNT_NAME  # Use standardized column name
                exact_raw_groups, raw_exact_map, candidate_pairs_exact_raw = (
                    find_exact_equals_groups(df_norm, settings, name_column)
                )

                # Write artifacts with no-overwrite policy
                write_exact_equals_artifacts(
                    exact_raw_groups,
                    raw_exact_map,
                    candidate_pairs_exact_raw,
                    interim_dir,
                    run_id,
                    settings,
                )

                # Create unique normalized dataset (representatives + singletons only)
                df_norm = create_unique_normalized(df_norm, raw_exact_map, settings)

                # Save unique normalized data
                unique_path = f"{interim_dir}/unique_normalized.parquet"
                df_norm.to_parquet(unique_path, index=False)
                logger.info(
                    f"exact_equals | written=unique_normalized.parquet | records={len(df_norm)} | path={unique_path}",
                )

            else:
                logger.info(
                    "Phase 1.35.2: Exact-Equals Phase-0 disabled in configuration",
                )

            dag.complete("exact_equals")
            logger.info("[stage:end] exact_equals")

        # Step 3: Generate candidate pairs
        if not resume_from or resume_from == "candidate_generation":
            logger.info("[stage:start] candidate_generation")
            dag.start("candidate_generation")

            logger.info("Generating candidate pairs")
            # Performance tracking removed - using built-in logging instead
            # Memory tracking removed - using built-in logging instead
            pairs_df = pair_scores(
                df_norm,
                settings,
                enable_progress,
                parallel_executor,
                interim_dir,
                profile,
            )
            perf_tracker.record_timing("blocking", 0.0)  # Blocking phase
            perf_tracker.record_timing("scoring", 0.0)  # Scoring phase

        # Apply memory optimization to pairs
        pairs_df = optimize_dataframe_memory(pairs_df, "candidate_pairs", verbose=False)

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
        stop_tokens = get_stop_tokens(settings)
        logger.info(f"Stop tokens: {stop_tokens}")

        # Performance tracking removed - using built-in logging instead
        # Memory tracking removed - using built-in logging instead
        logger.info("About to call create_groups_with_edge_gating")
        df_groups = create_groups_with_edge_gating(
            df_norm,
            pairs_df,
            settings,
            stop_tokens,
            enable_progress,
            profile,
        )
        logger.info(f"create_groups_with_edge_gating returned: {type(df_groups)}")
        if df_groups is not None:
            logger.info(f"df_groups shape: {df_groups.shape}")
        perf_tracker.record_timing("grouping", 0.0)  # Will be updated by log_perf

        # Apply memory optimization to groups
        df_groups = optimize_dataframe_memory(df_groups, "groups", verbose=False)

        # Save groups
        groups_path = f"{interim_dir}/groups.{interim_format}"
        df_groups.to_parquet(groups_path, index=False)
        logger.info(f"Saved groups to {groups_path}")

        dag.complete("grouping")
        logger.info("[stage:end] grouping")

        # Step 5: Select primary records
        if not resume_from or resume_from == "survivorship":
            dag.start("survivorship")
            logger.info("Selecting primary records")
        logger.info(f"df_groups shape: {df_groups.shape}")
        logger.info(f"df_groups columns: {list(df_groups.columns)}")
        logger.info(f"df_groups sample: {df_groups.head(2).to_dict()}")

        with time_stage("survivorship", logger):
            with track_memory_peak("survivorship", logger):
                df_primary = select_primary_records(
                    df_groups,
                    relationship_ranks,
                    settings,
                    enable_progress,
                    profile,
                )
                perf_tracker.record_timing(
                    "survivorship",
                    0.0,
                )  # Will be updated by log_perf

        # Generate merge preview
        df_primary = generate_merge_preview(df_primary, settings=settings)

        # Save survivorship results
        survivorship_path = f"{interim_dir}/survivorship.{interim_format}"
        save_survivorship_results(df_primary, survivorship_path)

        dag.complete("survivorship")
        logger.info("[stage:end] survivorship")

        # Phase 1.35.4: Generate group stats using DuckDB engine with memoization
        try:
            logger.info("Generating group stats for UI performance optimization")

            # Check backend configuration
            group_stats_backend = settings.get("group_stats", {}).get(
                "backend",
                "duckdb",
            )

            if group_stats_backend == "duckdb":
                logger.info("group_stats | backend=duckdb | memoization=enabled")

                # Import DuckDB engine
                from src.utils.duckdb_group_stats import (
                    create_duckdb_group_stats_engine,
                )
                from src.utils.parity_validator import create_parity_validator

                # Create DuckDB engine
                duckdb_engine = create_duckdb_group_stats_engine(settings, run_id)

                try:
                    # Generate config digest for memoization (more robust)
                    try:
                        # Try to serialize settings, fallback to string representation if it fails
                        settings_str = json.dumps(settings, sort_keys=True, default=str)
                    except (TypeError, ValueError, RecursionError):
                        # Fallback: use string representation of key settings
                        key_settings = {
                            "group_stats": settings.get("group_stats", {}),
                            "engine": settings.get("engine", {}),
                            "io": settings.get("io", {}),
                        }
                        settings_str = str(key_settings)

                    config_digest = hashlib.md5(settings_str.encode()).hexdigest()[:16]

                    # Compute group stats using DuckDB
                    df_group_stats, duckdb_metadata = duckdb_engine.compute_group_stats(
                        df_primary,
                        config_digest,
                    )

                    # Log DuckDB performance
                    # Safe throughput calculation
                    tp = duckdb_metadata.get(
                        "throughput",
                        (
                            (
                                duckdb_metadata.get("records", 0)
                                / duckdb_metadata["elapsed_sec"]
                            )
                            if duckdb_metadata.get("elapsed_sec")
                            else 0
                        ),
                    )

                    logger.info(
                        "group_stats | duckdb_complete | elapsed_sec=%.3f | groups=%s | records=%s | "
                        "throughput=%s recs/sec | memoize=%s | cache_hit=%s"
                        % (
                            duckdb_metadata.get("elapsed_sec", 0),
                            duckdb_metadata.get("groups"),
                            duckdb_metadata.get("records"),
                            f"{tp:.0f}",
                            duckdb_metadata.get("memoize"),
                            duckdb_metadata.get("cache_hit"),
                        ),
                    )

                    # Write optimized parquet using DuckDB
                    group_stats_path = str(
                        get_artifact_path(run_id, "group_stats_duckdb.parquet"),
                    )
                    parquet_metadata = duckdb_engine.write_optimized_parquet(
                        df_group_stats,
                        group_stats_path,
                    )

                    # Log parquet write performance
                    logger.info(
                        f"group_stats | parquet_write_complete | path={group_stats_path} | "
                        f"size_mb={parquet_metadata['size_mb']:.2f} | compression={parquet_metadata['compression']} | "
                        f"dictionary_encoding={parquet_metadata['dictionary_encoding']}",
                    )

                    # Write canonical group_stats.parquet if persistence is enabled
                    persist_artifacts = settings.get("group_stats", {}).get(
                        "persist_artifacts",
                        True,
                    )
                    # Check for environment variable override
                    if os.environ.get("CJ_GROUP_STATS_PERSIST_ARTIFACTS"):
                        persist_artifacts = (
                            os.environ.get(
                                "CJ_GROUP_STATS_PERSIST_ARTIFACTS",
                                "",
                            ).lower()
                            == "true"
                        )

                    if persist_artifacts:
                        canonical_path = str(
                            get_artifact_path(run_id, "group_stats.parquet"),
                        )
                        df_group_stats.to_parquet(canonical_path, index=False)
                        logger.info(
                            f"group_stats | canonical_file_written | path={canonical_path}",
                        )

                        # Also write the DuckDB-specific file for parity validation
                        duckdb_specific_path = str(
                            get_artifact_path(run_id, "group_stats_duckdb.parquet"),
                        )
                        df_group_stats.to_parquet(duckdb_specific_path, index=False)
                        logger.info(
                            f"group_stats | duckdb_specific_file_written | path={duckdb_specific_path}",
                        )

                    # Validate memoization performance

                    # Validate memoization performance
                    if duckdb_metadata["memoize"] and not duckdb_metadata["cache_hit"]:
                        min_cache_hit_percentage = (
                            settings.get("group_stats", {})
                            .get("memoization", {})
                            .get("min_cache_hit_percentage", 30)
                        )
                        if duckdb_metadata["elapsed_sec"] > 0:
                            # This is a cache miss, log for future reference
                            logger.info(
                                f"group_stats | memoization_cache_miss | key={duckdb_metadata['cache_key']}",
                            )

                    # Run parity validation if pandas backend is available for comparison
                    run_parity = settings.get("group_stats", {}).get(
                        "run_parity_validation",
                        False,
                    )
                    # Check for environment variable override
                    if os.environ.get("CJ_GROUP_STATS_RUN_PARITY"):
                        run_parity = (
                            os.environ.get("CJ_GROUP_STATS_RUN_PARITY", "").lower()
                            == "true"
                        )

                    if run_parity:
                        logger.info("group_stats | running_parity_validation")

                        # Compute pandas version for comparison
                        df_group_stats_pandas = _compute_group_stats_pandas(df_primary)

                        # Validate parity
                        parity_validator = create_parity_validator()
                        is_parity_valid, parity_report = (
                            parity_validator.validate_group_stats_parity(
                                df_group_stats,
                                df_group_stats_pandas,
                                run_id,
                            )
                        )

                        if is_parity_valid:
                            logger.info(
                                "group_stats | parity_validation_passed | mismatches=0",
                            )
                        else:
                            logger.error(
                                f"group_stats | parity_validation_failed | mismatches={parity_report['mismatches']}",
                            )

                        # Save pandas version for comparison
                        pandas_path = str(
                            get_artifact_path(run_id, "group_stats_pandas.parquet"),
                        )
                        df_group_stats_pandas.to_parquet(pandas_path, index=False)
                        logger.info(
                            f"group_stats | pandas_version_saved | path={pandas_path}",
                        )

                        # Generate parquet size report
                        try:
                            from src.utils.parquet_size_reporter import (
                                create_parquet_size_reporter,
                            )

                            size_reporter = create_parquet_size_reporter()

                            # Analyze both parquet files
                            duckdb_size_report = size_reporter.analyze_parquet_file(
                                group_stats_path,
                            )
                            pandas_size_report = size_reporter.analyze_parquet_file(
                                pandas_path,
                            )

                            # Generate comparison report
                            size_comparison = size_reporter.compare_parquet_files(
                                group_stats_path,
                                pandas_path,
                                run_id,
                            )

                            logger.info(
                                f"group_stats | size_report_generated | path={size_comparison}",
                            )
                        except Exception as e:
                            logger.warning(
                                f"group_stats | size_report_failed | error={e}",
                            )

                        # Generate benchmark report if this is a 94K run
                        try:
                            if len(df_primary) > 50000:  # Likely 94K dataset
                                benchmark_report = {
                                    "dataset_size": len(df_primary),
                                    "backend": "duckdb",
                                    "duckdb_timing": duckdb_metadata["elapsed_sec"],
                                    "pandas_timing": None,  # Would need separate pandas run
                                    "target_seconds": 50,
                                    "target_met": duckdb_metadata["elapsed_sec"] < 50,
                                    "memoization": {
                                        "enabled": duckdb_metadata["memoize"],
                                        "cache_hit": duckdb_metadata["cache_hit"],
                                        "cache_key": duckdb_metadata["cache_key"],
                                    },
                                    "environment": {
                                        "duckdb_threads": settings.get("engine", {})
                                        .get("duckdb", {})
                                        .get("threads", "auto"),
                                        "duckdb_memory": settings.get("engine", {})
                                        .get("duckdb", {})
                                        .get("memory_limit"),
                                        "compression": "zstd",
                                        "dictionary_encoding": True,
                                    },
                                    "generated_at": datetime.now().isoformat(),
                                }

                                benchmark_path = (
                                    "docs/reports/phase_1_35_4_benchmark.md"
                                )
                                os.makedirs(
                                    os.path.dirname(benchmark_path),
                                    exist_ok=True,
                                )

                                with open(benchmark_path, "w") as f:
                                    f.write("# Phase 1.35.4 Benchmark Report\n\n")
                                    f.write(
                                        f"**Generated**: {benchmark_report['generated_at']}\n",
                                    )
                                    f.write(
                                        f"**Dataset Size**: {benchmark_report['dataset_size']:,} records\n",
                                    )
                                    f.write(
                                        f"**Backend**: {benchmark_report['backend']}\n\n",
                                    )
                                    f.write("## Performance Results\n\n")
                                    f.write(
                                        f"- **DuckDB Runtime**: {benchmark_report['duckdb_timing']:.3f}s\n",
                                    )
                                    f.write(
                                        f"- **Target**: <{benchmark_report['target_seconds']}s\n",
                                    )
                                    f.write(
                                        f"- **Target Met**: {'✅ YES' if benchmark_report['target_met'] else '❌ NO'}\n\n",
                                    )
                                    f.write("## Memoization\n\n")
                                    f.write(
                                        f"- **Enabled**: {benchmark_report['memoization']['enabled']}\n",
                                    )
                                    f.write(
                                        f"- **Cache Hit**: {benchmark_report['memoization']['cache_hit']}\n",
                                    )
                                    f.write(
                                        f"- **Cache Key**: {benchmark_report['memoization']['cache_key']}\n\n",
                                    )
                                    f.write("## Environment\n\n")
                                    f.write(
                                        f"- **DuckDB Threads**: {benchmark_report['environment']['duckdb_threads']}\n",
                                    )
                                    f.write(
                                        f"- **DuckDB Memory**: {benchmark_report['environment']['duckdb_memory']}\n",
                                    )
                                    f.write(
                                        f"- **Compression**: {benchmark_report['environment']['compression']}\n",
                                    )
                                    f.write(
                                        f"- **Dictionary Encoding**: {benchmark_report['environment']['dictionary_encoding']}\n",
                                    )

                                logger.info(
                                    f"group_stats | benchmark_report_generated | path={benchmark_path}",
                                )
                        except Exception as e:
                            logger.warning(
                                f"group_stats | benchmark_report_failed | error={e}",
                            )

                finally:
                    duckdb_engine.close()

            else:
                # Fallback to pandas backend
                logger.info("group_stats | backend=pandas | memoization=disabled")
                df_group_stats = _compute_group_stats_pandas(df_primary)

                # Save pandas-specific file if persistence is enabled
                persist_artifacts = settings.get("group_stats", {}).get(
                    "persist_artifacts",
                    True,
                )
                # Check for environment variable override
                if os.environ.get("CJ_GROUP_STATS_PERSIST_ARTIFACTS"):
                    persist_artifacts = (
                        os.environ.get("CJ_GROUP_STATS_PERSIST_ARTIFACTS", "").lower()
                        == "true"
                    )

                if persist_artifacts:
                    pandas_path = str(
                        get_artifact_path(run_id, "group_stats_pandas.parquet"),
                    )
                    df_group_stats.to_parquet(pandas_path, index=False)
                    logger.info(
                        f"group_stats | pandas_specific_file_written | path={pandas_path}",
                    )

                # Save canonical file
                group_stats_path = str(get_artifact_path(run_id, "group_stats.parquet"))
                df_group_stats.to_parquet(group_stats_path, index=False)

                logger.info(
                    f"group_stats | pandas_complete | groups={len(df_group_stats)} | path={group_stats_path}",
                )

        except Exception as e:
            logger.warning(f"Failed to generate group stats: {e}")
            # Fallback to basic pandas implementation
            try:
                df_group_stats = _compute_group_stats_pandas(df_primary)
                group_stats_path = str(
                    get_artifact_path(run_id, "group_stats_fallback.parquet"),
                )
                df_group_stats.to_parquet(group_stats_path, index=False)
                logger.info(
                    f"group_stats | fallback_complete | groups={len(df_group_stats)} | path={group_stats_path}",
                )
            except Exception as fallback_e:
                logger.error(f"Group stats fallback also failed: {fallback_e}")
                df_group_stats = None

        # Step 6: Apply dispositions
        if not resume_from or resume_from == DISPOSITION:
            dag.start(DISPOSITION)

            logger.info("Applying disposition classification")
        with time_stage(DISPOSITION, logger):
            with track_memory_peak(DISPOSITION, logger):
                df_dispositions = apply_dispositions(df_primary, settings)
                perf_tracker.record_timing(
                    DISPOSITION,
                    0.0,
                )  # Will be updated by log_perf

        # Save dispositions
        dispositions_path = f"{interim_dir}/dispositions.{interim_format}"
        save_dispositions(df_dispositions, dispositions_path)

        dag.complete(DISPOSITION)
        logger.info(f"[stage:end] {DISPOSITION}")

        # Phase 1.22.1: Update group stats with final dispositions
        try:
            if "df_group_stats" in locals() and df_group_stats is not None:
                logger.info("Updating group stats with final dispositions")

                # Update dispositions in group stats
                for idx in df_group_stats.index:
                    row = df_group_stats.loc[idx]
                    group_id = row[GROUP_ID]
                    group_dispositions = df_dispositions[
                        df_dispositions[GROUP_ID] == group_id
                    ]

                    if len(group_dispositions) > 0:
                        # Get the disposition from the primary record
                        primary_disposition = group_dispositions[
                            group_dispositions[IS_PRIMARY]
                        ]
                        if len(primary_disposition) > 0:
                            df_group_stats.at[idx, "disposition"] = (
                                primary_disposition.iloc[0]["disposition"]
                            )

                # Re-save updated group stats
                # Ensure deterministic ordering and stable dtypes after update
                df_group_stats = df_group_stats.sort_values(
                    GROUP_ID,
                    kind="mergesort",
                ).reset_index(drop=True)

                # Re-apply dtypes to ensure consistency
                df_group_stats = df_group_stats.astype(
                    {
                        GROUP_ID: "string",
                        GROUP_SIZE: "int32",
                        MAX_SCORE: "float32",
                        PRIMARY_NAME: "string",
                        DISPOSITION: "string",
                    },
                )

                group_stats_path = str(get_artifact_path(run_id, "group_stats.parquet"))
                df_group_stats.to_parquet(group_stats_path, index=False)

                logger.info(
                    f"Updated group stats with final dispositions, saved to {group_stats_path}",
                )
        except Exception as e:
            logger.warning(f"Failed to update group stats with dispositions: {e}")

        # Phase 1.23.1: Generate group_details.parquet for fast UI details loading
        try:
            logger.info("Generating group details parquet for fast UI loading")

            # Create projected details dataframe with only essential columns
            details_cols = [
                GROUP_ID,
                ACCOUNT_ID,
                ACCOUNT_NAME,
                SUFFIX_CLASS,
                CREATED_DATE,
                DISPOSITION,
            ]

            # Filter to only include columns that exist in df_dispositions
            available_details_cols = [
                col for col in details_cols if col in df_dispositions.columns
            ]

            if (
                len(available_details_cols) >= 3
            ):  # Need at least group_id, account_id, and one more
                df_details = df_dispositions[available_details_cols].copy()

                # Ensure correct dtypes with explicit casting
                df_details = df_details.astype(
                    {
                        "group_id": "string",
                        "account_id": "string",
                        "account_name": "string",
                        "suffix_class": "string",
                        "created_date": "string",  # Keep as string for now
                        DISPOSITION: "string",
                    },
                )

                # Sort by group_id for optimal predicate pushdown
                df_details = df_details.sort_values(
                    "group_id",
                    kind="mergesort",
                ).reset_index(drop=True)

                # Save to processed directory for UI access
                details_path = str(get_artifact_path(run_id, "group_details.parquet"))
                df_details.to_parquet(details_path, index=False)

                logger.info(
                    f"Generated group details: {len(df_details)} records, saved to {details_path}",
                )
                logger.info(
                    f"Group details size: {df_details.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB",
                )
            else:
                logger.warning(
                    f"Insufficient columns for group details: found {available_details_cols}",
                )

        except Exception as e:
            logger.warning(f"Failed to generate group details parquet: {e}")

        # Step 7: Compute alias matches and cross-references
        if not resume_from or resume_from == "alias_matching":
            logger.info("[stage:start] alias_matching")
            dag.start("alias_matching")

            logger.info("Computing alias matches and cross-references")
        alias_matches_path = f"{interim_dir}/alias_matches.{interim_format}"
        # Performance tracking removed - using built-in logging instead
        result = compute_alias_matches(df_norm, df_groups, settings, parallel_executor)

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
        df_final = optimize_dataframe_memory(df_final, "review_ready", verbose=False)

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
                f"Alias pairs generated: {alias_stats.get('pairs_generated', 0)} (capped blocks: {alias_stats.get('capped_blocks', 0)})",
            )
            logger.info(
                f"Alias matches accepted (score ≥ high & suffix match): {alias_stats.get('accepted_matches', 0)}",
            )
            logger.info(
                f"Alias matching completed in {alias_stats.get('elapsed_time', 0):.2f}s",
            )

        # Print summary
        disposition_counts = df_final[DISPOSITION].value_counts()
        logger.info(f"Disposition summary: {disposition_counts.to_dict()}")

        group_count = len(df_final["group_id"].unique())
        logger.info(f"Total groups: {group_count}")

        # End performance tracking
        perf_tracker.end_run()
        perf_tracker.record_timing("export_ui", 0.0)  # Export phase

        # Log performance summary
        # log_performance_summary function removed - using built-in logging instead

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
            f"Run interrupted by user | run_id={run_id}, stage={active_stage}, saved_state=interrupted",
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
        description="Company Junction Deduplication Pipeline",
    )
    parser.add_argument("--input", required=True, help="Input CSV file path")
    parser.add_argument("--outdir", required=True, help="Output directory path")
    parser.add_argument(
        "--config",
        default=str(get_config_path()),
        help="Configuration file path",
    )
    parser.add_argument(
        "--progress",
        action="store_true",
        help="Enable tqdm progress bars",
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
        default=str(get_interim_dir("default") / "pipeline_state.json"),
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
    parser.add_argument(
        "--col",
        nargs="+",
        help="Override column mapping (e.g., --col account_name='Company Name' account_id='ID')",
    )
    parser.add_argument(
        "--profile",
        action="store_true",
        help="Enable performance profiling for pipeline stages",
    )

    args = parser.parse_args()

    # Validate input file exists
    if not os.path.exists(args.input):
        logger.error(f"Input file not found: {args.input}")
        sys.exit(1)

    # Parse column overrides
    col_overrides_dict = {}
    if args.col:
        for override_str in args.col:
            try:
                canonical_name, actual_name = override_str.split("=", 1)
                col_overrides_dict[canonical_name.strip()] = actual_name.strip()
                logger.info(f"Column override: {canonical_name} -> {actual_name}")
            except ValueError:
                logger.error(
                    f"Invalid column override format: {override_str}. Expected 'canonical_name=actual_name'",
                )
                sys.exit(1)

    # Run pipeline with interrupt handling
    try:
        run_pipeline(
            input_path=args.input,
            output_dir=args.outdir,
            config_path=args.config,
            enable_progress=args.progress,
            resume_from=args.resume_from,
            no_resume=args.no_resume,
            force=args.force,
            state_path=args.state_path,
            workers=args.workers,
            no_parallel=args.no_parallel,
            chunk_size=args.chunk_size,
            parallel_backend=args.parallel_backend,
            run_id=args.run_id,
            keep_runs=args.keep_runs,
            col_overrides=col_overrides_dict,
            profile=args.profile,
        )
    except KeyboardInterrupt:
        logger.warning("Pipeline interrupted by user (Ctrl+C)")
        sys.exit(130)  # Standard exit code for interrupt
    except Exception:
        sys.exit(87)  # Exit with code 87 after full traceback has been printed


if __name__ == "__main__":
    main()
