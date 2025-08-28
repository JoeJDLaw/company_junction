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
from typing import Dict
import os
from datetime import datetime

# Import local modules
from normalize import normalize_dataframe, excel_serial_to_datetime
from similarity import pair_scores, save_candidate_pairs, get_stop_tokens
from grouping import create_groups_with_edge_gating

# Hash utilities not used in this module
from survivorship import (
    select_primary_records,
    generate_merge_preview,
    save_survivorship_results,
)
from disposition import apply_dispositions, save_dispositions
from alias_matching import (
    compute_alias_matches,
    create_alias_cross_refs,
    save_alias_matches,
)

try:
    from src.utils.io_utils import load_settings, load_relationship_ranks
    from src.utils.logging_utils import setup_logging
    from src.utils.path_utils import ensure_directory_exists
    from src.utils.perf_utils import log_perf
    from src.utils.dtypes import optimize_dataframe_memory
except ImportError:
    from utils.io_utils import load_settings, load_relationship_ranks
    from utils.logging_utils import setup_logging
    from utils.path_utils import ensure_directory_exists
    from utils.perf_utils import log_perf
    from utils.dtypes import optimize_dataframe_memory
from performance import (
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


def _create_audit_snapshot(settings: Dict, alias_stats: Dict, output_dir: str) -> None:
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
        from disposition import get_blacklist_terms
        from manual_io import load_manual_blacklist

        builtin_count = len(get_blacklist_terms())
        manual_terms = load_manual_blacklist()
        manual_count = len(manual_terms)
        effective_count = builtin_count + manual_count

        # Get manual overrides count
        from manual_io import load_manual_overrides

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

        # Save performance summary
        save_performance_summary(summary, os.path.join(output_dir, "perf_summary.json"))

        logger.info(
            f"Enhanced performance summary written to: {output_dir}/perf_summary.json"
        )

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
        return pd.read_csv(file_path)
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


def run_pipeline(input_path: str, output_dir: str, config_path: str) -> None:
    """
    Run the complete deduplication pipeline.

    Args:
        input_path: Path to input CSV file
        output_dir: Directory for output files
        config_path: Path to configuration file
    """
    logger.info("Starting Company Junction deduplication pipeline")

    # Load configuration
    settings = load_settings(config_path)
    relationship_ranks = load_relationship_ranks("config/relationship_ranks.csv")

    # Setup logging
    setup_logging(settings.get("logging", {}).get("level", "INFO"))

    # Initialize performance tracker
    perf_tracker = PerformanceTracker()
    perf_tracker.start_run(settings)

    # Ensure output directories exist
    ensure_directory_exists(output_dir)
    ensure_directory_exists("data/interim")

    try:
        # Step 1: Load and validate data
        logger.info(f"Loading data from {input_path}")
        df = load_salesforce_data(input_path)

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

        # Enforce consistent IDs (handle NaN safely before .str)
        df["account_id"] = df["account_id"].astype("string").fillna("").str.strip()

        # Remove duplicate account_id records (keep first occurrence)
        initial_count = len(df)
        df = df.drop_duplicates(subset=["account_id"], keep="first")
        if len(df) < initial_count:
            logger.warning(
                f"Removed {initial_count - len(df)} duplicate account_id records"
            )

        # Handle Excel serial dates
        if "created_date" in df.columns:
            df["created_date"] = df["created_date"].apply(excel_serial_to_datetime)

        logger.info(f"Loaded {len(df)} records")

        # Step 2: Normalize data
        logger.info("Normalizing company names")
        name_column = "account_name"  # Use standardized column name
        with log_perf("normalization"):
            df_norm = normalize_dataframe(df, name_column)

            # Add name_core_tokens column for edge-gating
            import json

            df_norm["name_core_tokens"] = df_norm["name_core"].apply(
                lambda x: json.dumps(x.split()) if pd.notna(x) and x.strip() else "[]"
            )

            perf_tracker.record_timing(
                "clean_normalize", 0.0
            )  # Will be updated by log_perf

        # Step 2.5: Filter out problematic records for similarity analysis
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

        # Save normalized data
        interim_format = settings.get("io", {}).get("interim_format", "parquet")
        normalized_path = f"data/interim/accounts_normalized.{interim_format}"
        if interim_format == "parquet":
            df_norm.to_parquet(normalized_path, index=False)
        else:
            df_norm.to_csv(normalized_path, index=False)
        logger.info(f"Saved normalized data to {normalized_path}")

        # Step 3: Generate candidate pairs
        logger.info("Generating candidate pairs")
        with log_perf("candidate_generation"):
            pairs_df = pair_scores(df_norm, settings)
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
        pairs_path = f"data/interim/candidate_pairs.{interim_format}"
        save_candidate_pairs(pairs_df, pairs_path)

        # Step 4: Build groups with edge-gating
        logger.info("Building duplicate groups with edge-gating")

        # Get stop tokens for edge-gating
        stop_tokens = get_stop_tokens()
        logger.info(f"Stop tokens: {stop_tokens}")

        with log_perf("grouping"):
            logger.info("About to call create_groups_with_edge_gating")
            df_groups = create_groups_with_edge_gating(
                df_norm, pairs_df, settings, stop_tokens
            )
            logger.info(f"create_groups_with_edge_gating returned: {type(df_groups)}")
            if df_groups is not None:
                logger.info(f"df_groups shape: {df_groups.shape}")
            perf_tracker.record_timing("grouping", 0.0)  # Will be updated by log_perf

        # Apply memory optimization to groups
        df_groups = optimize_dataframe_memory(df_groups, "groups")

        # Save groups
        groups_path = f"data/interim/groups.{interim_format}"
        df_groups.to_parquet(groups_path, index=False)
        logger.info(f"Saved groups to {groups_path}")

        # Step 5: Select primary records
        logger.info("Selecting primary records")
        logger.info(f"df_groups shape: {df_groups.shape}")
        logger.info(f"df_groups columns: {list(df_groups.columns)}")
        logger.info(f"df_groups sample: {df_groups.head(2).to_dict()}")

        with log_perf("survivorship"):
            df_primary = select_primary_records(df_groups, relationship_ranks, settings)
            perf_tracker.record_timing(
                "survivorship", 0.0
            )  # Will be updated by log_perf

        # Generate merge preview
        df_primary = generate_merge_preview(df_primary)

        # Save survivorship results
        survivorship_path = f"data/interim/survivorship.{interim_format}"
        save_survivorship_results(df_primary, survivorship_path)

        # Step 6: Apply dispositions
        logger.info("Applying disposition classification")
        with log_perf("disposition"):
            df_dispositions = apply_dispositions(df_primary, settings)
            perf_tracker.record_timing(
                "disposition", 0.0
            )  # Will be updated by log_perf

        # Save dispositions
        dispositions_path = f"data/interim/dispositions.{interim_format}"
        save_dispositions(df_dispositions, dispositions_path)

        # Step 7: Compute alias matches and cross-references
        logger.info("Computing alias matches and cross-references")
        alias_matches_path = f"data/interim/alias_matches.{interim_format}"
        with log_perf("alias_matching"):
            result = compute_alias_matches(df_norm, df_groups, settings)

        if isinstance(result, tuple) and len(result) == 2:
            df_alias_matches, alias_stats = result
        else:
            df_alias_matches, alias_stats = result, {}

        save_alias_matches(df_alias_matches, alias_matches_path)

        # Add alias cross-references to dispositions
        df_dispositions = create_alias_cross_refs(df_dispositions, df_alias_matches)

        # Step 8: Create final review-ready output with explain metadata
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

        review_path = os.path.join(output_dir, "review_ready.csv")
        df_final.to_csv(review_path, index=False)

        # Also write Parquet version for UI
        try:
            parquet_path = os.path.join(output_dir, "review_ready.parquet")
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

        # Create audit snapshot
        _create_audit_snapshot(settings, alias_stats, output_dir)

        # Create comprehensive performance summary
        _create_performance_summary_enhanced(
            perf_tracker, df_norm, pairs_df, df_groups, df_final, output_dir
        )

    except Exception:
        logger.exception("Pipeline failed with exception:")
        raise


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Company Junction Deduplication Pipeline"
    )
    parser.add_argument("--input", required=True, help="Input CSV file path")
    parser.add_argument("--outdir", required=True, help="Output directory path")
    parser.add_argument(
        "--config", default="config/settings.yaml", help="Configuration file path"
    )

    args = parser.parse_args()

    # Validate input file exists
    if not os.path.exists(args.input):
        logger.error(f"Input file not found: {args.input}")
        sys.exit(1)

    # Run pipeline
    try:
        run_pipeline(args.input, args.outdir, args.config)
    except Exception:
        sys.exit(87)  # Exit with code 87 after full traceback has been printed


if __name__ == "__main__":
    main()
