"""Survivorship functionality for Company Junction deduplication.

This module handles:
- Primary record selection based on relationship rank and dates
- Merge preview generation
- Field comparison and conflict resolution
"""

import json
import logging
from typing import Any, Optional

import pandas as pd

try:
    from src.utils.progress import ProgressLogger
except ImportError:
    from src.utils.progress import ProgressLogger

logger = logging.getLogger(__name__)


def select_primary_records(
    df_groups: pd.DataFrame,
    relationship_ranks: dict[str, int],
    settings: dict[str, Any],
    enable_progress: bool = False,
    profile: bool = False,
) -> pd.DataFrame:
    """Optimized primary selection with safe vectorization:
    - Vectorized relationship rank mapping
    - Vectorized marking of singleton groups
    - Preserve existing multi-record selection via _select_primary_from_group
    """
    # Check if optimization is enabled
    if not settings.get("survivorship", {}).get("optimized", True):
        logger.info("Selecting primary records for each group (original)")
        return _select_primary_records_original(
            df_groups,
            relationship_ranks,
            settings,
            enable_progress,
        )

    # Enable profiling if requested
    if profile:
        try:
            import pyinstrument

            profiler = pyinstrument.Profiler()
            profiler.start()
            logger.info("Profiling enabled for survivorship")
        except ImportError:
            logger.warning("pyinstrument not available for profiling")
            profile = False

    # Check if vectorized performance is enabled
    perf_settings = settings.get("survivorship", {}).get("performance", {})
    use_vectorized = perf_settings.get("vectorized", False)

    if use_vectorized:
        logger.info("Selecting primary records for each group (vectorized)")
        result = _select_primary_records_vectorized(
            df_groups,
            relationship_ranks,
            settings,
            enable_progress,
        )
    else:
        logger.info("Selecting primary records for each group (optimized)")
        result = _select_primary_records_optimized(
            df_groups,
            relationship_ranks,
            settings,
            enable_progress,
        )

    # Stop profiling and save report if enabled
    if profile and "profiler" in locals():
        try:
            profiler.stop()
            # Save profile to interim directory if available
            profile_path = "data/interim/survivorship_profile.html"
            with open(profile_path, "w") as f:
                f.write(profiler.output_html())
            logger.info(f"Survivorship profile saved to {profile_path}")
        except Exception as e:
            logger.warning(f"Failed to save profile: {e}")

    return result


def _select_primary_records_optimized(
    df_groups: pd.DataFrame,
    relationship_ranks: dict[str, int],
    settings: dict[str, Any],
    enable_progress: bool = False,
) -> pd.DataFrame:
    """Hybrid optimized primary selection with vectorized singletons and iterative multi-groups.

    Args:
        df_groups: DataFrame with group assignments
        relationship_ranks: Dictionary mapping relationship names to ranks
        settings: Configuration settings
        enable_progress: Enable progress logging

    Returns:
        DataFrame with primary records marked

    """
    logger.info("Selecting primary records for each group (hybrid optimized)")

    df = df_groups.copy()

    # Vectorized relationship rank (default 60)
    rel_series = df.get("Relationship")
    if rel_series is not None:
        df["relationship_rank"] = (
            rel_series.astype("string")
            .map(pd.Series(relationship_ranks, dtype="int64"))
            .fillna(60)
            .astype("int64")  # Use int64 for JSON compatibility
        )
    else:
        df["relationship_rank"] = 60

    # Improve cache locality
    if "group_id" in df.columns:
        df = df.sort_values("group_id", kind="mergesort")

    # Vectorized singletons → primary=True
    group_sizes = df.groupby("group_id").size()
    singleton_groups = set(group_sizes[group_sizes == 1].index.tolist())

    # Micro-profiling: singleton statistics
    total_groups = len(group_sizes)
    singleton_count = len(singleton_groups)
    singleton_pct = (singleton_count / total_groups * 100) if total_groups > 0 else 0
    logger.info(
        f"survivorship_breakdown | total_groups={total_groups} | singletons={singleton_count} ({singleton_pct:.1f}%) | multi_groups={total_groups - singleton_count}",
    )

    df["is_primary"] = False
    if singleton_groups:
        singleton_mask = df["group_id"].isin(singleton_groups)
        df.loc[singleton_mask, "is_primary"] = True

    # Multi-record groups: preserve existing business rules
    multi_groups = [g for g, sz in group_sizes.items() if sz > 1 and g != -1]

    # Multi-group profiling
    if multi_groups:
        multi_group_sizes = [int(group_sizes.loc[g]) for g in multi_groups]  # type: ignore[call-overload]
        avg_size = sum(multi_group_sizes) / len(multi_group_sizes)
        p50_size = sorted(multi_group_sizes)[len(multi_group_sizes) // 2]
        p90_size = sorted(multi_group_sizes)[int(len(multi_group_sizes) * 0.9)]
        logger.info(
            f"multi_group_stats | count={len(multi_groups)} | avg_size={avg_size:.1f} | p50_size={p50_size} | p90_size={p90_size}",
        )

    if enable_progress:
        prog = ProgressLogger(
            total=len(multi_groups),
            label="survivorship",
            step_every=2_000,
            secs_every=5.0,
            enable_tqdm=True,
        )
        multi_iter = prog.wrap(multi_groups)
    else:
        multi_iter = iter(multi_groups)

    for gid in multi_iter:
        group_mask = df["group_id"] == gid
        group_data = df.loc[group_mask]

        primary_idx = _select_primary_from_group(
            group_data,
            relationship_ranks,
            settings,
        )

        df.loc[group_mask, "is_primary"] = False
        df.loc[primary_idx, "is_primary"] = True

    return df


def _select_primary_records_vectorized(
    df_groups: pd.DataFrame,
    relationship_ranks: dict[str, int],
    settings: dict[str, Any],
    enable_progress: bool = False,
) -> pd.DataFrame:
    """Fully vectorized primary selection for maximum performance.

    Args:
        df_groups: DataFrame with group assignments
        relationship_ranks: Dictionary mapping relationship names to ranks
        settings: Configuration settings
        enable_progress: Enable progress logging

    Returns:
        DataFrame with primary records marked

    """
    logger.info("Using vectorized primary selection")

    # Ensure pandas strings for consistent operations
    from src.utils.duckdb_utils import ensure_pandas_strings

    df_groups = ensure_pandas_strings(df_groups, ["group_id", "account_id"])
    logger.info("Ensured pandas string types for consistent operations")

    df = df_groups.copy()

    # Vectorized relationship rank mapping
    rel_series = df.get("Relationship")
    if rel_series is not None:
        df["relationship_rank"] = (
            rel_series.astype("string")
            .map(pd.Series(relationship_ranks, dtype="int64"))
            .fillna(60)
            .astype("int64")
        )
    else:
        df["relationship_rank"] = 60

    # Skip unassigned records
    df = df[df["group_id"] != -1].copy()

    # Get tie-breakers
    tie_breakers = settings.get("survivorship", {}).get(
        "tie_breakers",
        ["created_date", "account_id"],
    )

    # Build sort columns
    sort_columns = ["relationship_rank"]
    for tie_breaker in tie_breakers:
        if tie_breaker in df.columns:
            sort_columns.append(tie_breaker)

    # Vectorized primary selection using groupby + transform
    df_sorted = df.sort_values(sort_columns)

    # Mark first record in each group as primary
    df_sorted["is_primary"] = df_sorted.groupby("group_id").cumcount() == 0

    # Restore original order
    df_sorted = df_sorted.sort_index()

    # Log statistics
    total_groups = df_sorted["group_id"].nunique()
    singletons = (df_sorted.groupby("group_id").size() == 1).sum()
    multi_groups = total_groups - singletons

    logger.info(
        f"Vectorized survivorship: {total_groups} groups, {singletons} singletons, {multi_groups} multi-groups",
    )

    # Ensure output has consistent pandas string types
    df_sorted = ensure_pandas_strings(df_sorted, ["group_id", "account_id"])

    return df_sorted


def _select_primary_records_original(
    df_groups: pd.DataFrame,
    relationship_ranks: dict[str, int],
    settings: dict[str, Any],
    enable_progress: bool = False,
) -> pd.DataFrame:
    """Original primary selection logic (fallback when optimization is disabled)."""
    logger.info("Selecting primary records for each group (original)")

    result_df = df_groups.copy()

    # Add relationship_rank column for consistency with optimized version
    rel_series = result_df.get("Relationship")
    if rel_series is not None:
        result_df["relationship_rank"] = (
            rel_series.astype("string")
            .map(pd.Series(relationship_ranks, dtype="int64"))
            .fillna(60)
            .astype("int64")  # Use int64 for JSON compatibility
        )
    else:
        result_df["relationship_rank"] = 60

    # Process each group
    unique_groups = result_df["group_id"].unique()

    # Add progress logging for group processing
    group_progress = ProgressLogger(
        total=len(unique_groups),
        label="survivorship",
        step_every=2_000,
        secs_every=5.0,
        enable_tqdm=enable_progress,
    )

    for group_id in group_progress.wrap(unique_groups):
        if group_id == -1:  # Skip unassigned records
            continue

        group_mask = result_df["group_id"] == group_id
        group_data = result_df[group_mask].copy()

        if len(group_data) == 1:
            # Singleton group - mark as primary
            result_df.loc[group_data.index[0], "is_primary"] = True
            continue

        # Select primary for multi-record group
        primary_idx = _select_primary_from_group(
            group_data,
            relationship_ranks,
            settings,
        )

        # Update primary flag
        result_df.loc[group_data.index, "is_primary"] = False
        result_df.loc[primary_idx, "is_primary"] = True

        logger.debug(f"Group {group_id}: selected primary record {primary_idx}")

    return result_df


def _select_primary_from_group(
    group_data: pd.DataFrame,
    relationship_ranks: dict[str, int],
    settings: dict[str, Any],
) -> int:
    """Select primary record from a group using relationship rank and tie-breakers.

    Args:
        group_data: DataFrame containing group records
        relationship_ranks: Dictionary mapping relationship names to ranks
        settings: Configuration settings

    Returns:
        Index of selected primary record

    """
    # Calculate relationship rank for each record
    relationship_ranks_list = []

    for _, record in group_data.iterrows():
        relationship = record.get("Relationship", "Other/Miscellaneous")
        rank = relationship_ranks.get(
            relationship,
            60,
        )  # Default rank for unknown relationships
        relationship_ranks_list.append(rank)

    group_data = group_data.copy()
    group_data["relationship_rank"] = relationship_ranks_list

    # Sort by primary criteria
    tie_breakers = settings.get("survivorship", {}).get(
        "tie_breakers",
        ["created_date", "account_id"],
    )

    # Start with relationship rank (lower is better)
    sort_columns = ["relationship_rank"]

    # Add tie-breakers
    for tie_breaker in tie_breakers:
        if tie_breaker == "created_date" and "created_date" in group_data.columns:
            # For sorting, string dates in ISO format (YYYY-MM-DD) work fine
            # No conversion needed - just use the original column
            sort_columns.append("created_date")
        elif tie_breaker == "account_id" and "account_id" in group_data.columns:
            sort_columns.append("account_id")

    # Sort and select first record
    group_data_sorted = group_data.sort_values(sort_columns)
    primary_idx = group_data_sorted.index[0]

    return int(primary_idx)


def generate_merge_preview(
    df_groups: pd.DataFrame,
    selected_fields: Optional[list[str]] = None,
    settings: Optional[dict[str, Any]] = None,
) -> pd.DataFrame:
    """Generate merge preview showing field differences within groups.

    Args:
        df_groups: DataFrame with group assignments and primary selection
        selected_fields: List of fields to compare (if None, use common fields)
        settings: Configuration settings for performance options

    Returns:
        DataFrame with merge_preview_json column added

    """
    logger.info("Generating merge preview for groups")

    # Check performance settings
    perf_settings = (
        settings.get("survivorship", {}).get("performance", {}) if settings else {}
    )
    generate_by_group = perf_settings.get("generate_preview_by_group", False)
    skip_clean_groups = perf_settings.get("skip_clean_groups", False)
    preview_output = perf_settings.get("preview_output", "survivorship_preview.parquet")

    if selected_fields is None:
        # Default fields to compare
        selected_fields = [
            "account_name",  # Use canonical name
            "account_id",  # Use canonical name
            "Relationship",  # Keep original name if not mapped
            "created_date",  # Use canonical name
            "Account Owner: Full Name",  # Keep original name if not mapped
            "Main Address",  # Keep original name if not mapped
            "Main Country",  # Keep original name if not mapped
        ]

    # Filter to fields that exist in the DataFrame
    available_fields = [
        field for field in selected_fields if field in df_groups.columns
    ]

    if generate_by_group:
        # Generate per-group previews (more efficient for large datasets)
        return _generate_merge_preview_by_group(
            df_groups,
            available_fields,
            skip_clean_groups,
            preview_output,
        )
    # Original row-by-row preview generation
    return _generate_merge_preview_original(df_groups, available_fields)


def _generate_merge_preview_original(
    df_groups: pd.DataFrame,
    available_fields: list[str],
) -> pd.DataFrame:
    """Original row-by-row merge preview generation.

    Args:
        df_groups: DataFrame with group assignments
        available_fields: List of fields to compare

    Returns:
        DataFrame with merge_preview_json column added

    """
    result_df = df_groups.copy()
    result_df["merge_preview_json"] = ""

    # Process each group
    for group_id in result_df["group_id"].unique():
        if group_id == -1:  # Skip unassigned records
            continue

        group_mask = result_df["group_id"] == group_id
        group_data = result_df[group_mask].copy()

        if len(group_data) == 1:
            # Singleton group - no merge preview needed
            continue

        # Generate merge preview for this group
        merge_preview = _generate_group_merge_preview(group_data, available_fields)

        # Add merge preview to all records in the group
        result_df.loc[group_data.index, "merge_preview_json"] = json.dumps(
            merge_preview,
            indent=2,
        )

    return result_df


def _generate_merge_preview_by_group(
    df_groups: pd.DataFrame,
    available_fields: list[str],
    skip_clean_groups: bool = True,
    preview_output: str = "survivorship_preview.parquet",
) -> pd.DataFrame:
    """Generate merge previews by group for better performance.

    Args:
        df_groups: DataFrame with group assignments
        available_fields: List of fields to compare
        skip_clean_groups: Skip groups with no conflicts
        preview_output: Output path for preview file

    Returns:
        DataFrame with merge_preview_json column added

    """
    logger.info("Generating merge previews by group (optimized)")

    # Skip unassigned records
    df = df_groups[df_groups["group_id"] != -1].copy()

    # Group by group_id for vectorized processing
    group_stats = (
        df.groupby("group_id")
        .agg(
            {
                "group_id": "size",  # Group size
                **dict.fromkeys(available_fields, "nunique"),  # Unique values per field
            },
        )
        .rename(columns={"group_id": "group_size"})
    )

    # Identify groups with conflicts
    conflict_fields = [field for field in available_fields if field != "group_id"]
    has_conflicts = group_stats[conflict_fields].gt(1).any(axis=1)

    conflicted_groups = has_conflicts[has_conflicts].index.tolist()
    clean_groups = has_conflicts[~has_conflicts].index.tolist()

    logger.info(
        f"Merge preview analysis: {len(conflicted_groups)} conflicted groups, {len(clean_groups)} clean groups",
    )

    # Initialize result DataFrame
    result_df = df_groups.copy()
    result_df["merge_preview_json"] = ""

    # Process conflicted groups
    if conflicted_groups:
        logger.info(f"Processing {len(conflicted_groups)} conflicted groups")

        # Use orjson for faster JSON serialization
        try:
            import orjson

            def json_dumps(x: Any) -> str:
                return str(orjson.dumps(x).decode("utf-8"))

        except ImportError:

            def json_dumps(x: Any) -> str:
                return json.dumps(x)

        # Process groups in batches for better performance
        batch_size = 1000
        for i in range(0, len(conflicted_groups), batch_size):
            batch = conflicted_groups[i : i + batch_size]

            for group_id in batch:
                group_mask = df["group_id"] == group_id
                group_data = df[group_mask]

                if len(group_data) > 1:
                    merge_preview = _generate_group_merge_preview(
                        group_data,
                        available_fields,
                    )
                    preview_json = json_dumps(merge_preview)

                    # Add to all records in the group
                    result_df.loc[group_data.index, "merge_preview_json"] = preview_json

    # Skip clean groups if requested
    if skip_clean_groups:
        logger.info(f"Skipped {len(clean_groups)} clean groups (no conflicts)")
    else:
        logger.info(f"Processed {len(clean_groups)} clean groups")

    # Save preview summary if output path specified
    if preview_output and conflicted_groups:
        try:
            preview_summary = {
                "conflicted_groups": len(conflicted_groups),
                "clean_groups": len(clean_groups),
                "total_groups": len(conflicted_groups) + len(clean_groups),
                "conflict_rate": len(conflicted_groups)
                / (len(conflicted_groups) + len(clean_groups)),
            }

            preview_df = pd.DataFrame([preview_summary])
            preview_df.to_parquet(preview_output, index=False)
            logger.info(f"Merge preview summary saved to {preview_output}")
        except Exception as e:
            logger.warning(f"Failed to save preview summary: {e}")

    return result_df


def _generate_group_merge_preview(
    group_data: pd.DataFrame,
    fields: list[str],
) -> dict[str, Any]:
    """Generate merge preview for a specific group.

    Args:
        group_data: DataFrame containing group records
        fields: List of fields to compare

    Returns:
        Dictionary with merge preview information

    """

    # Helper function to safely convert values to strings
    def safe_str(val: Any) -> str:
        """Safely convert value to string, handling NA values."""
        if pd.isna(val):
            return ""
        return str(val)

    # Find primary record
    primary_mask = group_data["is_primary"]
    if not primary_mask.any():
        return {"error": "No primary record found"}

    primary_record = group_data[primary_mask].iloc[0]
    non_primary_records = group_data[~primary_mask]

    preview: dict[str, Any] = {
        "primary_record": {
            "index": int(str(primary_record.name)),
            "account_id": safe_str(primary_record.get("account_id", "")),
            "account_name": safe_str(primary_record.get("account_name", "")),
            "relationship": safe_str(
                primary_record.get("Relationship", ""),
            ),  # Keep original name if not mapped
            "relationship_rank": int(primary_record.get("relationship_rank", 60)),
        },
        "group_size": len(group_data),
        "field_comparisons": {},
        "non_primary_records": [],
    }

    # Compare fields
    for field in fields:
        field_values = group_data[field].dropna().unique()

        if len(field_values) > 1:
            # Field has conflicts
            preview["field_comparisons"][field] = {
                "primary_value": safe_str(primary_record.get(field, "")),
                "alternative_values": [
                    safe_str(val)
                    for val in field_values
                    if safe_str(val) != safe_str(primary_record.get(field, ""))
                ],
                "has_conflict": True,
            }
        else:
            # Field is consistent
            preview["field_comparisons"][field] = {
                "primary_value": safe_str(primary_record.get(field, "")),
                "alternative_values": [],
                "has_conflict": False,
            }

    # Add non-primary record summaries
    for _, record in non_primary_records.iterrows():
        preview["non_primary_records"].append(
            {
                "index": int(str(record.name)),
                "account_id": safe_str(record.get("account_id", "")),
                "account_name": safe_str(record.get("account_name", "")),
                "relationship": safe_str(
                    record.get("Relationship", ""),
                ),  # Keep original name if not mapped
                "relationship_rank": int(record.get("relationship_rank", 60)),
                "weakest_edge_to_primary": record.get("weakest_edge_to_primary", 0.0),
            },
        )

    return preview


def save_survivorship_results(df_survivorship: pd.DataFrame, output_path: str) -> None:
    """Save survivorship results to parquet file.

    Args:
        df_survivorship: DataFrame with survivorship results
        output_path: Output file path

    """
    df_survivorship.to_parquet(output_path, index=False)
    logger.info(f"Saved survivorship results to {output_path}")


def load_survivorship_results(input_path: str) -> pd.DataFrame:
    """Load survivorship results from parquet file.

    Args:
        input_path: Input file path

    Returns:
        DataFrame with survivorship results

    """
    try:
        df_survivorship = pd.read_parquet(input_path)
        logger.info(f"Loaded survivorship results from {input_path}")
        return df_survivorship
    except Exception as e:
        logger.error(f"Error loading survivorship results: {e}")
        return pd.DataFrame()
