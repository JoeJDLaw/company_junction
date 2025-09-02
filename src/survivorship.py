"""
Survivorship functionality for Company Junction deduplication.

This module handles:
- Primary record selection based on relationship rank and dates
- Merge preview generation
- Field comparison and conflict resolution
"""

import pandas as pd
import json
from typing import List, Dict, Optional, Any
import logging

try:
    from src.normalize import excel_serial_to_datetime
    from src.utils.progress import ProgressLogger
except ImportError:
    from src.normalize import excel_serial_to_datetime
    from src.utils.progress import ProgressLogger

logger = logging.getLogger(__name__)


def select_primary_records(
    df_groups: pd.DataFrame,
    relationship_ranks: Dict[str, int],
    settings: Dict[str, Any],
    enable_progress: bool = False,
) -> pd.DataFrame:
    """
    Select primary records for each group based on relationship rank and tie-breakers.

    Args:
        df_groups: DataFrame with group assignments
        relationship_ranks: Dictionary mapping relationship names to ranks
        settings: Configuration settings

    Returns:
        DataFrame with primary records selected
    """
    logger.info("Selecting primary records for each group")

    result_df = df_groups.copy()

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
            group_data, relationship_ranks, settings
        )

        # Update primary flag
        result_df.loc[group_data.index, "is_primary"] = False
        result_df.loc[primary_idx, "is_primary"] = True

        logger.debug(f"Group {group_id}: selected primary record {primary_idx}")

    return result_df


def _select_primary_from_group(
    group_data: pd.DataFrame,
    relationship_ranks: Dict[str, int],
    settings: Dict[str, Any],
) -> int:
    """
    Select primary record from a group using relationship rank and tie-breakers.

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
            relationship, 60
        )  # Default rank for unknown relationships
        relationship_ranks_list.append(rank)

    group_data = group_data.copy()
    group_data["relationship_rank"] = relationship_ranks_list

    # Sort by primary criteria
    tie_breakers = settings.get("survivorship", {}).get(
        "tie_breakers", ["created_date", "account_id"]
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
    df_groups: pd.DataFrame, selected_fields: Optional[List[str]] = None
) -> pd.DataFrame:
    """
    Generate merge preview showing field differences within groups.

    Args:
        df_groups: DataFrame with group assignments and primary selection
        selected_fields: List of fields to compare (if None, use common fields)

    Returns:
        DataFrame with merge_preview_json column added
    """
    logger.info("Generating merge preview for groups")

    if selected_fields is None:
        # Default fields to compare
        selected_fields = [
            "account_name",  # Use canonical name
            "account_id",    # Use canonical name
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
            merge_preview, indent=2
        )

    return result_df


def _generate_group_merge_preview(
    group_data: pd.DataFrame, fields: List[str]
) -> Dict[str, Any]:
    """
    Generate merge preview for a specific group.

    Args:
        group_data: DataFrame containing group records
        fields: List of fields to compare

    Returns:
        Dictionary with merge preview information
    """
    # Helper function to safely convert values to strings
    def safe_str(val):
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

    preview: Dict[str, Any] = {
        "primary_record": {
            "index": int(str(primary_record.name)),
            "account_id": safe_str(primary_record.get("account_id", "")),
            "account_name": safe_str(primary_record.get("account_name", "")),
            "relationship": safe_str(primary_record.get("Relationship", "")),  # Keep original name if not mapped
            "relationship_rank": primary_record.get("relationship_rank", 60),
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
                "relationship": safe_str(record.get("Relationship", "")),  # Keep original name if not mapped
                "relationship_rank": record.get("relationship_rank", 60),
                "weakest_edge_to_primary": record.get("weakest_edge_to_primary", 0.0),
            }
        )

    return preview


def save_survivorship_results(df_survivorship: pd.DataFrame, output_path: str) -> None:
    """
    Save survivorship results to parquet file.

    Args:
        df_survivorship: DataFrame with survivorship results
        output_path: Output file path
    """
    df_survivorship.to_parquet(output_path, index=False)
    logger.info(f"Saved survivorship results to {output_path}")


def load_survivorship_results(input_path: str) -> pd.DataFrame:
    """
    Load survivorship results from parquet file.

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
