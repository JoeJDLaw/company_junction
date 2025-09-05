"""Exact equals Phase-0 utilities for Company Junction.

This module handles:
- Raw exact string matching before normalization
- Representative selection with deterministic policy
- Fast-path union creation for exact matches
- Artifact generation for audit trail
"""

import logging
from pathlib import Path
from typing import Any, Dict, Tuple

import pandas as pd

logger = logging.getLogger(__name__)


def build_raw_exact_key(account_name: str, settings: Dict[str, Any]) -> str:
    """Build raw exact key by trim + collapse whitespace (no case/punct changes).

    Args:
        account_name: Raw account name from input
        settings: Configuration settings

    Returns:
        Raw exact key for grouping

    """
    if pd.isna(account_name) or not isinstance(account_name, str):
        return ""  # type: ignore[unreachable]

    # Phase 1.35.2: Simple trim + whitespace collapse (no case/punct changes)
    if (
        settings.get("pipeline", {})
        .get("exact_equals_first_pass", {})
        .get("key_trim", True)
    ):
        # Trim leading/trailing whitespace
        key = account_name.strip()
        # Collapse multiple whitespace to single space
        import re

        key = re.sub(r"\s+", " ", key)
        # Return empty string if result is only whitespace
        return "" if key.strip() == "" else key
    # No trimming - use original, but still handle whitespace-only
    if account_name.strip() == "":
        return ""
    return account_name


def find_exact_equals_groups(
    df: pd.DataFrame, settings: Dict[str, Any], name_column: str = "Account Name",
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Find exact equals groups before normalization.

    Args:
        df: Input DataFrame with account data
        settings: Configuration settings
        name_column: Column name for account names

    Returns:
        Tuple of (exact_raw_groups, raw_exact_map, candidate_pairs_exact_raw)

    """
    logger.info(
        f"exact_equals | backend=pandas | records={len(df)} | name_column={name_column}",
    )

    # Build raw exact keys
    df_with_keys = df.copy()
    df_with_keys["raw_exact_key"] = df_with_keys[name_column].apply(
        lambda x: build_raw_exact_key(x, settings),
    )

    # Filter out empty keys
    valid_keys_mask = df_with_keys["raw_exact_key"].str.len() > 0
    df_with_keys = df_with_keys[valid_keys_mask].copy()

    if len(df_with_keys) == 0:
        logger.warning("exact_equals | no_valid_keys | all_keys_empty")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    # Group by raw exact key
    exact_groups = (
        df_with_keys.groupby("raw_exact_key")
        .agg({"account_id": list, name_column: list})
        .reset_index()
    )

    # Filter groups with size >= min_group_size
    min_group_size = (
        settings.get("pipeline", {})
        .get("exact_equals_first_pass", {})
        .get("min_group_size", 2)
    )
    exact_groups["group_size"] = exact_groups["account_id"].apply(len)
    exact_groups = exact_groups[exact_groups["group_size"] >= min_group_size].copy()

    if len(exact_groups) == 0:
        logger.info(f"exact_equals | no_groups_found | min_group_size={min_group_size}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    # Select representatives using deterministic policy
    representative_policy = (
        settings.get("pipeline", {})
        .get("exact_equals_first_pass", {})
        .get("representative_policy", "min_account_id")
    )

    representatives = []
    for _, group in exact_groups.iterrows():
        account_ids = group["account_id"]
        names = group[name_column]

        if representative_policy == "min_account_id":
            # Select representative with minimum account_id (deterministic)
            rep_idx = min(range(len(account_ids)), key=lambda i: account_ids[i])
        else:
            # Default to first (fallback)
            rep_idx = 0

        representatives.append(
            {
                "raw_exact_key": group["raw_exact_key"],
                "representative_id": account_ids[rep_idx],
                "representative_name": names[rep_idx],
                "group_size": group["group_size"],
                "all_account_ids": account_ids,
                "all_names": names,
            },
        )

    # Create exact raw groups DataFrame
    exact_raw_groups = pd.DataFrame(representatives)

    # Create raw exact map (original -> representative)
    raw_exact_map = []
    for _, group in exact_groups.iterrows():
        raw_exact_key = group["raw_exact_key"]
        account_ids = group["account_id"]
        names = group[name_column]

        # Find representative
        rep_data = exact_raw_groups[
            exact_raw_groups["raw_exact_key"] == raw_exact_key
        ].iloc[0]
        rep_id = rep_data["representative_id"]

        # Map each member to representative
        for acc_id, name in zip(account_ids, names):
            raw_exact_map.append(
                {
                    "account_id": acc_id,
                    "account_name": name,
                    "raw_exact_key": raw_exact_key,
                    "representative_id": rep_id,
                    "representative_name": rep_data["representative_name"],
                    "group_size": group["group_size"],
                },
            )

    raw_exact_map_df = pd.DataFrame(raw_exact_map)

    # Create candidate pairs for exact equals (100-score edges)
    candidate_pairs_exact_raw = []
    for _, group in exact_groups.iterrows():
        account_ids = group["account_id"]
        if len(account_ids) >= 2:
            # Create pairs between all members (spanning tree approach)
            for i in range(len(account_ids)):
                for j in range(i + 1, len(account_ids)):
                    candidate_pairs_exact_raw.append(
                        {
                            "id_a": account_ids[i],
                            "id_b": account_ids[j],
                            "score": 100.0,  # Exact match
                            "group_join_reason": "exact_equal_raw",
                            "raw_exact_key": group["raw_exact_key"],
                        },
                    )

    candidate_pairs_df = pd.DataFrame(candidate_pairs_exact_raw)

    # Log results with standardized format
    total_members = sum(
        len(group["account_id"]) for _, group in exact_groups.iterrows()
    )
    total_reps = len(exact_groups)
    singletons = len(df) - total_members

    logger.info(
        f"exact_equals | built_groups={total_reps} | total_members={total_members} | "
        f"reps={total_reps} | singletons={singletons} | representative_policy={representative_policy}",
    )

    return exact_raw_groups, raw_exact_map_df, candidate_pairs_df


def write_exact_equals_artifacts(
    exact_raw_groups: pd.DataFrame,
    raw_exact_map: pd.DataFrame,
    candidate_pairs_exact_raw: pd.DataFrame,
    interim_dir: str,
    run_id: str,
    settings: Dict[str, Any],
) -> None:
    """Write exact equals artifacts with no-overwrite policy.

    Args:
        exact_raw_groups: DataFrame with exact groups
        raw_exact_map: DataFrame mapping accounts to representatives
        candidate_pairs_exact_raw: DataFrame with exact candidate pairs
        interim_dir: Interim directory path
        run_id: Run ID for file naming
        settings: Configuration settings

    """
    logger.info(f"exact_equals | writing_artifacts | run_id={run_id}")

    # Helper function to handle no-overwrite policy
    def get_safe_path(base_path: str, suffix: str = "") -> str:
        """Get safe file path with no-overwrite policy."""
        if Path(base_path).exists():
            # Create suffixed variant
            from datetime import datetime

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            name_parts = Path(base_path).name.split(".")
            if len(name_parts) >= 2:
                new_name = f"{name_parts[0]}_{timestamp}.{'.'.join(name_parts[1:])}"
            else:
                new_name = f"{base_path}_{timestamp}"
            new_path = str(Path(base_path).parent / new_name)
            logger.info(
                f"exact_equals | existing_file_present | fallback_path={new_path} | reason=no_overwrite_policy",
            )
            return new_path
        return base_path

    # Write exact_raw_groups.parquet
    if not exact_raw_groups.empty:
        groups_path = f"{interim_dir}/exact_raw_groups.parquet"
        safe_groups_path = get_safe_path(groups_path)
        exact_raw_groups.to_parquet(safe_groups_path, index=False)
        logger.info(
            f"exact_equals | written=exact_raw_groups.parquet | groups={len(exact_raw_groups)} | path={safe_groups_path}",
        )

    # Write raw_exact_map.parquet
    if not raw_exact_map.empty:
        map_path = f"{interim_dir}/raw_exact_map.parquet"
        safe_map_path = get_safe_path(map_path)
        raw_exact_map.to_parquet(safe_map_path, index=False)
        logger.info(
            f"exact_equals | written=raw_exact_map.parquet | mappings={len(raw_exact_map)} | path={safe_map_path}",
        )

    # Write candidate_pairs_exact_raw.parquet
    if not candidate_pairs_exact_raw.empty:
        pairs_path = f"{interim_dir}/candidate_pairs_exact_raw.parquet"
        safe_pairs_path = get_safe_path(pairs_path)
        candidate_pairs_exact_raw.to_parquet(safe_pairs_path, index=False)
        logger.info(
            f"exact_equals | written=candidate_pairs_exact_raw.parquet | pairs={len(candidate_pairs_exact_raw)} | path={safe_pairs_path}",
        )


def create_unique_normalized(
    df: pd.DataFrame, raw_exact_map: pd.DataFrame, settings: Dict[str, Any],
) -> pd.DataFrame:
    """Create unique normalized dataset with representatives + singletons only.

    Args:
        df: Original input DataFrame
        raw_exact_map: Mapping of accounts to representatives
        settings: Configuration settings

    Returns:
        DataFrame with representatives + singletons only

    """
    logger.info(f"exact_equals | creating_unique_normalized | input_records={len(df)}")

    if raw_exact_map.empty:
        # No exact groups, return original
        return df

    # Get representative IDs
    representative_ids = set(raw_exact_map["representative_id"].unique())

    # Get singleton IDs (not in any exact group)
    all_grouped_ids = set(raw_exact_map["account_id"].unique())
    singleton_ids = set(df["account_id"].unique()) - all_grouped_ids

    # Combine representatives + singletons
    unique_ids = list(representative_ids) + list(singleton_ids)

    # Filter DataFrame to unique records
    unique_df = df[df["account_id"].isin(unique_ids)].copy()

    logger.info(
        f"exact_equals | unique_normalized | representatives={len(representative_ids)} | "
        f"singletons={len(singleton_ids)} | total_unique={len(unique_df)}",
    )

    return unique_df
