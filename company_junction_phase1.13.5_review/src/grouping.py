"""
Grouping logic for company junction deduplication pipeline.

Handles connected component grouping with edge-gating and stable group IDs.
"""

import pandas as pd
from typing import Dict, List, Tuple, Set
import logging
import json
from collections import defaultdict

try:
    from src.utils.perf_utils import log_perf
    from src.utils.hash_utils import stable_group_id
except ImportError:
    from src.utils.perf_utils import log_perf
from src.utils.hash_utils import stable_group_id

logger = logging.getLogger(__name__)


def can_join_group(
    primary_id: str,
    candidate_id: str,
    edge_scores: Dict[Tuple[str, str], float],
    token_sets: Dict[str, Set[str]],
    config: Dict,
    stop_tokens: Set[str],
) -> Tuple[bool, str, float]:
    """
    Determine if a candidate can join a group based on edge-gating rules.

    Args:
        primary_id: ID of the group primary
        candidate_id: ID of the candidate to join
        edge_scores: Dict mapping (id1, id2) tuples to similarity scores
        token_sets: Dict mapping account IDs to their token sets
        config: Configuration dictionary
        stop_tokens: Set of stop tokens to exclude

    Returns:
        Tuple of (can_join, reason, score)
    """
    # Get edge score (check both directions)
    edge_key_forward = (primary_id, candidate_id)
    edge_key_reverse = (candidate_id, primary_id)

    score = edge_scores.get(edge_key_forward, edge_scores.get(edge_key_reverse, 0.0))

    # Get shared tokens (excluding stop tokens)
    primary_tokens = token_sets.get(primary_id, set())
    candidate_tokens = token_sets.get(candidate_id, set())
    shared_tokens = primary_tokens & candidate_tokens - stop_tokens

    # Edge-gating rules
    high_threshold = config.get("similarity", {}).get("high", 92)
    medium_threshold = config.get("similarity", {}).get("medium", 84)

    if score >= high_threshold:
        return True, "edge>=high", score

    # Check medium threshold with shared token requirement
    edge_gating_config = config.get("grouping", {}).get("edge_gating", {})
    allow_medium_plus_shared = edge_gating_config.get(
        "allow_medium_plus_shared_token", True
    )

    if (
        allow_medium_plus_shared
        and score >= medium_threshold
        and len(shared_tokens) > 0
    ):
        return True, "edge>=medium+shared_token", score

    return False, "insufficient_edge", score


def apply_canopy_bound(
    group_members: List[str],
    primary_id: str,
    candidate_id: str,
    edge_scores: Dict[Tuple[str, str], float],
    config: Dict,
) -> bool:
    """
    Apply canopy bound to prevent oversized groups.

    Args:
        group_members: Current group members
        primary_id: ID of the group primary
        candidate_id: ID of the candidate to join
        edge_scores: Dict mapping (id1, id2) tuples to similarity scores
        config: Configuration dictionary

    Returns:
        True if candidate can join despite canopy bound
    """
    canopy_config = (
        config.get("grouping", {}).get("edge_gating", {}).get("canopy_bound", {})
    )

    if not canopy_config.get("enabled", True):
        return True

    max_without_high = canopy_config.get("max_without_high_edge", 8)
    high_threshold = config.get("similarity", {}).get("high", 92)

    # If group is under the limit, allow joining
    if len(group_members) < max_without_high:
        return True

    # Check if candidate has high edge to primary
    edge_key_forward = (primary_id, candidate_id)
    edge_key_reverse = (candidate_id, primary_id)
    score = edge_scores.get(edge_key_forward, edge_scores.get(edge_key_reverse, 0.0))

    return score >= high_threshold


def create_groups_with_edge_gating(
    accounts_df: pd.DataFrame,
    candidate_pairs_df: pd.DataFrame,
    config: Dict,
    stop_tokens: Set[str],
) -> pd.DataFrame:
    """
    Create groups using edge-gating logic.

    Args:
        accounts_df: DataFrame with account information
        candidate_pairs_df: DataFrame with candidate pairs and scores
        config: Configuration dictionary
        stop_tokens: Set of stop tokens

    Returns:
        DataFrame with group assignments and explain metadata
    """
    with log_perf("grouping"):
        logger.info("Creating groups with edge-gating logic")
        logger.info(f"accounts_df shape: {accounts_df.shape}")
        logger.info(f"candidate_pairs_df shape: {candidate_pairs_df.shape}")
        logger.info(f"candidate_pairs_df columns: {list(candidate_pairs_df.columns)}")
        logger.info(f"accounts_df columns: {list(accounts_df.columns)}")

        # Sanity checks - handle both column naming conventions
        has_id_a_b = (
            "id_a" in candidate_pairs_df.columns
            and "id_b" in candidate_pairs_df.columns
        )
        has_account_id_1_2 = (
            "account_id_1" in candidate_pairs_df.columns
            and "account_id_2" in candidate_pairs_df.columns
        )

        if not has_id_a_b and not has_account_id_1_2:
            raise ValueError(
                "Candidate pairs DataFrame must have either 'id_a'/'id_b' or 'account_id_1'/'account_id_2' columns"
            )

        if "score" not in candidate_pairs_df.columns:
            raise ValueError("Candidate pairs DataFrame missing 'score' column")

        # Uniqueness & nulls in accounts
        if accounts_df["account_id"].isna().any():
            raise ValueError("accounts.account_id has nulls")

        if accounts_df["account_id"].duplicated().any():
            dupes = (
                accounts_df.loc[accounts_df["account_id"].duplicated(), "account_id"]
                .head(5)
                .tolist()
            )
            raise ValueError(f"accounts.account_id has duplicates, sample: {dupes}")

        # Type consistency
        assert accounts_df["account_id"].dtype.name in (
            "string",
            "object",
        ), f"accounts.account_id dtype is {accounts_df['account_id'].dtype.name}"

        # Check type consistency for pair ID columns
        if has_id_a_b:
            assert candidate_pairs_df["id_a"].dtype.name in (
                "string",
                "object",
            ), f"pairs.id_a dtype is {candidate_pairs_df['id_a'].dtype.name}"
            assert candidate_pairs_df["id_b"].dtype.name in (
                "string",
                "object",
            ), f"pairs.id_b dtype is {candidate_pairs_df['id_b'].dtype.name}"
        else:  # has_account_id_1_2
            assert candidate_pairs_df["account_id_1"].dtype.name in (
                "string",
                "object",
            ), f"pairs.account_id_1 dtype is {candidate_pairs_df['account_id_1'].dtype.name}"
            assert candidate_pairs_df["account_id_2"].dtype.name in (
                "string",
                "object",
            ), f"pairs.account_id_2 dtype is {candidate_pairs_df['account_id_2'].dtype.name}"

        # Check if edge-gating is enabled
        edge_gating_config = config.get("grouping", {}).get("edge_gating", {})
        if not edge_gating_config.get("enabled", True):
            logger.info("Edge-gating disabled, using standard grouping")
            return create_groups_standard(accounts_df, candidate_pairs_df, config)

        # Prepare data structures
        edge_scores = {}
        token_sets = {}

        # Build edge scores dict
        # Handle different column naming conventions
        if (
            "account_id_1" in candidate_pairs_df.columns
            and "account_id_2" in candidate_pairs_df.columns
        ):
            id_col1, id_col2 = "account_id_1", "account_id_2"
        elif (
            "id_a" in candidate_pairs_df.columns
            and "id_b" in candidate_pairs_df.columns
        ):
            id_col1, id_col2 = "id_a", "id_b"
        else:
            raise ValueError(
                "Candidate pairs DataFrame must have either 'account_id_1'/'account_id_2' or 'id_a'/'id_b' columns"
            )

        for _, row in candidate_pairs_df.iterrows():
            id1, id2 = row[id_col1], row[id_col2]
            edge_scores[(id1, id2)] = row["score"]

        # Build token sets
        # Handle different column naming conventions for account ID
        if "account_id" in accounts_df.columns:
            account_id_col = "account_id"
        elif "Account ID" in accounts_df.columns:
            account_id_col = "Account ID"
        else:
            raise ValueError(
                "Accounts DataFrame must have either 'account_id' or 'Account ID' column"
            )

        logger.debug(f"Using account_id column: {account_id_col}")
        logger.debug(
            f"Sample account IDs: {accounts_df[account_id_col].head().tolist()}"
        )

        for _, row in accounts_df.iterrows():
            account_id = row[account_id_col]
            tokens_str = row.get("name_core_tokens", "[]")
            try:
                tokens = set(json.loads(tokens_str))
                token_sets[account_id] = tokens
            except (json.JSONDecodeError, TypeError):
                token_sets[account_id] = set()

        # Initialize Union-Find structure
        parent = {}
        rank = {}

        def find(x):
            if parent[x] != x:
                parent[x] = find(parent[x])
            return parent[x]

        def union(x, y):
            px, py = find(x), find(y)
            if px == py:
                return
            if rank[px] < rank[py]:
                parent[px] = py
            elif rank[px] > rank[py]:
                parent[py] = px
            else:
                parent[py] = px
                rank[px] += 1

        # Initialize all accounts as their own groups
        logger.debug(f"Initializing Union-Find for {len(accounts_df)} accounts")
        for account_id in accounts_df[account_id_col]:
            parent[account_id] = account_id
            rank[account_id] = 0

        # Group membership tracking
        group_members = defaultdict(list)
        explain_metadata = {}

        # Process candidate pairs in score order (highest first)
        sorted_pairs = candidate_pairs_df.sort_values("score", ascending=False)

        # Handle different column naming conventions
        if (
            "account_id_1" in candidate_pairs_df.columns
            and "account_id_2" in candidate_pairs_df.columns
        ):
            id_col1, id_col2 = "account_id_1", "account_id_2"
        elif (
            "id_a" in candidate_pairs_df.columns
            and "id_b" in candidate_pairs_df.columns
        ):
            id_col1, id_col2 = "id_a", "id_b"
        else:
            raise ValueError(
                "Candidate pairs DataFrame must have either 'account_id_1'/'account_id_2' or 'id_a'/'id_b' columns"
            )

        for _, row in sorted_pairs.iterrows():
            id1, id2 = row[id_col1], row[id_col2]
            # score = row["score"]  # Not used in standard grouping

            # Skip if already in same group
            if find(id1) == find(id2):
                continue

            # Determine primary (lower ID for consistency)
            primary_id = min(id1, id2)
            candidate_id = max(id1, id2)

            # Check if candidate can join primary's group
            can_join, reason, actual_score = can_join_group(
                primary_id, candidate_id, edge_scores, token_sets, config, stop_tokens
            )

            if not can_join:
                continue

            # Check canopy bound
            current_group = [m for m in group_members[primary_id]] + [primary_id]
            if not apply_canopy_bound(
                current_group, primary_id, candidate_id, edge_scores, config
            ):
                logger.debug(
                    f"Canopy bound rejected {candidate_id} joining {primary_id}'s group"
                )
                continue

            # Perform union
            union(primary_id, candidate_id)

            # Update group membership
            group_members[primary_id].append(candidate_id)

            # Record explain metadata
            shared_tokens = (
                token_sets.get(primary_id, set())
                & token_sets.get(candidate_id, set()) - stop_tokens
            )
            explain_metadata[candidate_id] = {
                "group_join_reason": reason,
                "weakest_edge_to_primary": actual_score,
                "shared_tokens_count": len(shared_tokens),
            }

        # Build final groups dataframe
        groups_data = []

        for account_id in accounts_df["account_id"]:
            group_root = find(account_id)
            group_member_list = [m for m in group_members[group_root]] + [group_root]

            # Generate stable group ID
            group_id = stable_group_id(group_member_list, config)

            # Get explain metadata
            explain = explain_metadata.get(account_id, {})

            groups_data.append(
                {
                    "account_id": account_id,
                    "group_id": group_id,
                    "group_size": len(group_member_list),
                    "is_primary": account_id == group_root,
                    "group_join_reason": explain.get("group_join_reason", "primary"),
                    "weakest_edge_to_primary": explain.get(
                        "weakest_edge_to_primary", 0.0
                    ),
                    "shared_tokens_count": explain.get("shared_tokens_count", 0),
                }
            )

        groups_df = pd.DataFrame(groups_data)

        # Merge back with original account data
        groups_df = groups_df.merge(accounts_df, on="account_id", how="left")

        # Apply memory optimization
        try:
            from src.utils.dtypes import optimize_dataframe_memory
        except ImportError:
            from src.utils.dtypes import optimize_dataframe_memory
        groups_df = optimize_dataframe_memory(groups_df, "groups")

        logger.info(
            f"Created {groups_df['group_id'].nunique()} groups with edge-gating"
        )
        return groups_df


def create_groups_standard(
    accounts_df: pd.DataFrame, candidate_pairs_df: pd.DataFrame, config: Dict
) -> pd.DataFrame:
    """
    Create groups using standard connected components logic (fallback).

    Args:
        accounts_df: DataFrame with account information
        candidate_pairs_df: DataFrame with candidate pairs and scores
        config: Configuration dictionary

    Returns:
        DataFrame with group assignments
    """
    logger.info("Creating groups using standard connected components")

    # Standard Union-Find implementation
    parent = {}
    rank = {}

    def find(x):
        if parent[x] != x:
            parent[x] = find(parent[x])
        return parent[x]

    def union(x, y):
        px, py = find(x), find(y)
        if px == py:
            return
        if rank[px] < rank[py]:
            parent[px] = py
        elif rank[px] > rank[py]:
            parent[py] = px
        else:
            parent[py] = px
            rank[px] += 1

    # Initialize
    for account_id in accounts_df["account_id"]:
        parent[account_id] = account_id
        rank[account_id] = 0

    # Process pairs
    medium_threshold = config.get("similarity", {}).get("medium", 84)
    filtered_pairs = candidate_pairs_df[candidate_pairs_df["score"] >= medium_threshold]

    # Handle different column naming conventions
    if (
        "account_id_1" in candidate_pairs_df.columns
        and "account_id_2" in candidate_pairs_df.columns
    ):
        id_col1, id_col2 = "account_id_1", "account_id_2"
    elif "id_a" in candidate_pairs_df.columns and "id_b" in candidate_pairs_df.columns:
        id_col1, id_col2 = "id_a", "id_b"
    else:
        raise ValueError(
            "Candidate pairs DataFrame must have either 'account_id_1'/'account_id_2' or 'id_a'/'id_b' columns"
        )

    for _, row in filtered_pairs.iterrows():
        id1, id2 = row[id_col1], row[id_col2]
        union(id1, id2)

    # Build groups
    groups_data = []
    group_members = defaultdict(list)

    for account_id in accounts_df["account_id"]:
        group_root = find(account_id)
        group_members[group_root].append(account_id)

    for group_root, members in group_members.items():
        group_id = stable_group_id(members, config)

        for member_id in members:
            groups_data.append(
                {
                    "account_id": member_id,
                    "group_id": group_id,
                    "group_size": len(members),
                    "is_primary": member_id == group_root,
                    "group_join_reason": "standard_grouping",
                    "weakest_edge_to_primary": 0.0,
                    "shared_tokens_count": 0,
                }
            )

    groups_df = pd.DataFrame(groups_data)

    # Merge back with original account data
    groups_df = groups_df.merge(accounts_df, on="account_id", how="left")

    # Apply memory optimization
    try:
        from src.utils.dtypes import optimize_dataframe_memory
    except ImportError:
        from src.utils.dtypes import optimize_dataframe_memory
    groups_df = optimize_dataframe_memory(groups_df, "groups")

    logger.info(
        f"Created {groups_df['group_id'].nunique()} groups using standard logic"
    )
    return groups_df
