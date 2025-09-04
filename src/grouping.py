"""
Grouping logic for company junction deduplication pipeline.

Handles connected component grouping with edge-gating and stable group IDs.
"""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from typing import Any, Dict, List, Set, Tuple

import pandas as pd

logger = logging.getLogger(__name__)

# Optional imports with safe fallbacks
try:
    from src.utils.hash_utils import stable_group_id
except Exception:  # pragma: no cover
    stable_group_id = None  # type: ignore

try:
    from src.utils.progress import ProgressLogger
except Exception:  # pragma: no cover

    class ProgressLogger:  # minimal fallback
        def __init__(
            self, total: int, label: str, step_every: int, secs_every: float, enable_tqdm: bool
        ) -> None:
            self.total = total

        def wrap(self, iterator):
            return iterator


# -------------------------------
# Helper functions
# -------------------------------


def can_join_group(
    primary_id: str,
    candidate_id: str,
    edge_scores: Dict[Tuple[str, str], float],
    token_sets: Dict[str, Set[str]],
    config: Dict[str, Any],
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
    allow_medium_plus_shared = edge_gating_config.get("allow_medium_plus_shared_token", True)

    if allow_medium_plus_shared and score >= medium_threshold and len(shared_tokens) > 0:
        return True, "edge>=medium+shared_token", score

    return False, "insufficient_edge", score


def apply_canopy_bound(
    group_size: int,
    primary_id: str,
    candidate_id: str,
    edge_scores: Dict[Tuple[str, str], float],
    config: Dict[str, Any],
) -> bool:
    """
    Apply canopy bound to prevent oversized groups.

    Args:
        group_size: Current group size
        primary_id: ID of the group primary
        candidate_id: ID of the candidate to join
        edge_scores: Dict mapping (id1, id2) tuples to similarity scores
        config: Configuration dictionary

    Returns:
        True if candidate can join despite canopy bound
    """
    canopy_config = config.get("grouping", {}).get("edge_gating", {}).get("canopy_bound", {})

    if not canopy_config.get("enabled", True):
        return True

    max_without_high = canopy_config.get("max_without_high_edge", 8)
    high_threshold = config.get("similarity", {}).get("high", 92)

    # If group is under the limit, allow joining
    if group_size < max_without_high:
        return True

    # Check if candidate has high edge to primary
    edge_key_forward = (primary_id, candidate_id)
    edge_key_reverse = (candidate_id, primary_id)
    score = edge_scores.get(edge_key_forward, edge_scores.get(edge_key_reverse, 0.0))

    return float(score) >= float(high_threshold)


# -------------------------------
# Main grouping functions
# -------------------------------


def create_groups_with_edge_gating(
    accounts_df: pd.DataFrame,
    candidate_pairs_df: pd.DataFrame,
    config: Dict[str, Any],
    stop_tokens: Set[str],
    enable_progress: bool = False,
    profile: bool = False,
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
    # Performance tracking removed - using built-in logging instead
    logger.info("Creating groups with edge-gating logic")
    logger.info(f"accounts_df shape: {accounts_df.shape}")
    logger.info(f"candidate_pairs_df shape: {candidate_pairs_df.shape}")
    logger.info(f"candidate_pairs_df columns: {list(candidate_pairs_df.columns)}")
    logger.info(f"accounts_df columns: {list(accounts_df.columns)}")

    # Enable profiling if requested
    profiler = None
    if profile:
        try:
            import pyinstrument  # type: ignore

            profiler = pyinstrument.Profiler()
            profiler.start()
            logger.info("Profiling enabled for grouping")
        except ImportError:
            logger.warning("pyinstrument not available for profiling")
            profiler = None

    # Ensure pandas strings for consistent operations and DuckDB compatibility
    from src.utils.duckdb_utils import ensure_pandas_strings
    
    # Determine ID column names
    has_id_a_b = "id_a" in candidate_pairs_df.columns and "id_b" in candidate_pairs_df.columns
    has_account_id_1_2 = "account_id_1" in candidate_pairs_df.columns and "account_id_2" in candidate_pairs_df.columns
    
    if has_id_a_b:
        id_col1, id_col2 = "id_a", "id_b"
    else:  # has_account_id_1_2
        id_col1, id_col2 = "account_id_1", "account_id_2"
    
    # Ensure consistent pandas string types
    accounts_df = ensure_pandas_strings(accounts_df, ["account_id", "name_core"])
    candidate_pairs_df = ensure_pandas_strings(candidate_pairs_df, [id_col1, id_col2])
    logger.info("Ensured pandas string types for consistent operations")

    # Sanity checks - handle both column naming conventions
    has_id_a_b = "id_a" in candidate_pairs_df.columns and "id_b" in candidate_pairs_df.columns
    has_account_id_1_2 = "account_id_1" in candidate_pairs_df.columns and "account_id_2" in candidate_pairs_df.columns

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
        dupes = accounts_df.loc[accounts_df["account_id"].duplicated(), "account_id"].head(5).tolist()
        raise ValueError(f"accounts.account_id has duplicates, sample: {dupes}")

    # Type consistency
    assert accounts_df["account_id"].dtype.name in (
        "string",
        "object",
    ), f"accounts.account_id dtype is {accounts_df['account_id'].dtype.name}"

    # Type consistency checks for pair ID columns
    assert candidate_pairs_df[id_col1].dtype.name in (
        "string",
        "object",
    ), f"pairs.{id_col1} dtype is {candidate_pairs_df[id_col1].dtype.name}"
    assert candidate_pairs_df[id_col2].dtype.name in (
        "string",
        "object",
    ), f"pairs.{id_col2} dtype is {candidate_pairs_df[id_col2].dtype.name}"

    # Check if edge-gating is enabled
    edge_gating_config = config.get("grouping", {}).get("edge_gating", {})
    if not edge_gating_config.get("enabled", True):
        logger.info("Edge-gating disabled, using standard grouping")
        return create_groups_standard(accounts_df, candidate_pairs_df, config)

    # Prepare data structures
    edge_scores: Dict[Tuple[str, str], float] = {}
    token_sets: Dict[str, Set[str]] = {}

    # Build edge scores dict
    perf_settings = config.get("grouping", {}).get("edge_gating", {}).get("performance", {})
    vectorize_edge_scores = perf_settings.get("vectorize_edge_scores", False)

    if vectorize_edge_scores:
        # Vectorized edge_scores building without iterrows
        logger.info("Using vectorized edge_scores building")
        edge_scores = dict(
            zip(
                zip(candidate_pairs_df[id_col1], candidate_pairs_df[id_col2]),
                candidate_pairs_df["score"],
            )
        )
    else:
        # Original iterrows approach
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
        raise ValueError("Accounts DataFrame must have either 'account_id' or 'Account ID' column")

    logger.debug(f"Using account_id column: {account_id_col}")
    logger.debug(f"Sample account IDs: {accounts_df[account_id_col].head().tolist()}")

    # Check if optimized token parsing is enabled
    token_parse_mode = perf_settings.get("token_parse", "auto")

    if token_parse_mode == "auto":
        # Auto-detect and use optimized parsing
        try:
            from src.utils.perf_utils import parse_name_core_tokens  # type: ignore

            logger.info("Using optimized token parsing")
            token_series = accounts_df.get("name_core_tokens", pd.Series("[]", index=accounts_df.index))
            token_sets = {
                account_id: parse_name_core_tokens(tokens_str)
                for account_id, tokens_str in zip(accounts_df[account_id_col], token_series)
            }
        except ImportError:
            logger.warning("Optimized token parsing not available, using standard approach")
            token_parse_mode = "json"

    if token_parse_mode == "json":
        # Original JSON parsing approach
        for _, row in accounts_df.iterrows():
            account_id = row[account_id_col]
            tokens_str = row.get("name_core_tokens", "[]")
            try:
                tokens = set(json.loads(tokens_str))
                token_sets[account_id] = tokens
            except (json.JSONDecodeError, TypeError):
                token_sets[account_id] = set()

    # Union-Find selection
    maintain_unionfind_size = perf_settings.get("maintain_unionfind_size", False)
    if maintain_unionfind_size:
        # Use optimized Union-Find with size tracking
        try:
            from src.utils.union_find import DisjointSet  # type: ignore

            logger.info("Using optimized Union-Find with size tracking")

            uf = DisjointSet()
            for account_id in accounts_df[account_id_col]:
                uf.make_set(account_id)

            def find(x: str) -> str:
                return uf.find(x)

            def union(x: str, y: str) -> None:
                uf.union(x, y)

            def get_group_size(x: str) -> int:
                return uf.get_size(x)

        except ImportError:
            logger.warning("Optimized Union-Find not available, using standard approach")
            maintain_unionfind_size = False

    if not maintain_unionfind_size:
        # Standard Union-Find structure
        parent: Dict[str, str] = {}
        rank: Dict[str, int] = {}

        def find(x: str) -> str:
            if parent[x] != x:
                parent[x] = find(parent[x])
            return parent[x]

        def union(x: str, y: str) -> None:
            px, py = find(x), find(y)
            if px == py:
                return
            if rank[px] < rank[py]:
                parent[px] = py
            elif rank[px] > rank[py]:
                parent[py] = px
            else:
                # When ranks are equal, always choose the smaller root ID as the new parent
                # This ensures consistent results across runs
                if px < py:
                    parent[py] = px
                    rank[px] += 1
                else:
                    parent[px] = py
                    rank[py] += 1

        def get_group_size(x: str) -> int:
            # Fallback: count group members manually
            root = find(x)
            return sum(1 for p in parent.values() if find(p) == root)

            # Initialize all accounts as their own groups
    logger.debug(f"Initializing standard Union-Find for {len(accounts_df)} accounts")
    for account_id in accounts_df[account_id_col]:
        parent[account_id] = account_id
        rank[account_id] = 0

    # Phase 1.35.2: Fast-path union of exact equals pairs first
    exact_equals_unions = 0
    if "group_join_reason" in candidate_pairs_df.columns:
        exact_pairs = candidate_pairs_df[
            candidate_pairs_df["group_join_reason"] == "exact_equal_raw"
        ].copy()
        
        if not exact_pairs.empty:
            logger.info(f"grouping | processing_exact_equals | pairs={len(exact_pairs)} | backend=union_find")
            
            # Process exact equals pairs (score = 100.0)
            for _, row in exact_pairs.iterrows():
                id1, id2 = row[id_col1], row[id_col2]
                
                # Union the exact equals
                union(id1, id2)
                exact_equals_unions += 1
                
                # Track group membership
                root = find(id1)
                if root not in group_members:
                    group_members[root] = []
                group_members[root].extend([id1, id2])
            
            logger.info(f"grouping | exact_equals_complete | unions={exact_equals_unions} | backend=union_find")
    
    # Remove exact equals pairs from similarity processing
    if "group_join_reason" in candidate_pairs_df.columns:
        candidate_pairs_df = candidate_pairs_df[
            candidate_pairs_df["group_join_reason"] != "exact_equal_raw"
        ].copy()
        logger.info(f"grouping | filtered_exact_equals | remaining_pairs={len(candidate_pairs_df)} | backend=union_find")

    # Group membership tracking
    group_members: Dict[str, List[str]] = defaultdict(list)
    explain_metadata: Dict[str, Dict[str, Any]] = {}

    # Process candidate pairs in score order (highest first)
    perf_settings = config.get("grouping", {}).get("edge_gating", {}).get("performance", {})
    pair_columns = perf_settings.get("pair_columns", [id_col1, id_col2, "score"])  # noqa: F841

    try:
        from src.utils.perf_utils import narrow_sort  # type: ignore

        logger.info("Using narrow sorting for pairs DataFrame")
        sorted_pairs = narrow_sort(candidate_pairs_df, ["score", id_col1, id_col2])
    except Exception:
        # Fallback to standard sorting
        sorted_pairs = candidate_pairs_df.sort_values(
            ["score", id_col1, id_col2], ascending=[False, True, True], kind="mergesort"
        )

    # Add progress logging for pair processing
    pair_progress = ProgressLogger(
        total=len(sorted_pairs),
        label="grouping",
        step_every=50_000,
        secs_every=5.0,
        enable_tqdm=enable_progress,
    )

    # Performance counters
    start_time = pd.Timestamp.now()
    pairs_processed = 0
    unions_performed = 0
    canopy_rejections = 0
    
    # Track edge-gating decisions for tuning
    import collections
    gating_reasons = collections.Counter()

    for _, row in pair_progress.wrap(sorted_pairs.iterrows()):
        id1, id2 = row[id_col1], row[id_col2]
        pairs_processed += 1

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

        # Track gating decisions for tuning
        gating_reasons[reason] += 1

        if not can_join:
            continue

        # Check canopy bound using optimized group size if available
        if maintain_unionfind_size:
            current_group_size = get_group_size(primary_id)
        else:
            current_group = [m for m in group_members[primary_id]] + [primary_id]
            current_group_size = len(current_group)

        if not apply_canopy_bound(current_group_size, primary_id, candidate_id, edge_scores, config):
            logger.debug(
                f"Canopy bound rejected {candidate_id} joining {primary_id}'s group (size: {current_group_size})"
            )
            canopy_rejections += 1
            continue

        # Perform union
        union(primary_id, candidate_id)
        unions_performed += 1

        # Update group membership
        group_members[primary_id].append(candidate_id)

        # Record explain metadata
        shared_tokens = token_sets.get(primary_id, set()) & token_sets.get(candidate_id, set()) - stop_tokens
        explain_metadata[candidate_id] = {
            "group_join_reason": reason,
            "weakest_edge_to_primary": actual_score,
            "shared_tokens_count": len(shared_tokens),
        }

    # Log performance metrics
    end_time = pd.Timestamp.now()
    duration = (end_time - start_time).total_seconds()
    ops_per_sec = pairs_processed / duration if duration > 0 else 0

    # Phase 1.35.2: Include exact equals unions in logging
    total_unions = unions_performed + exact_equals_unions
    
    logger.info(
        f"grouping | backend=union_find | pairs_processed={pairs_processed} | "
        f"unions_similarity={unions_performed} | unions_exact={exact_equals_unions} | "
        f"unions_total={total_unions} | canopies={canopy_rejections} | "
        f"throughput={ops_per_sec:.1f}ops/sec | duration={duration:.1f}s"
    )
    
    # Log edge-gating breakdown for tuning
    logger.info(f"grouping | edge_gating_breakdown | reasons={dict(gating_reasons)}")

    # Stop profiling and save report if enabled
    if profiler is not None:
        try:
            profiler.stop()
            profile_path = "data/interim/grouping_profile.html"
            with open(profile_path, "w") as f:
                f.write(profiler.output_html())
            logger.info(f"Grouping profile saved to {profile_path}")
        except Exception as e:  # pragma: no cover
            logger.warning(f"Failed to save profile: {e}")

    # Build final groups dataframe
    groups_data: List[Dict[str, Any]] = []

    for account_id in accounts_df["account_id"]:
        group_root = find(account_id)
        group_member_list = [m for m in group_members[group_root]] + [group_root]

        # Generate stable group ID
        if stable_group_id is None:
            raise ImportError("stable_group_id is not available")
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
                "weakest_edge_to_primary": explain.get("weakest_edge_to_primary", 0.0),
                "shared_tokens_count": explain.get("shared_tokens_count", 0),
            }
        )

    groups_df = pd.DataFrame(groups_data)

    # Merge back with original account data
    groups_df = groups_df.merge(accounts_df, on="account_id", how="left")

    # Apply memory optimization
    try:
        from src.utils.dtypes import optimize_dataframe_memory  # type: ignore
    except Exception:
        optimize_dataframe_memory = None  # type: ignore

    if optimize_dataframe_memory:
        groups_df = optimize_dataframe_memory(groups_df, "groups")

    # Ensure output has consistent pandas string types
    groups_df = ensure_pandas_strings(groups_df, ["account_id", "group_id"])

    logger.info(f"Created {groups_df['group_id'].nunique()} groups with edge-gating")
    return groups_df


def create_groups_standard(
    accounts_df: pd.DataFrame, candidate_pairs_df: pd.DataFrame, config: Dict[str, Any]
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
    parent: Dict[str, str] = {}
    rank: Dict[str, int] = {}

    def find(x: str) -> str:
        if parent[x] != x:
            parent[x] = find(parent[x])
        return parent[x]

    def union(x: str, y: str) -> None:
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
    if "account_id_1" in candidate_pairs_df.columns and "account_id_2" in candidate_pairs_df.columns:
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
    groups_data: List[Dict[str, Any]] = []
    group_members: Dict[str, List[str]] = defaultdict(list)

    for account_id in accounts_df["account_id"]:
        group_root = find(account_id)
        group_members[group_root].append(account_id)

    if stable_group_id is None:
        raise ImportError("stable_group_id is not available")

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
        from src.utils.dtypes import optimize_dataframe_memory  # type: ignore
    except Exception:
        optimize_dataframe_memory = None  # type: ignore

    if optimize_dataframe_memory:
        groups_df = optimize_dataframe_memory(groups_df, "groups")

    logger.info(f"Created {groups_df['group_id'].nunique()} groups using standard logic")
    return groups_df