"""Blocking and candidate pair generation for similarity matching.
"""

import logging
from itertools import combinations
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd

from src.utils.parallel_protocols import ExecutorLike

from .diagnostics import write_blocking_diagnostics

logger = logging.getLogger(__name__)


def get_stop_tokens(settings: Dict[str, Any]) -> Set[str]:
    """Get stop tokens from configuration."""
    blocking_settings = settings.get("similarity", {}).get("blocking", {})
    stop_tokens = set(blocking_settings.get("stop_tokens", ["inc", "llc", "ltd"]))
    return stop_tokens


def generate_candidate_pairs_soft_ban(
    df_norm: pd.DataFrame,
    enable_progress: bool = False,
    parallel_executor: Optional[ExecutorLike] = None,
    interim_dir: Optional[str] = None,
    settings: Optional[Dict[str, Any]] = None,
) -> List[Tuple[int, int]]:
    """Generate candidate pairs using soft-ban blocking strategy.

    Args:
        df_norm: DataFrame with normalized names
        enable_progress: Enable progress logging
        parallel_executor: Optional parallel executor
        interim_dir: Directory for interim files
        settings: Configuration settings

    Returns:
        List of candidate pair tuples (index_a, index_b)

    """
    pairs: List[Tuple[int, int]] = []

    if df_norm.empty or "name_core" not in df_norm.columns:
        return pairs

    # Get blocking settings and normalize to lowercase
    blocking_settings = (
        settings.get("similarity", {}).get("blocking", {}) if settings else {}
    )
    allowlist_tokens = {
        t.lower() for t in blocking_settings.get("allowlist_tokens", [])
    }
    allowlist_bigrams = {
        t.lower() for t in blocking_settings.get("allowlist_bigrams", [])
    }
    denylist_tokens = {t.lower() for t in blocking_settings.get("denylist_tokens", [])}
    stop_tokens = {t.lower() for t in get_stop_tokens(settings or {})}

    soft_ban_settings = blocking_settings.get("soft_ban", {})
    shard_strategy = soft_ban_settings.get("shard_strategy", "second_token")
    fallback_shard = soft_ban_settings.get("fallback_shard", "char_trigram")
    max_shard_size = soft_ban_settings.get("max_shard_size", 200)
    char_bigram_gate = soft_ban_settings.get("char_bigram_gate", 0.1)
    length_window = soft_ban_settings.get("length_window", 10)
    min_token_overlap = soft_ban_settings.get("min_token_overlap", 1)
    max_candidates_per_record = soft_ban_settings.get("max_candidates_per_record", 50)
    block_cap = soft_ban_settings.get("block_cap", 800)

    # Create local Series for blocking keys (no mutation of input DataFrame)
    def get_first_token(name: str) -> str:
        tokens = name.split()
        for token in tokens:
            if token.lower() not in stop_tokens:
                return token.lower()
        return tokens[0].lower() if tokens else ""

    def get_bigram_key(name: str) -> str:
        tokens = name.split()
        if len(tokens) >= 2:
            bigram = f"{tokens[0].lower()} {tokens[1].lower()}"
            if bigram in allowlist_bigrams:
                return bigram
        return ""

    # Create local blocking keys
    block_key_series = df_norm["name_core"].apply(get_first_token).fillna("")
    bigram_key_series = df_norm["name_core"].apply(get_bigram_key).fillna("")

    # Ensure string dtype
    block_key_series = block_key_series.astype("string")
    bigram_key_series = bigram_key_series.astype("string")

    # Initialize diagnostics
    block_stats = []
    brand_suggestions: List[Dict[str, Any]] = []

    # Allowlisted bigram pass: force full pairing within those bigram groups
    if len(allowlist_bigrams) > 0:
        bigram_hits = bigram_key_series[bigram_key_series != ""]
        if not bigram_hits.empty:
            for bg in bigram_hits.unique().tolist():
                bg_mask = bigram_key_series == bg
                bg_df = df_norm[bg_mask]
                if len(bg_df) > 1:
                    if len(bg_df) > block_cap:
                        # Safety rail: shard huge bigram groups
                        bg_pairs = _apply_standard_sharding(
                            bg_df, bg, shard_strategy, block_cap, fallback_shard,
                        )
                        strategy = "allowlisted_bigram_sharded"
                    else:
                        bg_pairs = list(combinations(bg_df.index, 2))
                        strategy = "allowlisted_bigram"
                    pairs.extend(bg_pairs)
                    block_stats.append(
                        {
                            "token": bg,
                            "count": len(bg_df),
                            "strategy": strategy,
                            "pairs_generated": len(bg_pairs),
                            "pairs_capped": 0,
                        },
                    )

    # Group by block key
    unique_blocks = block_key_series.unique()
    logger.info(f"Processing {len(unique_blocks)} unique block keys")

    for i, block_key in enumerate(unique_blocks):
        if pd.isna(block_key) or block_key == "":
            continue

        # Get records for this block
        block_mask = block_key_series == block_key
        block_df = df_norm[block_mask].copy()
        block_size = len(block_df)

        if block_size <= 1:
            continue

        # Progress logging every 100 blocks
        if enable_progress and i % 100 == 0:
            logger.info(
                f"Processed {i}/{len(unique_blocks)} blocks, generated {len(pairs)} pairs so far",
            )

        # Determine strategy based on allowlist/denylist
        if block_key in allowlist_tokens:
            # Allowlisted: generate all pairs (with safety rail for huge blocks)
            if block_size > block_cap:
                # Keep recall, just shard deterministically (no prefilter)
                block_pairs = _apply_standard_sharding(
                    block_df, block_key, shard_strategy, block_cap, fallback_shard,
                )
                pairs_generated = len(block_pairs)
                pairs_capped = 0
                strategy = "allowlisted_sharded"
            else:
                block_pairs = list(combinations(block_df.index, 2))
                pairs_generated = len(block_pairs)
                pairs_capped = 0
                strategy = "allowlisted"

        elif block_key in denylist_tokens and block_size > block_cap:
            # Denylisted and large: apply soft-ban sharding
            strategy = "soft_ban_sharded"
            block_pairs = _apply_soft_ban_sharding(
                block_df,
                block_key,
                shard_strategy,
                fallback_shard,
                max_shard_size,
                char_bigram_gate,
                length_window,
                min_token_overlap,
                max_candidates_per_record,
            )
            pairs_generated = len(block_pairs)
            pairs_capped = max(
                0, (block_size * (block_size - 1) // 2) - pairs_generated,
            )

        elif block_size > block_cap:
            # Large but not denylisted: standard sharding
            strategy = "standard_sharded"
            block_pairs = _apply_standard_sharding(
                block_df, block_key, shard_strategy, block_cap, fallback_shard,
            )
            pairs_generated = len(block_pairs)
            pairs_capped = 0

        else:
            # Small block: generate all pairs
            strategy = "full_pairs"
            block_pairs = list(combinations(block_df.index, 2))
            pairs_generated = len(block_pairs)
            pairs_capped = 0

        pairs.extend(block_pairs)

        # Collect block statistics
        block_stats.append(
            {
                "token": block_key,
                "count": block_size,
                "strategy": strategy,
                "pairs_generated": pairs_generated,
                "pairs_capped": pairs_capped,
            },
        )

    # Deduplicate pairs using a set-based approach
    if pairs:
        pairs_set = set(pairs)
        pairs = list(pairs_set)

    # Write diagnostics
    if interim_dir:
        write_blocking_diagnostics(block_stats, brand_suggestions, interim_dir)

    # Log summary
    strategies_used = set(stat["strategy"] for stat in block_stats)
    logger.info(
        f"Soft-ban blocking: Generated {len(pairs)} candidate pairs from {len(block_stats)} blocks",
    )
    logger.info(f"Strategies used: {', '.join(sorted(strategies_used))}")

    return pairs


def _create_shards_with_fallback(
    df: pd.DataFrame, primary: str, fallback: str, max_size: int,
) -> List[pd.DataFrame]:
    """Create shards with fallback strategy for oversized shards."""
    shards = _create_shards(df, primary, max_size)
    out = []
    for s in shards:
        if len(s) > max_size and fallback and fallback != primary:
            out.extend(_create_shards(s, fallback, max_size))
        else:
            out.append(s)
    return out


def _apply_soft_ban_sharding(
    block_df: pd.DataFrame,
    block_key: str,
    shard_strategy: str,
    fallback_shard: str,
    max_shard_size: int,
    char_bigram_gate: float,
    length_window: int,
    min_token_overlap: int,
    max_candidates_per_record: int,
) -> List[Tuple[int, int]]:
    """Apply soft-ban sharding with prefiltering."""
    shards = _create_shards_with_fallback(
        block_df, shard_strategy, fallback_shard, max_shard_size,
    )

    pairs = []
    for shard_df in shards:
        if len(shard_df) <= 1:
            continue

        # Apply prefiltering within shard
        shard_pairs = _apply_prefiltering(
            shard_df,
            char_bigram_gate,
            length_window,
            min_token_overlap,
            max_candidates_per_record,
        )
        pairs.extend(shard_pairs)

    return pairs


def _apply_standard_sharding(
    block_df: pd.DataFrame,
    block_key: str,
    shard_strategy: str,
    block_cap: int,
    fallback_shard: Optional[str] = None,
) -> List[Tuple[int, int]]:
    """Apply standard sharding for large blocks."""
    shards = _create_shards_with_fallback(
        block_df, shard_strategy, fallback_shard or shard_strategy, block_cap,
    )

    pairs = []
    for shard_df in shards:
        if len(shard_df) > 1:
            shard_pairs = list(combinations(shard_df.index, 2))
            pairs.extend(shard_pairs)

    return pairs


def _create_shards(
    block_df: pd.DataFrame, shard_strategy: str, max_shard_size: int,
) -> List[pd.DataFrame]:
    """Create shards based on strategy."""
    shards = []

    if shard_strategy == "second_token":
        # Shard by second token
        block_df["shard_key"] = block_df["name_core"].str.split().str[1].fillna("")
        shard_groups = block_df.groupby("shard_key")

        for shard_key, shard_df in shard_groups:
            if len(shard_df) > max_shard_size:
                # Further shard by third token
                for _, sub_group in shard_df.groupby(
                    shard_df["name_core"].str.split().str[2].fillna(""),
                ):
                    shards.append(sub_group)
            else:
                shards.append(shard_df)

    elif shard_strategy == "char_trigram":
        # Shard by first 3 characters
        block_df["shard_key"] = block_df["name_core"].str[:3].fillna("")
        shard_groups = block_df.groupby("shard_key")

        for shard_key, shard_df in shard_groups:
            if len(shard_df) > max_shard_size:
                # Further shard by 4-gram
                for _, sub_group in shard_df.groupby(
                    shard_df["name_core"].str[:4].fillna(""),
                ):
                    shards.append(sub_group)
            else:
                shards.append(shard_df)

    else:
        # Fallback: no sharding
        shards.append(block_df)

    return shards


def _apply_prefiltering(
    shard_df: pd.DataFrame,
    char_bigram_gate: float,
    length_window: int,
    min_token_overlap: int,
    max_candidates_per_record: int,
) -> List[Tuple[int, int]]:
    """Apply prefiltering to candidate pairs within a shard."""
    names = shard_df["name_core"].tolist()
    indices = shard_df.index.tolist()

    # Precompute token sets to reduce inner-loop work
    token_sets = [set(n.split()) for n in names]

    pairs = []
    for i in range(len(indices)):
        candidates_for_i = 0
        for j in range(i + 1, len(indices)):
            if candidates_for_i >= max_candidates_per_record:
                break

            name_a, name_b = names[i], names[j]

            # Length window check
            if abs(len(name_a) - len(name_b)) > length_window:
                continue

            # Token overlap check (using precomputed sets)
            if len(token_sets[i] & token_sets[j]) < min_token_overlap:
                continue

            # Character bigram overlap check
            if _char_bigram_overlap(name_a, name_b) < char_bigram_gate:
                continue

            pairs.append((indices[i], indices[j]))
            candidates_for_i += 1

    return pairs


def _char_bigram_overlap(name_a: str, name_b: str) -> float:
    """Calculate character bigram overlap between two names."""

    def get_bigrams(text: str) -> set:
        return set(text[i : i + 2] for i in range(len(text) - 1))

    bigrams_a = get_bigrams(name_a.lower())
    bigrams_b = get_bigrams(name_b.lower())

    if not bigrams_a or not bigrams_b:
        return 0.0

    intersection = len(bigrams_a & bigrams_b)
    union = len(bigrams_a | bigrams_b)

    return intersection / union if union > 0 else 0.0
