"""
Similarity scoring and candidate pairing for Company Junction deduplication.

This module handles:
- Candidate pair generation with blocking
- Similarity scoring using rapidfuzz
- Composite score calculation with penalties
- Suffix and numeric style matching
"""

import pandas as pd
from rapidfuzz import fuzz
from typing import List, Tuple, Dict, Any
import logging
from itertools import combinations
import re
from src.utils.progress import ProgressLogger

logger = logging.getLogger(__name__)


def get_stop_tokens() -> set:
    """
    Get the set of stop tokens used for blocking.

    Returns:
        Set of stop tokens
    """
    return {"inc", "llc", "ltd"}


def pair_scores(
    df_norm: pd.DataFrame, settings: Dict, enable_progress: bool = False
) -> pd.DataFrame:
    """
    Generate candidate pairs and compute similarity scores.

    Args:
        df_norm: DataFrame with normalized name columns
        settings: Configuration settings

    Returns:
        DataFrame with candidate pairs and scores
    """
    logger.info(f"Generating candidate pairs for {len(df_norm)} records")

    # Get configuration
    medium_threshold = settings.get("similarity", {}).get("medium", 84)
    penalties = settings.get("similarity", {}).get("penalty", {})

    # Generate candidate pairs using blocking
    candidate_pairs = _generate_candidate_pairs(df_norm, enable_progress)

    if not candidate_pairs:
        logger.info("No candidate pairs found")
        return pd.DataFrame()

    # Compute similarity scores for each pair
    scores = []

    # Add progress logging for pair scoring
    progress = ProgressLogger(
        total=len(candidate_pairs),
        label="pair-scoring",
        step_every=10_000,
        secs_every=5.0,
        enable_tqdm=enable_progress,
    )

    for idx_a, idx_b in progress.wrap(candidate_pairs):
        try:
            score_data = _compute_pair_score(
                df_norm.loc[idx_a:idx_a].iloc[0],
                df_norm.loc[idx_b:idx_b].iloc[0],
                penalties,
            )
            # Use actual account_id values instead of row indices
            # Handle both standardized and original column names
            account_id_col = (
                "account_id" if "account_id" in df_norm.columns else "Account ID"
            )
            account_id_a = df_norm.loc[idx_a, account_id_col]
            account_id_b = df_norm.loc[idx_b, account_id_col]
            scores.append({"id_a": account_id_a, "id_b": account_id_b, **score_data})
        except KeyError as e:
            logger.warning(f"Skipping pair ({idx_a}, {idx_b}): index not found - {e}")
            continue

    # Create DataFrame and filter by medium threshold
    pairs_df = pd.DataFrame(scores)
    if not pairs_df.empty:
        pairs_df = pairs_df[pairs_df["score"] >= medium_threshold].copy()
        pairs_df = pairs_df.sort_values("score", ascending=False)

    logger.info(f"Generated {len(pairs_df)} candidate pairs above medium threshold")
    return pairs_df


def _generate_candidate_pairs(
    df_norm: pd.DataFrame, enable_progress: bool = False
) -> List[Tuple[int, int]]:
    """
    Generate candidate pairs using blocking strategy.

    Args:
        df_norm: DataFrame with normalized name columns

    Returns:
        List of (idx_a, idx_b) tuples
    """
    pairs: List[Tuple[int, int]] = []

    # Handle empty DataFrame
    if df_norm.empty or "name_core" not in df_norm.columns:
        return pairs

    # Stop tokens for first token blocking (common suffixes to avoid)
    stop_tokens = get_stop_tokens()

    # Safety limit: if too many records, use more aggressive blocking
    max_records = 50000  # Conservative limit
    if len(df_norm) > max_records:
        logger.warning(
            f"Large dataset detected ({len(df_norm)} records). Using aggressive blocking strategy."
        )
        # Use first two tokens for blocking instead of just first
        df_norm["block_key"] = (
            df_norm["name_core"].str.split().str[:2].str.join(" ").fillna("")
        )
    else:
        # Use first token for blocking with stop token logic
        def get_first_token(name: str) -> str:
            tokens = name.split()
            for token in tokens:
                if token.lower() not in stop_tokens:
                    return token
            return (
                tokens[0] if tokens else ""
            )  # Fallback to first token if all are stop tokens

        df_norm["block_key"] = df_norm["name_core"].apply(get_first_token).fillna("")

    # Count pairs to estimate memory usage
    total_pairs = 0
    max_pairs = 10000000  # 10M pairs limit

    unique_blocks = df_norm["block_key"].unique()
    logger.info(f"Using {len(unique_blocks)} blocks for candidate generation")

    # Compute and log top token distribution
    token_counts = df_norm["block_key"].value_counts()
    top_tokens = token_counts.head(10)
    logger.info(
        f"Top first-token keys: {', '.join([f'{k}={v}' for k, v in top_tokens.items()])}"
    )

    # Write block statistics
    try:
        block_stats = []
        for block_key in unique_blocks:
            if pd.isna(block_key) or block_key == "":
                continue
            mask = df_norm["block_key"] == block_key
            block_size = mask.sum()
            block_stats.append({"token": block_key, "count": block_size})

        block_df = pd.DataFrame(block_stats)
        block_df.to_csv("data/interim/block_top_tokens.csv", index=False)
        logger.info("Block statistics written to data/interim/block_top_tokens.csv")
    except Exception as e:
        logger.warning(f"Failed to write block statistics: {e}")

    # Add progress logging for block iteration
    block_progress = ProgressLogger(
        total=len(unique_blocks),
        label="blocks→pairs",
        step_every=500,
        secs_every=5.0,
        enable_tqdm=enable_progress,
    )

    for block_key in block_progress.wrap(unique_blocks):
        if pd.isna(block_key) or block_key == "":
            continue

        # Get indices for records with this block key
        mask = df_norm["block_key"] == block_key
        indices = df_norm[mask].index.tolist()

        # Generate pairs within this block
        if len(indices) > 1:
            block_pairs = list(combinations(indices, 2))
            total_pairs += len(block_pairs)

            # Safety check: if we're generating too many pairs, skip this block
            if total_pairs > max_pairs:
                logger.warning(
                    f"Too many candidate pairs ({total_pairs}). Skipping remaining blocks."
                )
                break

            pairs.extend(block_pairs)

    # Remove duplicates and return
    unique_pairs = list(set(pairs))
    logger.info(f"Generated {len(unique_pairs)} unique candidate pairs")

    if len(unique_pairs) > max_pairs:
        logger.warning(f"Generated {len(unique_pairs)} pairs, limiting to {max_pairs}")
        unique_pairs = unique_pairs[:max_pairs]

    return unique_pairs


def _compute_pair_score(
    row_a: pd.Series, row_b: pd.Series, penalties: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Compute similarity score for a pair of records.

    Args:
        row_a: First record
        row_b: Second record
        penalties: Penalty configuration

    Returns:
        Dictionary with score components
    """
    name_core_a = row_a["name_core"]
    name_core_b = row_b["name_core"]

    # Compute rapidfuzz ratios
    ratio_name = fuzz.token_sort_ratio(name_core_a, name_core_b)
    ratio_set = fuzz.token_set_ratio(name_core_a, name_core_b)

    # Compute Jaccard similarity
    tokens_a = set(name_core_a.split())
    tokens_b = set(name_core_b.split())

    if tokens_a and tokens_b:
        intersection = len(tokens_a & tokens_b)
        union = len(tokens_a | tokens_b)
        jaccard = intersection / union if union > 0 else 0
    else:
        jaccard = 0

    # Check numeric style match
    num_style_match = _check_numeric_style_match(name_core_a, name_core_b)

    # Check suffix match
    suffix_match = row_a["suffix_class"] == row_b["suffix_class"]

    # Check punctuation mismatch
    punctuation_mismatch = _check_punctuation_mismatch(row_a, row_b)

    # Compute base score
    base = 0.45 * ratio_name + 0.35 * ratio_set + 20 * jaccard

    # Apply penalties
    if not num_style_match:
        base -= penalties.get("num_style_mismatch", 5)

    if not suffix_match:
        base -= penalties.get("suffix_mismatch", 25)

    if punctuation_mismatch:
        base -= penalties.get("punctuation_mismatch", 3)

    # Clip to 0-100 range
    score = max(0, min(100, round(base)))

    return {
        "score": score,
        "ratio_name": ratio_name,
        "ratio_set": ratio_set,
        "jaccard": jaccard,
        "num_style_match": num_style_match,
        "suffix_match": suffix_match,
        "punctuation_mismatch": punctuation_mismatch,
        "base_score": base,
    }


def _check_numeric_style_match(name_a: str, name_b: str) -> bool:
    """
    Check if two names have matching numeric styles.

    Args:
        name_a: First normalized name
        name_b: Second normalized name

    Returns:
        True if numeric styles match
    """

    # Extract numeric patterns
    def extract_numeric_pattern(text: str) -> set[str]:
        # Find patterns like "20 20", "100 200", etc.
        pattern = r"\d+\s+\d+"
        matches = re.findall(pattern, text)
        return set(matches)

    pattern_a = extract_numeric_pattern(name_a)
    pattern_b = extract_numeric_pattern(name_b)

    # If both have numeric patterns, they should match
    if pattern_a and pattern_b:
        return pattern_a == pattern_b

    # If neither has numeric patterns, consider it a match
    if not pattern_a and not pattern_b:
        return True

    # If only one has numeric patterns, it's a mismatch
    return False


def save_candidate_pairs(pairs_df: pd.DataFrame, output_path: str) -> None:
    """
    Save candidate pairs to parquet file.

    Args:
        pairs_df: DataFrame with candidate pairs
        output_path: Output file path
    """
    if not pairs_df.empty:
        pairs_df.to_parquet(output_path, index=False)
        logger.info(f"Saved {len(pairs_df)} candidate pairs to {output_path}")
    else:
        logger.warning("No candidate pairs to save")


def load_candidate_pairs(input_path: str) -> pd.DataFrame:
    """
    Load candidate pairs from parquet file.

    Args:
        input_path: Input file path

    Returns:
        DataFrame with candidate pairs
    """
    try:
        pairs_df = pd.read_parquet(input_path)
        logger.info(f"Loaded {len(pairs_df)} candidate pairs from {input_path}")
        return pairs_df
    except Exception as e:
        logger.error(f"Error loading candidate pairs: {e}")
        return pd.DataFrame()


def _check_punctuation_mismatch(row_a: pd.Series, row_b: pd.Series) -> bool:
    """
    Check if two records have conflicting punctuation patterns.

    Args:
        row_a: First record
        row_b: Second record

    Returns:
        True if punctuation patterns conflict
    """
    # Check for semicolon mismatch
    has_semicolon_a = row_a.get("has_semicolon", False)
    has_semicolon_b = row_b.get("has_semicolon", False)

    if has_semicolon_a != has_semicolon_b:
        return True

    # Check for parentheses mismatch
    has_parentheses_a = row_a.get("has_parentheses", False)
    has_parentheses_b = row_b.get("has_parentheses", False)

    if has_parentheses_a != has_parentheses_b:
        return True

    return False
