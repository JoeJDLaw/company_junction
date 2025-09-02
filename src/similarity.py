"""
Similarity scoring and candidate pairing for Company Junction deduplication.

This module handles:
- Candidate pair generation with blocking
- Similarity scoring using rapidfuzz
- Composite score calculation with penalties
- Suffix and numeric style matching
- Parallel execution support
"""

import pandas as pd
import numpy as np
from rapidfuzz import fuzz
from typing import List, Tuple, Dict, Any, Optional
import logging
from itertools import combinations
import re
from src.utils.progress import ProgressLogger
from src.utils.parallel_utils import ParallelExecutor

logger = logging.getLogger(__name__)


def get_stop_tokens() -> set:
    """
    Get the set of stop tokens used for blocking.

    Returns:
        Set of stop tokens
    """
    return {"inc", "llc", "ltd"}


def pair_scores(
    df_norm: pd.DataFrame,
    settings: Dict,
    enable_progress: bool = False,
    parallel_executor: Optional[ParallelExecutor] = None,
    interim_dir: Optional[str] = None,
) -> pd.DataFrame:
    """
    Generate candidate pairs and compute similarity scores.

    Args:
        df_norm: DataFrame with normalized name columns
        settings: Configuration settings
        enable_progress: Enable progress logging
        parallel_executor: Optional parallel executor for parallel processing

    Returns:
        DataFrame with candidate pairs and scores
    """
    logger.info(f"Generating candidate pairs for {len(df_norm)} records")

    # Get configuration
    medium_threshold = settings.get("similarity", {}).get("medium", 84)
    penalties = settings.get("similarity", {}).get("penalty", {})

    # Generate candidate pairs using blocking
    candidate_pairs = _generate_candidate_pairs(
        df_norm, enable_progress, parallel_executor, interim_dir, settings
    )

    # Drop temporary blocking columns
    if "secondary_key" in df_norm.columns:
        df_norm.drop(columns=["secondary_key"], inplace=True)

    if not candidate_pairs:
        logger.info("No candidate pairs found")
        return pd.DataFrame()

    # Compute similarity scores for each pair
    scores = _compute_similarity_scores_parallel(
        df_norm,
        candidate_pairs,
        penalties,
        enable_progress,
        parallel_executor,
        interim_dir,
    )

    # Create DataFrame and filter by medium threshold
    pairs_df = pd.DataFrame(scores)
    if not pairs_df.empty:
        pairs_df = pairs_df[pairs_df["score"] >= medium_threshold].copy()
        # Ensure deterministic ordering
        pairs_df = pairs_df.sort_values(
            ["id_a", "id_b", "score"], ascending=[True, True, False]
        )

    logger.info(f"Generated {len(pairs_df)} candidate pairs above medium threshold")
    return pairs_df


def _generate_candidate_pairs(
    df_norm: pd.DataFrame,
    enable_progress: bool = False,
    parallel_executor: Optional[ParallelExecutor] = None,
    interim_dir: Optional[str] = None,
    settings: Optional[Dict[str, Any]] = None,
) -> List[Tuple[int, int]]:
    """
    Generate candidate pairs using blocking strategy with deterministic block cap and secondary blocking.

    Args:
        df_norm: DataFrame with normalized name columns
        enable_progress: Enable progress logging
        parallel_executor: Optional parallel executor for parallel processing
        interim_dir: Optional directory for interim files
        settings: Configuration settings

    Returns:
        List of (idx_a, idx_b) tuples
    """
    pairs: List[Tuple[int, int]] = []

    # Handle empty DataFrame
    if df_norm.empty or "name_core" not in df_norm.columns:
        return pairs

    # Get performance settings
    perf_settings = (
        settings.get("similarity", {}).get("performance", {}) if settings else {}
    )
    block_cap = perf_settings.get("block_cap", 800)
    secondary_blocking = perf_settings.get("secondary_blocking", "first_two_tokens")
    enable_prefilters = perf_settings.get("enable_vectorized_prefilters", True)
    max_length_diff = perf_settings.get("max_length_diff", 5)

    # Stop tokens for first token blocking
    stop_tokens = get_stop_tokens()

    # Primary blocking key
    def get_first_token(name: str) -> str:
        tokens = name.split()
        for token in tokens:
            if token.lower() not in stop_tokens:
                return token
        return tokens[0] if tokens else ""

    df_norm["block_key"] = df_norm["name_core"].apply(get_first_token).fillna("")

    # Secondary blocking key
    if secondary_blocking == "first_two_tokens":
        df_norm["secondary_key"] = (
            df_norm["name_core"].str.split().str[:2].str.join(" ").fillna("")
        )
    elif secondary_blocking == "char_bigrams":

        def get_bigrams(text: str) -> str:
            return " ".join([text[i : i + 2] for i in range(len(text) - 1)])

        df_norm["secondary_key"] = df_norm["name_core"].apply(get_bigrams).fillna("")
    else:
        df_norm["secondary_key"] = df_norm["block_key"]  # Fallback to primary key

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
        if interim_dir:
            block_stats_path = f"{interim_dir}/block_top_tokens.csv"
        else:
            block_stats_path = "data/interim/block_top_tokens.csv"
        block_df.to_csv(block_stats_path, index=False)
        logger.info(f"Block statistics written to {block_stats_path}")
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
        # Check for stop flag
        if parallel_executor and parallel_executor.stop_flag.is_set():
            logger.info("Stop flag set, interrupting candidate pair generation")
            break

        if pd.isna(block_key) or block_key == "":
            continue

        # Get indices for records with this block key
        mask = df_norm["block_key"] == block_key
        block_df = df_norm[mask].copy()
        block_size = len(block_df)

        # Skip tiny blocks
        if block_size <= 1:
            continue

        # Apply deterministic block cap if needed
        if block_size > block_cap:
            logger.info(
                f"Block {block_key} exceeds cap ({block_size} > {block_cap}). Using secondary blocking."
            )

            # Group by secondary key within the block
            secondary_groups = block_df.groupby("secondary_key")

            for _, group_df in secondary_groups:
                group_indices = group_df.index.tolist()
                if len(group_indices) > 1:
                    # Apply vectorized prefilters if enabled
                    if enable_prefilters:
                        # Get name lengths
                        name_lengths = group_df["name_core"].str.len()

                        # Create a matrix of length differences
                        length_diffs = abs(
                            name_lengths.values[:, None] - name_lengths.values
                        )

                        # Get valid pairs based on length difference
                        valid_pairs = np.where(length_diffs <= max_length_diff)

                        # Convert to list of tuples
                        filtered_pairs = list(
                            zip(
                                [group_indices[i] for i in valid_pairs[0]],
                                [group_indices[j] for j in valid_pairs[1]],
                            )
                        )

                        # Remove self-pairs and ensure i < j
                        filtered_pairs = [(i, j) for i, j in filtered_pairs if i < j]
                    else:
                        filtered_pairs = list(combinations(group_indices, 2))

                    total_pairs += len(filtered_pairs)
                    pairs.extend(filtered_pairs)

                    # Safety check: if we're generating too many pairs, skip remaining
                    if total_pairs > max_pairs:
                        logger.warning(
                            f"Too many candidate pairs ({total_pairs}). Skipping remaining blocks."
                        )
                        break
        else:
            # For small blocks, process all pairs
            block_indices = block_df.index.tolist()
            block_pairs = list(combinations(block_indices, 2))
            total_pairs += len(block_pairs)
            pairs.extend(block_pairs)

        # Safety check: if we're generating too many pairs, skip remaining blocks
        if total_pairs > max_pairs:
            logger.warning(
                f"Too many candidate pairs ({total_pairs}). Skipping remaining blocks."
            )
            break

    # Remove duplicates and return
    unique_pairs = list(set(pairs))
    logger.info(f"Generated {len(unique_pairs)} unique candidate pairs")

    if len(unique_pairs) > max_pairs:
        logger.warning(f"Generated {len(unique_pairs)} pairs, limiting to {max_pairs}")
        unique_pairs = unique_pairs[:max_pairs]

    return unique_pairs


def _compute_pair_score_optimized(
    name_core_a: str,
    name_core_b: str,
    suffix_class_a: str,
    suffix_class_b: str,
    account_id_a: str,
    account_id_b: str,
    penalties: Dict[str, Any],
    threshold: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Compute similarity score for a pair of records using optimized data layout.

    Args:
        name_core_a: First record's name_core
        name_core_b: Second record's name_core
        suffix_class_a: First record's suffix_class
        suffix_class_b: Second record's suffix_class
        idx_a: First record's index
        idx_b: Second record's index
        penalties: Penalty configuration

    Returns:
        Dictionary with score components
    """
    # Compute rapidfuzz ratios with score_cutoff
    # Use provided threshold or fall back to default
    score_cutoff = threshold if threshold is not None else 0
    ratio_name = fuzz.token_sort_ratio(
        name_core_a, name_core_b, score_cutoff=score_cutoff
    )
    ratio_set = fuzz.token_set_ratio(
        name_core_a, name_core_b, score_cutoff=score_cutoff
    )

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
    suffix_match = suffix_class_a == suffix_class_b

    # Compute base score
    base = 0.45 * ratio_name + 0.35 * ratio_set + 20 * jaccard

    # Apply penalties
    if not num_style_match:
        base -= penalties.get("num_style_mismatch", 5)

    if not suffix_match:
        base -= penalties.get("suffix_mismatch", 25)

    # Clip to 0-100 range
    score = max(0, min(100, round(base)))

    return {
        "id_a": account_id_a,
        "id_b": account_id_b,
        "score": score,
        "ratio_name": ratio_name,
        "ratio_set": ratio_set,
        "jaccard": jaccard,
        "num_style_match": num_style_match,
        "suffix_match": suffix_match,
        "base_score": base,
    }


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


def _compute_similarity_scores_parallel(
    df_norm: pd.DataFrame,
    candidate_pairs: List[Tuple[int, int]],
    penalties: Dict[str, Any],
    enable_progress: bool = False,
    parallel_executor: Optional[ParallelExecutor] = None,
    interim_dir: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Compute similarity scores for candidate pairs in parallel with optimized data layout.

    Args:
        df_norm: DataFrame with normalized name columns
        candidate_pairs: List of (idx_a, idx_b) tuples
        penalties: Dictionary of penalty values
        enable_progress: Enable progress logging
        parallel_executor: Optional parallel executor for parallel processing

    Returns:
        List of dictionaries with similarity scores and metadata
    """
    # Create index mapping from original to filtered indices
    filtered_indices = df_norm.index.values
    index_map = {idx: i for i, idx in enumerate(filtered_indices)}

    # Convert frequently used columns to numpy arrays for better memory layout
    name_core_array = df_norm["name_core"].values
    suffix_class_array = df_norm["suffix_class"].values
    account_id_array = df_norm["account_id"].values

    # Create memory-mapped arrays for parallel processing
    if parallel_executor and parallel_executor.backend == "loky":
        try:
            from joblib import dump, load
            import tempfile
            import os

            # Create temporary files for memory mapping
            with tempfile.NamedTemporaryFile(delete=False) as f_name:
                dump(name_core_array, f_name.name)
                name_core_mmap = load(f_name.name, mmap_mode="r")

            with tempfile.NamedTemporaryFile(delete=False) as f_suffix:
                dump(suffix_class_array, f_suffix.name)
                suffix_class_mmap = load(f_suffix.name, mmap_mode="r")

            # Use memory-mapped arrays
            name_core_array = name_core_mmap
            suffix_class_array = suffix_class_mmap

            # Clean up temp files when done
            def cleanup():
                os.unlink(f_name.name)
                os.unlink(f_suffix.name)

            import atexit

            atexit.register(cleanup)

        except ImportError:
            logger.warning(
                "joblib not available for memory mapping - using regular arrays"
            )
    """
    Compute similarity scores for candidate pairs using parallel execution.

    Args:
        df_norm: DataFrame with normalized name columns
        candidate_pairs: List of (idx_a, idx_b) tuples
        penalties: Penalty configuration
        enable_progress: Enable progress logging
        parallel_executor: Optional parallel executor for parallel processing

    Returns:
        List of score dictionaries
    """
    # Process pairs in parallel if executor available
    if parallel_executor:
        # Use execute_chunked for optimal parallel processing with balanced chunks
        # This will automatically create balanced chunks and handle parallel execution

        # Create a closure that uses the memory-mapped arrays
        def process_chunk(chunk):
            return [
                _compute_pair_score_optimized(
                    name_core_array[index_map[idx_a]],
                    name_core_array[index_map[idx_b]],
                    suffix_class_array[index_map[idx_a]],
                    suffix_class_array[index_map[idx_b]],
                    account_id_array[index_map[idx_a]],
                    account_id_array[index_map[idx_b]],
                    penalties,
                    threshold=0,  # No cutoff for scoring, filter later
                )
                for idx_a, idx_b in chunk
            ]

        # Add progress logging for pair scoring
        pair_progress = ProgressLogger(
            total=len(candidate_pairs),
            label="pair-scoring",
            step_every=50000,  # Increased from 5000 for better performance
            secs_every=5.0,
            enable_tqdm=enable_progress,
        )

        # Process all pairs using execute_chunked for optimal parallel processing
        scores = parallel_executor.execute_chunked(
            process_chunk,
            candidate_pairs,
            operation_name="similarity_scoring_parallel",
        )

        # Flatten the results - execute_chunked returns a list of lists
        flattened_scores = []
        for chunk_scores in scores:
            flattened_scores.extend(chunk_scores)
        scores = flattened_scores

    else:
        # Process pairs sequentially with optimized arrays
        pair_progress = ProgressLogger(
            total=len(candidate_pairs),
            label="pair-scoring",
            step_every=5000,
            secs_every=5.0,
            enable_tqdm=enable_progress,
        )

        scores = [
            _compute_pair_score_optimized(
                name_core_array[index_map[idx_a]],
                name_core_array[index_map[idx_b]],
                suffix_class_array[index_map[idx_a]],
                suffix_class_array[index_map[idx_b]],
                account_id_array[index_map[idx_a]],
                account_id_array[index_map[idx_b]],
                penalties,
                threshold=0,  # No cutoff for scoring, filter later
            )
            for idx_a, idx_b in pair_progress.wrap(candidate_pairs)
        ]

    logger.info(f"Computed scores for {len(candidate_pairs)} pairs")
    return scores
