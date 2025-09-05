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
    profile: bool = False,
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
    
        # Ensure pandas strings for consistent .str accessor behavior
    from src.utils.duckdb_utils import ensure_pandas_strings
    df_norm = ensure_pandas_strings(df_norm, ["name_core", "account_id"])
    logger.info("Ensured pandas string types for consistent string operations")
    
    # Enable profiling if requested
    if profile:
        try:
            import pyinstrument
            profiler = pyinstrument.Profiler()
            profiler.start()
            logger.info("Profiling enabled for similarity scoring")
        except ImportError:
            logger.warning("pyinstrument not available for profiling")
            profile = False

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
        settings,
    )

    # Create DataFrame and filter by medium threshold
    records = scores  # clearer than 'scores' at this point
    pairs_df = pd.DataFrame.from_records(records)
    

    # Non-invasive sanity: if we accidentally built from headers, 'score' won't exist.
    if not pairs_df.empty and "score" not in pairs_df.columns:
        raise TypeError(
            "similarity: expected records with a 'score' field; built DataFrame lacks 'score' column."
        )
    if not pairs_df.empty:
        # Log score distribution before filtering for debugging
        score_counts = pairs_df["score"].value_counts().sort_index(ascending=False)
        logger.info(f"Score distribution before filtering: {dict(score_counts.head(10))}")
        logger.info(f"Score range: {pairs_df['score'].min():.1f} - {pairs_df['score'].max():.1f}")
        logger.info(f"Medium threshold: {medium_threshold}")
        
        # Show top pairs for debugging
        top_pairs = pairs_df.nlargest(5, "score")[["id_a", "id_b", "score"]]
        logger.info(f"Top 5 pairs by score:\n{top_pairs.to_string()}")
        
        pairs_df = pairs_df[pairs_df["score"] >= medium_threshold].copy()
        
        # Use narrow sorting to reduce memory copies
        try:
            from src.utils.perf_utils import narrow_sort
            pairs_df = narrow_sort(pairs_df, ["id_a", "id_b", "score"])
        except ImportError:
            # Fallback to standard sorting with column selection to reduce memory traffic
            sort_columns = ["id_a", "id_b", "score"]
            if all(col in pairs_df.columns for col in sort_columns):
                # Sort only the needed columns to reduce memory copies
                sorted_indices = pairs_df[sort_columns].sort_values(
                    sort_columns, ascending=[True, True, False]
                ).index
                pairs_df = pairs_df.reindex(sorted_indices)
            else:
                # Fallback to standard sorting
                pairs_df = pairs_df.sort_values(
                    ["id_a", "id_b", "score"], ascending=[True, True, False]
                )

    # Log performance metrics
    total_pairs_generated = len(candidate_pairs) if 'candidate_pairs' in locals() else 0
    pairs_above_threshold = len(pairs_df)
    
    logger.info(f"Generated {pairs_above_threshold} candidate pairs above medium threshold")
    logger.info(f"Performance metrics: {total_pairs_generated} total pairs, {pairs_above_threshold} kept ({pairs_above_threshold/total_pairs_generated*100:.1f}% retention)")
    
    # Ensure output has consistent pandas string types
    if not pairs_df.empty:
        pairs_df = ensure_pandas_strings(pairs_df, ["id_a", "id_b"])
        logger.info("Ensured output pandas string types for consistency")
    
    # Stop profiling and save report if enabled
    if profile and 'profiler' in locals():
        try:
            profiler.stop()
            if interim_dir:
                profile_path = f"{interim_dir}/similarity_profile.html"
            else:
                profile_path = "data/interim/similarity_profile.html"
            
            with open(profile_path, 'w') as f:
                f.write(profiler.output_html())
            
            logger.info(f"Similarity scoring profile saved to {profile_path}")
        except Exception as e:
            logger.warning(f"Failed to save profile: {e}")
    
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
    shard_jumbo_blocks = perf_settings.get("shard_jumbo_blocks", True)
    shard_strategy = perf_settings.get("shard_strategy", "third_token_initial")
    ban_top_tokens = perf_settings.get("ban_top_tokens", {})
    ban_enabled = ban_top_tokens.get("enable", False)
    ban_top_k = ban_top_tokens.get("top_k", 30)

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
    
    # Ban top tokens if enabled
    banned_tokens = set()
    if ban_enabled:
        banned_tokens = set(token_counts.head(ban_top_k).index)
        logger.info(f"Banned top {len(banned_tokens)} tokens: {list(banned_tokens)[:5]}...")
        
        # Filter out banned tokens from processing
        df_norm = df_norm[~df_norm["block_key"].isin(banned_tokens)].copy()
        unique_blocks = df_norm["block_key"].unique()
        logger.info(f"After banning: {len(unique_blocks)} blocks remaining")

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

    # Temporarily bypass ProgressLogger to debug the issue
    for block_key in unique_blocks:
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

            # Apply jumbo block sharding if enabled
            if shard_jumbo_blocks and block_size > block_cap * 2:
                logger.info(f"Block {block_key} is jumbo ({block_size} records), applying sharding")
                
                if shard_strategy == "third_token_initial":
                    # Shard by first letter of third token
                    def get_third_token_initial(name):
                        tokens = name.split()
                        if len(tokens) >= 3:
                            return tokens[2][0].lower() if tokens[2] else ""
                        return ""
                    
                    block_df["shard_key"] = block_df["name_core"].apply(get_third_token_initial)
                    shard_groups = block_df.groupby("shard_key")
                    
                elif shard_strategy == "first_bigram":
                    # Shard by first two characters
                    block_df["shard_key"] = block_df["name_core"].str[:2].str.lower()
                    shard_groups = block_df.groupby("shard_key")
                    
                else:
                    # Fallback to secondary blocking
                    shard_groups = block_df.groupby("secondary_key")
                
                # Process each shard
                for _, shard_df in shard_groups:
                    shard_indices = shard_df.index.tolist()
                    if len(shard_indices) > 1:
                        filtered_pairs = _apply_length_window_prefilter(
                            shard_df, shard_indices, enable_prefilters, max_length_diff
                        )
                        total_pairs += len(filtered_pairs)
                        pairs.extend(filtered_pairs)
            else:
                # Use existing secondary blocking logic
                secondary_groups = block_df.groupby("secondary_key")
                for _, group_df in secondary_groups:
                    group_indices = group_df.index.tolist()
                    if len(group_indices) > 1:
                        filtered_pairs = _apply_length_window_prefilter(
                            group_df, group_indices, enable_prefilters, max_length_diff
                        )
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

    # Remove duplicates using numpy unique for better performance
    if pairs:
        # Convert to numpy array for efficient deduplication
        pairs_array = np.array(pairs)
        
        # Use numpy unique with return_index=True to get unique pairs
        unique_indices = np.unique(pairs_array, axis=0, return_index=True)[1]
        unique_pairs = [tuple(pairs_array[i]) for i in unique_indices]
        
        logger.info(f"Generated {len(unique_pairs)} unique candidate pairs (deduplicated from {len(pairs)})")
    else:
        unique_pairs = []
        logger.info("No candidate pairs generated")

    if len(unique_pairs) > max_pairs:
        logger.warning(f"Generated {len(unique_pairs)} pairs, limiting to {max_pairs}")
        unique_pairs = unique_pairs[:max_pairs]

    return unique_pairs


def _apply_length_window_prefilter(
    group_df: pd.DataFrame,
    group_indices: List[int],
    enable_prefilters: bool,
    max_length_diff: int
) -> List[Tuple[int, int]]:
    """
    Apply length window prefilter to reduce NxN comparisons.
    
    Args:
        group_df: DataFrame for the group
        group_indices: List of indices in the group
        enable_prefilters: Whether to enable length prefilters
        max_length_diff: Maximum length difference for filtering
        
    Returns:
        List of filtered pairs
    """
    if not enable_prefilters:
        return list(combinations(group_indices, 2))
    
    # Get name lengths
    name_lengths = group_df["name_core"].str.len()
    
    # Sort by length for sliding window approach
    length_idx_pairs = list(zip(name_lengths.values, group_indices))
    length_idx_pairs.sort(key=lambda x: x[0])
    
    filtered_pairs = []
    
    # Use sliding window instead of NxN matrix
    for i, (len_a, idx_a) in enumerate(length_idx_pairs):
        # Find valid range for this length
        min_len = len_a - max_length_diff
        max_len = len_a + max_length_diff
        
        # Find start and end indices in the sorted list
        start_idx = i
        while start_idx > 0 and length_idx_pairs[start_idx - 1][0] >= min_len:
            start_idx -= 1
        
        end_idx = i
        while end_idx < len(length_idx_pairs) - 1 and length_idx_pairs[end_idx + 1][0] <= max_len:
            end_idx += 1
        
        # Add pairs within the valid range
        for j in range(start_idx, end_idx + 1):
            if i != j:  # Skip self-pairs
                len_b, idx_b = length_idx_pairs[j]
                if idx_a < idx_b:  # Ensure i < j
                    filtered_pairs.append((idx_a, idx_b))
    
    return filtered_pairs


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


def _check_punctuation_mismatch(name_a: str, name_b: str) -> bool:
    """Check if names have different punctuation patterns."""
    import re
    
    # Extract punctuation patterns
    punct_a = re.findall(r'[^\w\s]', name_a)
    punct_b = re.findall(r'[^\w\s]', name_b)
    
    return punct_a != punct_b


def compute_score_components(name_core_a: str,
                             name_core_b: str,
                             suffix_class_a: str,
                             suffix_class_b: str,
                             penalties: Dict[str, Any],
                             punctuation_mismatch: Optional[bool] = None) -> Dict[str, Any]:
    """
    Canonical scorer function - single source of truth for similarity scoring.
    
    This function replaces the previous _compute_pair_score and _compute_vectorized_penalties
    functions to eliminate code duplication (DRY principle). Both parallel and bulk paths
    now use this single scorer for consistent results.
    
    Args:
        name_core_a: Core name for first entity
        name_core_b: Core name for second entity  
        suffix_class_a: Suffix class for first entity
        suffix_class_b: Suffix class for second entity
        penalties: Dictionary of penalty values
        punctuation_mismatch: Optional punctuation mismatch flag
        
    Returns:
        Dictionary with score components and final score
    """
    ratio_name = fuzz.token_sort_ratio(name_core_a, name_core_b)
    ratio_set  = fuzz.token_set_ratio(name_core_a, name_core_b)

    tokens_a = set(name_core_a.split())
    tokens_b = set(name_core_b.split())
    if tokens_a and tokens_b:
        intersection = len(tokens_a & tokens_b)
        union = len(tokens_a | tokens_b)
        jaccard = intersection / union if union > 0 else 0.0
    else:
        jaccard = 0.0

    num_style_match = _check_numeric_style_match(name_core_a, name_core_b)
    suffix_match = (suffix_class_a == suffix_class_b)

    base = 0.45 * ratio_name + 0.35 * ratio_set + 20.0 * jaccard
    if not num_style_match:
        base -= penalties.get("num_style_mismatch", 5)
    if not suffix_match:
        base -= penalties.get("suffix_mismatch", 25)
    if punctuation_mismatch is True:
        base -= penalties.get("punctuation_mismatch", 3)

    score = max(0, min(100, round(base)))
    return {
        "score": score,
        "ratio_name": ratio_name,
        "ratio_set": ratio_set,
        "jaccard": jaccard,
        "num_style_match": num_style_match,
        "suffix_match": suffix_match,
        "base_score": base,
        **({"punctuation_mismatch": punctuation_mismatch} if punctuation_mismatch is not None else {}),
    }





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





def _compute_similarity_scores_parallel(
    df_norm: pd.DataFrame,
    candidate_pairs: List[Tuple[int, int]],
    penalties: Dict[str, Any],
    enable_progress: bool = False,
    parallel_executor: Optional[ParallelExecutor] = None,
    interim_dir: Optional[str] = None,
    settings: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """
    Compute similarity scores for candidate pairs in parallel with optimized data layout.

    Args:
        df_norm: DataFrame with normalized name columns
        candidate_pairs: List of (idx_a, idx_b) tuples
        penalties: Dictionary of penalty values
        enable_progress: Enable progress logging
        parallel_executor: Optional parallel executor for parallel processing
        interim_dir: Optional directory for interim files
        settings: Configuration settings

    Returns:
        List of dictionaries with similarity scores and metadata
    """
    # Check if bulk scoring is enabled
    scoring_settings = settings.get("similarity", {}).get("scoring", {}) if settings else {}
    use_bulk_cdist = scoring_settings.get("use_bulk_cdist", False)
    gate_cutoff = scoring_settings.get("gate_cutoff", 72)
    
    # Log similarity configuration for run comparison
    logger.info(f"Similarity config: gate_cutoff={gate_cutoff}, use_bulk={use_bulk_cdist}, penalties={penalties}")
    
    if use_bulk_cdist and len(candidate_pairs) > 10000:
        logger.info("Using bulk pairwise scoring for large pair set")
        return _compute_similarity_scores_bulk(
            df_norm, candidate_pairs, penalties, enable_progress, 
            parallel_executor, interim_dir, settings, gate_cutoff
        )
    
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
    
    # Process pairs in parallel if executor available
    if parallel_executor:
        # Use the new execute_chunked function for optimal parallel processing
        try:
            from src.utils.parallel_utils import execute_chunked
            
            # Create a closure that uses the memory-mapped arrays
            def process_chunk(chunk):
                return [
                    {
                        "id_a": account_id_array[index_map[idx_a]],
                        "id_b": account_id_array[index_map[idx_b]],
                        **compute_score_components(
                            name_core_array[index_map[idx_a]],
                            name_core_array[index_map[idx_b]],
                            suffix_class_array[index_map[idx_a]],
                            suffix_class_array[index_map[idx_b]],
                            penalties
                        )
                    }
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

            # Use execute_chunked for optimal parallel processing
            try:
                from src.utils.parallel_utils import get_chunk_size_pairs
                chunk_size = get_chunk_size_pairs()
            except ImportError:
                chunk_size = 300000
                
            scores = execute_chunked(
                process_chunk,
                candidate_pairs,
                workers=parallel_executor.workers if hasattr(parallel_executor, 'workers') else None,
                backend=parallel_executor.backend if hasattr(parallel_executor, 'backend') else None,
                chunk_size=chunk_size
            )
            
            logger.info(f"Executed similarity scoring using execute_chunked with {len(scores)} results")
            
        except ImportError:
            logger.warning("execute_chunked not available, using standard parallel execution")
            # Fallback to existing parallel execution logic
            scores = parallel_executor.execute_chunked(
                process_chunk,
                candidate_pairs,
                operation_name="similarity_scoring_parallel",
            )

        # Flatten the results - execute_chunked returns a list of lists
        flattened_scores = []
        for chunk_scores in scores:
            if isinstance(chunk_scores, dict):
                flattened_scores.append(chunk_scores)
            elif isinstance(chunk_scores, list):
                # `extend` is safe here because elements are dicts
                flattened_scores.extend(chunk_scores)
            else:
                raise TypeError(
                    f"similarity: unexpected chunk type {type(chunk_scores)}; "
                    "expected list[dict] or dict."
                )
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

        scores = []
        for idx_a, idx_b in candidate_pairs:
            comp = compute_score_components(
                name_core_array[index_map[idx_a]],
                name_core_array[index_map[idx_b]],
                suffix_class_array[index_map[idx_a]],
                suffix_class_array[index_map[idx_b]],
                penalties
            )
            scores.append({
                "id_a": account_id_array[index_map[idx_a]],
                "id_b": account_id_array[index_map[idx_b]],
                **comp
            })

    logger.info(f"Computed scores for {len(candidate_pairs)} pairs")
    
    # Shape/type guard to catch the 17×9=153 bug immediately
    if not isinstance(scores, list):
        raise TypeError(f"similarity: expected list of dicts, got {type(scores)}")
    
    if scores and not all(isinstance(x, dict) for x in scores):
        # Helpful debug: show first bad element
        bad = next((type(x) for x in scores if not isinstance(x, dict)), None)
        raise TypeError(
            f"similarity: expected list[dict], but found element of type {bad}. "
            "This often occurs if code uses `scores += score` or `scores.extend(score)` "
            "instead of `scores.append(score)` when `score` is a dict."
        )
    
    return scores


def _compute_similarity_scores_bulk(
    df_norm: pd.DataFrame,
    candidate_pairs: List[Tuple[int, int]],
    penalties: Dict[str, Any],
    enable_progress: bool = False,
    parallel_executor: Optional[ParallelExecutor] = None,
    interim_dir: Optional[str] = None,
    settings: Optional[Dict[str, Any]] = None,
    gate_cutoff: int = 72,
) -> List[Dict[str, Any]]:
    """
    Compute similarity scores using RapidFuzz bulk cdist for large pair sets.
    
    Args:
        df_norm: DataFrame with normalized name columns
        candidate_pairs: List of (idx_a, idx_b) tuples
        penalties: Dictionary of penalty values
        enable_progress: Enable progress logging
        parallel_executor: Optional parallel executor for parallel processing
        interim_dir: Optional directory for interim files
        gate_cutoff: Gate cutoff for token_set_ratio
        
    Returns:
        List of dictionaries with similarity scores and metadata
    """
    logger.info(f"Bulk scoring {len(candidate_pairs)} pairs with gate cutoff {gate_cutoff}")
    
    # Create index mapping
    filtered_indices = df_norm.index.values
    index_map = {idx: i for i, idx in enumerate(filtered_indices)}
    
    # Extract arrays for bulk processing
    name_core_array = df_norm["name_core"].values
    suffix_class_array = df_norm["suffix_class"].values
    account_id_array = df_norm["account_id"].values
    
    # Build arrays for bulk processing
    a_indices = []
    b_indices = []
    a_names = []
    b_names = []
    
    for idx_a, idx_b in candidate_pairs:
        a_indices.append(index_map[idx_a])
        b_indices.append(index_map[idx_b])
        a_names.append(name_core_array[index_map[idx_a]])
        b_names.append(name_core_array[index_map[idx_b]])
    
    # Phase 1: Gate with token_set_ratio
    logger.info("Phase 1: Applying token_set_ratio gate")
    
    # Sanity checks - prevent the 5.3M survivors bug
    assert len(a_names) == len(b_names) == len(candidate_pairs), f"Array length mismatch: a_names={len(a_names)}, b_names={len(b_names)}, candidate_pairs={len(candidate_pairs)}"
    
    # Pairwise gate using token_set_ratio (fixes the cdist matrix bug)
    gate_scores = [fuzz.token_set_ratio(a, b) for a, b in zip(a_names, b_names)]
    assert len(gate_scores) == len(candidate_pairs), f"Gate scores length mismatch: {len(gate_scores)} vs {len(candidate_pairs)}"
    
    # Get indices of pairs that pass the gate
    gate_survivors = [i for i, s in enumerate(gate_scores) if s >= gate_cutoff]
    
    logger.info(f"Phase 1 gate: {len(gate_survivors)}/{len(candidate_pairs)} pairs survived ({len(gate_survivors)/len(candidate_pairs)*100:.1f}%)")
    
    if len(gate_survivors) == 0:
        logger.info("No pairs survived Phase 1 gate")
        return []
    
    # Phase 2: Compute final scores using canonical scorer
    logger.info("Phase 2: Computing final scores for survivors")
    
    results = []
    for i in gate_survivors:
        arr_idx_a = a_indices[i]
        arr_idx_b = b_indices[i]
        
        # Use canonical scorer for consistent results
        comp = compute_score_components(
            a_names[i], b_names[i],
            suffix_class_array[arr_idx_a],
            suffix_class_array[arr_idx_b],
            penalties
        )
        
        results.append({
            "id_a": account_id_array[arr_idx_a],
            "id_b": account_id_array[arr_idx_b],
            **comp
        })
    
    # Final sanity check - results should never exceed survivors
    assert len(results) == len(gate_survivors), f"Results count mismatch: {len(results)} vs {len(gate_survivors)} survivors"
    
    # Log performance metrics for bulk scoring
    total_pairs = len(candidate_pairs)
    gate_survivors_count = len(gate_survivors)
    final_results = len(results)
    
    logger.info(f"Bulk scoring completed: {final_results} pairs scored")
    logger.info(f"Bulk scoring metrics: {total_pairs} total pairs, {gate_survivors_count} post-gate survivors, {final_results} final results")
    logger.info(f"Gate efficiency: {gate_survivors_count/total_pairs*100:.1f}% survival rate")
    
    return results
