"""Similarity scoring functionality."""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Set, Tuple, cast

import pandas as pd
from rapidfuzz import fuzz

from src.similarity.types import ScoreComponents

# Replace concrete import with protocol contract
from src.utils.parallel_protocols import ExecutorLike

logger = logging.getLogger(__name__)


def compute_score_components(
    name_core_a: str,
    name_core_b: str,
    suffix_class_a: str,
    suffix_class_b: str,
    penalties: Dict[str, Any],
    settings: Optional[Dict[str, Any]] = None,
) -> ScoreComponents:
    """Canonical scorer function - single source of truth for similarity scoring.

    Args:
        name_core_a: Core name for first entity
        name_core_b: Core name for second entity
        suffix_class_a: Suffix class for first entity
        suffix_class_b: Suffix class for second entity
        penalties: Dictionary of penalty values
        settings: Configuration settings for enhanced normalization

    Returns:
        Dictionary with score components and final score

    """
    # Apply enhanced normalization if available
    tokens_a: Set[str]
    tokens_b: Set[str]

    try:
        from src.normalize import enhance_name_core, get_enhanced_tokens_for_jaccard

        # Enhance name cores for better matching
        enhanced_a, _ = enhance_name_core(name_core_a, settings)
        enhanced_b, _ = enhance_name_core(name_core_b, settings)

        # Use enhanced names for RapidFuzz ratios
        ratio_name = fuzz.token_sort_ratio(enhanced_a, enhanced_b)
        ratio_set = fuzz.token_set_ratio(enhanced_a, enhanced_b)

        # Use enhanced tokens excluding weak tokens for Jaccard
        tokens_a = set(get_enhanced_tokens_for_jaccard(name_core_a, settings))
        tokens_b = set(get_enhanced_tokens_for_jaccard(name_core_b, settings))

    except ImportError:
        # Fallback to original behavior if enhanced normalization not available
        ratio_name = fuzz.token_sort_ratio(name_core_a, name_core_b)
        ratio_set = fuzz.token_set_ratio(name_core_a, name_core_b)
        tokens_a = set(name_core_a.split())
        tokens_b = set(name_core_b.split())

    # Calculate Jaccard similarity
    if tokens_a and tokens_b:
        intersection = len(tokens_a & tokens_b)
        union = len(tokens_a | tokens_b)
        jaccard = intersection / union if union > 0 else 0.0
    else:
        jaccard = 0.0

    # Simple punctuation mismatch: if punctuation differs materially, apply penalty
    def _punct_pattern(s: str) -> str:
        import re

        return "".join(re.findall(r"[^\w\s]", s or ""))

    punct_mismatch = _punct_pattern(name_core_a) != _punct_pattern(name_core_b)

    num_style_match = _check_numeric_style_match(name_core_a, name_core_b)
    suffix_match = suffix_class_a == suffix_class_b

    base = 0.45 * ratio_name + 0.35 * ratio_set + 20.0 * jaccard
    if not num_style_match:
        base -= cast("int", penalties.get("num_style_mismatch", 5))
    if not suffix_match:
        base -= cast("int", penalties.get("suffix_mismatch", 25))
    if punct_mismatch:
        base -= cast("int", penalties.get("punctuation_mismatch", 0))

    score = max(0, min(100, round(base)))
    return ScoreComponents(
        score=score,
        ratio_name=int(ratio_name),
        ratio_set=int(ratio_set),
        jaccard=jaccard,
        num_style_match=num_style_match,
        suffix_match=suffix_match,
        punctuation_mismatch=punct_mismatch,
        base_score=float(base),
    )


def score_pairs_parallel(
    df_norm: pd.DataFrame,
    candidate_pairs: List[Tuple[int, int]],
    settings: Dict[str, Any],
    enable_progress: bool = False,
    parallel_executor: Optional[ExecutorLike] = None,
) -> List[Dict[str, Any]]:
    """Compute similarity scores for candidate pairs using parallel processing.

    Args:
        df_norm: DataFrame with normalized names
        candidate_pairs: List of candidate pair tuples
        settings: Configuration settings
        enable_progress: Enable progress logging
        parallel_executor: Optional parallel executor

    Returns:
        List of score dictionaries

    """
    if not candidate_pairs:
        return []

    # Get penalties from settings
    penalties = settings.get("similarity", {}).get("penalty", {})

    # Ensure suffix_class column exists with default values
    if "suffix_class" not in df_norm.columns:
        df_norm = df_norm.copy()
        df_norm["suffix_class"] = "NONE"

    # Convert to arrays for efficient access
    name_core_array = df_norm["name_core"].values
    suffix_class_array = df_norm["suffix_class"].values
    account_id_array = df_norm["account_id"].values

    # Create index mapping
    index_map = {old_idx: new_idx for new_idx, old_idx in enumerate(df_norm.index)}

    # Process pairs in parallel
    if parallel_executor and len(candidate_pairs) > 1000:
        # Use parallel processing for large datasets
        chunk_size = max(100, len(candidate_pairs) // parallel_executor.workers)
        chunks = [
            candidate_pairs[i : i + chunk_size]
            for i in range(0, len(candidate_pairs), chunk_size)
        ]

        def process_chunk(chunk: List[Tuple[int, int]]) -> List[Dict[str, Any]]:
            return [
                {
                    "id_a": account_id_array[index_map[idx_a]],
                    "id_b": account_id_array[index_map[idx_b]],
                    **compute_score_components(
                        name_core_array[index_map[idx_a]],
                        name_core_array[index_map[idx_b]],
                        suffix_class_array[index_map[idx_a]],
                        suffix_class_array[index_map[idx_b]],
                        penalties,
                        settings,
                    ),
                }
                for idx_a, idx_b in chunk
            ]

        results_iter = parallel_executor.map(process_chunk, chunks, chunksize=None)
        scores = [item for chunk_result in results_iter for item in chunk_result]
    else:
        # Sequential processing
        scores = []
        for idx_a, idx_b in candidate_pairs:
            comp = compute_score_components(
                name_core_array[index_map[idx_a]],
                name_core_array[index_map[idx_b]],
                suffix_class_array[index_map[idx_a]],
                suffix_class_array[index_map[idx_b]],
                penalties,
                settings,
            )
            scores.append(
                {
                    "id_a": account_id_array[index_map[idx_a]],
                    "id_b": account_id_array[index_map[idx_b]],
                    **comp,
                },
            )

    return scores


def score_pairs_bulk(
    df_norm: pd.DataFrame,
    candidate_pairs: List[Tuple[int, int]],
    settings: Dict[str, Any],
    enable_progress: bool = False,
) -> List[Dict[str, Any]]:
    """Compute similarity scores for candidate pairs using bulk processing.

    Args:
        df_norm: DataFrame with normalized names
        candidate_pairs: List of candidate pair tuples
        settings: Configuration settings
        enable_progress: Enable progress logging

    Returns:
        List of score dictionaries

    """
    if not candidate_pairs:
        return []

    # Get penalties and settings
    penalties = settings.get("similarity", {}).get("penalty", {})
    scoring_settings = settings.get("similarity", {}).get("scoring", {})
    _use_bulk_cdist = scoring_settings.get("use_bulk_cdist", True)
    gate_cutoff = scoring_settings.get("gate_cutoff", 72)

    # Ensure suffix_class column exists with default values
    if "suffix_class" not in df_norm.columns:
        df_norm = df_norm.copy()
        df_norm["suffix_class"] = "NONE"

    # Convert to arrays for efficient access
    name_core_array = df_norm["name_core"].values
    suffix_class_array = df_norm["suffix_class"].values
    account_id_array = df_norm["account_id"].values

    # Create index mapping
    index_map = {old_idx: new_idx for new_idx, old_idx in enumerate(df_norm.index)}

    # Extract names and indices for bulk processing
    a_indices = [index_map[idx_a] for idx_a, _ in candidate_pairs]
    b_indices = [index_map[idx_b] for _, idx_b in candidate_pairs]
    a_names = [name_core_array[idx] for idx in a_indices]
    b_names = [name_core_array[idx] for idx in b_indices]

    # Phase 1: Gate with token_set_ratio (pairwise, not full cdist)
    gate_scores = [fuzz.token_set_ratio(a, b) for a, b in zip(a_names, b_names)]
    gate_survivors = [i for i, s in enumerate(gate_scores) if s >= gate_cutoff]
    logger.info(
        f"Bulk gate: {len(gate_survivors)}/{len(candidate_pairs)} pairs passed token_set_ratio >= {gate_cutoff}",
    )

    # Phase 2: Compute final scores for survivors
    logger.info("Phase 2: Computing final scores for survivors")

    results = []
    for i in gate_survivors:
        arr_idx_a = a_indices[i]
        arr_idx_b = b_indices[i]

        # Use canonical scorer for consistent results
        comp = compute_score_components(
            a_names[i],
            b_names[i],
            suffix_class_array[arr_idx_a],
            suffix_class_array[arr_idx_b],
            penalties,
            settings,
        )

        results.append(
            {
                "id_a": account_id_array[arr_idx_a],
                "id_b": account_id_array[arr_idx_b],
                **comp,
            },
        )

    return results


def _check_numeric_style_match(name_a: str, name_b: str) -> bool:
    """Check if two names have matching numeric styles."""
    import re

    # Extract numeric patterns
    nums_a = re.findall(r"\d+", name_a)
    nums_b = re.findall(r"\d+", name_b)

    # Check if numeric patterns match
    if len(nums_a) != len(nums_b):
        return False

    return nums_a == nums_b
