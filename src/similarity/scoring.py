"""Similarity scoring functionality."""

from __future__ import annotations

import logging
from typing import Any, cast

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
    penalties: dict[str, Any],
    settings: dict[str, Any] | None = None,
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
    tokens_a: set[str]
    tokens_b: set[str]

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

    # Apply distractor guardrails if enabled
    distractor_penalty = 0
    distractor_penalty_applied = {}
    applied_penalties = {}
    
    if settings and settings.get("similarity", {}).get("distractor_guardrails", {}).get("enabled", False):
        distractor_penalty, distractor_penalty_applied, applied_penalties = _apply_distractor_guardrails(
            name_core_a, name_core_b, tokens_a, tokens_b, ratio_name, suffix_match, settings
        )
        base -= distractor_penalty

    score = max(0, min(100, round(base)))
    
    # Return both canonical and alias keys for API compatibility
    result = {
        # Canonical keys (current API)
        "composite_score": score,
        "token_set_ratio": int(ratio_set),
        "token_sort_ratio": int(ratio_name),
        "jaccard": jaccard,
        "num_style_match": num_style_match,
        "suffix_match": suffix_match,
        "punctuation_mismatch": punct_mismatch,
        "base_score": float(base),
        # Alias keys for backward compatibility
        "score": score,
        "ratio_set": int(ratio_set),
        "ratio_name": int(ratio_name),
    }
    
    # Add distractor guardrails information if applied
    if distractor_penalty > 0:
        result["distractor_penalty_applied"] = distractor_penalty_applied
        result["applied_penalties"] = applied_penalties
    
    return result


def score_pairs_parallel(
    df_norm: pd.DataFrame,
    candidate_pairs: list[tuple[int, int]],
    settings: dict[str, Any],
    enable_progress: bool = False,
    parallel_executor: ExecutorLike | None = None,
) -> list[dict[str, Any]]:
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

        def process_chunk(chunk: list[tuple[int, int]]) -> list[dict[str, Any]]:
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
    candidate_pairs: list[tuple[int, int]],
    settings: dict[str, Any],
    enable_progress: bool = False,
) -> list[dict[str, Any]]:
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
    penalties = settings["similarity"]["penalty"]
    scoring_settings = settings["similarity"]["scoring"]
    _use_bulk_cdist = scoring_settings["use_bulk_cdist"]
    gate_cutoff = scoring_settings["gate_cutoff"]

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


def _apply_distractor_guardrails(
    name_core_a: str,
    name_core_b: str,
    tokens_a: set[str],
    tokens_b: set[str],
    ratio_name: float,
    suffix_match: bool,
    settings: dict[str, Any],
) -> tuple[int, dict[str, Any], dict[str, Any]]:
    """Apply distractor guardrails to prevent false-positive groupings.
    
    Args:
        name_core_a: Core name for first entity
        name_core_b: Core name for second entity
        tokens_a: Token set for first entity
        tokens_b: Token set for second entity
        ratio_name: Token sort ratio (for strong corroboration)
        suffix_match: Whether suffix classes match
        settings: Configuration settings
        
    Returns:
        Tuple of (penalty_amount, penalty_details, applied_penalties)
    """
    guardrails_config = settings.get("similarity", {}).get("distractor_guardrails", {})
    distractor_tokens = guardrails_config.get("distractor_tokens", {})
    penalty_weights = guardrails_config.get("penalty_weights", {})
    evidence_req = guardrails_config.get("evidence_requirements", {})
    
    # Flatten all distractor tokens into a single set for lookup
    all_distractor_tokens = set()
    for category, tokens in distractor_tokens.items():
        all_distractor_tokens.update(token.lower() for token in tokens)
    
    # Find shared tokens and categorize them
    shared_tokens = tokens_a & tokens_b
    shared_distractor_tokens = shared_tokens & all_distractor_tokens
    shared_non_distractor_tokens = shared_tokens - all_distractor_tokens
    
    # Count non-distractor evidence
    non_distractor_count = len(shared_non_distractor_tokens)
    min_required = evidence_req.get("min_non_distractor_tokens", 2)
    strong_threshold = evidence_req.get("strong_corroboration_threshold", 90)
    require_suffix = evidence_req.get("require_suffix_match_for_corroboration", True)
    
    # Check if we have sufficient non-distractor evidence
    has_sufficient_evidence = (
        non_distractor_count >= min_required or
        (non_distractor_count >= 1 and 
         ratio_name >= strong_threshold and 
         (suffix_match or not require_suffix))
    )
    
    # If we have sufficient evidence, no penalty
    if has_sufficient_evidence:
        return 0, {}, {}
    
    # Calculate penalty based on distractor categories found
    total_penalty = 0
    penalty_details = {
        "insufficient_evidence": True,
        "shared_distractor_tokens": list(shared_distractor_tokens),
        "shared_non_distractor_tokens": list(shared_non_distractor_tokens),
        "non_distractor_count": non_distractor_count,
        "min_required": min_required,
    }
    applied_penalties = {}
    
    # Apply penalties for each distractor category found
    for category, tokens in distractor_tokens.items():
        category_tokens = set(token.lower() for token in tokens)
        found_tokens = shared_distractor_tokens & category_tokens
        
        if found_tokens:
            penalty_weight = penalty_weights.get(category, 0)
            if penalty_weight > 0:
                # Apply penalty proportional to the number of distractor tokens
                category_penalty = penalty_weight * len(found_tokens)
                total_penalty += category_penalty
                applied_penalties[f"distractor_{category}"] = category_penalty
                penalty_details[f"distractor_{category}_tokens"] = list(found_tokens)
                penalty_details[f"distractor_{category}_penalty"] = category_penalty
    
    # Apply base insufficient evidence penalty if no specific distractor penalties
    if total_penalty == 0 and not has_sufficient_evidence:
        base_penalty = 50  # Default penalty for insufficient evidence
        total_penalty = base_penalty
        applied_penalties["insufficient_evidence"] = base_penalty
        penalty_details["base_insufficient_evidence_penalty"] = base_penalty
    
    return int(total_penalty), penalty_details, applied_penalties
