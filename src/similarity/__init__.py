"""Similarity module for Company Junction.

This module provides similarity scoring and blocking functionality.
"""

import logging
from typing import Any, Dict, Optional

import pandas as pd

from src.utils.duckdb_utils import ensure_pandas_strings
from src.utils.parallel_protocols import ExecutorLike
from src.utils.parallel_utils import ParallelExecutor

from .blocking import generate_candidate_pairs_soft_ban, get_stop_tokens
from .diagnostics import generate_brand_suggestions, write_blocking_diagnostics
from .scoring import compute_score_components, score_pairs_bulk, score_pairs_parallel

logger = logging.getLogger(__name__)


def pair_scores(
    df_norm: pd.DataFrame,
    settings: Dict,
    enable_progress: bool = False,
    parallel_executor: Optional[ExecutorLike] = None,
    interim_dir: Optional[str] = None,
    profile: bool = False,
) -> pd.DataFrame:
    """Generate candidate pairs and compute similarity scores.

    Args:
        df_norm: DataFrame with normalized name columns
        settings: Configuration settings
        enable_progress: Enable progress logging
        parallel_executor: Optional parallel executor for parallel processing
        interim_dir: Directory for interim files
        profile: Enable performance profiling

    Returns:
        DataFrame with candidate pairs and scores

    """
    logger.info(f"Generating candidate pairs for {len(df_norm)} records")

    # Ensure pandas strings for consistent .str accessor behavior
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
            logger.warning("pyinstrument not available, profiling disabled")
            profiler = None
    else:
        profiler = None

    try:
        # Generate candidate pairs using soft-ban strategy
        pairs = generate_candidate_pairs_soft_ban(
            df_norm,
            enable_progress,
            parallel_executor,
            interim_dir,
            settings,
        )

        if not pairs:
            logger.info("No candidate pairs generated")
            return pd.DataFrame()

        logger.info(f"Generated {len(pairs)} candidate pairs")

        # Compute similarity scores
        scoring_settings = settings.get("similarity", {}).get("scoring", {})
        use_bulk_cdist = scoring_settings.get("use_bulk_cdist", True)

        if use_bulk_cdist and len(pairs) > 1000:
            logger.info("Using bulk scoring for large dataset")
            scores = score_pairs_bulk(df_norm, pairs, settings, enable_progress)
        else:
            logger.info("Using parallel scoring")
            scores = score_pairs_parallel(
                df_norm,
                pairs,
                settings,
                enable_progress,
                parallel_executor,
            )

        if not scores:
            logger.info("No scores computed")
            return pd.DataFrame()

        # Convert to DataFrame
        pairs_df = pd.DataFrame.from_records(scores)

        # Filter on medium threshold (single canonical key)
        medium_threshold = settings.get("similarity", {}).get("medium", 84)
        pairs_df = pairs_df[pairs_df["score"] >= medium_threshold].copy()

        # Sort explicitly: id_a, id_b ascending, score descending
        pairs_df = pairs_df.sort_values(
            ["id_a", "id_b", "score"],
            ascending=[True, True, False],
        )

        # Ensure string types for consistency
        pairs_df = ensure_pandas_strings(pairs_df, ["id_a", "id_b"])

        # Save candidate pairs if interim directory provided
        if interim_dir:
            candidate_pairs_path = f"{interim_dir}/candidate_pairs.parquet"
            pairs_df.to_parquet(candidate_pairs_path, index=False)
            logger.info(f"Candidate pairs saved to {candidate_pairs_path}")

        logger.info(
            f"Final result: {len(pairs_df)} pairs above medium threshold ({medium_threshold})",
        )

        return pairs_df

    finally:
        # Stop profiling if enabled
        if profiler:
            profiler.stop()
            profile_path = (
                f"{interim_dir}/similarity_profile.html"
                if interim_dir
                else "similarity_profile.html"
            )
            html = profiler.output_html()
            with open(profile_path, "w", encoding="utf-8") as f:
                f.write(html)
            logger.info(f"Performance profile saved to {profile_path}")


# get_stop_tokens is defined in blocking.py to maintain single source of truth


def save_candidate_pairs(pairs_df: pd.DataFrame, pairs_path: str) -> None:
    """Save candidate pairs DataFrame to file."""
    pairs_df.to_parquet(pairs_path, index=False)
    logger.info(f"Candidate pairs saved to {pairs_path}")


# Export all functions

__all__ = [
    "compute_score_components",
    "generate_brand_suggestions",
    "generate_candidate_pairs_soft_ban",
    "get_stop_tokens",
    "pair_scores",
    "save_candidate_pairs",
    "score_pairs_bulk",
    "score_pairs_parallel",
    "write_blocking_diagnostics",
]
