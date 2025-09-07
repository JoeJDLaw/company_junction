"""Artifact management utilities for ui_helpers refactor.

This module handles core path helpers for artifacts.
"""

import os
from typing import Optional

from src.utils.path_utils import get_artifact_path, get_interim_dir, get_processed_dir


def get_artifact_paths(run_id: str, output_dir: Optional[str] = None) -> dict[str, str]:
    """Get artifact paths for a run.

    Args:
        run_id: The run ID
        output_dir: Optional output directory override

    Returns:
        Dictionary of artifact paths

    """
    # Check if run exists in interim or processed
    interim_dir = get_interim_dir(run_id, output_dir)
    processed_dir = get_processed_dir(run_id, output_dir)

    # Determine which directory to use based on file availability
    # Priority: processed directory first (has review_ready files), then interim (has candidate_pairs)
    if os.path.exists(processed_dir):
        base_dir = str(processed_dir)
        interim_format = "parquet"
    elif os.path.exists(interim_dir):
        base_dir = str(interim_dir)
        interim_format = "parquet"
    else:
        # Fallback to processed directory
        base_dir = str(processed_dir)
        interim_format = "parquet"

    # Get candidate pairs path - prioritize interim directory for this specific file
    candidate_pairs_path = None
    interim_candidate_pairs = f"{interim_dir}/candidate_pairs.parquet"
    processed_candidate_pairs = f"{processed_dir}/candidate_pairs.parquet"
    
    if os.path.exists(interim_candidate_pairs):
        candidate_pairs_path = interim_candidate_pairs
    elif os.path.exists(processed_candidate_pairs):
        candidate_pairs_path = processed_candidate_pairs
    else:
        candidate_pairs_path = f"{base_dir}/candidate_pairs.{interim_format}"

    return {
        "review_ready_csv": f"{base_dir}/review_ready.csv",
        "review_ready_parquet": f"{base_dir}/review_ready.parquet",
        "review_meta": f"{base_dir}/review_meta.json",
        "pipeline_state": f"{base_dir}/pipeline_state.json",
        "candidate_pairs": candidate_pairs_path,
        "groups": f"{base_dir}/groups.{interim_format}",
        "survivorship": f"{base_dir}/survivorship.{interim_format}",
        "dispositions": f"{base_dir}/dispositions.{interim_format}",
        "alias_matches": f"{base_dir}/alias_matches.{interim_format}",
        "block_top_tokens": f"{base_dir}/block_top_tokens.csv",
        "group_stats_parquet": str(get_artifact_path(run_id, "group_stats.parquet", output_dir)),
        "group_details_parquet": str(
            get_artifact_path(run_id, "group_details.parquet", output_dir),
        ),
    }
