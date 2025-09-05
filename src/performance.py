"""Performance monitoring and summary generation for the pipeline.

Provides functions to track timing, memory usage, and generate
comprehensive performance summaries for pipeline runs.
"""

import json
import logging
import subprocess
import tracemalloc
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

import pandas as pd

try:
    from src.utils.hash_utils import stable_schema_hash as _stable_schema_hash

    HashFunc = Callable[[Dict[str, Any]], str]
    stable_schema_hash: Optional[HashFunc] = _stable_schema_hash
except ImportError:
    stable_schema_hash = None

logger = logging.getLogger(__name__)


class PerformanceTracker:
    """Tracks performance metrics throughout the pipeline."""

    def __init__(self) -> None:
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        self.timings: Dict[str, float] = {}
        self.memory_snapshots: List[Any] = []
        self.peak_memory: float = 0.0
        self.config_hash: Optional[str] = None

    def start_run(self, config_dict: Dict[str, Any]) -> None:
        """Start tracking performance for a pipeline run."""
        self.start_time = datetime.now(timezone.utc)
        if stable_schema_hash is not None:
            self.config_hash = stable_schema_hash(config_dict)
        else:
            self.config_hash = "unknown"
        tracemalloc.start()
        logger.info(f"Performance tracking started at {self.start_time.isoformat()}")

    def end_run(self) -> None:
        """End performance tracking for a pipeline run."""
        self.end_time = datetime.now(timezone.utc)
        current, peak = tracemalloc.get_traced_memory()
        self.peak_memory = peak / 1024 / 1024  # Convert to MB
        tracemalloc.stop()
        logger.info(f"Performance tracking ended at {self.end_time.isoformat()}")

    def record_timing(self, stage: str, duration_sec: float) -> None:
        """Record timing for a pipeline stage."""
        self.timings[stage] = duration_sec
        logger.debug(f"Stage '{stage}' completed in {duration_sec:.2f}s")

    def get_git_commit(self) -> str:
        """Get current git commit hash."""
        try:
            result = subprocess.run(
                ["git", "rev-parse", "HEAD"], capture_output=True, text=True, check=True,
            )
            return result.stdout.strip()[:8]  # Return first 8 chars
        except (subprocess.CalledProcessError, FileNotFoundError):
            return "unknown"

    def generate_summary(
        self,
        dataset_stats: Dict[str, int],
        candidate_stats: Dict[str, int],
        group_stats: Dict[str, Any],
        block_stats: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Generate comprehensive performance summary.

        Args:
            dataset_stats: Dict with 'rows_in', 'rows_cleaned'
            candidate_stats: Dict with pair counts
            group_stats: Dict with group information
            block_stats: Dict with block information

        Returns:
            Performance summary dict matching the required schema

        """
        if not self.start_time or not self.end_time:
            raise ValueError("Performance tracking not started/ended")

        return {
            "run_meta": {
                "git_commit": self.get_git_commit(),
                "config_hash": self.config_hash,
                "started_at_utc": self.start_time.isoformat(),
                "ended_at_utc": self.end_time.isoformat(),
            },
            "dataset": {
                "rows_in": dataset_stats.get("rows_in", 0),
                "rows_cleaned": dataset_stats.get("rows_cleaned", 0),
            },
            "candidates": {
                "pairs_total": candidate_stats.get("pairs_total", 0),
                "pairs_scored": candidate_stats.get("pairs_scored", 0),
                "pairs_ge_medium": candidate_stats.get("pairs_ge_medium", 0),
                "pairs_ge_high": candidate_stats.get("pairs_ge_high", 0),
            },
            "groups": {
                "count": group_stats.get("count", 0),
                "size_histogram": group_stats.get(
                    "size_histogram", {"1": 0, "2": 0, "3": 0, "4_plus": 0},
                ),
                "max_group_size": group_stats.get("max_group_size", 0),
            },
            "blocks": {"top_tokens": block_stats.get("top_tokens", [])},
            "timings_sec": {
                "clean_normalize": self.timings.get("clean_normalize", 0.0),
                "blocking": self.timings.get("blocking", 0.0),
                "scoring": self.timings.get("scoring", 0.0),
                "grouping": self.timings.get("grouping", 0.0),
                "survivorship": self.timings.get("survivorship", 0.0),
                "disposition": self.timings.get("disposition", 0.0),
                "export_ui": self.timings.get("export_ui", 0.0),
            },
            "memory": {
                "peak_rss_mb": self.peak_memory,
                "tracemalloc_peak_mb": self.peak_memory,
            },
        }


def save_performance_summary(
    summary: Dict[str, Any], output_path: str = "data/processed/perf_summary.json",
) -> None:
    """Save performance summary to JSON file.

    Args:
        summary: Performance summary dict
        output_path: Output file path

    """
    try:
        with open(output_path, "w") as f:
            json.dump(summary, f, indent=2)
        logger.info(f"Performance summary saved to {output_path}")
    except Exception as e:
        logger.error(f"Failed to save performance summary: {e}")
        raise


def compute_group_size_histogram(groups_df: pd.DataFrame) -> Dict[str, int]:
    """Compute group size histogram from groups dataframe.

    Args:
        groups_df: DataFrame with group information

    Returns:
        Dict with size histogram

    """
    if groups_df.empty:
        return {"1": 0, "2": 0, "3": 0, "4_plus": 0}

    # Count group sizes
    size_counts = groups_df["group_size"].value_counts().to_dict()

    # Build histogram
    histogram = {"1": 0, "2": 0, "3": 0, "4_plus": 0}

    for size, count in size_counts.items():
        # Convert numpy types to standard Python types for JSON serialization
        size_int = int(size) if pd.notna(size) else 0
        count_int = int(count) if pd.notna(count) else 0

        if size_int == 1:
            histogram["1"] = count_int
        elif size_int == 2:
            histogram["2"] = count_int
        elif size_int == 3:
            histogram["3"] = count_int
        else:
            histogram["4_plus"] += count_int

    return histogram


def compute_block_top_tokens(
    blocks_df: pd.DataFrame, top_n: int = 10,
) -> List[Dict[str, Any]]:
    """Compute top tokens from blocking statistics.

    Args:
        blocks_df: DataFrame with block information
        top_n: Number of top tokens to return

    Returns:
        List of token statistics

    """
    if blocks_df.empty:
        return []

    # Count tokens and their block sizes
    token_stats = (
        blocks_df.groupby("block_key")
        .agg({"block_size": "first", "account_id": "count"})
        .reset_index()
    )

    # Sort by block size and get top N
    top_tokens = token_stats.nlargest(top_n, "block_size")

    return [
        {
            "token": row["block_key"],
            "count": int(row["account_id"]),
            "cap": None,  # Will be filled by blocking logic if caps are applied
        }
        for _, row in top_tokens.iterrows()
    ]
