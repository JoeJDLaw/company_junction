"""Diagnostics and brand suggestions functionality."""

import logging
from typing import Any, Dict, List, Optional

import pandas as pd

logger = logging.getLogger(__name__)


def write_blocking_diagnostics(
    block_stats: List[Dict[str, Any]],
    brand_suggestions: List[Dict[str, Any]],
    interim_dir: Optional[str] = None,
) -> None:
    """Write blocking diagnostics to CSV/Parquet files."""
    try:
        import os

        output_dir = interim_dir or "data/interim"
        os.makedirs(output_dir, exist_ok=True)

        # Write block statistics
        if block_stats:
            block_df = pd.DataFrame(block_stats)
            csv_path = f"{output_dir}/block_stats.csv"
            block_df.to_csv(csv_path, index=False)
            logger.info(f"Wrote {len(block_stats)} block rows to {csv_path}")

        # Write brand suggestions
        if brand_suggestions:
            suggestions_df = pd.DataFrame(brand_suggestions)
            csv_path = f"{output_dir}/brand_suggestions.csv"
            suggestions_df.to_csv(csv_path, index=False)
            logger.info(
                f"Wrote {len(brand_suggestions)} brand suggestion rows to {csv_path}",
            )

    except Exception as e:
        logger.warning(f"Failed to write blocking diagnostics: {e}")


def generate_brand_suggestions(
    block_stats: List[Dict[str, Any]],
    groups_df: pd.DataFrame,
    settings: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """Generate brand suggestions based on block statistics and group analysis.

    Args:
        block_stats: List of block statistics from blocking phase
        groups_df: DataFrame with group information (group_id, group_size, etc.)
        settings: Configuration settings

    Returns:
        List of brand suggestions with confidence scores

    """
    if not block_stats or groups_df.empty:
        return []

    # Get configuration
    min_count = (
        settings.get("similarity", {})
        .get("blocking", {})
        .get("min_suggestion_count", 10)
    )
    min_singleton_pct = (
        settings.get("similarity", {}).get("blocking", {}).get("min_singleton_pct", 0.6)
    )

    # Get current allowlist and denylist (normalized to lowercase)
    blocking_settings = settings.get("similarity", {}).get("blocking", {})
    allowlist_tokens = {
        t.lower() for t in blocking_settings.get("allowlist_tokens", [])
    }
    denylist_tokens = {t.lower() for t in blocking_settings.get("denylist_tokens", [])}

    # Calculate singleton rates for each token
    token_singleton_stats = {}

    for stat in block_stats:
        token = stat["token"]
        count = stat["count"]

        if token in allowlist_tokens or token in denylist_tokens:
            continue  # Skip already configured tokens

        # Calculate singleton rate for this token
        # NOTE: This is a placeholder heuristic - in practice, you'd need to map tokens back to records
        # to get true singleton rates. This mock calculation is for demonstration purposes only.
        if not groups_df.empty and "group_size" in groups_df.columns:
            # Mock calculation: assume tokens with high frequency tend to have more singletons
            # In a real implementation, you'd map tokens back to their records and calculate actual singleton rates
            singleton_rate = min(
                1.0,
                count / 100.0,
            )  # Mock: higher count = higher singleton rate
        else:
            singleton_rate = 0.0

        token_singleton_stats[token] = {
            "count": count,
            "singleton_rate": singleton_rate,
            "pairs_generated": stat.get("pairs_generated", 0),
            "pairs_capped": stat.get("pairs_capped", 0),
        }

    # Generate suggestions
    suggestions = []
    for token, stats in token_singleton_stats.items():
        count = stats["count"]
        singleton_rate = stats["singleton_rate"]

        # Apply suggestion criteria
        if count >= min_count and singleton_rate >= min_singleton_pct:
            # Calculate suggestion confidence
            count_score = min(1.0, count / 50.0)  # Normalize count to [0,1]
            singleton_score = max(
                0.0,
                (singleton_rate - 0.5) / 0.5,
            )  # Normalize singleton rate
            confidence = 0.5 * count_score + 0.5 * singleton_score
            confidence = max(0.0, min(1.0, confidence))  # Clamp to [0,1]

            suggestions.append(
                {
                    "token": token,
                    "count": count,
                    "pct_singletons": singleton_rate,
                    "pairs_generated": stats["pairs_generated"],
                    "pairs_capped": stats["pairs_capped"],
                    "suggestion_confidence": confidence,
                },
            )

    # Sort by confidence (highest first)
    suggestions.sort(key=lambda x: x["suggestion_confidence"], reverse=True)

    return suggestions
