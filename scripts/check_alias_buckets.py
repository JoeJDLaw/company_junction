#!/usr/bin/env python3
"""
Analyze block sizes and validate that block cap doesn't cause recall loss.
"""

import argparse
import logging
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, Any, List, Tuple, Set

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

def load_parquet_safe(path: str) -> pd.DataFrame:
    """Load parquet file with error handling."""
    try:
        return pd.read_parquet(path)
    except Exception as e:
        logger.error(f"Failed to load {path}: {e}")
        return pd.DataFrame()

def analyze_block_sizes(block_stats_path: str) -> Dict[str, Any]:
    """Analyze block size distribution."""
    df = pd.read_csv(block_stats_path)
    stats = {
        "total_blocks": len(df),
        "total_records": df["count"].sum(),
        "max_block_size": df["count"].max(),
        "mean_block_size": df["count"].mean(),
        "median_block_size": df["count"].median(),
        "blocks_over_800": len(df[df["count"] > 800]),
        "records_in_large_blocks": df[df["count"] > 800]["count"].sum(),
    }
    return stats

def compare_alias_matches(
    legacy_path: str,
    optimized_path: str,
    block_stats_path: str,
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """Compare alias matches between legacy and optimized paths."""
    
    # Load alias matches
    legacy_df = load_parquet_safe(legacy_path)
    optimized_df = load_parquet_safe(optimized_path)
    
    if legacy_df.empty or optimized_df.empty:
        logger.error("Failed to load alias matches")
        return {}, []
    
    # Create unique pair identifiers
    def get_pair_id(row: pd.Series) -> str:
        return f"{min(row['id_a'], row['id_b'])}_{max(row['id_a'], row['id_b'])}"
    
    legacy_pairs = set(legacy_df.apply(get_pair_id, axis=1))
    optimized_pairs = set(optimized_df.apply(get_pair_id, axis=1))
    
    # Find differences
    only_in_legacy = legacy_pairs - optimized_pairs
    only_in_optimized = optimized_pairs - legacy_pairs
    
    # Get block statistics
    block_stats = analyze_block_sizes(block_stats_path)
    
    # Prepare summary
    summary = {
        "total_legacy_pairs": len(legacy_pairs),
        "total_optimized_pairs": len(optimized_pairs),
        "pairs_only_in_legacy": len(only_in_legacy),
        "pairs_only_in_optimized": len(only_in_optimized),
        "block_stats": block_stats,
    }
    
    # Prepare detailed diff report
    diff_report = []
    
    # Analyze missing pairs
    if only_in_legacy:
        legacy_scores = legacy_df[legacy_df.apply(get_pair_id, axis=1).isin(only_in_legacy)]
        for _, row in legacy_scores.iterrows():
            diff_report.append({
                "pair_id": get_pair_id(row),
                "id_a": row["id_a"],
                "id_b": row["id_b"],
                "score": row["score"],
                "status": "missing_in_optimized",
            })
    
    if only_in_optimized:
        optimized_scores = optimized_df[optimized_df.apply(get_pair_id, axis=1).isin(only_in_optimized)]
        for _, row in optimized_scores.iterrows():
            diff_report.append({
                "pair_id": get_pair_id(row),
                "id_a": row["id_a"],
                "id_b": row["id_b"],
                "score": row["score"],
                "status": "only_in_optimized",
            })
    
    return summary, diff_report

def main():
    parser = argparse.ArgumentParser(description="Analyze block sizes and validate recall")
    parser.add_argument("--legacy", required=True, help="Path to legacy alias matches")
    parser.add_argument("--optimized", required=True, help="Path to optimized alias matches")
    parser.add_argument("--block-stats", required=True, help="Path to block statistics CSV")
    parser.add_argument("--output", help="Path to write detailed diff report")
    args = parser.parse_args()
    
    # Run comparison
    summary, diff_report = compare_alias_matches(
        args.legacy,
        args.optimized,
        args.block_stats,
    )
    
    # Print summary
    logger.info("=== Block Size Analysis ===")
    for key, value in summary["block_stats"].items():
        logger.info(f"{key}: {value}")
    
    logger.info("\n=== Pair Comparison ===")
    logger.info(f"Total legacy pairs: {summary['total_legacy_pairs']}")
    logger.info(f"Total optimized pairs: {summary['total_optimized_pairs']}")
    logger.info(f"Pairs only in legacy: {summary['pairs_only_in_legacy']}")
    logger.info(f"Pairs only in optimized: {summary['pairs_only_in_optimized']}")
    
    # Write detailed report if requested
    if args.output and diff_report:
        output_path = Path(args.output)
        pd.DataFrame(diff_report).to_csv(output_path, index=False)
        logger.info(f"\nDetailed diff report written to {output_path}")
    
    # Exit with status
    if summary["pairs_only_in_legacy"] > 0:
        logger.error("❌ Found pairs in legacy that are missing in optimized path")
        exit(1)
    else:
        logger.info("✅ No recall loss detected")
        exit(0)

if __name__ == "__main__":
    main()