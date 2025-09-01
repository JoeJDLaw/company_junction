#!/usr/bin/env python3
"""
Check alias results for equivalence and determinism.

This script validates that alias matching produces consistent and equivalent results
across different runs and optimization settings.
"""

import argparse
import hashlib
import sys
from pathlib import Path

import pandas as pd

from src.utils.logging_utils import get_logger

logger = get_logger(__name__)


def load_sorted_alias_matches(path: Path) -> pd.DataFrame:
    """Load and sort alias matches from parquet file with flexible schema handling.
    
    Args:
        path: Path to alias_matches.parquet file
        
    Returns:
        Sorted DataFrame with consistent column order
    """
    try:
        df = pd.read_parquet(path)
        
        # Log actual schema for debugging
        logger.info(f"Loaded {len(df)} alias matches from {path}")
        logger.info(f"Actual columns: {list(df.columns)}")
        
        # Handle different schema versions gracefully
        if len(df.columns) == 2 and 'alias_text' in df.columns and 'match_group_id' in df.columns:
            # Current schema: ['alias_text', 'match_group_id']
            logger.info("Using current schema (alias_text + match_group_id)")
            expected_cols = ['alias_text', 'match_group_id']
        else:
            # Full schema: ['record_id', 'alias_text', 'alias_source', 'match_record_id', 'match_group_id', 'score', 'suffix_match']
            logger.info("Using full schema (all columns)")
            expected_cols = [
                "record_id",
                "alias_text",
                "alias_source",
                "match_record_id",
                "match_group_id",
                "score",
                "suffix_match",
            ]
        
        # Check which expected columns are available
        available_cols = [col for col in expected_cols if col in df.columns]
        if len(available_cols) < len(expected_cols):
            missing = set(expected_cols) - set(available_cols)
            logger.warning(f"Missing expected columns in {path}: {missing}")
            logger.warning("Proceeding with available columns for comparison")
        
        # Select and sort columns for consistent comparison
        df = df[available_cols].sort_values(available_cols).reset_index(drop=True)
        
        return df
        
    except Exception as e:
        logger.error(f"Failed to load {path}: {e}")
        sys.exit(1)


def compute_checksum(df: pd.DataFrame) -> str:
    """Compute stable SHA256 checksum of DataFrame rows.

    Args:
        df: DataFrame to checksum

    Returns:
        SHA256 hash string
    """
    # Convert DataFrame to string representation for hashing
    df_str = df.to_string(index=False)
    return hashlib.sha256(df_str.encode()).hexdigest()


def check_equivalence(path_a: Path, path_b: Path) -> bool:
    """Check if two alias results files are equivalent.

    Args:
        path_a: Path to first results file
        path_b: Path to second results file

    Returns:
        True if equivalent, False otherwise
    """
    logger.info(f"Checking equivalence between {path_a} and {path_b}")

    df_a = load_sorted_alias_matches(path_a)
    df_b = load_sorted_alias_matches(path_b)

    if df_a.equals(df_b):
        logger.info("✅ Results are equivalent")
        return True

    # Find differences
    logger.error("❌ Results are NOT equivalent")

    # Find rows only in A
    only_in_a = pd.concat([df_a, df_b, df_b]).drop_duplicates(keep=False)
    # Find rows only in B
    only_in_b = pd.concat([df_b, df_a, df_a]).drop_duplicates(keep=False)

    if len(only_in_a) > 0:
        logger.error(f"Rows only in {path_a.name} (showing first 5):")
        logger.error(only_in_a.head().to_string())

    if len(only_in_b) > 0:
        logger.error(f"Rows only in {path_b.name} (showing first 5):")
        logger.error(only_in_b.head().to_string())

    logger.error(f"Total differences: {len(only_in_a) + len(only_in_b)} rows")
    return False


def check_determinism(path_a: Path, path_b: Path) -> bool:
    """Check if two alias results files have identical checksums.

    Args:
        path_a: Path to first results file
        path_b: Path to second results file

    Returns:
        True if identical checksums, False otherwise
    """
    logger.info(f"Checking determinism between {path_a} and {path_b}")

    df_a = load_sorted_alias_matches(path_a)
    df_b = load_sorted_alias_matches(path_b)

    checksum_a = compute_checksum(df_a)
    checksum_b = compute_checksum(df_b)

    logger.info(f"Checksum A: {checksum_a}")
    logger.info(f"Checksum B: {checksum_b}")

    if checksum_a == checksum_b:
        logger.info("✅ Results are deterministic (identical checksums)")
        return True
    else:
        logger.error("❌ Results are NOT deterministic (different checksums)")
        return False


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Check alias results for equivalence and determinism"
    )
    parser.add_argument(
        "--mode",
        choices=["equivalence", "determinism"],
        required=True,
        help="Check mode: equivalence or determinism",
    )
    parser.add_argument(
        "--run-a",
        type=Path,
        required=True,
        help="Path to first alias_matches.parquet file",
    )
    parser.add_argument(
        "--run-b",
        type=Path,
        required=True,
        help="Path to second alias_matches.parquet file",
    )

    args = parser.parse_args()

    # Validate file paths
    if not args.run_a.exists():
        logger.error(f"File not found: {args.run_a}")
        sys.exit(1)

    if not args.run_b.exists():
        logger.error(f"File not found: {args.run_b}")
        sys.exit(1)

    # Run the appropriate check
    if args.mode == "equivalence":
        success = check_equivalence(args.run_a, args.run_b)
    else:  # determinism
        success = check_determinism(args.run_a, args.run_b)

    # Exit with appropriate code
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
