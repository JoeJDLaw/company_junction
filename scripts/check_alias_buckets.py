#!/usr/bin/env python3
"""
Check alias matching first-token bucket sizes.

This script scans the name_core column to identify large first-token buckets
that might trigger warnings during alias matching.
"""

import argparse
import sys
from collections import Counter
from pathlib import Path
from typing import List, Tuple

import pandas as pd

from src.utils.logging_utils import get_logger

logger = get_logger(__name__)


def extract_first_tokens(name_core_series: pd.Series) -> Counter:
    """Extract first tokens from name_core series.

    Args:
        name_core_series: Series of company names

    Returns:
        Counter of first token frequencies
    """
    first_tokens = []

    for name in name_core_series:
        if pd.isna(name) or not name:
            continue

        # Extract first token (word before first space)
        first_token = name.split()[0] if name else ""
        if first_token:
            first_tokens.append(first_token.lower())

    return Counter(first_tokens)


def analyze_buckets(csv_path: Path, top_n: int = 10) -> List[Tuple[str, int]]:
    """Analyze first-token bucket sizes from CSV file.

    Args:
        csv_path: Path to CSV file with name_core column
        top_n: Number of top buckets to report

    Returns:
        List of (token, count) tuples sorted by count descending
    """
    logger.info(f"Analyzing first-token buckets from {csv_path}")

    try:
        # Read CSV file
        df = pd.read_csv(csv_path)

        # Check if name_core column exists
        if "name_core" not in df.columns:
            logger.error("Column 'name_core' not found in CSV file")
            logger.info(f"Available columns: {list(df.columns)}")
            sys.exit(1)

        # Extract first tokens
        first_tokens = extract_first_tokens(df["name_core"])

        # Get top N buckets
        top_buckets = first_tokens.most_common(top_n)

        logger.info(f"Found {len(first_tokens)} unique first tokens")
        logger.info(f"Total records: {len(df)}")

        return top_buckets

    except Exception as e:
        logger.error(f"Failed to analyze {csv_path}: {e}")
        sys.exit(1)


def print_bucket_report(
    buckets: List[Tuple[str, int]], warning_threshold: int = 10000
) -> None:
    """Print bucket analysis report.

    Args:
        buckets: List of (token, count) tuples
        warning_threshold: Threshold for large bucket warnings
    """
    logger.info("=" * 60)
    logger.info("FIRST-TOKEN BUCKET ANALYSIS")
    logger.info("=" * 60)

    # Print header
    print(f"{'Token':<30} {'Count':<10} {'Status':<15}")
    print("-" * 60)

    # Print buckets
    for token, count in buckets:
        if count >= warning_threshold:
            status = "⚠️  LARGE"
        elif count >= 1000:
            status = "📊 MEDIUM"
        else:
            status = "✅ NORMAL"

        print(f"{token:<30} {count:<10} {status:<15}")

    print("-" * 60)

    # Summary statistics
    total_records = sum(count for _, count in buckets)
    large_buckets = sum(1 for _, count in buckets if count >= warning_threshold)
    medium_buckets = sum(1 for _, count in buckets if 1000 <= count < warning_threshold)

    logger.info("Summary:")
    logger.info(f"  Total records analyzed: {total_records}")
    logger.info(f"  Large buckets (≥{warning_threshold}): {large_buckets}")
    logger.info(f"  Medium buckets (1k-{warning_threshold}): {medium_buckets}")

    if large_buckets > 0:
        logger.warning(
            f"⚠️  {large_buckets} large bucket(s) detected - may trigger warnings during alias matching"
        )

    # Recommendations
    if large_buckets > 0:
        logger.info("Recommendations:")
        logger.info(
            "  - Consider adjusting alias matching thresholds for large buckets"
        )
        logger.info("  - Monitor alias matching performance for these tokens")
        logger.info("  - Large buckets may benefit most from optimization")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Analyze first-token bucket sizes for alias matching"
    )
    parser.add_argument(
        "--input",
        type=Path,
        required=True,
        help="Path to input CSV file with name_core column",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=10,
        help="Number of top buckets to report (default: 10)",
    )
    parser.add_argument(
        "--warning-threshold",
        type=int,
        default=10000,
        help="Threshold for large bucket warnings (default: 10000)",
    )

    args = parser.parse_args()

    # Validate inputs
    if not args.input.exists():
        logger.error(f"Input file not found: {args.input}")
        sys.exit(1)

    if args.top_n <= 0:
        logger.error("top-n must be positive")
        sys.exit(1)

    if args.warning_threshold <= 0:
        logger.error("warning-threshold must be positive")
        sys.exit(1)

    logger.info("Starting bucket analysis")
    logger.info(f"Input: {args.input}")
    logger.info(f"Top N: {args.top_n}")
    logger.info(f"Warning threshold: {args.warning_threshold}")

    # Analyze buckets
    buckets = analyze_buckets(args.input, args.top_n)

    # Print report
    print_bucket_report(buckets, args.warning_threshold)

    logger.info("Bucket analysis completed successfully")


if __name__ == "__main__":
    main()
