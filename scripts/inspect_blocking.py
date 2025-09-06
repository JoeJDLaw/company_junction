#!/usr/bin/env python3
"""Blocking Inspector CLI - Quick utility to inspect blocking behavior and suggest allowlist updates.

Usage:
    python scripts/inspect_blocking.py --run-id <run_id> --top-n 20
    python scripts/inspect_blocking.py --csv data/interim/block_stats.csv
"""

import argparse
import sys
from pathlib import Path
from typing import cast

import pandas as pd
import yaml


def load_block_stats(csv_path: str) -> pd.DataFrame:
    """Load block statistics from CSV file."""
    try:
        df = pd.read_csv(csv_path)
        return df
    except Exception as e:
        print(f"Error loading block stats from {csv_path}: {e}")
        sys.exit(1)


def suggest_allowlist_additions(df: pd.DataFrame, top_n: int = 20) -> pd.DataFrame:
    """Suggest tokens that might belong in the allowlist based on:
    - High frequency
    - High singleton rate (suggesting they might be business names)
    - Currently not in allowlist
    """
    # Calculate singleton rate (groups of size 1)
    df["singleton_rate"] = df.apply(
        lambda row: (
            1.0
            if row["strategy"] == "full_pairs" and row["pairs_generated"] == 0
            else 0.0
        ),
        axis=1,
    )

    # Calculate suggestion score
    df["suggestion_score"] = df["count"] * (1 + df["singleton_rate"])

    # Sort by suggestion score
    suggestions = df.nlargest(top_n, "suggestion_score")

    return suggestions[
        [
            "token",
            "count",
            "strategy",
            "pairs_generated",
            "singleton_rate",
            "suggestion_score",
        ]
    ]


def print_blocking_summary(df: pd.DataFrame) -> None:
    """Print a summary of blocking behavior."""
    print("\n=== Blocking Summary ===")
    print(f"Total tokens processed: {len(df)}")
    print(f"Total records: {df['count'].sum()}")
    print(f"Total pairs generated: {df['pairs_generated'].sum()}")

    # Strategy breakdown
    strategy_counts = df["strategy"].value_counts()
    print("\nStrategy breakdown:")
    for strategy, count in strategy_counts.items():
        print(f"  {strategy}: {count} tokens")

    # Top tokens by frequency
    print("\nTop 10 tokens by frequency:")
    top_tokens = df.nlargest(10, "count")
    for _, row in top_tokens.iterrows():
        print(
            f"  {row['token']}: {row['count']} records, {row['pairs_generated']} pairs",
        )


def print_allowlist_suggestions(
    suggestions: pd.DataFrame,
    current_allowlist: list,
) -> None:
    """Print suggested allowlist additions."""
    print("\n=== Allowlist Suggestions ===")
    print("Tokens that might belong in allowlist (high frequency + singleton rate):")

    for _, row in suggestions.iterrows():
        token = row["token"]
        if token not in current_allowlist and len(token) > 1:  # Skip single characters
            print(
                f"  {token}: {row['count']} records, {row['singleton_rate']:.1%} singleton rate, score: {row['suggestion_score']:.1f}",
            )


def load_brand_suggestions(csv_path: str) -> pd.DataFrame:
    """Load brand suggestions from CSV file."""
    try:
        df = pd.read_csv(csv_path)
        return df
    except Exception as e:
        print(f"Error loading brand suggestions from {csv_path}: {e}")
        return pd.DataFrame()


def print_brand_suggestions(suggestions_df: pd.DataFrame, top_n: int = 20) -> None:
    """Print top brand suggestions."""
    if suggestions_df.empty:
        print("\n=== Brand Suggestions ===")
        print("No brand suggestions available.")
        return

    print(f"\n=== Brand Suggestions (Top {top_n}) ===")
    print("Tokens suggested for allowlist based on frequency and singleton rate:")

    top_suggestions = suggestions_df.head(top_n)
    for _, row in top_suggestions.iterrows():
        confidence = row.get("suggestion_confidence", 0.0)
        print(
            f"  {row['token']}: {row['count']} records, {row['pct_singletons']:.1%} singletons, confidence: {confidence:.2f}",
        )


def explain_token(
    token: str,
    block_stats_df: pd.DataFrame,
    suggestions_df: pd.DataFrame,
) -> None:
    """Explain a specific token's statistics."""
    print(f"\n=== Token Explanation: '{token}' ===")

    # Find in block stats
    block_row = block_stats_df[block_stats_df["token"] == token]
    if not block_row.empty:
        row = block_row.iloc[0]
        print("Block Statistics:")
        print(f"  Count: {row['count']} records")
        print(f"  Strategy: {row.get('strategy', 'unknown')}")
        print(f"  Pairs Generated: {row.get('pairs_generated', 0)}")
        print(f"  Pairs Capped: {row.get('pairs_capped', 0)}")
    else:
        print(f"Token '{token}' not found in block statistics.")

    # Find in suggestions
    suggestion_row = suggestions_df[suggestions_df["token"] == token]
    if not suggestion_row.empty:
        row = suggestion_row.iloc[0]
        print("Brand Suggestion:")
        print(f"  Confidence: {row.get('suggestion_confidence', 0.0):.2f}")
        print(f"  Singleton Rate: {row.get('pct_singletons', 0.0):.1%}")
    else:
        print(f"Token '{token}' not found in brand suggestions.")


def export_high_confidence_suggestions(
    suggestions_df: pd.DataFrame,
    output_path: str,
    min_confidence: float = 0.7,
) -> None:
    """Export high-confidence suggestions to CSV."""
    if suggestions_df.empty:
        print("No suggestions to export.")
        return

    high_conf = suggestions_df[
        suggestions_df.get("suggestion_confidence", 0) >= min_confidence
    ]

    if high_conf.empty:
        print(f"No suggestions with confidence >= {min_confidence} found.")
        return

    high_conf.to_csv(output_path, index=False)
    print(f"Exported {len(high_conf)} high-confidence suggestions to {output_path}")


def load_current_allowlist(config_path: str = "config/settings.yaml") -> list[str]:
    """Load current allowlist from config."""
    try:
        with open(config_path) as f:
            config = yaml.safe_load(f)
        return cast(
            "list[str]",
            config.get("similarity", {})
            .get("blocking", {})
            .get("allowlist_tokens", []),
        )
    except Exception as e:
        print(f"Warning: Could not load allowlist from {config_path}: {e}")
        return []


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Inspect blocking behavior and suggest allowlist updates",
    )
    parser.add_argument("--run-id", help="Run ID to inspect")
    parser.add_argument("--csv", help="Path to block_stats.csv file")
    parser.add_argument(
        "--top-n",
        type=int,
        default=20,
        help="Number of top suggestions to show",
    )
    parser.add_argument(
        "--config",
        default="config/settings.yaml",
        help="Path to config file",
    )
    parser.add_argument(
        "--suggest-brands",
        action="store_true",
        help="Show brand suggestions",
    )
    parser.add_argument("--explain", help="Explain a specific token's stats")
    parser.add_argument(
        "--export-suggestions",
        help="Export high-confidence suggestions to CSV file",
    )

    args = parser.parse_args()

    # Determine CSV path
    if args.csv:
        csv_path = args.csv
    elif args.run_id:
        csv_path = f"data/processed/{args.run_id}/block_stats.csv"
    else:
        print("Error: Must provide either --run-id or --csv")
        sys.exit(1)

    # Check if file exists
    if not Path(csv_path).exists():
        print(f"Error: Block stats file not found: {csv_path}")
        sys.exit(1)

    # Load data
    print(f"Loading block stats from: {csv_path}")
    df = load_block_stats(csv_path)

    # Load current allowlist
    current_allowlist = load_current_allowlist(args.config)
    print(f"Current allowlist has {len(current_allowlist)} tokens")

    # Handle brand suggestions
    if args.suggest_brands or args.explain or args.export_suggestions:
        # Try to load brand suggestions
        suggestions_path = csv_path.replace("block_stats.csv", "brand_suggestions.csv")
        suggestions_df = load_brand_suggestions(suggestions_path)

        if args.suggest_brands:
            print_brand_suggestions(suggestions_df, args.top_n)

        if args.explain:
            explain_token(args.explain, df, suggestions_df)

        if args.export_suggestions:
            export_high_confidence_suggestions(suggestions_df, args.export_suggestions)

    # Print summary
    print_blocking_summary(df)

    # Generate suggestions
    suggestions = suggest_allowlist_additions(df, args.top_n)
    print_allowlist_suggestions(suggestions, current_allowlist)

    # Show current allowlist
    print("\n=== Current Allowlist ===")
    print(f"Tokens: {', '.join(current_allowlist)}")

    print("\n=== Usage Tips ===")
    print("To add a token to allowlist:")
    print("1. Edit config/settings.yaml")
    print("2. Add token to similarity.blocking.allowlist_tokens")
    print("3. Restart pipeline")
    print("\nTo see detailed blocking decisions:")
    print("Check data/interim/block_stats.csv after running pipeline")


if __name__ == "__main__":
    main()
