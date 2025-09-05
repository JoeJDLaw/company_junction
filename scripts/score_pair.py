#!/usr/bin/env python3
"""Score Pair CLI - Debug utility to trace similarity scoring for two company names.

Usage:
    python scripts/score_pair.py "99 Cents Only Stores LLC" "99 Cents Store Inc"
    python scripts/score_pair.py "7-Eleven Store #123" "7 Eleven Inc" --suffix-a "NONE" --suffix-b "INC"
"""

import argparse
import sys
from pathlib import Path
from typing import Any, cast

import yaml
from rapidfuzz import fuzz

# Add project root to path for imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.normalize import (  # noqa: E402
    enhance_name_core,
    get_enhanced_tokens_for_jaccard,
    normalize_name,
)
from src.similarity.scoring import _check_numeric_style_match  # noqa: E402


def load_settings(config_path: str = "config/settings.yaml") -> dict[str, Any]:
    """Load settings from config file."""
    try:
        with open(config_path) as f:
            return cast("dict[str, Any]", yaml.safe_load(f))
    except Exception as e:
        print(f"Warning: Could not load config from {config_path}: {e}")
        return {}


def trace_scoring(
    name_a: str,
    name_b: str,
    suffix_a: str = "NONE",
    suffix_b: str = "NONE",
    settings: dict[str, Any] | None = None,
) -> int:
    """Trace the complete scoring process for two names."""
    print("=" * 80)
    print("SIMILARITY SCORING TRACE")
    print("=" * 80)

    # Step 1: Normalize names
    print("\n1. INPUT NAMES:")
    print(f"   Name A: '{name_a}' (suffix: {suffix_a})")
    print(f"   Name B: '{name_b}' (suffix: {suffix_b})")

    # Normalize using the pipeline
    norm_a = normalize_name(name_a)
    norm_b = normalize_name(name_b)

    print("\n2. NORMALIZATION:")
    print(f"   Name A: '{norm_a.name_core}' (suffix_class: {norm_a.suffix_class})")
    print(f"   Name B: '{norm_b.name_core}' (suffix_class: {norm_b.suffix_class})")

    # Override suffix if provided
    if suffix_a != "NONE":
        norm_a.suffix_class = suffix_a
    if suffix_b != "NONE":
        norm_b.suffix_class = suffix_b

    # Step 3: Enhanced normalization
    if settings:
        norm_settings = settings.get("similarity", {}).get("normalization", {})
        print("\n3. ENHANCED NORMALIZATION:")

        enhanced_a, weak_a = enhance_name_core(norm_a.name_core, norm_settings)
        enhanced_b, weak_b = enhance_name_core(norm_b.name_core, norm_settings)

        print(f"   Enhanced A: '{enhanced_a}' (weak tokens: {weak_a})")
        print(f"   Enhanced B: '{enhanced_b}' (weak tokens: {weak_b})")

        # Get tokens for Jaccard (excluding weak tokens)
        tokens_a = get_enhanced_tokens_for_jaccard(norm_a.name_core, norm_settings)
        tokens_b = get_enhanced_tokens_for_jaccard(norm_b.name_core, norm_settings)

        print(f"   Jaccard tokens A: {tokens_a}")
        print(f"   Jaccard tokens B: {tokens_b}")
    else:
        enhanced_a = norm_a.name_core
        enhanced_b = norm_b.name_core
        tokens_a = set(norm_a.name_core.split())
        tokens_b = set(norm_b.name_core.split())
        print("\n3. ENHANCED NORMALIZATION: (disabled - no settings)")
        print("   Using original names for scoring")

    # Step 4: RapidFuzz ratios
    print("\n4. RAPIDFUZZ RATIOS:")
    ratio_name = fuzz.token_sort_ratio(enhanced_a, enhanced_b)
    ratio_set = fuzz.token_set_ratio(enhanced_a, enhanced_b)
    print(f"   token_sort_ratio: {ratio_name}")
    print(f"   token_set_ratio: {ratio_set}")

    # Step 5: Jaccard similarity
    print("\n5. JACCARD SIMILARITY:")
    if tokens_a and tokens_b:
        intersection = len(tokens_a & tokens_b)
        union = len(tokens_a | tokens_b)
        jaccard = intersection / union if union > 0 else 0.0
        print(f"   Intersection: {tokens_a & tokens_b} ({intersection} tokens)")
        print(f"   Union: {tokens_a | tokens_b} ({union} tokens)")
        print(f"   Jaccard: {intersection}/{union} = {jaccard:.3f}")
    else:
        jaccard = 0.0
        print("   Jaccard: 0.0 (empty token sets)")

    # Step 6: Penalty checks
    print("\n6. PENALTY CHECKS:")
    num_style_match = _check_numeric_style_match(norm_a.name_core, norm_b.name_core)
    suffix_match = norm_a.suffix_class == norm_b.suffix_class
    print(f"   Numeric style match: {num_style_match}")
    print(
        f"   Suffix match: {suffix_match} ({norm_a.suffix_class} vs {norm_b.suffix_class})",
    )

    # Step 7: Final score calculation
    print("\n7. FINAL SCORE CALCULATION:")
    base = 0.45 * ratio_name + 0.35 * ratio_set + 20.0 * jaccard
    print(
        f"   Base score: 0.45 × {ratio_name} + 0.35 × {ratio_set} + 20.0 × {jaccard:.3f} = {base:.1f}",
    )

    # Apply penalties
    penalties = settings.get("similarity", {}).get("penalty", {}) if settings else {}
    num_penalty = penalties.get("num_style_mismatch", 5)
    suffix_penalty = penalties.get("suffix_mismatch", 25)

    if not num_style_match:
        base -= num_penalty
        print(f"   - Numeric style penalty: -{num_penalty}")
    if not suffix_match:
        base -= suffix_penalty
        print(f"   - Suffix mismatch penalty: -{suffix_penalty}")

    final_score = max(0, min(100, round(base)))
    print(f"   Final score: {final_score}")

    # Step 8: Grouping decision
    print("\n8. GROUPING DECISION:")
    high_threshold = settings.get("similarity", {}).get("high", 92) if settings else 92
    medium_threshold = (
        settings.get("similarity", {}).get("medium", 84) if settings else 84
    )

    if final_score >= high_threshold:
        print(
            f"   ✅ HIGH THRESHOLD: {final_score} >= {high_threshold} → Direct grouping",
        )
    elif final_score >= medium_threshold and (tokens_a & tokens_b):
        print(
            f"   ✅ MEDIUM+SHARED: {final_score} >= {medium_threshold} + shared tokens → Grouping",
        )
    else:
        print(
            f"   ❌ NO GROUPING: {final_score} < {medium_threshold} or no shared tokens",
        )

    print("=" * 80)
    return final_score


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Trace similarity scoring for two company names",
    )
    parser.add_argument("name_a", help="First company name")
    parser.add_argument("name_b", help="Second company name")
    parser.add_argument(
        "--suffix-a", default="NONE", help="Suffix class for first name (override)",
    )
    parser.add_argument(
        "--suffix-b", default="NONE", help="Suffix class for second name (override)",
    )
    parser.add_argument(
        "--config", default="config/settings.yaml", help="Path to config file",
    )

    args = parser.parse_args()

    # Load settings
    settings = load_settings(args.config)

    # Trace scoring
    score = trace_scoring(
        args.name_a, args.name_b, args.suffix_a, args.suffix_b, settings,
    )

    print(f"\n🎯 FINAL RESULT: {score}% similarity")

    # Show config summary
    if settings:
        norm_settings = settings.get("similarity", {}).get("normalization", {})
        if norm_settings:
            print("\n📋 NORMALIZATION SETTINGS:")
            print(f"   Weak tokens: {norm_settings.get('weak_tokens', [])}")
            print(f"   Plural mapping: {norm_settings.get('plural_singular_map', {})}")
            print(
                f"   Canonical terms: {norm_settings.get('canonical_retail_terms', {})}",
            )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
