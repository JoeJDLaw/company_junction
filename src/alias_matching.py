"""
Alias matching functionality for Phase 1.5.

This module handles the extraction and matching of alias candidates
across records without merging groups.
"""

import logging
import pandas as pd
from typing import Dict, List, Tuple
from rapidfuzz import fuzz

logger = logging.getLogger(__name__)


def compute_alias_matches(
    df_norm: pd.DataFrame, df_groups: pd.DataFrame, settings: Dict
) -> Tuple[pd.DataFrame, Dict]:
    """
    Compute alias matches across records.

    Args:
        df_norm: DataFrame with normalized data and alias candidates
        df_groups: DataFrame with group assignments
        settings: Configuration settings

    Returns:
        Tuple of (DataFrame with alias matches, performance stats)
    """
    import time

    start_time = time.time()
    logger.info("Computing alias matches")

    # Get thresholds
    high_threshold = settings.get("similarity", {}).get("high", 92)
    max_alias_pairs = settings.get("similarity", {}).get("max_alias_pairs", 100000)

    # Performance counters
    total_pairs_generated = 0
    capped_blocks = 0
    accepted_matches = 0

    alias_matches = []

    # Process each record with aliases
    for idx, record in df_norm.iterrows():
        alias_candidates = record.get("alias_candidates", [])
        alias_sources = record.get("alias_sources", [])

        if not alias_candidates:
            continue

        # Normalize aliases
        normalized_aliases = []
        for alias in alias_candidates:
            normalized = _normalize_alias(alias)
            if normalized:
                normalized_aliases.append(normalized)

        # Score each alias against other records' name_core
        for i, (alias, source) in enumerate(zip(normalized_aliases, alias_sources)):
            alias_matches.extend(
                _score_alias_against_records(
                    record, alias, source, df_norm, df_groups, high_threshold
                )
            )

    # Limit results if too many
    if len(alias_matches) > max_alias_pairs:
        logger.warning(
            f"Limiting alias matches to {max_alias_pairs} (found {len(alias_matches)})"
        )
        capped_blocks = 1
        alias_matches = alias_matches[:max_alias_pairs]

    # Create DataFrame
    if alias_matches:
        df_matches = pd.DataFrame(alias_matches)
        accepted_matches = len(df_matches)
    else:
        df_matches = pd.DataFrame(
            columns=[
                "record_id",
                "alias_text",
                "alias_source",
                "match_record_id",
                "match_group_id",
                "score",
                "suffix_match",
            ]
        )

    # Calculate performance stats
    elapsed_time = time.time() - start_time
    total_pairs_generated = len(alias_matches)

    performance_stats = {
        "pairs_generated": total_pairs_generated,
        "capped_blocks": capped_blocks,
        "accepted_matches": accepted_matches,
        "elapsed_time": elapsed_time,
    }

    logger.info(f"Generated {len(df_matches)} alias matches in {elapsed_time:.2f}s")
    return df_matches, performance_stats


def _normalize_alias(alias: str) -> str:
    """
    Normalize an alias using the same rules as name_core.

    Args:
        alias: Raw alias string

    Returns:
        Normalized alias
    """
    if not alias:
        return ""

    # Convert to lowercase
    alias = alias.lower()

    # Map common symbols
    symbol_map = {
        "&": " and ",
        "/": " ",
        "-": " ",
        "@": " at ",
        "+": " plus ",
        ",": " ",
        ".": " ",
        ";": " ",
        ":": " ",
        "_": " ",
    }

    for symbol, replacement in symbol_map.items():
        alias = alias.replace(symbol, replacement)

    # Collapse multiple spaces
    import re

    alias = re.sub(r"\s+", " ", alias)

    # Keep only alphanumeric and spaces
    alias = re.sub(r"[^a-z0-9\s]", "", alias)

    return alias.strip()


def _score_alias_against_records(
    record: pd.Series,
    alias: str,
    source: str,
    df_norm: pd.DataFrame,
    df_groups: pd.DataFrame,
    high_threshold: int,
) -> List[Dict]:
    """
    Score an alias against all other records' name_core.

    Args:
        record: Source record
        alias: Normalized alias
        source: Alias source (semicolon, numbered, parentheses)
        df_norm: All normalized records
        df_groups: Group assignments
        high_threshold: Minimum score threshold

    Returns:
        List of match dictionaries
    """
    matches = []
    record_id = record.name

    # Use blocking to limit comparisons
    alias_first_token = alias.split()[0] if alias else ""

    for idx, other_record in df_norm.iterrows():
        if idx == record_id:  # Skip self
            continue

        other_name_core = other_record.get("name_core", "")
        if not other_name_core:
            continue

        # Blocking: only compare if first tokens match
        other_first_token = other_name_core.split()[0] if other_name_core else ""
        if (
            alias_first_token
            and other_first_token
            and alias_first_token != other_first_token
        ):
            continue

        # Compute similarity
        score = fuzz.token_sort_ratio(alias, other_name_core)

        # Check suffix match
        suffix_match = record.get("suffix_class") == other_record.get("suffix_class")

        # Only keep high-confidence matches with suffix match
        if score >= high_threshold and suffix_match:
            match_group_id = (
                df_groups.loc[idx, "group_id"] if idx in df_groups.index else -1
            )

            matches.append(
                {
                    "record_id": record_id,
                    "alias_text": alias,
                    "alias_source": source,
                    "match_record_id": idx,
                    "match_group_id": match_group_id,
                    "score": score,
                    "suffix_match": suffix_match,
                }
            )

    return matches


def create_alias_cross_refs(
    df_norm: pd.DataFrame, df_alias_matches: pd.DataFrame
) -> pd.DataFrame:
    """
    Create alias cross-references for each record.

    Args:
        df_norm: DataFrame with normalized data
        df_alias_matches: DataFrame with alias matches

    Returns:
        DataFrame with alias_cross_refs column added
    """
    if df_alias_matches.empty:
        df_norm["alias_cross_refs"] = [[] for _ in range(len(df_norm))]
        return df_norm

    # Group matches by record_id
    cross_refs: Dict[str, List[str]] = {}
    for _, match in df_alias_matches.iterrows():
        record_id = match["record_id"]
        if record_id not in cross_refs:
            cross_refs[record_id] = []

        cross_refs[record_id].append(
            {
                "alias": match["alias_text"],
                "group_id": match["match_group_id"],
                "score": match["score"],
                "source": match["alias_source"],
            }
        )

    # Add cross_refs to DataFrame
    df_result = df_norm.copy()
    df_result["alias_cross_refs"] = [cross_refs.get(idx, []) for idx in df_result.index]

    return df_result


def save_alias_matches(df_alias_matches: pd.DataFrame, output_path: str) -> None:
    """
    Save alias matches DataFrame to parquet file.

    Args:
        df_alias_matches: DataFrame with alias matches
        output_path: Output file path
    """
    df_alias_matches.to_parquet(output_path, index=False)
    logger.info(f"Saved alias matches to {output_path}")


def load_alias_matches(input_path: str) -> pd.DataFrame:
    """
    Load alias matches DataFrame from parquet file.

    Args:
        input_path: Input file path

    Returns:
        DataFrame with alias matches
    """
    try:
        df_alias_matches = pd.read_parquet(input_path)
        logger.info(f"Loaded alias matches from {input_path}")
        return df_alias_matches
    except Exception as e:
        logger.error(f"Error loading alias matches: {e}")
        return pd.DataFrame()
