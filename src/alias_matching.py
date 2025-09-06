"""Alias matching functionality for Phase 1.5.

This module handles the extraction and matching of alias candidates
across records without merging groups.
"""

import logging
import time
from collections import defaultdict
from itertools import zip_longest
from typing import Any, Optional

import numpy as np
import pandas as pd
from rapidfuzz import fuzz, process

from src.utils.parallel_utils import ParallelExecutor

logger = logging.getLogger(__name__)


def _build_first_token_bucket(
    name_core: pd.Series,
) -> tuple[dict[str, np.ndarray], dict[int, int], dict[int, int]]:
    """Build a bucket mapping first tokens to row indices and an index mapping.

    Args:
        name_core: Series of normalized names

    Returns:
        Tuple of:
        - Dictionary mapping first token to array of row indices
        - Dictionary mapping original indices to new contiguous indices
        - Dictionary mapping new contiguous indices back to original indices

    """
    bucket = defaultdict(list)

    # Create index mapping (old index -> new contiguous index)
    index_map = {old_idx: new_idx for new_idx, old_idx in enumerate(name_core.index)}
    reverse_map = {new_idx: old_idx for old_idx, new_idx in index_map.items()}

    # Build buckets using mapped indices
    for idx, name in name_core.items():
        if pd.isna(name) or not name:
            continue
        first_token = name.split()[0] if name else ""
        if first_token:
            bucket[first_token].append(index_map[idx])

    # Convert to numpy arrays for efficiency
    bucket_arrays = {
        token: np.array(indices, dtype=int) for token, indices in bucket.items()
    }

    # Log bucket statistics
    logger.debug("Index mapping stats:")
    logger.debug(f"  Original indices: {min(name_core.index)}..{max(name_core.index)}")
    logger.debug(f"  Mapped indices: 0..{len(name_core)-1}")
    logger.debug(f"  Total buckets: {len(bucket_arrays)}")

    return bucket_arrays, index_map, reverse_map


def _records_with_aliases(df_norm: pd.DataFrame) -> list[Any]:
    """Get sorted list of record indices that have non-empty alias candidates.

    Args:
        df_norm: DataFrame with normalized data

    Returns:
        Sorted list of record indices with aliases

    """
    records_with_aliases = []

    for idx, record in df_norm.iterrows():
        alias_candidates: list[str] = record.get("alias_candidates", [])

        # Handle numpy arrays properly
        if hasattr(alias_candidates, "size"):
            if alias_candidates.size > 0:
                records_with_aliases.append(idx)
        elif alias_candidates:
            records_with_aliases.append(idx)

    return sorted(records_with_aliases)


def _process_one_record_optimized(
    record_id: int,
    df_norm: pd.DataFrame,
    df_groups: pd.DataFrame,
    name_core: pd.Series,
    suffix_class: pd.Series,
    group_id_by_idx: pd.Series,
    bucket: dict[str, np.ndarray],
    index_map: dict[int, int],
    reverse_map: dict[int, int],
    high_threshold: int,
    debug: bool = False,
) -> list[dict[str, Any]]:
    """Process one record's aliases using optimized vectorized approach.

    Args:
        record_id: Record index to process
        df_norm: DataFrame with normalized data
        df_groups: DataFrame with group assignments
        name_core: Series of normalized names
        suffix_class: Series of suffix classes
        group_id_by_idx: Series of group IDs by index
        bucket: First token bucket
        index_map: Mapping from original indices to new contiguous indices
        reverse_map: Mapping from new contiguous indices back to original indices
        high_threshold: Minimum score threshold
        debug: Enable debug logging

    Returns:
        List of match dictionaries

    """
    record = df_norm.loc[record_id]
    alias_candidates: list[str] = list(record.get("alias_candidates", []))
    alias_sources: list[str] = list(record.get("alias_sources", []))

    # Map record_id to new index space
    record_idx = index_map[record_id]

    if debug:
        logger.info(f"[DEBUG] Processing record {record_id}:")
        logger.info(f"[DEBUG]   name_core: {record.get('name_core', '')}")
        logger.info(f"[DEBUG]   suffix_class: {record.get('suffix_class', '')}")
        logger.info(f"[DEBUG]   alias_candidates: {alias_candidates}")
        logger.info(f"[DEBUG]   alias_sources: {alias_sources}")

    # Handle numpy arrays properly
    if hasattr(alias_candidates, "size"):
        if alias_candidates.size == 0:
            return []
    elif not alias_candidates:
        return []

    # Validate alias_candidates vs alias_sources length
    if len(alias_candidates) != len(alias_sources):
        logger.warning(
            f"Record {record_id}: alias_candidates length ({len(alias_candidates)}) "
            f"!= alias_sources length ({len(alias_sources)})",
        )

    # Normalize aliases
    normalized_aliases = []
    for alias in alias_candidates:
        normalized = _normalize_alias(alias)
        if normalized:
            normalized_aliases.append(normalized)

    matches: list[dict[str, Any]] = []
    record_suffix = suffix_class.loc[record_id]

    # Process each alias
    for alias, source in zip_longest(normalized_aliases, alias_sources, fillvalue=""):
        if not alias:
            continue

        # Get first token for blocking
        first_token = alias.split()[0] if alias else ""
        if not first_token:
            if debug:
                logger.info(f"[DEBUG]   Skipping alias '{alias}': no first token")
            continue

        # Get candidate indices from bucket (already in mapped space)
        candidate_indices = bucket.get(first_token, np.empty(0, dtype=int))
        if candidate_indices.size == 0:
            if debug:
                logger.info(f"[DEBUG]   No candidates for first token '{first_token}'")
            continue

        if debug:
            logger.info(
                f"[DEBUG]   First token '{first_token}': {len(candidate_indices)} candidates",
            )
            logger.info(f"[DEBUG]   Record index mapping: {record_id} → {record_idx}")
            logger.info(
                f"[DEBUG]   Candidate indices (mapped): {candidate_indices[:5]}...",
            )

        # Apply suffix and self-match filters using mapped indices
        mask = (suffix_class.iloc[candidate_indices].values == record_suffix) & (
            candidate_indices != record_idx  # Use mapped index for self-match
        )
        if debug:
            logger.info(
                f"[DEBUG]   Suffix class '{record_suffix}': {mask.sum()} matches",
            )
            logger.info(
                f"[DEBUG]   Candidates before suffix filter: {candidate_indices.size}",
            )

        candidate_indices = candidate_indices[mask]
        if debug:
            logger.info(
                f"[DEBUG]   Candidates after suffix filter: {candidate_indices.size}",
            )

        if candidate_indices.size == 0:
            if debug:
                logger.info("[DEBUG]   No candidates after suffix filtering")
            continue

        # Get candidate names
        candidate_names = name_core.iloc[candidate_indices].tolist()

        # Use rapidfuzz.process.extract for vectorized scoring
        # Benchmark showed extract is ~1.8x faster than cdist for our use case
        results = process.extract(
            alias,
            candidate_names,
            scorer=fuzz.token_sort_ratio,
            score_cutoff=high_threshold,
            limit=None,
        )

        if debug:
            logger.info(f"[DEBUG]   Scoring {len(candidate_names)} candidates")
            logger.info(
                f"[DEBUG]   Got {len(results)} matches above threshold {high_threshold}",
            )

            # Convert results to match dictionaries
        for candidate_name, score, result_idx in results:
            # Get mapped index and convert back to original space using reverse_map
            mapped_idx = candidate_indices[result_idx]
            original_idx = reverse_map[mapped_idx]

            if debug:
                logger.info(
                    f"[DEBUG]   Match: {candidate_name} (score={score}, idx={mapped_idx}→{original_idx})",
                )
                logger.info(
                    f"[DEBUG]   Index mapping: result_idx={result_idx}, mapped_idx={mapped_idx}, original_idx={original_idx}",
                )
                logger.info(
                    f"[DEBUG]   reverse_map[mapped_idx]={reverse_map[mapped_idx]}",
                )
                logger.info(
                    f"[DEBUG]   df_groups.index[mapped_idx]={df_groups.index[mapped_idx]}",
                )

            # Get group ID with fallback for missing indices
            try:
                match_group_id = df_groups.loc[original_idx, "group_id"]
            except KeyError:
                match_group_id = ""

            if debug:
                logger.info(
                    f"[DEBUG]   Group ID: {match_group_id} for record {original_idx}",
                )
                logger.info(f"[DEBUG]   Candidate name: {candidate_name}")
                logger.info(f"[DEBUG]   Original name: {name_core.loc[original_idx]}")
                logger.info(
                    f"[DEBUG]   Group ID lookup: mask={mask.sum()}, id={match_group_id}",
                )

            # Check if this is a valid match
            if score >= high_threshold:
                matches.append(
                    {
                        "record_id": record_id,
                        "alias_text": alias,
                        "alias_source": source,
                        "match_record_id": original_idx,  # Use original index in output
                        "match_group_id": match_group_id,  # Use group ID from df_groups
                        "score": score,
                        "suffix_match": True,  # Already filtered by suffix
                    },
                )

    return matches


def compute_alias_matches(
    df_norm: pd.DataFrame,
    df_groups: pd.DataFrame,
    settings: dict[str, Any],
    parallel_executor: Optional[ParallelExecutor] = None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Compute alias matches across records.

    Args:
        df_norm: DataFrame with normalized data and alias candidates
        df_groups: DataFrame with group assignments
        settings: Configuration settings

    Returns:
        Tuple of (DataFrame with alias matches, performance stats)

    """
    start_time = time.time()
    logger.info("Computing alias matches")

    # Sort DataFrames by index and group_id for deterministic lookups
    df_norm = df_norm.sort_index()
    df_groups = df_groups.sort_values(["group_id", "account_id"]).sort_index().copy()
    df_groups = df_groups.sort_values(["group_id", "account_id"]).sort_index().copy()
    df_groups = (
        df_groups.sort_values(["group_id"]).sort_index().copy()
    )  # Final sort by group_id and index
    df_groups = df_groups.copy()  # Make another copy to ensure index is preserved
    df_groups = (
        df_groups.sort_values(["group_id"]).sort_index().copy()
    )  # Final sort by group_id and index

    # Enable debug logging for specific records
    debug_records = {243, 2725, 2726, 2727, 2728}  # Aegis records

    # Get thresholds and optimization settings
    high_threshold = settings.get("similarity", {}).get("high", 92)
    max_alias_pairs = settings.get("similarity", {}).get("max_alias_pairs", 100000)
    optimize = settings.get("alias", {}).get("optimize", True)
    progress_interval = settings.get("alias", {}).get("progress_interval_s", 1.0)

    # Get worker count from multiple sources with fallback logic
    workers = (
        settings.get("alias", {}).get("workers")  # Alias-specific workers
        or settings.get("parallelism", {}).get("workers")  # Config file
        or settings.get("workers")  # Direct setting
        or settings.get("effective_workers")  # Computed/CLI value
        or 1  # Fallback to sequential
    )

    # Enhanced logging for debugging
    logger.info(
        f"Alias optimization config: optimize={optimize}, workers={workers}, "
        f"progress_interval={progress_interval}s",
    )

    # Performance counters
    total_pairs_generated = 0
    capped_blocks = 0
    accepted_matches = 0

    # Check if we can actually use parallel execution
    can_parallel = (
        optimize
        and workers
        and workers > 1
        and parallel_executor
        and parallel_executor.should_use_parallel(len(df_norm))
    )

    if can_parallel:
        # Optimized path with parallelization
        logger.info(f"✅ Using optimized alias matching with {workers} workers")
    else:
        # Sequential path - determine reason
        if not optimize:
            reason = "optimize=false"
        elif not workers or workers <= 1:
            reason = f"workers={workers} (≤1)"
        elif not parallel_executor:
            reason = "ParallelExecutor not available"
        else:
            reason = "input size below parallel threshold"
        logger.info(f"Using sequential alias matching: {reason}")

    # Initialize alias_matches list
    alias_matches: list[dict[str, Any]] = []

    if can_parallel:
        # Optimized path with parallelization already logged above

        # Precompute indices and data structures
        name_core = df_norm["name_core"].astype("string").fillna("")
        suffix_class = df_norm["suffix_class"].astype("string").fillna("")
        group_id_by_idx = (
            df_groups["group_id"].reindex(df_norm.index).astype("string").fillna("")
        )

        # Build first token bucket with index mapping
        bucket, index_map, reverse_map = _build_first_token_bucket(name_core)

        # Check for large buckets and warn
        for token, indices in bucket.items():
            if len(indices) > 10000:
                logger.warning(
                    f"Large first-token bucket for '{token}': {len(indices)} records",
                )

        # Get records with aliases
        records_with_aliases = _records_with_aliases(df_norm)
        total_records = len(records_with_aliases)

        if total_records == 0:
            logger.info("No records with aliases found")
        else:
            logger.info(
                f"Processing {total_records} records with aliases using {workers} workers",
            )

            # Progress tracking
            _last_progress_time = time.time()
            _processed_count = 0

            def process_one_record(record_id: Any) -> list[dict[str, Any]]:
                return _process_one_record_optimized(
                    record_id,
                    df_norm,
                    df_groups,
                    name_core,
                    suffix_class,
                    group_id_by_idx,
                    bucket,
                    index_map,
                    reverse_map,
                    high_threshold,
                    debug=record_id in debug_records,
                )

            # Use ParallelExecutor for consistent parallelism with similarity module
            if parallel_executor and parallel_executor.should_use_parallel(
                len(records_with_aliases),
            ):
                logger.info(
                    f"Using ParallelExecutor for alias matching with {parallel_executor.workers} workers",
                )

                # Process all records using execute_chunked for optimal parallel processing
                results = parallel_executor.execute_chunked(
                    process_one_record,
                    records_with_aliases,
                    operation_name="alias_matching_parallel",
                )

                # Flatten results like similarity module
                alias_matches = [match for chunk in results for match in chunk]
            else:
                # Sequential processing already logged above

                # Sequential processing with progress tracking
                for i, record_id in enumerate(records_with_aliases):
                    chunk_results = process_one_record(record_id)
                    alias_matches.extend(chunk_results)

                    # Progress logging
                    if (i + 1) % 100 == 0 or (i + 1) == total_records:
                        rate = (i + 1) / (time.time() - start_time + 1e-6)
                        eta = (total_records - (i + 1)) / (rate + 1e-6)
                        logger.info(
                            f"Alias progress: {i + 1}/{total_records} "
                            f"({(i + 1)/total_records*100:.1f}%) "
                            f"rate: {rate:.1f} rec/s ETA: {eta:.1f}s",
                        )

            # Sort matches by record IDs first, then alias text and group ID
            alias_matches.sort(
                key=lambda x: (
                    x["record_id"],
                    x["match_record_id"],
                    x["alias_text"],
                    x["match_group_id"],
                ),
            )

    else:
        # Legacy sequential path
        logger.info("Using legacy sequential alias matching")

        # Process each record with aliases
        for idx, record in df_norm.iterrows():
            alias_candidates: list[str] = record.get("alias_candidates", [])
            alias_sources: list[str] = record.get("alias_sources", [])

            # Handle numpy arrays properly
            if hasattr(alias_candidates, "size"):
                if alias_candidates.size == 0:
                    continue
            elif not alias_candidates:
                continue

            # Normalize aliases
            normalized_aliases = []
            for alias in alias_candidates:
                normalized = _normalize_alias(alias)
                if normalized:
                    normalized_aliases.append(normalized)

            # Score each alias against other records' name_core
            for _i, (alias, source) in enumerate(
                zip(normalized_aliases, alias_sources)
            ):
                alias_matches.extend(
                    _score_alias_against_records(
                        record,
                        alias,
                        source,
                        df_norm,
                        df_groups,
                        high_threshold,
                        debug=idx in debug_records,
                    ),
                )

    # Limit results if too many
    if len(alias_matches) > max_alias_pairs:
        logger.warning(
            f"Limiting alias matches to {max_alias_pairs} (found {len(alias_matches)})",
        )
        capped_blocks = 1
        alias_matches = alias_matches[:max_alias_pairs]

    # Create DataFrame and sort matches
    if alias_matches:
        # Sort matches by record IDs first, then alias text and group ID
        alias_matches.sort(
            key=lambda x: (
                x["record_id"],
                x["match_record_id"],
                x["alias_text"],
                x["match_group_id"],
            ),
        )
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
            ],
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
    """Normalize an alias using the same rules as name_core.

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
    debug: bool = False,
) -> list[dict[str, Any]]:
    """Score an alias against all other records' name_core.

    Args:
        record: Source record
        alias: Normalized alias
        source: Alias source (semicolon, numbered, parentheses)
        df_norm: All normalized records
        df_groups: Group assignments
        high_threshold: Minimum score threshold
        debug: Enable debug logging

    Returns:
        List of match dictionaries

    """
    matches: list[dict[str, Any]] = []
    record_id = record.name

    if debug:
        logger.info(f"[DEBUG-LEGACY] Processing record {record_id}:")
        logger.info(f"[DEBUG-LEGACY]   name_core: {record.get('name_core', '')}")
        logger.info(f"[DEBUG-LEGACY]   suffix_class: {record.get('suffix_class', '')}")
        logger.info(f"[DEBUG-LEGACY]   alias: {alias}")
        logger.info(f"[DEBUG-LEGACY]   source: {source}")

    # Use blocking to limit comparisons
    alias_first_token = alias.split()[0] if alias else ""
    if not alias_first_token and debug:
        logger.info("[DEBUG-LEGACY]   No first token, skipping")
        return matches

    if debug:
        logger.info(f"[DEBUG-LEGACY]   First token: {alias_first_token}")

    candidates = 0
    suffix_matches = 0
    score_matches = 0

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

        candidates += 1

        # Check suffix match first
        suffix_match = record.get("suffix_class") == other_record.get("suffix_class")
        if not suffix_match:
            continue

        suffix_matches += 1

        # Compute similarity
        score = fuzz.token_sort_ratio(alias, other_name_core)

        # Only keep high-confidence matches with suffix match
        if score >= high_threshold:
            score_matches += 1
            mask = df_groups.index == idx
            match_group_id = (
                df_groups.loc[mask, "group_id"].iloc[0] if mask.any() else ""
            )

            if debug:
                logger.info(
                    f"[DEBUG-LEGACY]   Match: {other_name_core} (score={score})",
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
                },
            )

    if debug:
        logger.info(f"[DEBUG-LEGACY]   Candidates: {candidates}")
        logger.info(f"[DEBUG-LEGACY]   Suffix matches: {suffix_matches}")
        logger.info(f"[DEBUG-LEGACY]   Score matches: {score_matches}")
        logger.info(f"[DEBUG-LEGACY]   Total matches: {len(matches)}")

    return matches


def create_alias_cross_refs(
    df_norm: pd.DataFrame,
    df_alias_matches: pd.DataFrame,
) -> pd.DataFrame:
    """Create alias cross-references for each record.

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
    cross_refs: dict[str, list[dict[str, Any]]] = {}
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
            },
        )

    # Add cross_refs to DataFrame
    df_result = df_norm.copy()
    df_result["alias_cross_refs"] = [cross_refs.get(idx, []) for idx in df_result.index]

    return df_result


def save_alias_matches(df_alias_matches: pd.DataFrame, output_path: str) -> None:
    """Save alias matches DataFrame to parquet file.

    Args:
        df_alias_matches: DataFrame with alias matches
        output_path: Output file path

    """
    # Sanitize to parquet-friendly columns only
    sanitized_columns = [
        "account_id",
        "alias_text",
        "matched_account_id",
        "match_group_id",
        "match_score",
        "source",
    ]

    # Keep only columns that exist
    available_columns = [
        col for col in sanitized_columns if col in df_alias_matches.columns
    ]
    df_sanitized = df_alias_matches[available_columns].copy()

    # Force proper dtypes
    for col in df_sanitized.columns:
        if col in [
            "account_id",
            "alias_text",
            "matched_account_id",
            "match_group_id",
            "source",
        ]:
            df_sanitized[col] = df_sanitized[col].astype("string")
        elif col == "match_score":
            df_sanitized[col] = df_sanitized[col].astype("float32")

    df_sanitized.to_parquet(output_path, index=False)
    logger.info(f"Saved {len(df_sanitized)} alias matches to {output_path}")


def load_alias_matches(input_path: str) -> pd.DataFrame:
    """Load alias matches DataFrame from parquet file.

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
