"""Performance utilities for the company junction pipeline.

This module provides common performance optimization functions and utilities.
"""

import logging
import time
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Optional, Union

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def to_arrow_strings(df: pd.DataFrame) -> pd.DataFrame:
    """DEPRECATED: PyArrow string optimization removed.

    Args:
        df: Input DataFrame

    Returns:
        DataFrame unchanged (PyArrow backend removed)

    """
    logger.warning("PyArrow string optimization is deprecated and disabled")
    return df


def narrow_sort(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """Sort DataFrame using only specified columns to reduce memory copies.

    Args:
        df: Input DataFrame
        columns: Columns to use for sorting

    Returns:
        Sorted DataFrame

    """
    # Select only needed columns before sorting
    narrow_df = df[columns].copy()
    sorted_df = narrow_df.sort_values(columns)

    # Reindex original DataFrame to match sorted order
    return df.reindex(sorted_df.index)


def parse_name_core_tokens(value: Union[str, list, None]) -> frozenset[str]:
    """Parse name_core_tokens, auto-detecting format.

    Args:
        value: Value to parse (string, list, or None)

    Returns:
        Frozen set of tokens

    """
    if value is None:
        return frozenset()

    if isinstance(value, list):
        return frozenset(str(token) for token in value)

    if isinstance(value, str):
        try:
            import orjson  # optional dep
        except ImportError:
            orjson = None

        if orjson is not None:
            try:
                parsed = orjson.loads(value)
                if isinstance(parsed, list):
                    return frozenset(str(token) for token in parsed)
            except ValueError:
                # Fallback to simple string splitting if orjson fails
                pass

        # Simple fallback: split on spaces and clean
        tokens = value.strip("[]").split(",")
        return frozenset(
            token.strip().strip("\"'") for token in tokens if token.strip()
        )


def build_vectorized_masks(
    df: pd.DataFrame,
    blacklist_regex: str,
    manual_blacklist: Optional[set] = None,
) -> dict:
    """Build vectorized boolean masks for disposition classification.

    Args:
        df: Input DataFrame
        blacklist_regex: Compiled regex for blacklist detection
        manual_blacklist: Optional set of manual blacklist terms

    Returns:
        Dictionary of boolean masks

    """
    masks = {}

    # Blacklisted names
    if blacklist_regex:
        masks["blacklisted_mask"] = df["account_name"].str.contains(
            blacklist_regex,
            case=False,
            regex=True,
            na=False,
        )

    # Manual blacklist terms
    if manual_blacklist:
        manual_mask = (
            df["account_name"]
            .str.lower()
            .str.contains("|".join(manual_blacklist), case=False, na=False)
        )
        if "blacklisted_mask" in masks:
            masks["blacklisted_mask"] |= manual_mask
        else:
            masks["blacklisted_mask"] = manual_mask

    # Multiple names
    masks["multi_name_mask"] = df.get(
        "has_multiple_names",
        pd.Series(False, index=df.index),
    )

    # Alias cross-references
    alias_col = df.get("alias_cross_refs", pd.Series([], index=df.index))
    masks["alias_mask"] = alias_col.str.len().fillna(0) > 0

    # Suffix mismatch (per-group)
    if "suffix_class" in df.columns and "group_id" in df.columns:
        suffix_counts = df.groupby("group_id")["suffix_class"].transform("nunique")
        masks["suffix_mismatch_mask"] = suffix_counts > 1
    else:
        masks["suffix_mismatch_mask"] = pd.Series(False, index=df.index)

    # Singletons
    masks["singleton_mask"] = df.get("group_size", pd.Series(1, index=df.index)) == 1

    return masks


def apply_vectorized_disposition(
    df: pd.DataFrame,
    masks: dict,
    reason_values: list[str],
    manual_overrides: Optional[dict] = None,
) -> tuple[pd.Series, pd.Series]:
    """Apply vectorized disposition classification using numpy.select.

    Args:
        df: Input DataFrame
        masks: Dictionary of boolean masks
        reason_values: List of reason values corresponding to masks
        manual_overrides: Optional manual override mapping

    Returns:
        Tuple of (dispositions, reasons)

    """
    # Build conditions and choices for np.select
    conditions = list(masks.values())
    choices = reason_values

    # Apply vectorized classification
    reasons = np.select(conditions, choices, default="no_conflicts")

    # Map reasons to dispositions
    disposition_map = {
        "blacklisted_name": "Delete",
        "multi_name_string_requires_split": "Verify",
        "alias_cross_references": "Update",
        "suffix_mismatch": "Verify",
        "suspicious_singleton": "Verify",
        "no_conflicts": "Keep",
    }

    dispositions = pd.Series(
        [disposition_map.get(r, "Keep") for r in reasons],
        index=df.index,
    )

    # Apply manual overrides if provided
    if manual_overrides:
        override_mask = df.index.isin(manual_overrides.keys())
        if override_mask.any():
            for idx in df.index[override_mask]:
                if idx in manual_overrides:
                    dispositions.loc[idx] = manual_overrides[idx]

    return dispositions, pd.Series(reasons, index=df.index)


def optimize_dataframe_memory(df: pd.DataFrame) -> pd.DataFrame:
    """Optimize DataFrame memory usage by downcasting numeric types.

    Args:
        df: Input DataFrame

    Returns:
        Memory-optimized DataFrame

    """
    try:
        # Downcast numeric types
        for col in df.select_dtypes(include=["int64"]).columns:
            df[col] = pd.to_numeric(df[col], downcast="integer")

        for col in df.select_dtypes(include=["float64"]).columns:
            df[col] = pd.to_numeric(df[col], downcast="float")

        # Convert object columns to category where beneficial
        for col in df.select_dtypes(include=["object"]).columns:
            if df[col].nunique() / len(df) < 0.5:  # Less than 50% unique values
                df[col] = df[col].astype("category")

        return df
    except Exception as e:
        logger.warning(f"Memory optimization failed: {e}")
        return df


# =============================================================================
# LEGACY PERFORMANCE FUNCTIONS - Required for backward compatibility
# =============================================================================


@contextmanager
def time_stage(stage: str, logger: logging.Logger) -> Iterator[None]:
    """Context manager for timing pipeline stages.

    Args:
        stage: Stage name for logging
        logger: Logger instance

    Yields:
        None

    """
    start_time = time.time()
    logger.info(f"[stage:start] {stage}")
    try:
        yield
    finally:
        duration = time.time() - start_time
        logger.info(f"[stage:end] {stage} ({duration:.2f}s)")


@contextmanager
def track_memory_peak(stage: str, logger: logging.Logger) -> Iterator[None]:
    """Context manager for tracking memory usage during pipeline stages.

    Args:
        stage: Stage name for logging
        logger: Logger instance

    Yields:
        None

    """
    # Simple memory tracking - can be enhanced with psutil if available
    logger.debug(f"Memory tracking started for {stage}")
    try:
        yield
    finally:
        logger.debug(f"Memory tracking completed for {stage}")


def log_performance_summary(logger: logging.Logger) -> None:
    """Log performance summary (placeholder for backward compatibility).

    Args:
        logger: Logger instance

    """
    logger.info("Performance summary logging disabled - using built-in logging instead")
