"""
Data type utilities for memory-efficient pipeline processing.

Provides functions to apply consistent dtypes and validate data types
across the pipeline to prevent memory bloat and ensure data consistency.
"""

import pandas as pd
from typing import Dict, Set, Optional
import logging

try:
    from src.dtypes_map import (
        ALLOWED_OBJECT_COLUMNS,
        INTERMEDIATE_COLUMNS_TO_DROP,
    )
except ImportError:
    from src.dtypes_map import ALLOWED_OBJECT_COLUMNS, INTERMEDIATE_COLUMNS_TO_DROP

from src.utils.schema_utils import (
    GROUP_ID,
    ACCOUNT_ID,
    DISPOSITION,
    SCORE,
    NAME_CORE,
)

logger = logging.getLogger(__name__)


def apply_dtypes(df: pd.DataFrame, schema: Dict[str, str]) -> pd.DataFrame:
    """
    Apply dtype mapping to dataframe, handling missing columns gracefully.

    Args:
        df: Input dataframe
        schema: Dict mapping column names to dtypes

    Returns:
        DataFrame with applied dtypes
    """
    if df.empty:
        return df

    # Only apply dtypes to columns that exist in the dataframe
    existing_columns = set(df.columns)
    schema_columns = set(schema.keys())
    applicable_columns = existing_columns & schema_columns

    if not applicable_columns:
        logger.warning(
            f"No schema columns found in dataframe. Available: {list(existing_columns)}"
        )
        return df

    # Apply dtypes to existing columns
    dtype_dict = {col: schema[col] for col in applicable_columns}

    try:
        result = df.astype(dtype_dict)
        logger.debug(
            f"Applied dtypes to {len(applicable_columns)} columns: {list(applicable_columns)}"
        )
        return result
    except Exception as e:
        logger.error(f"Failed to apply dtypes: {e}")
        logger.error("Dataframe info:")
        df.info()
        raise


def assert_no_unexpected_object_columns(
    df: pd.DataFrame, allowed: Optional[Set[str]] = None, context: str = "dataframe"
) -> None:
    """
    Assert that no unexpected object columns exist in the dataframe.

    Args:
        df: Dataframe to check
        allowed: Set of column names allowed to be object dtype
        context: Context string for error messages

    Raises:
        AssertionError: If unexpected object columns are found
    """
    if df.empty:
        return

    allowed_set = allowed or ALLOWED_OBJECT_COLUMNS

    # Find object columns
    object_columns = df.select_dtypes(include=["object"]).columns.tolist()

    # Filter out allowed columns
    unexpected_columns = [col for col in object_columns if col not in allowed_set]

    if unexpected_columns:
        error_msg = (
            f"Unexpected object columns found in {context}: {unexpected_columns}\n"
            f"Allowed object columns: {list(allowed_set)}\n"
            f"Dataframe columns: {list(df.columns)}"
        )
        logger.error(error_msg)
        raise AssertionError(error_msg)

    logger.debug(
        f"Object column validation passed for {context}. "
        f"Object columns: {object_columns}"
    )


def drop_intermediate_columns(
    df: pd.DataFrame, context: str = "dataframe"
) -> pd.DataFrame:
    """
    Drop intermediate columns that are not needed downstream.

    Args:
        df: Input dataframe
        context: Context string for logging

    Returns:
        DataFrame with intermediate columns removed
    """
    if df.empty:
        return df

    existing_columns = set(df.columns)
    columns_to_drop = existing_columns & INTERMEDIATE_COLUMNS_TO_DROP

    if columns_to_drop:
        result = df.drop(columns=list(columns_to_drop))
        logger.debug(
            f"Dropped {len(columns_to_drop)} intermediate columns from {context}: {list(columns_to_drop)}"
        )
        return result

    return df


def optimize_dataframe_memory(
    df: pd.DataFrame, context: str = "dataframe", verbose: bool = True
) -> pd.DataFrame:
    """
    Apply comprehensive memory optimization to dataframe.

    Args:
        df: Input dataframe
        context: Context string for logging

    Returns:
        Memory-optimized dataframe
    """
    if df.empty:
        return df

    initial_memory = df.memory_usage(deep=True).sum() / 1024 / 1024  # MB

    # Drop intermediate columns
    df = drop_intermediate_columns(df, context)

    # Apply dtypes based on schema detection
    schema = _detect_schema(df)
    if schema:
        df = apply_dtypes(df, schema)

    # Validate object columns
    assert_no_unexpected_object_columns(df, context=context)

    final_memory = df.memory_usage(deep=True).sum() / 1024 / 1024  # MB
    memory_saved = initial_memory - final_memory
    reduction_percent = memory_saved/initial_memory*100 if initial_memory > 0 else 0

    # Only log if verbose or if significant memory was saved
    if verbose or memory_saved > 0.1 or reduction_percent > 1.0:
        logger.info(
            f"Memory optimization for {context}: {initial_memory:.1f}MB → {final_memory:.1f}MB "
            f"({memory_saved:.1f}MB saved, {reduction_percent:.1f}% reduction)"
        )

    return df


def _detect_schema(df: pd.DataFrame) -> Optional[Dict[str, str]]:
    """
    Detect which schema to apply based on dataframe columns.

    Args:
        df: Input dataframe

    Returns:
        Schema dict or None if no match
    """
    columns = set(df.columns)

    # Detect schema based on key columns
    if ACCOUNT_ID in columns and NAME_CORE in columns:
        if GROUP_ID in columns and DISPOSITION in columns:
            return get_dtypes_for_schema("review_ready")
        elif GROUP_ID in columns:
            return get_dtypes_for_schema("groups")
        elif SCORE in columns:
            return get_dtypes_for_schema("pairs")
        else:
            return get_dtypes_for_schema("accounts")

    return None


def get_dtypes_for_schema(schema_name: str) -> Dict[str, str]:
    """
    Get dtype mapping for specific pipeline schema.

    Args:
        schema_name: Name of the schema (e.g., 'accounts', 'pairs', 'groups')

    Returns:
        Dict mapping column names to dtypes
    """
    try:
        from src.dtypes_map import get_dtypes_for_schema as _get_dtypes
    except ImportError:
        from src.dtypes_map import get_dtypes_for_schema as _get_dtypes
    return _get_dtypes(schema_name)
