"""Engine selection utilities for Company Junction.

This module provides centralized engine selection logic for choosing between
pandas and DuckDB backends based on configuration, data characteristics, and
availability.

Engine Selection Logic:
- If requested is "pandas" → choose pandas
- If requested is "duckdb" → choose DuckDB if available and safe
- If requested is "auto" → prefer DuckDB when available, safe, and above threshold
- Always log the decision with detailed reasoning

Safety Checks:
- DuckDB import availability
- Object dtype compatibility (DuckDB can handle object dtypes in many cases)
- Row count thresholds
- Configuration overrides
"""

import logging
from typing import Any, Optional

import pandas as pd

from .opt_deps import DUCKDB

logger = logging.getLogger(__name__)


def choose_backend(
    stage: str,
    settings: dict[str, Any],
    n_rows: int,
    df: Optional[pd.DataFrame] = None,
) -> str:
    """Choose the appropriate backend for a processing stage.

    Args:
        stage: Name of the processing stage (e.g., "filtering", "exact_equals")
        settings: Configuration settings dictionary
        n_rows: Number of rows in the dataset
        df: Optional DataFrame for dtype analysis

    Returns:
        Selected backend name ("pandas" or "duckdb")

    """
    # Get requested backend from configuration
    requested = (
        settings.get("engines", {})
        .get(stage, "auto")
        .lower()
    )

    # Check DuckDB availability
    try:
        import duckdb  # noqa: F401
        duckdb_ok = True
        duckdb_reason = "duckdb_import_ok"
    except Exception as e:
        duckdb_ok = False
        duckdb_reason = f"duckdb_import_failed:{type(e).__name__}"

    # Get threshold configuration
    threshold = int(
        settings.get("engines", {})
        .get("duckdb_threshold_rows", 50000)
    )

    # Analyze data characteristics
    has_object_cols = False
    if df is not None:
        has_object_cols = bool((df.dtypes == "object").any())

    # Build decision factors
    size_reason = f"n_rows={n_rows} threshold={threshold}"
    obj_reason = f"object_cols={has_object_cols}"

    # Make decision based on requested backend
    if requested == "pandas":
        chosen = "pandas"
        reason = f"chosen=pandas | requested=pandas | {duckdb_reason} | {size_reason} | {obj_reason}"
    elif requested == "duckdb":
        if duckdb_ok:
            chosen = "duckdb"
            reason = f"chosen=duckdb | requested=duckdb | {duckdb_reason} | {size_reason} | {obj_reason}"
        else:
            chosen = "pandas"
            reason = f"chosen=pandas | requested=duckdb | {duckdb_reason} | {size_reason} | {obj_reason} | fallback=import_failed"
    else:  # auto
        if duckdb_ok and n_rows >= threshold:
            chosen = "duckdb"
            reason = f"chosen=duckdb | requested=auto | {duckdb_reason} | {size_reason} | {obj_reason} | auto_selected=above_threshold"
        else:
            chosen = "pandas"
            if not duckdb_ok:
                reason = f"chosen=pandas | requested=auto | {duckdb_reason} | {size_reason} | {obj_reason} | auto_selected=import_failed"
            elif n_rows < threshold:
                reason = f"chosen=pandas | requested=auto | {duckdb_reason} | {size_reason} | {obj_reason} | auto_selected=below_threshold"
            else:
                reason = f"chosen=pandas | requested=auto | {duckdb_reason} | {size_reason} | {obj_reason} | auto_selected=fallback"

    # Log the decision
    logger.info(f"engine_selection | stage={stage} | {reason}")

    return chosen


def get_engine_config(stage: str, settings: dict[str, Any]) -> dict[str, Any]:
    """Get engine configuration for a specific stage.

    Args:
        stage: Name of the processing stage
        settings: Configuration settings dictionary

    Returns:
        Engine configuration dictionary

    """
    return settings.get("engines", {}).get(stage, {})


def is_duckdb_available() -> bool:
    """Check if DuckDB is available for use.

    Returns:
        True if DuckDB can be imported, False otherwise

    """
    return DUCKDB is not None


def get_duckdb_threshold(settings: dict[str, Any]) -> int:
    """Get the DuckDB threshold for automatic backend selection.

    Args:
        settings: Configuration settings dictionary

    Returns:
        Row count threshold for DuckDB selection

    """
    return int(
        settings.get("engines", {})
        .get("duckdb_threshold_rows", 50000)
    )


def filtering_duckdb(df_norm: pd.DataFrame, settings: dict[str, Any]) -> pd.DataFrame:
    """DuckDB implementation for filtering stage (stub).
    
    Args:
        df_norm: Input DataFrame to filter
        settings: Configuration settings
        
    Returns:
        Filtered DataFrame
        
    """
    # TODO: Implement DuckDB-based filtering
    # This would use DuckDB SQL for filtering operations
    logger.warning("filtering_duckdb | not_implemented | falling_back_to_pandas")
    return df_norm


def exact_equals_duckdb(
    df: pd.DataFrame, 
    settings: dict[str, Any], 
    name_column: str
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """DuckDB implementation for exact equals stage (stub).
    
    Args:
        df: Input DataFrame
        settings: Configuration settings
        name_column: Column name for account names
        
    Returns:
        Tuple of (exact_raw_groups, raw_exact_map, candidate_pairs_exact_raw)
        
    """
    # TODO: Implement DuckDB-based exact equals
    # This would use DuckDB SQL for grouping and pair generation
    logger.warning("exact_equals_duckdb | not_implemented | falling_back_to_pandas")
    
    # Return empty DataFrames as placeholders
    return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
