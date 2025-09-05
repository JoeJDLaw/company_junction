"""Validation utilities for the company junction pipeline.
"""

import logging
from typing import List

import pandas as pd


def validate_dataframe(df: pd.DataFrame, required_columns: List[str]) -> bool:
    """Validate that DataFrame contains required columns.

    Args:
        df: DataFrame to validate
        required_columns: List of column names that must be present

    Returns:
        True if validation passes, False otherwise

    """
    missing_columns = set(required_columns) - set(df.columns)
    if missing_columns:
        logging.error(f"Missing required columns: {missing_columns}")
        return False
    return True
