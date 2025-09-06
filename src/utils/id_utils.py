"""Salesforce ID canonicalization utilities.

This module provides functions to convert Salesforce 15-character IDs to their
18-character canonical form using Salesforce's standard algorithm.
"""

import logging
import re
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)

# Salesforce's base32 alphabet for checksum calculation
_BASE32 = "ABCDEFGHIJKLMNOPQRSTUVWXYZ012345"


def _chunk_checksum(chunk: str) -> str:
    """Calculate checksum for a 5-character chunk of a Salesforce ID.

    Args:
        chunk: 5-character string from the 15-char ID

    Returns:
        Single character checksum from _BASE32 alphabet

    Raises:
        ValueError: If chunk is not exactly 5 characters

    """
    if len(chunk) != 5:
        raise ValueError(f"Chunk must be exactly 5 characters, got {len(chunk)}")

    bits = 0
    for i, c in enumerate(chunk):
        if "A" <= c <= "Z":
            bits |= 1 << i

    return _BASE32[bits]


def sfid15_to_18(sfid15: str) -> str:
    """Convert a 15-character Salesforce ID to its 18-character canonical form.

    Args:
        sfid15: 15-character Salesforce ID (case-sensitive)

    Returns:
        18-character canonical Salesforce ID

    Raises:
        TypeError: If input is not a string
        ValueError: If input is not exactly 15 alphanumeric characters

    """
    if not isinstance(sfid15, str):
        raise TypeError(f"sfid15 must be a string, got {type(sfid15)}")

    if len(sfid15) != 15:
        raise ValueError(f"sfid15 must be exactly 15 characters, got {len(sfid15)}")

    if not re.fullmatch(r"[A-Za-z0-9]{15}", sfid15):
        raise ValueError(f"sfid15 must contain only alphanumeric characters: {sfid15}")

    # Calculate 3-character suffix from 5-character chunks
    suffix = "".join(_chunk_checksum(sfid15[i : i + 5]) for i in range(0, 15, 5))

    return sfid15 + suffix


def normalize_sfid_series(series: pd.Series) -> pd.Series:
    """Normalize a pandas Series of Salesforce IDs to canonical 18-character form.

    Args:
        series: Pandas Series containing Salesforce IDs

    Returns:
        Series with all IDs converted to 18-character canonical form

    Raises:
        ValueError: If any ID is not 15 or 18 characters, or contains invalid characters

    """
    if series.empty:
        return series

    # Convert to string, handle NaN/None
    s = series.astype("string").fillna("").str.strip()

    # Filter out empty strings (from NaN values)
    non_empty = s != ""
    s_filtered = s[non_empty]

    if s_filtered.empty:
        return s  # Return original series if all values were empty

    # Identify 15-char and 18-char IDs on the filtered series
    is18 = s_filtered.str.len() == 18
    is15 = s_filtered.str.len() == 15

    # Create output series
    out = s.copy()

    # Convert 15-char IDs to 18-char
    if is15.any():
        logger.info(
            f"Converting {is15.sum()} 15-character IDs to 18-character canonical form",
        )
        # Use the filtered series indices directly to avoid index mismatch
        out.loc[s_filtered[is15].index] = s_filtered[is15].map(sfid15_to_18)

    # Pass through 18-char IDs unchanged
    if is18.any():
        logger.info(f"Passing through {is18.sum()} 18-character IDs unchanged")

    # Check for invalid IDs (only for non-empty values)
    bad_count = (~(is15 | is18)).sum()
    if bad_count > 0:
        sample = s_filtered[~(is15 | is18)].head(5).tolist()
        raise ValueError(
            f"Found {bad_count} non 15/18-char Salesforce IDs. Sample: {sample}",
        )

    return out


def validate_sfid_format(sfid: str | Any) -> bool:
    """Validate that a string is a valid Salesforce ID format.

    Args:
        sfid: String to validate

    Returns:
        True if valid 15 or 18 character Salesforce ID, False otherwise

    """
    if not isinstance(sfid, str):
        return False

    if len(sfid) not in (15, 18):
        return False

    return bool(re.fullmatch(r"[A-Za-z0-9]{15,18}", sfid))
