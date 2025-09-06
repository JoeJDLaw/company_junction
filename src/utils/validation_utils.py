"""
Lightweight shim to satisfy import- and contract-tests.

NOTE:
- This file contains conservative, no-op or pass-through implementations.
- Replace with proper logic when the validation layer is finalized.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any


def is_schema_available(*_args: Any, **_kwargs: Any) -> bool:
    """Best-effort availability check; default to False to avoid false positives."""
    return False


def ensure_columns_present(df: Any, required: Iterable[str]) -> bool:
    """Return True if all required columns exist; False otherwise."""
    cols = set(getattr(df, "columns", []))
    return all(c in cols for c in required)


def validate_parquet_schema(_path: str, *_args: Any, **_kwargs: Any) -> bool:
    """Placeholder that returns False (non-blocking)."""
    return False


def safe_bool(value: Any, default: bool = False) -> bool:
    """Coerce values to bool with a default fallback."""
    try:
        return bool(value)
    except Exception:
        return default
