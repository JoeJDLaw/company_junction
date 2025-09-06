"""Optional dependencies management for ui_helpers refactor.

This module centralizes capability checks for optional dependencies
like duckdb, pyarrow, and streamlit.
"""

import importlib
from typing import Any, Optional


def try_import(module: str) -> Optional[Any]:
    """Try to import a module, return None if unavailable."""
    try:
        return importlib.import_module(module)
    except ImportError:
        return None


def try_import_many(modules: dict[str, str]) -> dict[str, Optional[Any]]:
    """Try to import multiple modules, return dict of results."""
    result = {}
    for alias, module_name in modules.items():
        result[alias] = try_import(module_name)
    return result


# Centralized capability checks
DUCKDB = try_import("duckdb")
PYARROW = try_import_many(
    {"pc": "pyarrow.compute", "ds": "pyarrow.dataset", "pq": "pyarrow.parquet"},
)
STREAMLIT = try_import("streamlit")
PROMETHEUS = try_import("prometheus_client")

# Export handles & flags
DUCKDB_AVAILABLE = DUCKDB is not None
PC, DS, PQ = PYARROW.get("pc"), PYARROW.get("ds"), PYARROW.get("pq")
PYARROW_AVAILABLE = all(PYARROW.values())
STREAMLIT_AVAILABLE = STREAMLIT is not None
PROMETHEUS_AVAILABLE = PROMETHEUS is not None
