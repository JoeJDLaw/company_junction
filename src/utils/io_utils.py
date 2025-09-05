"""IO utilities for robust CSV reading and schema inference."""

import functools
import logging
import yaml
import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional, Any

from src.utils.logging_utils import get_logger
from src.utils.path_utils import get_config_path

logger = get_logger(__name__)

# Settings loading counter for debugging
_settings_load_count = 0


@functools.lru_cache(maxsize=1)
def load_settings(path: str) -> Dict[str, Any]:
    """Load settings from YAML file with defaults.

    This function is cached to prevent repeated file I/O and parsing.
    Use reload_settings() to force a fresh load.

    Args:
        path: Path to settings YAML file

    Returns:
        Dictionary with settings (user config merged over defaults)
    """
    global _settings_load_count
    _settings_load_count += 1

    logger.debug(f"Settings loaded (count: {_settings_load_count}) from {path}")

    # Default settings
    DEFAULTS = {
        "data": {
            "name_column": "Account Name",
            "supported_formats": [".csv", ".xlsx", ".xls"],
            "output_pattern": "cleaned_{object_type}_{timestamp}.csv",
        },
        "similarity": {
            "high": 92,
            "medium": 84,
            "penalty": {"suffix_mismatch": 25, "num_style_mismatch": 5},
        },
        "llm": {"enabled": False, "delete_threshold": 85},
        "survivorship": {"tie_breakers": ["created_date", "account_id"]},
        "io": {"interim_format": "parquet"},
        "salesforce": {
            "object_types": ["Account", "Contact", "Lead", "Opportunity"],
            "batch_size": 200,
            "max_retries": 3,
            "retry_delay": 5,
        },
        "logging": {
            "level": "INFO",
            "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            "file": "pipeline.log",
        },
        "paths": {
            "raw_data": str(get_config_path().parent / "data" / "raw"),
            "interim_data": str(get_config_path().parent / "data" / "interim"),
            "processed_data": str(get_config_path().parent / "data" / "processed"),
            "test_fixtures": str(get_config_path().parent / "tests" / "fixtures"),
        },
        "csv": {
            "engine": "auto",
            "sample_rows": 20000,
            "force_string_cols": ["account_id", "parent_account_id"],
        },
    }

    try:
        with open(path, "r") as f:
            user_config = yaml.safe_load(f) or {}

        # Deep merge user config over defaults
        def deep_merge(base: Dict[str, Any], update: Dict[str, Any]) -> Dict[str, Any]:
            for key, value in update.items():
                if (
                    key in base
                    and isinstance(base[key], dict)
                    and isinstance(value, dict)
                ):
                    deep_merge(base[key], value)
                else:
                    base[key] = value
            return base

        return deep_merge(DEFAULTS.copy(), user_config)

    except FileNotFoundError:
        logging.warning(f"Settings file not found: {path}. Using defaults.")
        return DEFAULTS
    except Exception as e:
        logging.error(f"Error loading settings: {e}. Using defaults.")
        return DEFAULTS


def reload_settings(path: str) -> Dict[str, Any]:
    """Force reload settings from file (clears cache).

    Args:
        path: Path to settings YAML file

    Returns:
        Freshly loaded settings
    """
    load_settings.cache_clear()
    return load_settings(path)


def get_settings_load_count() -> int:
    """Get the total number of times settings have been loaded.

    Returns:
        Count of settings loads (for debugging)
    """
    return _settings_load_count


def load_relationship_ranks(path: str) -> Dict[str, int]:
    """Load relationship ranks from CSV file.

    Args:
        path: Path to relationship ranks CSV file

    Returns:
        Dictionary mapping relationship names to ranks (lower is better)
    """
    try:
        df = pd.read_csv(path)
        return dict(zip(df["Relationship"], df["Rank"]))
    except Exception as e:
        logging.error(f"Error loading relationship ranks: {e}")
        return {}


def get_file_info(file_path: str) -> dict:
    """Get information about a data file.

    Args:
        file_path: Path to the file

    Returns:
        Dictionary containing file information
    """
    path = Path(file_path)
    if not path.exists():
        return {"error": "File not found"}

    return {
        "name": path.name,
        "size": path.stat().st_size,
        "extension": path.suffix,
        "modified": path.stat().st_mtime,
    }


def list_data_files(
    directory: str, extensions: Optional[List[str]] = None
) -> List[str]:
    """List data files in a directory.

    Args:
        directory: Directory to search
        extensions: List of file extensions to include (e.g., ['.csv', '.xlsx'])

    Returns:
        List of file paths
    """
    if extensions is None:
        extensions = [".csv", ".xlsx", ".xls"]

    files: List[Path] = []
    for ext in extensions:
        files.extend(Path(directory).glob(f"*{ext}"))

    return [str(f) for f in files]


def infer_csv_schema(file_path: str, sample_rows: int = 20000) -> Dict[str, str]:
    """Infer a stable CSV schema by analyzing a sample of the data.

    Args:
        file_path: Path to CSV file
        sample_rows: Number of rows to sample for schema inference

    Returns:
        Dictionary mapping column names to pandas dtypes
    """
    file_path_obj = Path(file_path)
    if not file_path_obj.exists():
        raise FileNotFoundError(f"CSV file not found: {file_path}")

    logger.info(f"Inferring schema for {file_path} using {sample_rows} sample rows")

    # Read sample with low_memory=False to avoid dtype warnings during inference
    try:
        # Use c engine for consistent performance (PyArrow removed)
        sample_df = pd.read_csv(
            file_path_obj,
            nrows=sample_rows,
            low_memory=False,
            engine="c",
            na_values=["", "NA", "NaN", "null", "None"],
            keep_default_na=True,
        )
    except Exception as e:
        logger.warning(f"CSV reading failed, using c engine: {e}")
        sample_df = pd.read_csv(
            file_path_obj,
            nrows=sample_rows,
            low_memory=False,
            engine="c",
            na_values=["", "NA", "NaN", "null", "None"],
            keep_default_na=True,
        )

    dtype_map = {}

    for column in sample_df.columns:
        col_data = sample_df[column].dropna()

        if len(col_data) == 0:
            # Empty column, use string
            dtype_map[column] = "string"
            continue

        # Check if likely to be an ID column
        if _is_likely_id_column(column, col_data):
            dtype_map[column] = "string"
            continue

        # Check if all values are numeric
        if _is_numeric_column(col_data):
            # Check for decimal points
            if _has_decimal_points(col_data):
                dtype_map[column] = "Float64"
            else:
                dtype_map[column] = "Int64"
        else:
            # Mixed or non-numeric, use string
            dtype_map[column] = "string"

    logger.info(f"Schema inference complete: {len(dtype_map)} columns")
    for col, dtype in dtype_map.items():
        logger.debug(f"  {col}: {dtype}")

    return dtype_map


def read_csv_stable(
    file_path: str,
    dtype_map: Optional[Dict[str, str]] = None,
    engine: Optional[str] = None,
) -> pd.DataFrame:
    """Read CSV file with stable dtypes to avoid mixed-type warnings.

    Args:
        file_path: Path to CSV file
        dtype_map: Optional dtype mapping (inferred if None)
        engine: Optional engine preference (auto if None)

    Returns:
        DataFrame with stable dtypes
    """
    file_path_obj = Path(file_path)
    if not file_path_obj.exists():
        raise FileNotFoundError(f"CSV file not found: {file_path}")

    # Determine engine (PyArrow removed)
    if engine is None or engine == "auto":
        engine = "c"

    # Infer schema if not provided
    if dtype_map is None:
        dtype_map = infer_csv_schema(str(file_path))

    logger.info(
        f"Reading {file_path} with engine={engine}, dtypes={len(dtype_map)} columns"
    )

    # Read with stable dtypes (PyArrow removed)
    try:
        df = pd.read_csv(
            file_path_obj,
            dtype=dtype_map,
            engine=engine,
            low_memory=False,
            na_values=["", "NA", "NaN", "null", "None"],
            keep_default_na=True,
        )
    except Exception as e:
        logger.warning(f"Engine {engine} failed, falling back to c engine: {e}")
        df = pd.read_csv(
            file_path_obj,
            dtype=dtype_map,
            engine="c",
            low_memory=False,
            na_values=["", "NA", "NaN", "null", "None"],
            keep_default_na=True,
        )

    logger.info(f"Loaded {len(df)} rows, {len(df.columns)} columns from {file_path}")
    return df  # type: ignore[no-any-return]


def _is_pyarrow_available() -> bool:
    """Check if pyarrow is available for pandas."""
    try:
        import importlib.util

        return importlib.util.find_spec("pyarrow") is not None
    except ImportError:
        return False


def _is_pandas_2_plus() -> bool:
    """Check if pandas version is 2.0 or higher."""
    return pd.__version__ >= "2.0.0"


def _is_likely_id_column(column_name: str, data: pd.Series) -> bool:
    """Check if a column is likely to be an ID column."""
    # Check column name patterns
    id_patterns = ["id", "ID", "Id", "_id", "_ID"]
    if any(pattern in column_name.lower() for pattern in id_patterns):
        # Check if values look like IDs (consistent length, alphanumeric)
        if len(data) > 0:
            sample_values = data.head(100).astype(str)
            # Check for consistent length (common ID lengths)
            lengths = sample_values.str.len()
            if lengths.nunique() == 1:
                length = lengths.iloc[0]
                # Accept common ID lengths: 1-3 digits, 15, 18, or other reasonable lengths
                if length <= 3 or length in [15, 18] or (length <= 20):
                    # Check if alphanumeric or just digits (common for test IDs)
                    if (
                        sample_values.str.match(r"^[a-zA-Z0-9]+$").all()
                        or sample_values.str.match(r"^\d+$").all()
                    ):
                        return True
    return False


def _is_numeric_column(data: pd.Series) -> bool:
    """Check if a column contains only numeric data."""
    if len(data) == 0:
        return False

    # Try to convert to numeric
    try:
        pd.to_numeric(data, errors="raise")
        return True
    except (ValueError, TypeError):
        return False


def _has_decimal_points(data: pd.Series) -> bool:
    """Check if numeric data contains decimal points."""
    if len(data) == 0:
        return False

    # Convert to string and check for decimal points
    str_data = data.astype(str)
    return str_data.str.contains(r"\.").any()


def get_csv_engine_preference() -> str:
    """Get the preferred CSV engine based on available libraries."""
    # PyArrow removed, using c engine for consistency
    return "c"


def validate_csv_file(file_path: str) -> bool:
    """Validate that a CSV file can be read without errors.

    Args:
        file_path: Path to CSV file

    Returns:
        True if file is valid, False otherwise
    """
    try:
        # Try to read just the header
        pd.read_csv(file_path, nrows=0)
        return True
    except Exception as e:
        logger.error(f"CSV validation failed for {file_path}: {e}")
        return False
