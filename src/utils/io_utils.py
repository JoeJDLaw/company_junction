"""IO utilities for robust CSV reading and schema inference."""

import functools
import logging
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, Optional, cast

import pandas as pd
import yaml

from src.utils.logging_utils import get_logger
from src.utils.path_utils import get_config_path

logger = get_logger(__name__)

# Settings loading counter for debugging
_settings_load_count = 0


@functools.lru_cache(maxsize=1)
def load_settings(path: str) -> dict[str, Any]:
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
            "supported_formats": [".csv", ".xlsx", ".xls", ".json", ".xml"],
            "output_pattern": "cleaned_{object_type}_{timestamp}.csv",
        },
        "similarity": {
            "high": 92,
            "medium": 84,
            "penalty": {"suffix_mismatch": 25, "num_style_mismatch": 5},
            "max_alias_pairs": 100000,
            "scoring": {"use_bulk_cdist": True, "gate_cutoff": 72},
        },
        "llm": {"enabled": False, "delete_threshold": 85},
        "survivorship": {"tie_breakers": ["created_date", "account_id"]},
        "io": {"interim_format": "parquet"},
        "group_stats": {"backend": "duckdb"},
        "pipeline": {
            "exact_equals_first_pass": {"enable": True}
        },
        "alias": {"optimize": True},
        "salesforce": {
            "object_types": ["Account", "Contact", "Lead", "Opportunity"],
            "batch_size": 200,
            "max_retries": 3,
            "retry_delay": 5,
        },
        "ingest": {
            "name_synonyms": ["account name","name","company","company name","legal name","organization","org name"],
            "id_synonyms": ["account id","id","sfid","external id","uuid","guid","record id"],
            "use_input_disposition": False
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
        with open(path) as f:
            user_config = yaml.safe_load(f) or {}

        # Deep merge user config over defaults
        def deep_merge(base: dict[str, Any], update: dict[str, Any]) -> dict[str, Any]:
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
        logging.exception(f"Error loading settings: {e}. Using defaults.")
        return DEFAULTS


def reload_settings(path: str) -> dict[str, Any]:
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


def load_relationship_ranks(path: str) -> dict[str, int]:
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
        logging.exception(f"Error loading relationship ranks: {e}")
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
    directory: str,
    extensions: Optional[list[str]] = None,
) -> list[str]:
    """List data files in a directory.

    Args:
        directory: Directory to search
        extensions: List of file extensions to include (e.g., ['.csv', '.xlsx'])

    Returns:
        List of file paths

    """
    if extensions is None:
        extensions = [".csv", ".xlsx", ".xls", ".json", ".xml"]

    files: list[Path] = []
    for ext in extensions:
        files.extend(Path(directory).glob(f"*{ext}"))

    return [str(f) for f in files]


def _read_csv_typed(
    path: Path,
    *,
    dtype: Optional[Mapping[str, str]] = None,
    engine: str,
    low_memory: bool,
    na_values: Sequence[str],
    keep_default_na: bool,
    nrows: Optional[int] = None,
) -> pd.DataFrame:
    """Narrow, keyword-only wrapper around pandas.read_csv to make mypy's
    overload resolution unambiguous. **No behavioral change.**
    """
    return cast(
        "pd.DataFrame",
        pd.read_csv(  # type: ignore[call-overload]
            path,
            dtype=dtype,
            engine=engine,
            low_memory=low_memory,
            na_values=list(na_values),
            keep_default_na=keep_default_na,
            nrows=nrows,
        ),
    )


def infer_csv_schema(file_path: str, sample_rows: int = 20000) -> dict[str, str]:
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
        sample_df = _read_csv_typed(
            file_path_obj,
            nrows=sample_rows,
            low_memory=False,
            engine="c",
            na_values=["", "NA", "NaN", "null", "None"],
            keep_default_na=True,
        )
    except Exception as e:
        logger.warning(f"CSV reading failed, using c engine: {e}")
        sample_df = _read_csv_typed(
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
    dtype_map: Optional[dict[str, str]] = None,
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

    # At this point, dtype_map is guaranteed to be Dict[str, str]
    assert dtype_map is not None

    logger.info(
        f"Reading {file_path} with engine={engine}, dtypes={len(dtype_map)} columns",
    )

    # Read with stable dtypes (PyArrow removed)
    try:
        df = _read_csv_typed(
            file_path_obj,
            dtype=dtype_map,
            engine=engine,
            low_memory=False,
            na_values=["", "NA", "NaN", "null", "None"],
            keep_default_na=True,
        )
    except Exception as e:
        logger.warning(f"Engine {engine} failed, falling back to c engine: {e}")
        df = _read_csv_typed(
            file_path_obj,
            dtype=dtype_map,
            engine="c",
            low_memory=False,
            na_values=["", "NA", "NaN", "null", "None"],
            keep_default_na=True,
        )

    logger.info(f"Loaded {len(df)} rows, {len(df.columns)} columns from {file_path}")
    return df


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
    return bool(str_data.str.contains(r"\.").any())


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


def detect_file_format(path: str) -> str:
    """Detect file format based on extension and basic content sniffing.
    
    Args:
        path: File path to analyze
        
    Returns:
        File format string: 'csv', 'xlsx', 'xls', 'json', 'xml', or 'unsupported'
    """
    from pathlib import Path
    
    p = Path(path)
    suffix = p.suffix.lower()
    
    if suffix == ".csv":
        return "csv"
    if suffix == ".xlsx":
        return "xlsx"
    if suffix == ".xls":
        return "xls"
    if suffix == ".json":
        return "json"
    if suffix == ".xml":
        return "xml"
    
    # Light content sniff: if no extension but starts like CSV header
    try:
        with open(p, "rb") as f:
            head = f.read(64).lower()
        if b"," in head and b"account_name" in head:
            return "csv"
        if head.startswith(b"<?xml") or head.startswith(b"<"):
            return "xml"
        if head.startswith(b"{"):
            return "json"
    except Exception:
        pass
    
    return "unsupported"


def read_input_file(
    path: str, 
    *, 
    col_overrides: dict[str, str] | None = None,
    json_record_path: str | None = None,
    xml_record_path: str | None = None,
    sheet: str | None = None,
    add_source_path: bool = False,  # NEW
) -> pd.DataFrame:
    """Read input file with robust format detection and ordinal tracking.
    
    Args:
        path: Path to input file
        col_overrides: Optional column name mapping (old_name -> new_name)
        json_record_path: Optional JSON record path for JSON files
        xml_record_path: Optional XML record XPath for XML files
        sheet: Optional Excel sheet name or index
        add_source_path: If True, JSON/XML rows will include per-row __source_path
        
    Returns:
        DataFrame with source ordinal tracking and normalized columns (and __source_path for JSON/XML if requested)
        
    Raises:
        ValueError: If file format is unsupported or required columns are missing
        ImportError: If required engine dependencies are missing
    """
    import importlib.util
    
    fmt = detect_file_format(path)
    if fmt == "unsupported":
        raise ValueError(f"Unsupported file format for: {path}")

    if fmt == "csv":
        df = pd.read_csv(path, dtype=str)
        # Add source ordinal for CSV (1-based line index) - P1 Fix: cast to int32
        df["__source_row_ordinal"] = pd.Series(range(1, len(df) + 1), dtype="int32")
    elif fmt == "xlsx":
        if importlib.util.find_spec("openpyxl") is None:
            raise ImportError("openpyxl required to read .xlsx files")
        df = pd.read_excel(path, dtype=str, engine="openpyxl", sheet_name=sheet)
        # Add source ordinal for Excel (1-based row index) - P1 Fix: cast to int32
        df["__source_row_ordinal"] = pd.Series(range(1, len(df) + 1), dtype="int32")
    elif fmt == "xls":
        if importlib.util.find_spec("xlrd") is None:
            raise ImportError("xlrd required to read .xls files")
        df = pd.read_excel(path, dtype=str, engine="xlrd", sheet_name=sheet)
        # Add source ordinal for Excel (1-based row index) - P1 Fix: cast to int32
        df["__source_row_ordinal"] = pd.Series(range(1, len(df) + 1), dtype="int32")
    elif fmt == "json":
        df = parse_json_to_dataframe(path, json_record_path, add_source_path=add_source_path)
    elif fmt == "xml":
        df = parse_xml_to_dataframe(path, xml_record_path, add_source_path=add_source_path)
    else:
        raise ValueError(f"Unsupported file format: {fmt}")

    if col_overrides:
        df = df.rename(columns=col_overrides)

    # Note: Column validation is handled by schema resolution in the pipeline
    # This function focuses on file I/O and basic normalization

    # Normalize created_date to YYYY-MM-DD string format
    if "created_date" not in df.columns:
        df["created_date"] = "1970-01-01"
    else:
        # Parse dates and normalize to YYYY-MM-DD string format
        dt = pd.to_datetime(df["created_date"], errors="coerce")
        dt = dt.fillna(pd.Timestamp("1970-01-01"))
        df["created_date"] = dt.dt.strftime("%Y-%m-%d")

    return df


def parse_json_to_dataframe(
    path: str, 
    record_path: str | None = None,
    add_source_path: bool = False
) -> pd.DataFrame:
    """Parse JSON file to DataFrame with ordinal tracking and optional per-row source paths."""
    import json
    from pathlib import Path
    
    # P1 Fix: Guard optional deps for JSON/XML
    try:
        from jsonpath_ng import parse
    except ImportError as e:
        raise ImportError("--json-record-path requires jsonpath-ng. Install with `pip install jsonpath-ng`.") from e
    
    with open(path, 'r', encoding='utf-8') as f:
        doc = json.load(f)
    
    if not record_path:
        raise ValueError("record_path is required for JSON parsing")
    
    expr = parse(record_path)
    matches = list(expr.find(doc))
    
    rows = []
    for i, m in enumerate(matches, start=1):
        # Flatten the matched value
        rec = pd.json_normalize(m.value, max_level=1)
        rec["__source_row_ordinal"] = pd.Series([i], dtype="int32")
        
        if add_source_path:
            # Try to capture the specific, per-row path. Fallback to the selector.
            path_str = None
            try:
                # jsonpath-ng may expose full path on recent versions
                path_str = str(getattr(m, "full_path", None) or m.path)
            except Exception:
                path_str = record_path  # safe fallback
            
            # Include file name for extra clarity
            rec["__source_path"] = f"{Path(path).name}:{path_str}"
        
        rows.append(rec)
    
    if not rows:
        columns = ["__source_row_ordinal"]
        if add_source_path:
            columns.append("__source_path")
        return pd.DataFrame(columns=columns)
    
    df = pd.concat(rows, ignore_index=True)
    
    # Coerce passthroughs to string for safety
    for c in df.columns:
        if c not in ("__source_row_ordinal", "__source_path"):
            df[c] = df[c].astype("string")
    
    return df


def parse_xml_to_dataframe(
    path: str, 
    record_xpath: str | None = None,
    add_source_path: bool = False
) -> pd.DataFrame:
    """Parse XML file to DataFrame with ordinal tracking and optional per-row source paths."""
    from pathlib import Path
    
    # P1 Fix: Guard optional deps for JSON/XML
    try:
        from lxml import etree
    except ImportError as e:
        raise ImportError("--xml-record-path requires lxml. Install with `pip install lxml`.") from e
    
    if not record_xpath:
        raise ValueError("XML parsing requires --xml-record-path")
    
    parser = etree.XMLParser(recover=True)
    tree = etree.parse(path, parser)
    nodes = tree.xpath(record_xpath)
    
    rows = []
    for i, node in enumerate(nodes, start=1):
        # Extract immediate children to columns
        rec_dict = {}
        for child in node:
            rec_dict[child.tag] = child.text or ""
        
        rec = pd.DataFrame([rec_dict])
        rec["__source_row_ordinal"] = pd.Series([i], dtype="int32")
        
        if add_source_path:
            try:
                abs_xpath = tree.getpath(node)  # absolute, index-inclusive
            except Exception:
                abs_xpath = record_xpath  # safe fallback
            
            rec["__source_path"] = f"{Path(path).name}:{abs_xpath}"
        
        rows.append(rec)
    
    if not rows:
        columns = ["__source_row_ordinal"]
        if add_source_path:
            columns.append("__source_path")
        return pd.DataFrame(columns=columns)
    
    df = pd.concat(rows, ignore_index=True)
    
    # Coerce passthroughs to string for safety
    for c in df.columns:
        if c not in ("__source_row_ordinal", "__source_path"):
            df[c] = df[c].astype("string")
    
    return df


def _require_pyarrow():
    """Require pyarrow to be available for parquet operations."""
    import importlib.util
    if importlib.util.find_spec("pyarrow") is None:
        raise ImportError("pyarrow required for parquet IO")




def write_parquet_safely(df: pd.DataFrame, path: str) -> None:
    """Write DataFrame to parquet file with safety checks.
    
    Args:
        df: DataFrame to write
        path: Output file path
        
    Raises:
        ImportError: If pyarrow is not available
    """
    _require_pyarrow()
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)  # pandas will default to pyarrow engine


def read_parquet_safely(path: str) -> pd.DataFrame:
    """Read parquet file with safety checks.
    
    Args:
        path: Input file path
        
    Returns:
        DataFrame from parquet file
        
    Raises:
        ImportError: If pyarrow is not available
    """
    _require_pyarrow()
    return pd.read_parquet(path)
