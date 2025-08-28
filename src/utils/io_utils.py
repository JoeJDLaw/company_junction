"""
I/O utilities for the company junction pipeline.
"""

import logging
import yaml
import pandas as pd
from pathlib import Path
from typing import List, Optional, Dict, Any


def get_file_info(file_path: str) -> dict:
    """
    Get information about a data file.

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
    """
    List data files in a directory.

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


def load_settings(path: str) -> Dict[str, Any]:
    """
    Load settings from YAML file with defaults.

    Args:
        path: Path to settings YAML file

    Returns:
        Dictionary with settings (user config merged over defaults)
    """
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
            "raw_data": "data/raw",
            "interim_data": "data/interim",
            "processed_data": "data/processed",
            "test_fixtures": "tests/fixtures",
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


def load_relationship_ranks(path: str) -> Dict[str, int]:
    """
    Load relationship ranks from CSV file.

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
