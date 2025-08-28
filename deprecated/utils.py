"""
Utility functions for the Company Junction pipeline.

This module contains helper functions for:
- File path management
- Data validation
- Logging setup
- Common data operations
"""

import os
import logging
from pathlib import Path
from typing import Optional, List, Dict
import pandas as pd


def setup_logging(level: str = "INFO") -> None:
    """
    Configure logging for the pipeline.
    
    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR)
    """
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('pipeline.log')
        ]
    )


def get_project_root() -> Path:
    """
    Get the project root directory.
    
    Returns:
        Path to the project root
    """
    return Path(__file__).parent.parent


def ensure_directory_exists(directory_path: str) -> None:
    """
    Create directory if it doesn't exist.
    
    Args:
        directory_path: Path to the directory to create
    """
    Path(directory_path).mkdir(parents=True, exist_ok=True)


def get_data_paths() -> dict:
    """
    Get standard data directory paths.
    
    Returns:
        Dictionary containing paths to raw, interim, and processed data directories
    """
    project_root = get_project_root()
    return {
        'raw': project_root / 'data' / 'raw',
        'interim': project_root / 'data' / 'interim',
        'processed': project_root / 'data' / 'processed'
    }


def validate_dataframe(df: pd.DataFrame, required_columns: List[str]) -> bool:
    """
    Validate that DataFrame contains required columns.
    
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
        return {'error': 'File not found'}
    
    return {
        'name': path.name,
        'size': path.stat().st_size,
        'extension': path.suffix,
        'modified': path.stat().st_mtime
    }


def list_data_files(directory: str, extensions: Optional[List[str]] = None) -> List[str]:
    """
    List data files in a directory.
    
    Args:
        directory: Directory to search
        extensions: List of file extensions to include (e.g., ['.csv', '.xlsx'])
        
    Returns:
        List of file paths
    """
    if extensions is None:
        extensions = ['.csv', '.xlsx', '.xls']
    
    files = []
    for ext in extensions:
        files.extend(Path(directory).glob(f"*{ext}"))
    
    return [str(f) for f in files]


def load_settings(path: str) -> dict:
    """
    Load settings from YAML file with defaults.
    
    Args:
        path: Path to settings YAML file
        
    Returns:
        Dictionary with settings (user config merged over defaults)
    """
    import yaml
    
    # Default settings
    DEFAULTS = {
        "data": {
            "name_column": "Account Name",
            "supported_formats": [".csv", ".xlsx", ".xls"],
            "output_pattern": "cleaned_{object_type}_{timestamp}.csv"
        },
        "similarity": {
            "high": 92,
            "medium": 84,
            "penalty": {
                "suffix_mismatch": 25,
                "num_style_mismatch": 5
            }
        },
        "llm": {
            "enabled": False,
            "delete_threshold": 85
        },
        "survivorship": {
            "tie_breakers": ["created_date", "account_id"]
        },
        "io": {
            "interim_format": "parquet"
        },
        "salesforce": {
            "object_types": ["Account", "Contact", "Lead", "Opportunity"],
            "batch_size": 200,
            "max_retries": 3,
            "retry_delay": 5
        },
        "logging": {
            "level": "INFO",
            "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            "file": "pipeline.log"
        },
        "paths": {
            "raw_data": "data/raw",
            "interim_data": "data/interim",
            "processed_data": "data/processed",
            "test_fixtures": "tests/fixtures"
        }
    }
    
    try:
        with open(path, 'r') as f:
            user_config = yaml.safe_load(f) or {}
        
        # Deep merge user config over defaults
        def deep_merge(base, update):
            for key, value in update.items():
                if key in base and isinstance(base[key], dict) and isinstance(value, dict):
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
        return dict(zip(df['Relationship'], df['Rank']))
    except Exception as e:
        logging.error(f"Error loading relationship ranks: {e}")
        return {}
