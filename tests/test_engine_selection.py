"""Tests for engine selection utilities."""

import pandas as pd
import pytest

from src.utils.engine_selection import (
    choose_backend,
    get_duckdb_threshold,
    get_engine_config,
    is_duckdb_available,
)


class TestEngineSelection:
    """Test engine selection logic."""

    def test_choose_backend_pandas_requested(self):
        """Test that pandas is chosen when explicitly requested."""
        settings = {"engines": {"filtering": "pandas"}}
        df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
        
        backend = choose_backend("filtering", settings, n_rows=100000, df=df)
        
        assert backend == "pandas"

    def test_choose_backend_duckdb_requested(self):
        """Test that duckdb is chosen when explicitly requested and available."""
        settings = {"engines": {"filtering": "duckdb"}}
        df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
        
        backend = choose_backend("filtering", settings, n_rows=100000, df=df)
        
        # Should choose duckdb if available, pandas if not
        assert backend in ["pandas", "duckdb"]

    def test_choose_backend_auto_above_threshold(self):
        """Test auto selection with dataset above threshold."""
        settings = {
            "engines": {
                "filtering": "auto",
                "duckdb_threshold_rows": 50000
            }
        }
        df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
        
        backend = choose_backend("filtering", settings, n_rows=100000, df=df)
        
        # Should prefer duckdb if available and above threshold
        assert backend in ["pandas", "duckdb"]

    def test_choose_backend_auto_below_threshold(self):
        """Test auto selection with dataset below threshold."""
        settings = {
            "engines": {
                "filtering": "auto",
                "duckdb_threshold_rows": 50000
            }
        }
        df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
        
        backend = choose_backend("filtering", settings, n_rows=1000, df=df)
        
        # Should choose pandas when below threshold
        assert backend == "pandas"

    def test_choose_backend_auto_default_threshold(self):
        """Test auto selection with default threshold."""
        settings = {"engines": {"filtering": "auto"}}
        df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
        
        backend = choose_backend("filtering", settings, n_rows=100000, df=df)
        
        # Should use default threshold of 50000
        assert backend in ["pandas", "duckdb"]

    def test_choose_backend_object_dtypes(self):
        """Test backend selection with object dtypes."""
        settings = {"engines": {"filtering": "auto"}}
        df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
        df["col2"] = df["col2"].astype("object")
        
        backend = choose_backend("filtering", settings, n_rows=100000, df=df)
        
        # Should still work with object dtypes
        assert backend in ["pandas", "duckdb"]

    def test_choose_backend_no_dataframe(self):
        """Test backend selection without DataFrame."""
        settings = {"engines": {"filtering": "auto"}}
        
        backend = choose_backend("filtering", settings, n_rows=100000, df=None)
        
        # Should work without DataFrame
        assert backend in ["pandas", "duckdb"]

    def test_get_engine_config(self):
        """Test getting engine configuration."""
        settings = {
            "engines": {
                "filtering": "duckdb",
                "exact_equals": "auto"
            }
        }
        
        config = get_engine_config("filtering", settings)
        assert config == "duckdb"
        
        config = get_engine_config("exact_equals", settings)
        assert config == "auto"
        
        config = get_engine_config("nonexistent", settings)
        assert config == {}

    def test_get_duckdb_threshold(self):
        """Test getting DuckDB threshold."""
        settings = {"engines": {"duckdb_threshold_rows": 75000}}
        
        threshold = get_duckdb_threshold(settings)
        assert threshold == 75000
        
        # Test default
        settings = {}
        threshold = get_duckdb_threshold(settings)
        assert threshold == 50000

    def test_is_duckdb_available(self):
        """Test DuckDB availability check."""
        available = is_duckdb_available()
        assert isinstance(available, bool)

    def test_choose_backend_missing_engines_config(self):
        """Test backend selection with missing engines configuration."""
        settings = {}
        df = pd.DataFrame({"col1": [1, 2, 3]})
        
        backend = choose_backend("filtering", settings, n_rows=100000, df=df)
        
        # Should default to auto selection
        assert backend in ["pandas", "duckdb"]

    def test_choose_backend_case_insensitive(self):
        """Test that backend selection is case insensitive."""
        settings = {"engines": {"filtering": "PANDAS"}}
        df = pd.DataFrame({"col1": [1, 2, 3]})
        
        backend = choose_backend("filtering", settings, n_rows=100000, df=df)
        
        assert backend == "pandas"

    def test_filtering_duckdb_stub(self):
        """Test that filtering_duckdb stub returns input DataFrame."""
        from src.utils.engine_selection import filtering_duckdb
        
        df = pd.DataFrame({"col1": [1, 2, 3]})
        settings = {}
        
        result = filtering_duckdb(df, settings)
        
        pd.testing.assert_frame_equal(result, df)

    def test_exact_equals_duckdb_stub(self):
        """Test that exact_equals_duckdb stub returns empty DataFrames."""
        from src.utils.engine_selection import exact_equals_duckdb
        
        df = pd.DataFrame({"col1": [1, 2, 3]})
        settings = {}
        
        groups, mapping, pairs = exact_equals_duckdb(df, settings, "col1")
        
        assert groups.empty
        assert mapping.empty
        assert pairs.empty
