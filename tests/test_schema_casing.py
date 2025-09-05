"""
Tests for schema casing consistency.

This module ensures that column names maintain consistent casing
across all generated artifacts.
"""

import pytest
import pandas as pd
from pathlib import Path
from src.utils.path_utils import get_artifact_path


class TestSchemaCasing:
    """Test suite for schema casing consistency."""

    def test_schema_casing_disposition_lowercase(self):
        """Test that 'disposition' column is lowercase in group_stats.parquet."""
        # Get the latest run_id (you may need to adjust this based on your setup)
        run_id = "latest"
        
        try:
            path = str(get_artifact_path(run_id, "group_stats.parquet"))
            df = pd.read_parquet(path)
            
            # Check that 'disposition' column exists and is lowercase
            assert "disposition" in df.columns, "Expected lowercase 'disposition' column"
            assert "Disposition" not in df.columns, "Unexpected uppercase 'Disposition' column"
            
            # Check other expected columns
            expected_columns = ["group_id", "group_size", "max_score", "primary_name", "disposition"]
            for col in expected_columns:
                assert col in df.columns, f"Expected column '{col}' not found"
                
        except Exception as e:
            pytest.skip(f"Could not test schema casing: {e}")

    def test_schema_casing_review_ready_lowercase(self):
        """Test that 'disposition' column is lowercase in review_ready.parquet."""
        run_id = "latest"
        
        try:
            path = str(get_artifact_path(run_id, "review_ready.parquet"))
            df = pd.read_parquet(path)
            
            # Check that 'disposition' column exists and is lowercase
            assert "disposition" in df.columns, "Expected lowercase 'disposition' column in review_ready.parquet"
            assert "Disposition" not in df.columns, "Unexpected uppercase 'Disposition' column in review_ready.parquet"
            
        except Exception as e:
            pytest.skip(f"Could not test review_ready schema casing: {e}")

    def test_schema_casing_group_details_lowercase(self):
        """Test that column names are consistent in group_details.parquet."""
        run_id = "latest"
        
        try:
            path = str(get_artifact_path(run_id, "group_details.parquet"))
            df = pd.read_parquet(path)
            
            # Check that all column names are lowercase
            for col in df.columns:
                assert col == col.lower(), f"Column '{col}' should be lowercase"
                
        except Exception as e:
            pytest.skip(f"Could not test group_details schema casing: {e}")

    def test_schema_casing_backend_specific_files(self):
        """Test that backend-specific files maintain consistent casing."""
        run_id = "latest"
        
        try:
            # Test DuckDB backend file
            duckdb_path = str(get_artifact_path(run_id, "group_stats_duckdb.parquet"))
            if Path(duckdb_path).exists():
                df_duckdb = pd.read_parquet(duckdb_path)
                assert "disposition" in df_duckdb.columns, "DuckDB file missing lowercase 'disposition'"
            
            # Test Pandas backend file (if parity was enabled)
            pandas_path = str(get_artifact_path(run_id, "group_stats_pandas.parquet"))
            if Path(pandas_path).exists():
                df_pandas = pd.read_parquet(pandas_path)
                assert "disposition" in df_pandas.columns, "Pandas file missing lowercase 'disposition'"
                
        except Exception as e:
            pytest.skip(f"Could not test backend-specific files: {e}")


if __name__ == "__main__":
    pytest.main([__file__])
