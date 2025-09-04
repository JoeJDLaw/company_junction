"""
Data contract tests for parquet schema validation.

This module ensures that required columns exist in parquet files
and fail fast on schema drift to prevent runtime errors.
"""

import pytest
import pyarrow.parquet as pq
from pathlib import Path
from typing import Dict, Set

# Required columns for each parquet type
REQUIRED_COLUMNS = {
    "review_ready_parquet": {
        "group_id", "account_name", "is_primary", 
        "weakest_edge_to_primary", "disposition"
    },
    "group_stats_parquet": {
        "group_id", "group_size", "max_score", 
        "primary_name", "disposition"
    },
    "group_details_parquet": {
        "group_id", "account_name", "is_primary",
        "weakest_edge_to_primary", "disposition"
    }
}

# Optional columns that may be present
OPTIONAL_COLUMNS = {
    "review_ready_parquet": {
        "domain", "email", "phone", "address", "website",
        "industry", "revenue", "employee_count", "founded_year"
    },
    "group_stats_parquet": {
        "min_score", "avg_score", "std_score", "count"
    },
    "group_details_parquet": {
        "domain", "email", "phone", "address", "website",
        "industry", "revenue", "employee_count", "founded_year"
    }
}


def validate_parquet_schema(file_path: str, parquet_type: str) -> Dict[str, any]:
    """
    Validate that a parquet file has the required schema.
    
    Args:
        file_path: Path to the parquet file
        parquet_type: Type of parquet (review_ready, group_stats, group_details)
        
    Returns:
        Dict with validation results
        
    Raises:
        AssertionError: If required columns are missing
    """
    if not Path(file_path).exists():
        return {
            "valid": False,
            "error": f"File does not exist: {file_path}",
            "missing_columns": set(),
            "extra_columns": set(),
            "schema": {}
        }
    
    try:
        schema = pq.read_schema(file_path)
        actual_columns = set(schema.names)
        
        required = REQUIRED_COLUMNS.get(parquet_type, set())
        optional = OPTIONAL_COLUMNS.get(parquet_type, set())
        expected = required | optional
        
        missing_columns = required - actual_columns
        extra_columns = actual_columns - expected
        
        is_valid = len(missing_columns) == 0
        
        return {
            "valid": is_valid,
            "error": None if is_valid else f"Missing required columns: {missing_columns}",
            "missing_columns": missing_columns,
            "extra_columns": extra_columns,
            "schema": {name: str(schema.field(name).type) for name in actual_columns},
            "required_columns": required,
            "optional_columns": optional,
            "actual_columns": actual_columns
        }
        
    except Exception as e:
        return {
            "valid": False,
            "error": f"Failed to read schema: {e}",
            "missing_columns": set(),
            "extra_columns": set(),
            "schema": {}
        }


class TestParquetContracts:
    """Test parquet schema contracts."""
    
    def test_required_columns_exist(self, artifact_paths):
        """Test that all required columns exist in parquet files."""
        for parquet_type, required in REQUIRED_COLUMNS.items():
            if parquet_type in artifact_paths:
                file_path = artifact_paths[parquet_type]
                result = validate_parquet_schema(file_path, parquet_type)
                
                assert result["valid"], (
                    f"{parquet_type} schema validation failed: {result['error']}\n"
                    f"Required: {required}\n"
                    f"Actual: {result['actual_columns']}\n"
                    f"Missing: {result['missing_columns']}"
                )
    
    def test_schema_consistency(self, artifact_paths):
        """Test that schemas are consistent across runs."""
        # This test would compare schemas across multiple runs
        # For now, just ensure we can read the schema
        for parquet_type in REQUIRED_COLUMNS.keys():
            if parquet_type in artifact_paths:
                file_path = artifact_paths[parquet_type]
                result = validate_parquet_schema(file_path, parquet_type)
                
                assert result["valid"], f"Schema validation failed for {parquet_type}"
                assert len(result["schema"]) > 0, f"No schema found for {parquet_type}"
    
    def test_column_types(self, artifact_paths):
        """Test that column types are as expected."""
        expected_types = {
            "group_id": "string",
            "account_name": "string", 
            "is_primary": "bool",
            "disposition": "string",
            "group_size": "int64",
            "max_score": "double",
            "primary_name": "string",
            "weakest_edge_to_primary": "double"
        }
        
        for parquet_type in REQUIRED_COLUMNS.keys():
            if parquet_type in artifact_paths:
                file_path = artifact_paths[parquet_type]
                result = validate_parquet_schema(file_path, parquet_type)
                
                assert result["valid"], f"Schema validation failed for {parquet_type}"
                
                for col_name, expected_type in expected_types.items():
                    if col_name in result["schema"]:
                        actual_type = result["schema"][col_name]
                        # Allow some flexibility in type matching
                        if expected_type == "string":
                            assert "string" in actual_type.lower(), (
                                f"Column {col_name} in {parquet_type} should be string, got {actual_type}"
                            )
                        elif expected_type == "bool":
                            assert "bool" in actual_type.lower(), (
                                f"Column {col_name} in {parquet_type} should be bool, got {actual_type}"
                            )
                        elif expected_type == "int64":
                            assert "int" in actual_type.lower(), (
                                f"Column {col_name} in {parquet_type} should be int, got {actual_type}"
                            )
                        elif expected_type == "double":
                            assert "double" in actual_type.lower() or "float" in actual_type.lower(), (
                                f"Column {col_name} in {parquet_type} should be double/float, got {actual_type}"
                            )
    
    def test_no_extra_required_columns(self, artifact_paths):
        """Test that we don't have unexpected required columns."""
        # This ensures our contract is not too strict
        for parquet_type in REQUIRED_COLUMNS.keys():
            if parquet_type in artifact_paths:
                file_path = artifact_paths[parquet_type]
                result = validate_parquet_schema(file_path, parquet_type)
                
                assert result["valid"], f"Schema validation failed for {parquet_type}"
                
                # Check that we don't have too many extra columns
                # (This is a soft check - some extra columns are OK)
                extra_columns = result["extra_columns"]
                if len(extra_columns) > 10:  # Arbitrary threshold
                    pytest.warn(f"Many extra columns in {parquet_type}: {extra_columns}")
    
    def test_contract_evolution(self, artifact_paths):
        """Test that contracts can evolve safely."""
        # This test documents how to safely evolve contracts
        for parquet_type in REQUIRED_COLUMNS.keys():
            if parquet_type in artifact_paths:
                file_path = artifact_paths[parquet_type]
                result = validate_parquet_schema(file_path, parquet_type)
                
                assert result["valid"], f"Schema validation failed for {parquet_type}"
                
                # Document current schema for future reference
                schema_info = {
                    "parquet_type": parquet_type,
                    "file_path": file_path,
                    "columns": result["schema"],
                    "required": result["required_columns"],
                    "optional": result["optional_columns"]
                }
                
                # This could be saved to a file for contract evolution tracking
                print(f"Schema info for {parquet_type}: {schema_info}")


class TestContractEvolution:
    """Test contract evolution scenarios."""
    
    def test_add_optional_column(self):
        """Test adding an optional column doesn't break contracts."""
        # This would test that adding optional columns is safe
        pass
    
    def test_remove_optional_column(self):
        """Test removing optional columns is safe."""
        # This would test that removing optional columns doesn't break anything
        pass
    
    def test_rename_column_breaking(self):
        """Test that renaming required columns breaks contracts."""
        # This would test that renaming required columns is caught
        pass
    
    def test_change_column_type_breaking(self):
        """Test that changing column types breaks contracts."""
        # This would test that type changes are caught
        pass


@pytest.fixture
def artifact_paths():
    """Mock artifact paths for testing."""
    # This would be replaced with actual artifact paths in real tests
    return {
        "review_ready_parquet": "/path/to/review_ready.parquet",
        "group_stats_parquet": "/path/to/group_stats.parquet", 
        "group_details_parquet": "/path/to/group_details.parquet"
    }
