"""Data contract tests for parquet schema validation.

This module ensures that required columns exist in parquet files
and fail fast on schema drift to prevent runtime errors.
"""

import ast
import os
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Set

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

# Required columns for each parquet type
REQUIRED_COLUMNS = {
    "review_ready_parquet": {
        "group_id",
        "account_name",
        "is_primary",
        "weakest_edge_to_primary",
        "disposition",
    },
    "group_stats_parquet": {
        "group_id",
        "group_size",
        "max_score",
        "primary_name",
        "disposition",
    },
    "group_details_parquet": {
        "group_id",
        "account_id",
        "account_name",
        "suffix_class",
        "created_date",
        "disposition",
    },
}

# Optional columns that may be present
OPTIONAL_COLUMNS = {
    "review_ready_parquet": {
        "domain",
        "email",
        "phone",
        "address",
        "website",
        "industry",
        "revenue",
        "employee_count",
        "founded_year",
    },
    "group_stats_parquet": {"min_score", "avg_score", "std_score", "count"},
    "group_details_parquet": {
        "is_primary",
        "weakest_edge_to_primary",
        "domain",
        "email",
        "phone",
        "address",
        "website",
        "industry",
        "revenue",
        "employee_count",
        "founded_year",
    },
}


def validate_parquet_schema(file_path: str, parquet_type: str) -> Dict[str, Any]:
    """Validate that a parquet file has the required schema.

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
            "schema": {},
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
            "error": (
                None if is_valid else f"Missing required columns: {missing_columns}"
            ),
            "missing_columns": missing_columns,
            "extra_columns": extra_columns,
            "schema": {name: str(schema.field(name).type) for name in actual_columns},
            "required_columns": required,
            "optional_columns": optional,
            "actual_columns": actual_columns,
        }

    except Exception as e:
        return {
            "valid": False,
            "error": f"Failed to read schema: {e}",
            "missing_columns": set(),
            "extra_columns": set(),
            "schema": {},
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
        for parquet_type in REQUIRED_COLUMNS:
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
            "weakest_edge_to_primary": "double",
        }

        for parquet_type in REQUIRED_COLUMNS:
            if parquet_type in artifact_paths:
                file_path = artifact_paths[parquet_type]
                result = validate_parquet_schema(file_path, parquet_type)

                assert result["valid"], f"Schema validation failed for {parquet_type}"

                for col_name, expected_type in expected_types.items():
                    if col_name in result["schema"]:
                        actual_type = result["schema"][col_name]
                        # Allow some flexibility in type matching
                        if expected_type == "string":
                            assert (
                                "string" in actual_type.lower()
                            ), f"Column {col_name} in {parquet_type} should be string, got {actual_type}"
                        elif expected_type == "bool":
                            assert (
                                "bool" in actual_type.lower()
                            ), f"Column {col_name} in {parquet_type} should be bool, got {actual_type}"
                        elif expected_type == "int64":
                            assert (
                                "int" in actual_type.lower()
                            ), f"Column {col_name} in {parquet_type} should be int, got {actual_type}"
                        elif expected_type == "double":
                            assert (
                                "double" in actual_type.lower()
                                or "float" in actual_type.lower()
                            ), f"Column {col_name} in {parquet_type} should be double/float, got {actual_type}"

    def test_no_extra_required_columns(self, artifact_paths):
        """Test that we don't have unexpected required columns."""
        # This ensures our contract is not too strict
        for parquet_type in REQUIRED_COLUMNS:
            if parquet_type in artifact_paths:
                file_path = artifact_paths[parquet_type]
                result = validate_parquet_schema(file_path, parquet_type)

                assert result["valid"], f"Schema validation failed for {parquet_type}"

                # Check that we don't have too many extra columns
                # (This is a soft check - some extra columns are OK)
                extra_columns = result["extra_columns"]
                if len(extra_columns) > 10:  # Arbitrary threshold
                    pytest.warns(
                        UserWarning,
                        match=f"Many extra columns in {parquet_type}: {extra_columns}",
                    )

    def test_contract_evolution(self, artifact_paths):
        """Test that contracts can evolve safely."""
        # This test documents how to safely evolve contracts
        for parquet_type in REQUIRED_COLUMNS:
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
                    "optional": result["optional_columns"],
                }

                # This could be saved to a file for contract evolution tracking
                print(f"Schema info for {parquet_type}: {schema_info}")


class TestHardcodedColumnAssumptions:
    """Test that hardcoded column assumptions are detected and flagged."""

    def test_detect_hardcoded_column_lists(self):
        """Test that hardcoded column lists in code are detected."""
        # Known problematic patterns
        problematic_patterns = [
            # Pattern: [GROUP_ID, ACCOUNT_NAME, IS_PRIMARY, WEAKEST_EDGE_TO_PRIMARY, DISPOSITION]
            r"\[.*GROUP_ID.*ACCOUNT_NAME.*IS_PRIMARY.*WEAKEST_EDGE_TO_PRIMARY.*DISPOSITION.*\]",
            # Pattern: SELECT with hardcoded columns
            r"SELECT.*GROUP_ID.*ACCOUNT_NAME.*IS_PRIMARY.*WEAKEST_EDGE_TO_PRIMARY.*DISPOSITION",
            # Pattern: columns= with hardcoded lists
            r"columns=.*\[.*GROUP_ID.*ACCOUNT_NAME.*IS_PRIMARY.*WEAKEST_EDGE_TO_PRIMARY.*DISPOSITION.*\]",
        ]

        # Files to check
        src_dir = Path(__file__).parent.parent.parent / "src"
        utils_dir = src_dir / "utils"

        found_issues = []

        for py_file in utils_dir.rglob("*.py"):
            try:
                with open(py_file) as f:
                    content = f.read()

                for pattern in problematic_patterns:
                    import re

                    if re.search(pattern, content, re.IGNORECASE):
                        found_issues.append(
                            f"Hardcoded column assumption in {py_file}: {pattern}",
                        )

            except Exception:
                # Skip files that can't be read
                continue

        # Report findings
        if found_issues:
            pytest.fail(
                f"Found {len(found_issues)} hardcoded column assumptions:\n"
                + "\n".join(found_issues)
                + "\n\nThese should be replaced with dynamic column detection.",
            )

    def test_detect_schema_constants_usage(self):
        """Test that schema constants are used consistently."""
        # Check that DETAILS_COLUMNS and similar constants are defined and used
        src_dir = Path(__file__).parent.parent.parent / "src"
        utils_dir = src_dir / "utils"

        schema_files = []
        for py_file in utils_dir.rglob("*.py"):
            try:
                with open(py_file) as f:
                    content = f.read()

                # Look for column constant definitions
                if "DETAILS_COLUMNS" in content or "GROUP_STATS_COLUMNS" in content:
                    schema_files.append(py_file)

            except Exception:
                continue

        # Ensure we have schema constant definitions
        assert len(schema_files) > 0, "No schema constant definitions found in utils/"

        # Check that constants are used consistently
        for schema_file in schema_files:
            try:
                with open(schema_file) as f:
                    content = f.read()

                # Look for hardcoded column lists that should use constants
                if (
                    "[" in content
                    and "GROUP_ID" in content
                    and "ACCOUNT_NAME" in content
                ):
                    # This is a potential hardcoded list - check if it's in a constant definition
                    lines = content.split("\n")
                    for i, line in enumerate(lines):
                        if (
                            "[" in line
                            and "GROUP_ID" in line
                            and "ACCOUNT_NAME" in line
                        ):
                            # Check if this line defines a constant
                            if "=" in line and (
                                "DETAILS_COLUMNS" in line
                                or "GROUP_STATS_COLUMNS" in line
                            ):
                                continue  # This is a constant definition, which is OK
                            pytest.fail(
                                f"Potential hardcoded column list in {schema_file}:{i+1}\n"
                                f"Line: {line.strip()}\n"
                                f"Consider using a schema constant instead.",
                            )

            except Exception:
                continue

    def test_schema_constants_match_contracts(self):
        """Test that schema constants in code match contract test expectations."""
        # Import the actual constants from the code
        try:
            from src.utils.group_details import DETAILS_COLUMNS
            from src.utils.schema_utils import (
                ACCOUNT_ID,
                ACCOUNT_NAME,
                CREATED_DATE,
                DISPOSITION,
                GROUP_ID,
                SUFFIX_CLASS,
            )

            # Check that DETAILS_COLUMNS matches the expected schema
            expected_details_columns = {
                GROUP_ID,
                ACCOUNT_ID,
                ACCOUNT_NAME,
                SUFFIX_CLASS,
                CREATED_DATE,
                DISPOSITION,
            }
            actual_details_columns = set(DETAILS_COLUMNS)

            assert actual_details_columns == expected_details_columns, (
                f"DETAILS_COLUMNS mismatch:\n"
                f"Expected: {expected_details_columns}\n"
                f"Actual: {actual_details_columns}\n"
                f"This should match the group_details_parquet contract."
            )

        except ImportError as e:
            pytest.skip(f"Could not import schema constants: {e}")

        # Check that the contract test expectations match
        contract_details_columns = REQUIRED_COLUMNS["group_details_parquet"]
        assert actual_details_columns == contract_details_columns, (
            f"Contract test mismatch for group_details_parquet:\n"
            f"Code constants: {actual_details_columns}\n"
            f"Contract test: {contract_details_columns}\n"
            f"These should be identical."
        )


class TestContractEvolution:
    """Test contract evolution scenarios."""

    def test_add_optional_column(self):
        """Test adding an optional column doesn't break contracts."""
        # This would test that adding optional columns is safe

    def test_remove_optional_column(self):
        """Test removing optional columns is safe."""
        # This would test that removing optional columns doesn't break anything

    def test_rename_column_breaking(self):
        """Test that renaming required columns breaks contracts."""
        # This would test that renaming required columns is caught

    def test_change_column_type_breaking(self):
        """Test that changing column types breaks contracts."""
        # This would test that type changes are caught


class TestLegacyColumnHandling:
    """Test that both backends handle missing legacy columns gracefully."""

    @pytest.fixture
    def sample_data_with_legacy_columns(self):
        """Create sample data with legacy columns."""
        return pa.table(
            {
                "group_id": ["group1", "group2", "group3"],
                "account_id": ["acc1", "acc2", "acc3"],
                "account_name": ["Company A", "Company B", "Company C"],
                "suffix_class": ["INC", "LLC", "CORP"],
                "created_date": ["2023-01-01", "2023-01-02", "2023-01-03"],
                "disposition": ["Keep", "Update", "Delete"],
                "is_primary": [True, False, False],
                "weakest_edge_to_primary": [0.95, 0.85, 0.75],
            },
        )

    @pytest.fixture
    def sample_data_without_legacy_columns(self):
        """Create sample data without legacy columns."""
        return pa.table(
            {
                "group_id": ["group1", "group2", "group3"],
                "account_id": ["acc1", "acc2", "acc3"],
                "account_name": ["Company A", "Company B", "Company C"],
                "suffix_class": ["INC", "LLC", "CORP"],
                "created_date": ["2023-01-01", "2023-01-02", "2023-01-03"],
                "disposition": ["Keep", "Update", "Delete"],
            },
        )

    def test_duckdb_handles_missing_legacy_columns(
        self, sample_data_without_legacy_columns,
    ):
        """Test that DuckDB backend handles missing legacy columns gracefully."""
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp_file:
            pq.write_table(sample_data_without_legacy_columns, tmp_file.name)

            try:
                # Test that DuckDB can read the file without crashing
                import duckdb

                conn = duckdb.connect()

                # Test basic query
                result = conn.execute(
                    f"SELECT * FROM read_parquet('{tmp_file.name}')",
                ).fetchall()
                assert len(result) == 3

                # Test filtering by min_edge_strength (should be silently skipped)
                result = conn.execute(
                    f"""
                    SELECT * FROM read_parquet('{tmp_file.name}')
                    WHERE group_id = 'group1'
                """,
                ).fetchall()
                assert len(result) == 1

                conn.close()

            finally:
                os.unlink(tmp_file.name)

    def test_pyarrow_handles_missing_legacy_columns(
        self, sample_data_without_legacy_columns,
    ):
        """Test that PyArrow backend handles missing legacy columns gracefully."""
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp_file:
            pq.write_table(sample_data_without_legacy_columns, tmp_file.name)

            try:
                # Test that PyArrow can read the file without crashing
                table = pq.read_table(tmp_file.name)
                assert table.num_rows == 3

                # Test filtering (should work without legacy columns)
                filtered = table.filter(
                    pa.compute.equal(table["group_id"], pa.scalar("group1")),
                )
                assert filtered.num_rows == 1

            finally:
                os.unlink(tmp_file.name)

    def test_duckdb_handles_present_legacy_columns(
        self, sample_data_with_legacy_columns,
    ):
        """Test that DuckDB backend works with present legacy columns."""
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp_file:
            pq.write_table(sample_data_with_legacy_columns, tmp_file.name)

            try:
                import duckdb

                conn = duckdb.connect()

                # Test filtering by min_edge_strength (should work with legacy columns)
                result = conn.execute(
                    f"""
                    SELECT * FROM read_parquet('{tmp_file.name}')
                    WHERE weakest_edge_to_primary >= 0.8
                """,
                ).fetchall()
                assert len(result) == 3

                # Test filtering by is_primary
                result = conn.execute(
                    f"""
                    SELECT * FROM read_parquet('{tmp_file.name}')
                    WHERE is_primary = true
                """,
                ).fetchall()
                assert len(result) == 1

                conn.close()

            finally:
                os.unlink(tmp_file.name)

    def test_pyarrow_handles_present_legacy_columns(
        self, sample_data_with_legacy_columns,
    ):
        """Test that PyArrow backend works with present legacy columns."""
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp_file:
            pq.write_table(sample_data_with_legacy_columns, tmp_file.name)

            try:
                table = pq.read_table(tmp_file.name)
                assert table.num_rows == 3

                # Test filtering by min_edge_strength (should work with legacy columns)
                filtered = table.filter(
                    pa.compute.greater_equal(
                        table["weakest_edge_to_primary"], pa.scalar(0.8),
                    ),
                )
                assert filtered.num_rows == 3

                # Test filtering by is_primary
                filtered = table.filter(
                    pa.compute.equal(table["is_primary"], pa.scalar(True)),
                )
                assert filtered.num_rows == 1

            finally:
                os.unlink(tmp_file.name)

    def test_conditional_filtering_skips_missing_columns(
        self, sample_data_without_legacy_columns,
    ):
        """Test that conditional filtering silently skips missing columns."""
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp_file:
            pq.write_table(sample_data_without_legacy_columns, tmp_file.name)

            try:
                # Test the conditional filtering logic
                from src.utils.filtering import apply_filters_pyarrow

                # Get available columns
                table = pq.read_table(tmp_file.name)
                available_columns = table.column_names

                # Test filtering with min_edge_strength (should be silently skipped)
                filters = {"min_edge_strength": 0.8}
                filtered = apply_filters_pyarrow(table, filters, available_columns)

                # Should return all rows since filtering was skipped
                assert filtered.num_rows == 3

            finally:
                os.unlink(tmp_file.name)

    def test_conditional_filtering_works_with_present_columns(
        self, sample_data_with_legacy_columns,
    ):
        """Test that conditional filtering works when columns are present."""
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp_file:
            pq.write_table(sample_data_with_legacy_columns, tmp_file.name)

            try:
                # Test the conditional filtering logic
                from src.utils.filtering import apply_filters_pyarrow

                # Get available columns
                table = pq.read_table(tmp_file.name)
                available_columns = table.column_names

                # Test filtering with min_edge_strength (should work with legacy columns)
                filters = {"min_edge_strength": 0.8}
                filtered = apply_filters_pyarrow(table, filters, available_columns)

                # Should return filtered rows
                assert filtered.num_rows == 3  # All rows have score >= 0.8

                # Test with higher threshold
                filters = {"min_edge_strength": 0.9}
                filtered = apply_filters_pyarrow(table, filters, available_columns)

                # Should return fewer rows
                assert filtered.num_rows == 1  # Only one row has score >= 0.9

            finally:
                os.unlink(tmp_file.name)


@pytest.fixture
def artifact_paths() -> Dict[str, str]:
    """Mock artifact paths for testing."""
    # This would be replaced with actual artifact paths in real tests
    return {
        "review_ready_parquet": "/path/to/review_ready.parquet",
        "group_stats_parquet": "/path/to/group_stats.parquet",
        "group_details_parquet": "/path/to/group_details.parquet",
    }
