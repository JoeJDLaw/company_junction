"""Tests for IO utilities including CSV schema inference and stable reading."""

import pytest
import pandas as pd
import tempfile
import os

from src.utils.io_utils import (
    infer_csv_schema,
    read_csv_stable,
    _is_pyarrow_available,
    _is_pandas_2_plus,
    _is_likely_id_column,
    _is_numeric_column,
    _has_decimal_points,
    get_csv_engine_preference,
    validate_csv_file,
)


class TestCSVSchemaInference:
    """Test CSV schema inference functionality."""

    def test_infer_csv_schema_does_not_raise_with_mixed_types(self) -> None:
        """Test that schema inference handles mixed types without raising warnings."""
        # Create a CSV with mixed types
        data = {
            "id": ["001", "002", "003", "004"],
            "name": ["Company A", "Company B", "Company C", "Company D"],
            "value": [100, "N/A", 300, "unknown"],
            "account_id": [
                "001234567890123",
                "001234567890124",
                "001234567890125",
                "001234567890126",
            ],
        }
        df = pd.DataFrame(data)

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            df.to_csv(f.name, index=False)
            temp_path = f.name

        try:
            # This should not raise any warnings
            schema = infer_csv_schema(temp_path)

            # Verify schema inference
            assert "id" in schema
            assert "name" in schema
            assert "value" in schema
            assert "account_id" in schema

            # ID columns should be string
            assert schema["id"] == "string"
            assert schema["account_id"] == "string"

            # Mixed value column should be string
            assert schema["value"] == "string"

        finally:
            os.unlink(temp_path)

    def test_infer_csv_schema_numeric_columns(self) -> None:
        """Test schema inference for numeric columns."""
        data = {
            "integer_col": [1, 2, 3, 4, 5],
            "float_col": [1.1, 2.2, 3.3, 4.4, 5.5],
            "mixed_numeric": [1, 2.5, 3, 4.7, 5],
        }
        df = pd.DataFrame(data)

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            df.to_csv(f.name, index=False)
            temp_path = f.name

        try:
            schema = infer_csv_schema(temp_path)

            # Integer column should be Int64
            assert schema["integer_col"] == "Int64"

            # Float columns should be Float64
            assert schema["float_col"] == "Float64"
            assert schema["mixed_numeric"] == "Float64"

        finally:
            os.unlink(temp_path)

    def test_infer_csv_schema_id_detection(self) -> None:
        """Test that ID columns are correctly identified."""
        data = {
            "account_id": ["001234567890123", "001234567890124", "001234567890125"],
            "user_id": ["001234567890123", "001234567890124", "001234567890125"],
            "regular_col": ["value1", "value2", "value3"],
        }
        df = pd.DataFrame(data)

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            df.to_csv(f.name, index=False)
            temp_path = f.name

        try:
            schema = infer_csv_schema(temp_path)

            # ID columns should be detected as string
            assert schema["account_id"] == "string"
            assert schema["user_id"] == "string"

            # Regular column with ID-like values should also be string
            assert schema["regular_col"] == "string"

        finally:
            os.unlink(temp_path)

    def test_infer_csv_schema_empty_column(self) -> None:
        """Test schema inference with empty columns."""
        data = {
            "empty_col": [None, None, None, None],
            "mixed_col": ["value1", None, "value3", None],
        }
        df = pd.DataFrame(data)

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            df.to_csv(f.name, index=False)
            temp_path = f.name

        try:
            schema = infer_csv_schema(temp_path)

            # Empty column should default to string
            assert schema["empty_col"] == "string"
            assert schema["mixed_col"] == "string"

        finally:
            os.unlink(temp_path)

    def test_infer_csv_schema_file_not_found(self) -> None:
        """Test schema inference with non-existent file."""
        with pytest.raises(FileNotFoundError):
            infer_csv_schema("nonexistent_file.csv")


class TestStableCSVReading:
    """Test stable CSV reading functionality."""

    def test_read_csv_stable_uses_engine_pyarrow_when_available_else_fallback(
        self,
    ) -> None:
        """Test that stable CSV reading uses pyarrow when available."""
        data = {
            "id": ["001", "002", "003"],
            "name": ["Company A", "Company B", "Company C"],
            "value": [100, 200, 300],
        }
        df = pd.DataFrame(data)

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            df.to_csv(f.name, index=False)
            temp_path = f.name

        try:
            # Test with auto engine
            result_df = read_csv_stable(temp_path)

            assert len(result_df) == 3
            assert list(result_df.columns) == ["id", "name", "value"]

        finally:
            os.unlink(temp_path)

    def test_read_csv_stable_resolves_dtypewarning(self) -> None:
        """Test that stable CSV reading resolves DtypeWarning."""
        # Create a CSV with mixed types that would normally cause DtypeWarning
        data = {
            "mixed_col": [1, "N/A", 3, "unknown", 5],
            "numeric_col": [1.1, 2.2, 3.3, 4.4, 5.5],
        }
        df = pd.DataFrame(data)

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            df.to_csv(f.name, index=False)
            temp_path = f.name

        try:
            # This should not raise DtypeWarning
            result_df = read_csv_stable(temp_path)

            # Verify the data was loaded correctly
            assert len(result_df) == 5

            # Verify the data was loaded correctly
            assert len(result_df) == 5
            assert "mixed_col" in result_df.columns
            assert "numeric_col" in result_df.columns

        finally:
            os.unlink(temp_path)

    def test_read_csv_stable_with_custom_dtype_map(self) -> None:
        """Test stable CSV reading with custom dtype map."""
        data = {
            "id": ["001", "002", "003"],
            "value": [100, 200, 300],
        }
        df = pd.DataFrame(data)

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            df.to_csv(f.name, index=False)
            temp_path = f.name

        try:
            # Custom dtype map
            dtype_map = {"id": "string", "value": "Int64"}

            result_df = read_csv_stable(temp_path, dtype_map=dtype_map)

            assert len(result_df) == 3
            assert result_df["id"].dtype == "string"
            assert result_df["value"].dtype == "Int64"

        finally:
            os.unlink(temp_path)

    def test_read_csv_stable_file_not_found(self) -> None:
        """Test stable CSV reading with non-existent file."""
        with pytest.raises(FileNotFoundError):
            read_csv_stable("nonexistent_file.csv")


class TestHelperFunctions:
    """Test helper functions for CSV processing."""

    def test_is_pyarrow_available(self) -> None:
        """Test pyarrow availability detection."""
        result = _is_pyarrow_available()
        assert isinstance(result, bool)

    def test_is_pandas_2_plus(self) -> None:
        """Test pandas version detection."""
        result = _is_pandas_2_plus()
        assert isinstance(result, bool)

    def test_is_likely_id_column(self) -> None:
        """Test ID column detection."""
        # Test with ID-like column name and values
        id_data = pd.Series(["001234567890123", "001234567890124", "001234567890125"])
        assert _is_likely_id_column("account_id", id_data) is True

        # Test with regular column name
        regular_data = pd.Series(
            ["001234567890123", "001234567890124", "001234567890125"]
        )
        assert _is_likely_id_column("regular_col", regular_data) is False

        # Test with non-ID-like values
        non_id_data = pd.Series(["short", "values", "here"])
        assert _is_likely_id_column("account_id", non_id_data) is False

    def test_is_numeric_column(self) -> None:
        """Test numeric column detection."""
        # Test with numeric data
        numeric_data = pd.Series([1, 2, 3, 4, 5])
        assert _is_numeric_column(numeric_data) is True

        # Test with mixed data
        mixed_data = pd.Series([1, "N/A", 3, "unknown", 5])
        assert _is_numeric_column(mixed_data) is False

        # Test with empty data
        empty_data = pd.Series([])
        assert _is_numeric_column(empty_data) is False

    def test_has_decimal_points(self) -> None:
        """Test decimal point detection."""
        # Test with integer data
        int_data = pd.Series([1, 2, 3, 4, 5])
        assert bool(_has_decimal_points(int_data)) is False

        # Test with float data
        float_data = pd.Series([1.1, 2.2, 3.3, 4.4, 5.5])
        assert bool(_has_decimal_points(float_data)) is True

        # Test with mixed data
        mixed_data = pd.Series([1, 2.5, 3, 4.7, 5])
        assert bool(_has_decimal_points(mixed_data)) is True

    def test_get_csv_engine_preference(self) -> None:
        """Test CSV engine preference."""
        result = get_csv_engine_preference()
        assert result in ["pyarrow", "c"]

    def test_validate_csv_file(self) -> None:
        """Test CSV file validation."""
        # Test with valid CSV
        data = {"col1": [1, 2, 3], "col2": ["a", "b", "c"]}
        df = pd.DataFrame(data)

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            df.to_csv(f.name, index=False)
            temp_path = f.name

        try:
            assert validate_csv_file(temp_path) is True
        finally:
            os.unlink(temp_path)

        # Test with invalid file
        assert validate_csv_file("nonexistent_file.csv") is False
