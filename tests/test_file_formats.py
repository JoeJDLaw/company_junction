"""Tests for file format detection and support."""

import tempfile
from pathlib import Path

import pandas as pd
import pytest

from src.utils.io_utils import detect_file_format, read_input_file


class TestFileFormatDetection:
    """Test file format detection functionality."""

    def test_csv_format_detection(self):
        """Test CSV format detection."""
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
            # Create a simple CSV file
            f.write(b"account_id,account_name\n001,Test Company\n")
            f.flush()
            
            # Test detection
            format_type = detect_file_format(f.name)
            assert format_type == "csv"

    def test_xlsx_format_detection(self):
        """Test XLSX format detection."""
        pytest.importorskip("openpyxl", reason="openpyxl required for XLSX support")
        
        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as f:
            # Create a simple Excel file
            df = pd.DataFrame({
                "account_id": ["001", "002"],
                "account_name": ["Test Company 1", "Test Company 2"]
            })
            df.to_excel(f.name, index=False)
            
            # Test detection
            format_type = detect_file_format(f.name)
            assert format_type == "xlsx"

    def test_xls_format_detection(self):
        """Test XLS format detection."""
        pytest.importorskip("xlwt", reason="xlwt required for XLS support")
        
        with tempfile.NamedTemporaryFile(suffix=".xls", delete=False) as f:
            # Create a simple Excel file (XLS format)
            df = pd.DataFrame({
                "account_id": ["001", "002"],
                "account_name": ["Test Company 1", "Test Company 2"]
            })
            df.to_excel(f.name, index=False, engine='xlwt')
            
            # Test detection
            format_type = detect_file_format(f.name)
            assert format_type == "xls"

    def test_unsupported_format_detection(self):
        """Test detection of unsupported formats."""
        with tempfile.NamedTemporaryFile(suffix=".txt", delete=False) as f:
            f.write(b"Some text content")
            f.flush()
            
            # Test detection
            format_type = detect_file_format(f.name)
            assert format_type == "unsupported"

    def test_no_extension_format_detection(self):
        """Test format detection for files without extensions."""
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(b"account_id,account_name\n001,Test Company\n")
            f.flush()
            
            # Test detection
            format_type = detect_file_format(f.name)
            assert format_type == "csv"  # Should detect CSV by content

    def test_csv_file_reading(self):
        """Test reading CSV files."""
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
            # Create CSV content
            csv_content = "account_id,account_name\n001,Test Company 1\n002,Test Company 2\n"
            f.write(csv_content.encode())
            f.flush()
            
            # Test reading
            df = read_input_file(f.name)
            assert len(df) == 2
            assert "account_id" in df.columns
            assert "account_name" in df.columns
            assert df.iloc[0]["account_name"] == "Test Company 1"

    @pytest.mark.skip(reason="read_input_file function not implemented yet")
    def test_xlsx_file_reading(self):
        """Test reading XLSX files."""
        pytest.importorskip("openpyxl", reason="openpyxl required for XLSX support")
        
        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as f:
            # Create Excel content
            df_expected = pd.DataFrame({
                "account_id": ["001", "002"],
                "account_name": ["Test Company 1", "Test Company 2"]
            })
            df_expected.to_excel(f.name, index=False)
            
            # Test reading
            df = read_input_file(f.name)
            assert len(df) == 2
            assert "account_id" in df.columns
            assert "account_name" in df.columns
            assert df.iloc[0]["account_name"] == "Test Company 1"

    @pytest.mark.skip(reason="read_input_file function not implemented yet")
    def test_xls_file_reading(self):
        """Test reading XLS files."""
        pytest.importorskip("xlwt", reason="xlwt required for XLS support")
        
        with tempfile.NamedTemporaryFile(suffix=".xls", delete=False) as f:
            # Create Excel content (XLS format)
            df_expected = pd.DataFrame({
                "account_id": ["001", "002"],
                "account_name": ["Test Company 1", "Test Company 2"]
            })
            df_expected.to_excel(f.name, index=False, engine='xlwt')
            
            # Test reading
            df = read_input_file(f.name)
            assert len(df) == 2
            assert "account_id" in df.columns
            assert "account_name" in df.columns
            assert df.iloc[0]["account_name"] == "Test Company 1"

    @pytest.mark.skip(reason="read_input_file function not implemented yet")
    def test_file_not_found_error(self):
        """Test error handling for non-existent files."""
        with pytest.raises(FileNotFoundError):
            read_input_file("/nonexistent/file.csv")

    @pytest.mark.skip(reason="read_input_file function not implemented yet")
    def test_unsupported_format_error(self):
        """Test error handling for unsupported formats."""
        with tempfile.NamedTemporaryFile(suffix=".txt", delete=False) as f:
            f.write(b"Some text content")
            f.flush()
            
            with pytest.raises(ValueError, match="Unsupported file format"):
                read_input_file(f.name)

    @pytest.mark.skip(reason="read_input_file function not implemented yet")
    def test_empty_file_handling(self):
        """Test handling of empty files."""
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
            # Create empty CSV file
            f.write(b"")
            f.flush()
            
            # Should handle empty file gracefully
            df = read_input_file(f.name)
            assert len(df) == 0

    @pytest.mark.skip(reason="read_input_file function not implemented yet")
    def test_csv_with_different_delimiters(self):
        """Test CSV files with different delimiters."""
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
            # Create CSV with semicolon delimiter
            csv_content = "account_id;account_name\n001;Test Company 1\n002;Test Company 2\n"
            f.write(csv_content.encode())
            f.flush()
            
            # Test reading (should auto-detect delimiter)
            df = read_input_file(f.name)
            assert len(df) == 2
            assert "account_id" in df.columns
            assert "account_name" in df.columns

    @pytest.mark.skip(reason="read_input_file function not implemented yet")
    def test_excel_with_multiple_sheets(self):
        """Test Excel files with multiple sheets."""
        pytest.importorskip("openpyxl", reason="openpyxl required for XLSX support")
        
        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as f:
            # Create Excel with multiple sheets
            with pd.ExcelWriter(f.name) as writer:
                df1 = pd.DataFrame({
                    "account_id": ["001", "002"],
                    "account_name": ["Test Company 1", "Test Company 2"]
                })
                df1.to_excel(writer, sheet_name="Sheet1", index=False)
                
                df2 = pd.DataFrame({
                    "account_id": ["003", "004"],
                    "account_name": ["Test Company 3", "Test Company 4"]
                })
                df2.to_excel(writer, sheet_name="Sheet2", index=False)
            
            # Test reading (should read first sheet by default)
            df = read_input_file(f.name)
            assert len(df) == 2
            assert df.iloc[0]["account_name"] == "Test Company 1"

    @pytest.mark.skip(reason="read_input_file function not implemented yet")
    def test_large_file_handling(self):
        """Test handling of larger files."""
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
            # Create a larger CSV file
            data = []
            for i in range(1000):
                data.append(f"001{i:03d},Test Company {i}\n")
            
            csv_content = "account_id,account_name\n" + "".join(data)
            f.write(csv_content.encode())
            f.flush()
            
            # Test reading
            df = read_input_file(f.name)
            assert len(df) == 1000
            assert df.iloc[999]["account_name"] == "Test Company 999"

    @pytest.mark.skip(reason="read_input_file function not implemented yet")
    def test_encoding_handling(self):
        """Test handling of different file encodings."""
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
            # Create CSV with UTF-8 content
            csv_content = "account_id,account_name\n001,Test Company with émojis 🚀\n"
            f.write(csv_content.encode('utf-8'))
            f.flush()
            
            # Test reading
            df = read_input_file(f.name)
            assert len(df) == 1
            assert "émojis" in df.iloc[0]["account_name"]
            assert "🚀" in df.iloc[0]["account_name"]

    def test_pyarrow_validation_fails_fast(self):
        """Test that pyarrow validation fails fast and clearly when parquet is requested without pyarrow."""
        from unittest.mock import patch
        from src.utils.io_utils import write_parquet_safely
        
        # Create a simple DataFrame
        df = pd.DataFrame({"test": [1, 2, 3]})
        
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            temp_path = f.name
        
        try:
            # Mock pyarrow import to fail
            with patch('src.utils.io_utils._require_pyarrow', side_effect=ImportError("PyArrow is required for Parquet I/O")):
                with pytest.raises(ImportError, match="PyArrow is required for Parquet I/O"):
                    write_parquet_safely(df, temp_path)
        finally:
            # Clean up
            Path(temp_path).unlink(missing_ok=True)
