"""
Test the _is_non_empty function for safe truthiness checking.
"""

import pandas as pd
import numpy as np
import pyarrow as pa

from src.utils.ui_helpers import _is_non_empty


class TestIsNonEmpty:
    """Test the _is_non_empty function with various data types."""

    def test_none_values(self):
        """Test that None values return False."""
        assert not _is_non_empty(None)

    def test_empty_lists(self):
        """Test that empty lists return False."""
        assert not _is_non_empty([])
        assert not _is_non_empty(list())

    def test_non_empty_lists(self):
        """Test that non-empty lists return True."""
        assert _is_non_empty([1, 2, 3])
        assert _is_non_empty([""])
        assert _is_non_empty([None])

    def test_empty_strings(self):
        """Test that empty strings return False."""
        assert not _is_non_empty("")
        assert not _is_non_empty(str())

    def test_non_empty_strings(self):
        """Test that non-empty strings return True."""
        assert _is_non_empty("hello")
        assert _is_non_empty(" ")

    def test_pandas_series(self):
        """Test pandas Series handling."""
        # Empty series
        empty_series = pd.Series([])
        assert not _is_non_empty(empty_series)

        # Non-empty series
        non_empty_series = pd.Series([1, 2, 3])
        assert _is_non_empty(non_empty_series)

    def test_pandas_dataframe(self):
        """Test pandas DataFrame handling."""
        # Empty dataframe
        empty_df = pd.DataFrame()
        assert not _is_non_empty(empty_df)

        # Non-empty dataframe
        non_empty_df = pd.DataFrame({"col": [1, 2, 3]})
        assert _is_non_empty(non_empty_df)

    def test_numpy_arrays(self):
        """Test numpy array handling."""
        # Empty array
        empty_array = np.array([])
        assert not _is_non_empty(empty_array)

        # Non-empty array
        non_empty_array = np.array([1, 2, 3])
        assert _is_non_empty(non_empty_array)

    def test_pyarrow_tables(self):
        """Test pyarrow Table handling."""
        # Empty table
        empty_table = pa.table({})
        assert not _is_non_empty(empty_table)

        # Non-empty table
        non_empty_table = pa.table({"col": [1, 2, 3]})
        assert _is_non_empty(non_empty_table)

    def test_pyarrow_arrays(self):
        """Test pyarrow Array handling."""
        # Empty array
        empty_array = pa.array([])
        assert not _is_non_empty(empty_array)

        # Non-empty array
        non_empty_array = pa.array([1, 2, 3])
        assert _is_non_empty(non_empty_array)

    def test_pyarrow_chunked_arrays(self):
        """Test pyarrow ChunkedArray handling."""
        # Empty chunked array (need to specify type)
        empty_chunked = pa.chunked_array([], type=pa.int64())
        assert not _is_non_empty(empty_chunked)

        # Non-empty chunked array
        non_empty_chunked = pa.chunked_array([[1, 2], [3, 4]])
        assert _is_non_empty(non_empty_chunked)

    def test_objects_without_length(self):
        """Test objects that don't have __len__ method."""
        # Objects without __len__ should return True
        assert _is_non_empty(42)
        assert _is_non_empty(True)
        assert _is_non_empty(False)
        assert _is_non_empty(object())

    def test_objects_with_length_errors(self):
        """Test objects that have __len__ but raise errors."""

        class BadLength:
            def __len__(self):
                raise ValueError("Bad length")

        # Should handle length errors gracefully
        assert not _is_non_empty(BadLength())
