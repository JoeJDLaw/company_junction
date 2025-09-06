"""Unit tests for Salesforce ID canonicalization utilities."""

import numpy as np
import pandas as pd
import pytest

from src.utils.id_utils import (
    _chunk_checksum,
    normalize_sfid_series,
    sfid15_to_18,
    validate_sfid_format,
)


class TestChunkChecksum:
    """Test checksum calculation for 5-character chunks."""

    def test_chunk_checksum_all_lowercase(self) -> None:
        """Test checksum for all lowercase characters."""
        result = _chunk_checksum("abcde")
        assert result == "A"  # No uppercase letters = bits = 0

    def test_chunk_checksum_all_uppercase(self) -> None:
        """Test checksum for all uppercase characters."""
        result = _chunk_checksum("ABCDE")
        assert result == "5"  # All uppercase = bits = 31 (11111) = index 5 in _BASE32

    def test_chunk_checksum_mixed_case(self) -> None:
        """Test checksum for mixed case characters."""
        result = _chunk_checksum("AbCdE")
        assert result == "V"  # Positions 0,2,4 are uppercase = bits = 21 (10101)

    def test_chunk_checksum_with_numbers(self) -> None:
        """Test checksum with numbers (should be treated as lowercase)."""
        result = _chunk_checksum("A1C3E")
        assert result == "V"  # Positions 0,2,4 are uppercase = bits = 21 (10101)

    def test_chunk_checksum_wrong_length(self) -> None:
        """Test that wrong length raises ValueError."""
        with pytest.raises(ValueError, match="exactly 5 characters"):
            _chunk_checksum("abcd")


class TestSfid15To18:
    """Test 15-character to 18-character Salesforce ID conversion."""

    def test_sfid15_to_18_known_cases(self) -> None:
        """Test known 15→18 conversion cases."""
        # Test cases with known expected results
        test_cases = [
            ("001Hs000054S8kI", "001Hs000054S8kIIAS"),  # Mixed case
            ("001HS000054S8KI", "001HS000054S8KIYA0"),  # All uppercase
        ]

        for sfid15, expected in test_cases:
            result = sfid15_to_18(sfid15)
            assert result == expected
            assert len(result) == 18

    def test_sfid15_to_18_case_sensitivity(self) -> None:
        """Test that case differences produce different 18-char IDs."""
        id1 = sfid15_to_18("001Hs000054S8kI")
        id2 = sfid15_to_18("001HS000054S8KI")

        assert id1 != id2
        assert id1.startswith("001Hs000054S8kI")
        assert id2.startswith("001HS000054S8KI")

    def test_sfid15_to_18_with_numbers(self) -> None:
        """Test conversion with numeric characters."""
        result = sfid15_to_18("001Hs000054S8kI")
        assert len(result) == 18
        assert result.startswith("001Hs000054S8kI")

    def test_sfid15_to_18_invalid_inputs(self) -> None:
        """Test that invalid inputs raise appropriate errors."""
        # Wrong type
        with pytest.raises(TypeError, match="must be a string"):
            sfid15_to_18(123)  # type: ignore[arg-type]

        # Wrong length
        with pytest.raises(ValueError, match="exactly 15 characters"):
            sfid15_to_18("12345678901234")  # 14 chars

        with pytest.raises(ValueError, match="exactly 15 characters"):
            sfid15_to_18("1234567890123456")  # 16 chars

        # Invalid characters
        with pytest.raises(ValueError, match="alphanumeric characters"):
            sfid15_to_18("001Hs000054S8k-")  # Contains hyphen


class TestNormalizeSfidSeries:
    """Test pandas Series normalization."""

    def test_normalize_sfid_series_15_to_18(self) -> None:
        """Test converting 15-char IDs to 18-char."""
        series = pd.Series(["001Hs000054S8kI", "001Hs000054SAQt", "001Hs000054SDWt"])

        result = normalize_sfid_series(series)

        assert len(result) == 3
        assert all(len(id_) == 18 for id_ in result)
        assert result.iloc[0].startswith("001Hs000054S8kI")
        assert result.iloc[1].startswith("001Hs000054SAQt")
        assert result.iloc[2].startswith("001Hs000054SDWt")

    def test_normalize_sfid_series_pass_through_18(self) -> None:
        """Test that 18-char IDs pass through unchanged."""
        series = pd.Series(
            [
                "001Hs000054S8kIAAA",  # 18-char
                "001Hs000054SAQtBBB",  # 18-char
            ],
        )

        result = normalize_sfid_series(series)

        assert len(result) == 2
        assert result.iloc[0] == "001Hs000054S8kIAAA"
        assert result.iloc[1] == "001Hs000054SAQtBBB"

    def test_normalize_sfid_series_mixed_15_and_18(self) -> None:
        """Test mixed 15-char and 18-char IDs."""
        series = pd.Series(
            [
                "001Hs000054S8kI",  # 15-char
                "001Hs000054SAQtBBB",  # 18-char
                "001Hs000054SDWt",  # 15-char
            ],
        )

        result = normalize_sfid_series(series)

        assert len(result) == 3
        assert all(len(id_) == 18 for id_ in result)
        assert result.iloc[0].startswith("001Hs000054S8kI")
        assert result.iloc[1] == "001Hs000054SAQtBBB"  # Unchanged
        assert result.iloc[2].startswith("001Hs000054SDWt")

    def test_normalize_sfid_series_handles_whitespace(self) -> None:
        """Test that whitespace is handled properly."""
        series = pd.Series(
            [
                " 001Hs000054S8kI ",  # With whitespace
                "001Hs000054SAQtBBB",  # Clean 18-char
            ],
        )

        result = normalize_sfid_series(series)

        assert len(result) == 2
        assert all(len(id_) == 18 for id_ in result)
        assert result.iloc[0].startswith("001Hs000054S8kI")
        assert result.iloc[1] == "001Hs000054SAQtBBB"

    def test_normalize_sfid_series_handles_nan(self) -> None:
        """Test that NaN values are handled properly."""
        series = pd.Series(
            [
                "001Hs000054S8kI",
                np.nan,
                "001Hs000054SAQtBBB",
            ],
        )

        result = normalize_sfid_series(series)

        assert len(result) == 3
        assert result.iloc[0].startswith("001Hs000054S8kI")
        assert result.iloc[1] == ""  # NaN becomes empty string
        assert result.iloc[2] == "001Hs000054SAQtBBB"

    def test_normalize_sfid_series_empty_series(self) -> None:
        """Test empty series handling."""
        series = pd.Series([], dtype="string")
        result = normalize_sfid_series(series)
        assert len(result) == 0

    def test_normalize_sfid_series_invalid_ids(self) -> None:
        """Test that invalid IDs raise ValueError."""
        series = pd.Series(
            [
                "001Hs000054S8kI",  # Valid 15-char
                "001Hs000054S8k",  # Invalid: 14-char
                "001Hs000054SAQtBBB",  # Valid 18-char
            ],
        )

        with pytest.raises(ValueError, match="non 15/18-char Salesforce IDs"):
            normalize_sfid_series(series)

    def test_normalize_sfid_series_invalid_characters(self) -> None:
        """Test that IDs with invalid characters raise ValueError."""
        series = pd.Series(
            [
                "001Hs000054S8kI",  # Valid 15-char
                "001Hs000054S8k-",  # Invalid: contains hyphen
                "001Hs000054SAQtBBB",  # Valid 18-char
            ],
        )

        with pytest.raises(
            ValueError,
            match="sfid15 must contain only alphanumeric characters",
        ):
            normalize_sfid_series(series)


class TestValidateSfidFormat:
    """Test Salesforce ID format validation."""

    def test_validate_sfid_format_valid_15_char(self) -> None:
        """Test valid 15-character IDs."""
        assert validate_sfid_format("001Hs000054S8kI")
        assert validate_sfid_format("001HS000054S8KI")

    def test_validate_sfid_format_valid_18_char(self) -> None:
        """Test valid 18-character IDs."""
        assert validate_sfid_format("001Hs000054S8kIAAA")
        assert validate_sfid_format("001HS000054S8KIAAA")

    def test_validate_sfid_format_invalid_lengths(self) -> None:
        """Test invalid lengths."""
        assert not validate_sfid_format("001Hs000054S8k")  # 14 chars
        assert not validate_sfid_format("001Hs000054S8kIAA")  # 17 chars
        assert not validate_sfid_format("001Hs000054S8kIAAAA")  # 19 chars

    def test_validate_sfid_format_invalid_characters(self) -> None:
        """Test invalid characters."""
        assert not validate_sfid_format("001Hs000054S8k-")  # Contains hyphen
        assert not validate_sfid_format("001Hs000054S8k_")  # Contains underscore
        assert not validate_sfid_format("001Hs000054S8k ")  # Contains space

    def test_validate_sfid_format_non_string(self) -> None:
        """Test non-string inputs."""
        assert not validate_sfid_format(123)
        assert not validate_sfid_format(None)
        assert not validate_sfid_format([])
