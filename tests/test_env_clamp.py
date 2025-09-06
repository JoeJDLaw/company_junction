"""Test BLAS environment variable clamping functionality."""

import os
from unittest.mock import patch

from src.utils.parallel_utils import ensure_single_thread_blas


def test_ensure_single_thread_blas_sets_unset_vars() -> None:
    """Test that unset BLAS environment variables are set to 1."""
    blas_vars = [
        "OMP_NUM_THREADS",
        "OPENBLAS_NUM_THREADS",
        "MKL_NUM_THREADS",
        "VECLIB_MAXIMUM_THREADS",
        "NUMEXPR_MAX_THREADS",
    ]

    # Clear any existing values
    for var in blas_vars:
        if var in os.environ:
            del os.environ[var]

    # Call the function
    ensure_single_thread_blas()

    # Check that all variables are set to "1"
    for var in blas_vars:
        assert var in os.environ, f"Variable {var} should be set"
        assert (
            os.environ[var] == "1"
        ), f"Variable {var} should be set to '1', got '{os.environ[var]}'"


def test_ensure_single_thread_blas_respects_existing_values() -> None:
    """Test that existing BLAS environment variables are not overridden."""
    blas_vars = [
        "OMP_NUM_THREADS",
        "OPENBLAS_NUM_THREADS",
        "MKL_NUM_THREADS",
        "VECLIB_MAXIMUM_THREADS",
        "NUMEXPR_MAX_THREADS",
    ]

    # Set some variables to custom values
    custom_values = {
        "OMP_NUM_THREADS": "4",
        "OPENBLAS_NUM_THREADS": "2",
        "MKL_NUM_THREADS": "8",
    }

    # Set the custom values
    for var, value in custom_values.items():
        os.environ[var] = value

    # Clear the others
    for var in blas_vars:
        if var not in custom_values:
            if var in os.environ:
                del os.environ[var]

    # Call the function
    ensure_single_thread_blas()

    # Check that custom values are preserved
    for var, expected_value in custom_values.items():
        assert (
            os.environ[var] == expected_value
        ), f"Variable {var} should preserve value '{expected_value}', got '{os.environ[var]}'"

    # Check that unset variables are set to "1"
    for var in blas_vars:
        if var not in custom_values:
            assert (
                os.environ[var] == "1"
            ), f"Variable {var} should be set to '1', got '{os.environ[var]}'"


def test_ensure_single_thread_blas_mixed_scenario() -> None:
    """Test mixed scenario with some variables set and others not."""
    # Set only some variables
    os.environ["OMP_NUM_THREADS"] = "6"
    os.environ["OPENBLAS_NUM_THREADS"] = "3"

    # Clear others
    for var in ["MKL_NUM_THREADS", "VECLIB_MAXIMUM_THREADS", "NUMEXPR_MAX_THREADS"]:
        if var in os.environ:
            del os.environ[var]

    # Call the function
    ensure_single_thread_blas()

    # Check results
    assert os.environ["OMP_NUM_THREADS"] == "6"  # Preserved
    assert os.environ["OPENBLAS_NUM_THREADS"] == "3"  # Preserved
    assert os.environ["MKL_NUM_THREADS"] == "1"  # Set
    assert os.environ["VECLIB_MAXIMUM_THREADS"] == "1"  # Set
    assert os.environ["NUMEXPR_MAX_THREADS"] == "1"  # Set


def test_parallel_map_uses_blas_clamp() -> None:
    """Test that parallel_map calls ensure_single_thread_blas."""
    from src.utils.parallel_utils import parallel_map

    # Mock the ensure_single_thread_blas function
    with patch("src.utils.parallel_utils.ensure_single_thread_blas") as mock_clamp:
        # Call parallel_map with workers > 1
        result = parallel_map(lambda x: x * 2, [1, 2, 3], workers=2)

        # Check that the clamp function was called
        mock_clamp.assert_called_once()

        # Check that the result is correct
        assert result == [2, 4, 6]


def test_parallel_map_sequential_fallback() -> None:
    """Test that parallel_map falls back to sequential when workers <= 1."""
    from src.utils.parallel_utils import parallel_map

    # Mock the ensure_single_thread_blas function
    with patch("src.utils.parallel_utils.ensure_single_thread_blas") as mock_clamp:
        # Call parallel_map with workers = 1
        result = parallel_map(lambda x: x * 2, [1, 2, 3], workers=1)

        # Check that the clamp function was NOT called (sequential fallback)
        mock_clamp.assert_not_called()

        # Check that the result is correct
        assert result == [2, 4, 6]
