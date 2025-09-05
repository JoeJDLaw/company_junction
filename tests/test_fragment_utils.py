"""Tests for fragment utilities.

Tests the fragment API detection and unified decorator functionality.
"""

from unittest.mock import MagicMock, patch

import pytest

from src.utils.fragment_utils import _USE_STABLE_FRAGMENT, fragment


def test_fragment_decorator_availability():
    """Test that the fragment decorator is available."""
    assert fragment is not None
    assert callable(fragment)


def test_fragment_api_detection():
    """Test that the fragment API detection works correctly."""
    # Should be True for Streamlit >= 1.29.0
    assert isinstance(_USE_STABLE_FRAGMENT, bool)


@patch("streamlit.__version__", "1.49.0")
def test_stable_fragment_detection():
    """Test that stable fragment is detected for Streamlit >= 1.29."""
    # Re-import to get the mocked version
    import importlib

    import src.utils.fragment_utils

    importlib.reload(src.utils.fragment_utils)

    # Should use stable fragment for 1.49.0
    assert src.utils.fragment_utils._USE_STABLE_FRAGMENT is True


def test_experimental_fragment_detection():
    """Test that experimental fragment is detected for Streamlit < 1.29."""
    # Test with a version that should use experimental fragment
    with patch("streamlit.__version__", "1.28.0"):
        # Mock the module to simulate older Streamlit
        with patch("src.utils.fragment_utils.st") as mock_st:
            mock_st.fragment = MagicMock()
            mock_st.experimental_fragment = MagicMock()

            # Re-import to get the mocked version
            import importlib

            import src.utils.fragment_utils

            importlib.reload(src.utils.fragment_utils)

            # Should use experimental fragment for 1.28.0
            assert src.utils.fragment_utils._USE_STABLE_FRAGMENT is False


def test_fragment_import_smoke():
    """Test that fragment utility can be imported without errors."""
    try:
        from src.utils.fragment_utils import fragment

        assert fragment is not None
    except ImportError as e:
        pytest.fail(f"Failed to import fragment utility: {e}")


def test_fragment_decorator_functionality():
    """Test that the fragment decorator can be applied to functions."""
    from src.utils.fragment_utils import fragment

    @fragment
    def test_function():
        return "test"

    # Should not raise an error
    assert test_function is not None
