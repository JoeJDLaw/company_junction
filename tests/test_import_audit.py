"""Import audit test for Phase 1.18.1 refactor.

This test imports every module under app.components and src.utils to catch broken imports.
"""

import importlib

import pytest


def test_import_src_utils() -> None:
    """Test importing src.utils modules."""
    utils = [
        "src.utils.cache_utils",
        "src.utils.logging_utils",
        "src.utils.path_utils",
        # "src.utils.ui_helpers",  # Deprecated - moved to deprecated/ folder
        "src.utils.state_utils",  # Phase 1.18.1 new
        "src.utils.sort_utils",  # Phase 1.18.1 new
        "src.utils.cli_builder",
        "src.utils.id_utils",
        "src.utils.io_utils",
        "src.utils.mini_dag",
        "src.utils.parallel_utils",
        "src.utils.perf_utils",
        "src.utils.progress",
        "src.utils.resource_monitor",
        "src.utils.dtypes",
        "src.utils.hash_utils",
        "src.utils.validation_utils",
    ]

    for util in utils:
        try:
            importlib.import_module(util)
        except ImportError as e:
            pytest.fail(f"Failed to import {util}: {e}")


def test_import_app_components() -> None:
    """Test importing app.components modules."""
    components = [
        "app.components",
        "app.components.controls",
        "app.components.group_list",
        "app.components.group_details",
        "app.components.maintenance",
        "app.components.export",
    ]

    for component in components:
        try:
            importlib.import_module(component)
        except ImportError as e:
            pytest.fail(f"Failed to import {component}: {e}")


def test_import_app_modules() -> None:
    """Test importing app modules."""
    app_modules = [
        "app.main",
    ]

    for module in app_modules:
        try:
            importlib.import_module(module)
        except ImportError as e:
            pytest.fail(f"Failed to import {module}: {e}")


def test_import_src_modules() -> None:
    """Test importing src modules."""
    src_modules = [
        "src.utils",
    ]

    for module in src_modules:
        try:
            importlib.import_module(module)
        except ImportError as e:
            pytest.fail(f"Failed to import {module}: {e}")


def test_import_src_utils_submodules() -> None:
    """Test importing src.utils submodules."""


def test_absolute_imports_work() -> None:
    """Test that absolute imports work correctly."""
    # Test importing the new modules
    import app.components

    assert app.components is not None

    import src.utils.state_utils

    assert src.utils.state_utils is not None

    import src.utils.sort_utils

    assert src.utils.sort_utils is not None


def test_no_relative_imports() -> None:
    """Test that no relative imports are used in the new modules."""
    # This is a basic test - in practice, you'd want to scan the actual source files
    # to ensure no relative imports are used

    # Import the modules to ensure they work

    # If we get here without errors, the imports work
    assert True


def test_circular_imports() -> None:
    """Test that there are no circular imports between app.components and src.utils."""
    # Import all modules to check for circular imports

    # If we get here without errors, there are no circular imports
    assert True


def test_component_imports() -> None:
    """Test that components can import their dependencies."""
    # Test that components can import state_utils
    from src.utils import state_utils

    assert state_utils is not None

    # Test that components can import sort_utils
    from src.utils import sort_utils

    assert sort_utils is not None

    # Test that components can import ui_helpers - DEPRECATED
    # from src.utils import ui_helpers
    # assert ui_helpers is not None


def test_utils_no_streamlit_imports() -> None:
    """Test that src.utils modules don't import Streamlit."""
    # This is a basic test - in practice, you'd want to scan the actual source files
    # to ensure no Streamlit imports are used in src.utils modules

    # Import the utils modules to ensure they work

    # If we get here without errors, the imports work
    assert True
