"""
Test that all modules can be imported successfully.

This test ensures that all modules in the project can be imported without errors,
which helps catch import issues early.
"""

import pytest


def test_absolute_imports() -> None:
    """Test that all modules can be imported using absolute imports."""
    modules_to_test = [
        # Core modules
        "src.utils.cache_utils",
        "src.utils.dtypes",
        "src.utils.hash_utils",
        "src.utils.io_utils",
        "src.utils.logging_utils",
        "src.utils.parallel_utils",
        "src.utils.path_utils",
        "src.utils.perf_utils",
        "src.utils.resource_monitor",
        "src.utils.sort_utils",
        "src.utils.state_utils",
        "src.utils.validation_utils",
        "src.utils.fragment_utils",  # Phase 1.18.3 addition
        "src.utils.ui_helpers",
        "src.alias_matching",
        "src.cleaning",
        "src.disposition",
        "src.grouping",
        "src.manual_io",
        "src.normalize",
        "src.performance",
        "src.salesforce",
        "src.similarity",
        "src.survivorship",
        # App modules
        "app.components.controls",
        "app.components.export",
        "app.components.group_details",
        "app.components.group_list",
        "app.components.maintenance",
        "app.main",
    ]

    for module_name in modules_to_test:
        try:
            module = __import__(module_name, fromlist=[""])
            assert module is not None
        except ImportError as e:
            pytest.fail(f"Failed to import {module_name}: {e}")


def test_component_imports() -> None:
    """Test that all component modules can be imported."""
    component_modules = [
        "app.components.controls",
        "app.components.export",
        "app.components.group_details",
        "app.components.group_list",
        "app.components.maintenance",
    ]

    for module_name in component_modules:
        try:
            module = __import__(module_name, fromlist=[""])
            assert module is not None
        except ImportError as e:
            pytest.fail(f"Failed to import component {module_name}: {e}")


def test_utils_imports() -> None:
    """Test that all utility modules can be imported."""
    utils_modules = [
        "src.utils.cache_utils",
        "src.utils.dtypes",
        "src.utils.hash_utils",
        "src.utils.io_utils",
        "src.utils.logging_utils",
        "src.utils.parallel_utils",
        "src.utils.path_utils",
        "src.utils.perf_utils",
        "src.utils.resource_monitor",
        "src.utils.sort_utils",
        "src.utils.state_utils",
        "src.utils.validation_utils",
        "src.utils.fragment_utils",  # Phase 1.18.3 addition
        "src.utils.ui_helpers",
    ]

    for module_name in utils_modules:
        try:
            module = __import__(module_name, fromlist=[""])
            assert module is not None
        except ImportError as e:
            pytest.fail(f"Failed to import utility {module_name}: {e}")
