#!/usr/bin/env python3
"""
Comprehensive import test for the company junction pipeline.
This script tests all imports to ensure the codebase is properly structured.
"""

import importlib
import pytest

# modules we want to be importable via the canonical package path
SRC_MODULES = [
    ("src.cleaning", "Main pipeline orchestration"),
    ("src.normalize", "Data normalization"),
    ("src.similarity", "Similarity computation"),
    ("src.grouping", "Group creation logic"),
    ("src.survivorship", "Primary record selection"),
    ("src.disposition", "Disposition classification"),
    ("src.alias_matching", "Alias matching"),
    ("src.manual_io", "Manual data I/O"),
    ("src.salesforce", "Salesforce utilities"),
    ("src.performance", "Performance tracking"),
    ("src.dtypes_map", "Data type mappings"),
]

UTILS_MODULES = [
    ("src.utils", "Main utils package"),
    ("src.utils.dtypes", "Data type utilities"),
    ("src.utils.logging_utils", "Logging utilities"),
    ("src.utils.path_utils", "Path utilities"),
    ("src.utils.validation_utils", "Validation utilities"),
    ("src.utils.io_utils", "I/O utilities"),
    ("src.utils.perf_utils", "Performance utilities"),
    ("src.utils.hash_utils", "Hash utilities"),
    ("src.utils.id_utils", "Salesforce ID utilities"),
]

CROSS_IMPORTS = [
    ("src.cleaning", "imports utils via absolute path"),
    ("src.grouping", "imports utils via absolute path"),
    ("src.survivorship", "imports src.normalize"),
    ("src.performance", "imports utils via absolute path"),
    ("src.utils.dtypes", "imports src.dtypes_map"),
]


@pytest.mark.parametrize("module_name, _desc", SRC_MODULES + UTILS_MODULES)
def test_importable(module_name: str, _desc: str) -> None:
    importlib.import_module(module_name)


@pytest.mark.parametrize("module_name, _desc", CROSS_IMPORTS)
def test_cross_importable(module_name: str, _desc: str) -> None:
    importlib.import_module(module_name)
