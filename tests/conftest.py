from __future__ import annotations

import importlib.util
import os
import random
from pathlib import Path

import numpy as np
import pytest
from hypothesis import settings

# ---- Helpers ----------------------------------------------------


def _module_available(modname: str) -> bool:
    return importlib.util.find_spec(modname) is not None


def _data_present(*candidates: str) -> bool:
    for p in candidates:
        if Path(p).exists():
            return True
    return False


# ---- Global environment flags ----------------------------------

HAS_PYARROW = _module_available("pyarrow")
HAS_STREAMLIT = _module_available("streamlit")
HAS_ORJSON = _module_available("orjson")

# Some tests need datasets; allow an env var override.
DATA_OK = _data_present("tests/data", "data") or bool(os.environ.get("TEST_DATA_OK"))

# ---- Clean test suite - no skip logic needed -----------------
# All problematic tests have been deleted, so no dynamic skipping required


# Configure pytest for non-strict xfail behavior
def pytest_configure(config: pytest.Config) -> None:
    # Set xfail_strict to False globally
    config.option.xfail_strict = False


# ---- Deterministic Testing Configuration ---------------------

# Global deterministic seed
DETERMINISTIC_SEED = 42


@pytest.fixture(autouse=True)
def set_deterministic_seed():
    """Set deterministic seed for all tests"""
    random.seed(DETERMINISTIC_SEED)
    np.random.seed(DETERMINISTIC_SEED)
    yield
    # Reset after test
    random.seed()
    np.random.seed()


# Hypothesis settings for all property-based tests
settings.register_profile("deterministic", 
    deadline=None, 
    max_examples=200, 
    derandomize=False,  # Use deterministic draw order
    database=None
)
settings.load_profile("deterministic")

# Set global seed for Hypothesis
from hypothesis import seed
seed(DETERMINISTIC_SEED)
