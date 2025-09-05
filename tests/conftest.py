"""Pytest configuration and shared fixtures.

This file provides shared fixtures and configuration for all tests.
"""

import os
import tempfile
from collections.abc import Generator
from pathlib import Path
from typing import Any, Dict, cast

import pytest
import yaml


def deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    """Deep merge two dictionaries, with override taking precedence."""
    result = base.copy()
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = value
    return result


@pytest.fixture
def settings_from_config():
    """Load config/settings.yaml and allow deep-merging overrides."""

    def _settings_from_config(
        overrides: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        config_path = Path(__file__).parent.parent / "config" / "settings.yaml"
        with open(config_path) as f:
            base_config = yaml.safe_load(f)

        if overrides is None:
            return cast("Dict[str, Any]", base_config)

        return deep_merge(base_config, overrides)

    return _settings_from_config


@pytest.fixture
def enable_destructive_fuse() -> Generator[None, None, None]:
    """Enable destructive fuse for cache utils tests.

    This fixture temporarily enables the destructive fuse that allows
    cache operations like deletion and pruning to work.
    """
    # Store original environment
    original_env = os.environ.copy()

    # Enable destructive fuse
    os.environ["PHASE1_DESTRUCTIVE_FUSE"] = "true"

    yield

    # Restore original environment
    os.environ.clear()
    os.environ.update(original_env)


@pytest.fixture
def temp_workspace() -> Generator[Path, None, None]:
    """Create a temporary workspace for destructive operations.

    This fixture creates a temporary directory that can be used
    for testing destructive operations without affecting the main workspace.
    """
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        # Create subdirectories that cache utils expects
        (temp_path / "data" / "processed").mkdir(parents=True, exist_ok=True)
        (temp_path / "data" / "interim").mkdir(parents=True, exist_ok=True)

        # Store original working directory
        original_cwd = os.getcwd()

        try:
            # Change to temp directory
            os.chdir(temp_path)
            yield temp_path
        finally:
            # Restore original working directory
            os.chdir(original_cwd)


@pytest.fixture
def cache_utils_workspace(
    enable_destructive_fuse: Generator[None, None, None], temp_workspace: Path,
) -> Path:
    """Combined fixture for cache utils tests.

    This fixture provides both destructive fuse and a temporary workspace
    for testing cache operations safely.
    """
    return temp_workspace
