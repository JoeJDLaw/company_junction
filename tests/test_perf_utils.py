"""Tests for performance profiling utilities."""

import logging
import time
from unittest.mock import patch

import pytest

from src.utils.perf_utils import (
    log_performance_summary,
    time_stage,
    track_memory_peak,
)


def test_track_memory_peak(caplog: pytest.LogCaptureFixture) -> None:
    """Test memory peak tracking context manager."""
    # Set up logging to capture messages
    caplog.set_level(logging.INFO)
    logger = logging.getLogger("test")

    with track_memory_peak("test_stage", logger):
        # Simulate some memory usage
        pass

    # Should log memory peak
    assert "Memory peak at 'test_stage'" in caplog.text


def test_time_stage(caplog: pytest.LogCaptureFixture) -> None:
    """Test stage timing context manager."""
    # Set up logging to capture messages
    caplog.set_level(logging.INFO)
    logger = logging.getLogger("test")

    with time_stage("test_stage", logger):
        # Simulate some work
        time.sleep(0.01)

    # Should log timing
    assert "Stage 'test_stage' completed in" in caplog.text


def test_log_performance_summary(caplog: pytest.LogCaptureFixture) -> None:
    """Test performance summary logging."""
    # Set up logging to capture messages
    caplog.set_level(logging.INFO)
    logger = logging.getLogger("test")

    log_performance_summary(logger)

    # Should log performance summary
    assert "Performance summary" in caplog.text


def test_context_managers_exception_handling() -> None:
    """Test that context managers properly handle exceptions."""
    logger = logging.getLogger("test")

    # Test time_stage with exception
    try:
        with time_stage("test_stage", logger):
            raise ValueError("Test exception")
    except ValueError:
        pass  # Exception should be re-raised

    # Test track_memory_peak with exception
    try:
        with track_memory_peak("test_stage", logger):
            raise ValueError("Test exception")
    except ValueError:
        pass  # Exception should be re-raised
