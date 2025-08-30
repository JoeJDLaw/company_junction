"""
Tests for performance profiling utilities.
"""

import json
import logging
import tempfile
import time
from pathlib import Path
from unittest.mock import patch

import pytest

from src.utils.perf_utils import (
    compare_performance,
    detect_performance_regression,
    get_memory_usage,
    load_performance_baseline,
    log_memory_usage,
    log_performance_summary,
    save_performance_baseline,
    time_stage,
    track_memory_peak,
)


def test_get_memory_usage() -> None:
    """Test memory usage retrieval."""
    memory = get_memory_usage()

    # Should return a dictionary with expected keys
    assert isinstance(memory, dict)
    assert "rss" in memory
    assert "vms" in memory

    # Values should be floats
    assert isinstance(memory["rss"], float)
    assert isinstance(memory["vms"], float)

    # Values should be non-negative
    assert memory["rss"] >= 0
    assert memory["vms"] >= 0


def test_get_memory_usage_without_psutil() -> None:
    """Test memory usage when psutil is not available."""
    with patch("src.utils.perf_utils.PSUTIL_AVAILABLE", False):
        memory = get_memory_usage()

        assert memory["rss"] == 0.0
        assert memory["vms"] == 0.0


def test_log_memory_usage(caplog: pytest.LogCaptureFixture) -> None:
    """Test memory usage logging."""
    # Set up logging to capture messages
    caplog.set_level(logging.INFO)
    logger = logging.getLogger("test")

    log_memory_usage("test_stage", logger)

    # Should log memory usage
    assert "Memory usage at 'test_stage'" in caplog.text
    assert "RSS=" in caplog.text
    assert "VMS=" in caplog.text


def test_track_memory_peak(caplog: pytest.LogCaptureFixture) -> None:
    """Test memory peak tracking context manager."""
    # Set up logging to capture messages
    caplog.set_level(logging.INFO)
    logger = logging.getLogger("test")

    with track_memory_peak("test_stage", logger):
        # Simulate some memory usage
        pass

    # Should log memory tracking information
    assert "Stage 'test_stage' memory:" in caplog.text
    assert "start=" in caplog.text
    assert "end=" in caplog.text
    assert "peak=" in caplog.text
    assert "delta=" in caplog.text


def test_time_stage(caplog: pytest.LogCaptureFixture) -> None:
    """Test stage timing context manager."""
    # Set up logging to capture messages
    caplog.set_level(logging.INFO)
    logger = logging.getLogger("test")

    with time_stage("test_stage", logger):
        # Simulate some work
        time.sleep(0.01)

    # Should log timing information
    assert "Stage 'test_stage' completed in" in caplog.text
    assert "memory delta:" in caplog.text


def test_log_performance_summary(caplog: pytest.LogCaptureFixture) -> None:
    """Test performance summary logging."""
    # Set up logging to capture messages
    caplog.set_level(logging.INFO)
    logger = logging.getLogger("test")

    log_performance_summary(logger)

    # Should log performance summary
    assert "Pipeline performance summary:" in caplog.text
    assert "Final memory usage:" in caplog.text
    assert "RSS=" in caplog.text
    assert "VMS=" in caplog.text


def test_compare_performance_no_regression() -> None:
    """Test performance comparison with no regression."""
    baseline = {"time": 100.0, "memory": 50.0}
    current = {"time": 95.0, "memory": 48.0}  # Better performance

    result = compare_performance(baseline, current, threshold=0.1)
    assert result is True  # No regression


def test_compare_performance_within_threshold() -> None:
    """Test performance comparison within acceptable threshold."""
    baseline = {"time": 100.0, "memory": 50.0}
    current = {"time": 105.0, "memory": 52.0}  # 5% increase, within 10% threshold

    result = compare_performance(baseline, current, threshold=0.1)
    assert result is True  # No regression


def test_compare_performance_regression_detected() -> None:
    """Test performance comparison with regression detected."""
    baseline = {"time": 100.0, "memory": 50.0}
    current = {"time": 120.0, "memory": 60.0}  # 20% increase, exceeds 10% threshold

    result = compare_performance(baseline, current, threshold=0.1)
    assert result is False  # Regression detected


def test_compare_performance_missing_metrics() -> None:
    """Test performance comparison with missing metrics."""
    baseline = {"time": 100.0, "memory": 50.0}
    current = {"time": 95.0}  # Missing memory metric

    result = compare_performance(baseline, current, threshold=0.1)
    assert result is True  # No regression (missing metrics ignored)


def test_save_performance_baseline() -> None:
    """Test saving performance baseline to file."""
    with tempfile.TemporaryDirectory() as temp_dir:
        baseline_file = Path(temp_dir) / "baseline.json"
        baseline_data = {"time": 100.0, "memory": 50.0}

        save_performance_baseline(baseline_data, str(baseline_file))

        # File should be created
        assert baseline_file.exists()

        # Content should be valid JSON
        with open(baseline_file, "r") as f:
            loaded_data = json.load(f)

        assert loaded_data == baseline_data


def test_load_performance_baseline() -> None:
    """Test loading performance baseline from file."""
    with tempfile.TemporaryDirectory() as temp_dir:
        baseline_file = Path(temp_dir) / "baseline.json"
        baseline_data = {"time": 100.0, "memory": 50.0}

        # Save baseline first
        with open(baseline_file, "w") as f:
            json.dump(baseline_data, f)

        # Load baseline
        loaded_data = load_performance_baseline(str(baseline_file))

        assert loaded_data == baseline_data


def test_load_performance_baseline_nonexistent() -> None:
    """Test loading performance baseline from nonexistent file."""
    with tempfile.TemporaryDirectory() as temp_dir:
        baseline_file = Path(temp_dir) / "nonexistent.json"

        loaded_data = load_performance_baseline(str(baseline_file))

        assert loaded_data is None


def test_load_performance_baseline_invalid_json() -> None:
    """Test loading performance baseline from invalid JSON file."""
    with tempfile.TemporaryDirectory() as temp_dir:
        baseline_file = Path(temp_dir) / "invalid.json"

        # Create invalid JSON file
        with open(baseline_file, "w") as f:
            f.write("invalid json content")

        loaded_data = load_performance_baseline(str(baseline_file))

        assert loaded_data is None


def test_detect_performance_regression_no_baseline(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Test performance regression detection with no baseline file."""
    # Set up logging to capture messages
    caplog.set_level(logging.WARNING)
    logger = logging.getLogger("test")
    current_metrics = {"time": 100.0, "memory": 50.0}

    with tempfile.TemporaryDirectory() as temp_dir:
        baseline_file = Path(temp_dir) / "nonexistent.json"

        result = detect_performance_regression(
            str(baseline_file), current_metrics, logger=logger
        )

        assert result is False
        assert "No baseline file found" in caplog.text


def test_detect_performance_regression_no_regression() -> None:
    """Test performance regression detection with no regression."""
    current_metrics = {"time": 95.0, "memory": 48.0}

    with tempfile.TemporaryDirectory() as temp_dir:
        baseline_file = Path(temp_dir) / "baseline.json"
        baseline_data = {"time": 100.0, "memory": 50.0}

        # Save baseline
        save_performance_baseline(baseline_data, str(baseline_file))

        result = detect_performance_regression(
            str(baseline_file), current_metrics, threshold=0.1
        )

        assert result is False  # No regression


def test_detect_performance_regression_with_regression(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Test performance regression detection with regression."""
    # Set up logging to capture messages
    caplog.set_level(logging.WARNING)
    logger = logging.getLogger("test")
    current_metrics = {"time": 120.0, "memory": 60.0}

    with tempfile.TemporaryDirectory() as temp_dir:
        baseline_file = Path(temp_dir) / "baseline.json"
        baseline_data = {"time": 100.0, "memory": 50.0}

        # Save baseline
        save_performance_baseline(baseline_data, str(baseline_file))

        result = detect_performance_regression(
            str(baseline_file), current_metrics, threshold=0.1, logger=logger
        )

        assert result is True  # Regression detected
        assert "Performance regression detected!" in caplog.text


def test_context_managers_exception_handling() -> None:
    """Test that context managers handle exceptions gracefully."""
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
