"""Tests for resource monitoring functionality.

This module tests:
- System information gathering
- Memory estimation
- Worker count calculation
- Disk space monitoring
- Resource guardrails
"""

import os
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from src.utils.resource_monitor import (
    calculate_optimal_workers,
    check_disk_space,
    estimate_memory_per_worker,
    get_memory_usage,
    get_system_info,
    log_resource_summary,
    monitor_parallel_execution,
)


def test_get_system_info() -> None:
    """Test system information gathering."""
    info = get_system_info()

    # Basic required fields
    assert "cpu_count" in info
    assert "psutil_available" in info
    assert info["cpu_count"] > 0

    # psutil-dependent fields (may not be available)
    if info["psutil_available"]:
        assert "total_memory_gb" in info
        assert "available_memory_gb" in info
        assert "memory_percent" in info
        assert "total_disk_gb" in info
        assert "free_disk_gb" in info
        assert "disk_percent" in info

        # Validate ranges
        assert info["total_memory_gb"] > 0
        assert info["available_memory_gb"] >= 0
        assert 0 <= info["memory_percent"] <= 100
        assert info["total_disk_gb"] > 0
        assert info["free_disk_gb"] >= 0
        assert 0 <= info["disk_percent"] <= 100


def test_estimate_memory_per_worker() -> None:
    """Test memory estimation per worker."""
    memory = estimate_memory_per_worker()

    # Should return a positive value
    assert memory > 0

    # Should be reasonable (between 0.5GB and 10GB)
    assert 0.5 <= memory <= 10.0


@patch("src.utils.resource_monitor.PSUTIL_AVAILABLE", True)
@patch("src.utils.resource_monitor.psutil.virtual_memory")
@patch("src.utils.resource_monitor.psutil.Process")
def test_calculate_optimal_workers_mock(
    mock_process: Any,
    mock_virtual_memory: Any,
) -> None:
    """Test worker calculation with mocked psutil."""
    # Mock memory info
    mock_memory = MagicMock()
    mock_memory.total = 16 * 1024**3  # 16GB
    mock_virtual_memory.return_value = mock_memory

    # Mock process info
    mock_process_instance = MagicMock()
    mock_process_instance.memory_info.return_value.rss = 1 * 1024**3  # 1GB
    mock_process.return_value = mock_process_instance

    # Test with no requested workers (auto-detection)
    workers = calculate_optimal_workers()
    assert workers > 0
    cpu_count = os.cpu_count() or 1
    assert workers <= cpu_count

    # Test with requested workers
    workers = calculate_optimal_workers(requested_workers=4)
    assert workers == 4

    # Test with excessive requested workers
    workers = calculate_optimal_workers(requested_workers=100)
    assert workers <= cpu_count


@patch("src.utils.resource_monitor.PSUTIL_AVAILABLE", False)
def test_calculate_optimal_workers_no_psutil() -> None:
    """Test worker calculation without psutil."""
    # Should fall back to default formula
    workers = calculate_optimal_workers()
    cpu_count = os.cpu_count() or 1
    expected = min(cpu_count, max(1, cpu_count - 2))
    assert workers == expected


@patch("src.utils.resource_monitor.PSUTIL_AVAILABLE", True)
@patch("src.utils.resource_monitor.psutil.disk_usage")
def test_check_disk_space(mock_disk_usage: Any) -> None:
    """Test disk space checking."""
    # Mock disk with plenty of space
    mock_disk = MagicMock()
    mock_disk.total = 100 * 1024**3  # 100GB
    mock_disk.free = 80 * 1024**3  # 80GB free
    mock_disk_usage.return_value = mock_disk

    # Should return True (adequate space)
    result = check_disk_space(warning_threshold_percent=20.0)
    assert result

    # Mock disk with low space
    mock_disk.free = 10 * 1024**3  # 10GB free (10%)
    result = check_disk_space(warning_threshold_percent=20.0)
    assert not result


@patch("src.utils.resource_monitor.PSUTIL_AVAILABLE", False)
def test_check_disk_space_no_psutil() -> None:
    """Test disk space checking without psutil."""
    # Should return True (assume adequate space)
    result = check_disk_space()
    assert result


@patch("src.utils.resource_monitor.PSUTIL_AVAILABLE", True)
@patch("src.utils.resource_monitor.psutil.virtual_memory")
@patch("src.utils.resource_monitor.psutil.Process")
def test_get_memory_usage(mock_process: Any, mock_virtual_memory: Any) -> None:
    """Test memory usage statistics."""
    # Mock memory info
    mock_memory = MagicMock()
    mock_memory.total = 16 * 1024**3  # 16GB
    mock_memory.used = 8 * 1024**3  # 8GB used
    mock_memory.available = 8 * 1024**3  # 8GB available
    mock_memory.percent = 50.0  # 50% usage
    mock_virtual_memory.return_value = mock_memory

    # Mock process info
    mock_process_instance = MagicMock()
    mock_process_instance.memory_info.return_value.rss = 1 * 1024**3  # 1GB
    mock_process_instance.memory_info.return_value.vms = 2 * 1024**3  # 2GB
    mock_process.return_value = mock_process_instance

    memory_info = get_memory_usage()

    assert "total_gb" in memory_info
    assert "available_gb" in memory_info
    assert "used_gb" in memory_info
    assert "percent" in memory_info
    assert "process_rss_gb" in memory_info
    assert "process_vms_gb" in memory_info

    assert memory_info["total_gb"] == 16.0
    assert memory_info["used_gb"] == 8.0
    assert memory_info["available_gb"] == 8.0
    assert memory_info["percent"] == 50.0
    assert memory_info["process_rss_gb"] == 1.0
    assert memory_info["process_vms_gb"] == 2.0


@patch("src.utils.resource_monitor.PSUTIL_AVAILABLE", False)
def test_get_memory_usage_no_psutil() -> None:
    """Test memory usage without psutil."""
    memory_info = get_memory_usage()
    assert "error" in memory_info
    assert memory_info["error"] == "psutil not available"


def test_log_resource_summary() -> None:
    """Test resource summary logging."""
    # Should not raise any exceptions
    log_resource_summary()


def test_monitor_parallel_execution() -> None:
    """Test parallel execution monitoring."""
    # Should not raise any exceptions
    monitor_parallel_execution(4, "test_operation")


@patch("src.utils.resource_monitor.PSUTIL_AVAILABLE", True)
@patch("src.utils.resource_monitor.psutil.virtual_memory")
@patch("src.utils.resource_monitor.psutil.Process")
def test_memory_cap_guard(mock_process: Any, mock_virtual_memory: Any) -> None:
    """Test memory cap guardrail."""
    # Mock low memory system
    mock_memory = MagicMock()
    mock_memory.total = 4 * 1024**3  # 4GB total
    mock_virtual_memory.return_value = mock_memory

    # Mock high memory process
    mock_process_instance = MagicMock()
    mock_process_instance.memory_info.return_value.rss = 2 * 1024**3  # 2GB
    mock_process.return_value = mock_process_instance

    # Test memory cap calculation
    workers = calculate_optimal_workers(memory_cap_percent=75.0)

    # Should reduce workers due to memory constraints
    # With 4GB total, 75% cap = 3GB, 2GB per worker = max 1 worker
    assert workers <= 1


def test_worker_count_limits() -> None:
    """Test worker count limits."""
    cpu_count = os.cpu_count() or 1

    # Test default calculation
    workers = calculate_optimal_workers()
    assert workers > 0
    assert workers <= cpu_count

    # Test with user override
    workers = calculate_optimal_workers(requested_workers=cpu_count + 10)
    assert workers == cpu_count  # Should cap at CPU count

    # Test with reasonable override
    if cpu_count > 1:
        workers = calculate_optimal_workers(requested_workers=cpu_count - 1)
        assert workers == cpu_count - 1


def test_error_handling() -> None:
    """Test error handling in resource monitoring."""
    # Test with mocked exceptions
    with patch(
        "src.utils.resource_monitor.psutil.virtual_memory",
        side_effect=Exception("Test error"),
    ):
        with patch("src.utils.resource_monitor.PSUTIL_AVAILABLE", True):
            # Should handle exceptions gracefully
            info = get_system_info()
            assert "psutil_available" in info
            assert info["psutil_available"]  # Should still be True even if call fails

    # Test memory estimation with exceptions
    with patch(
        "src.utils.resource_monitor.psutil.Process",
        side_effect=Exception("Test error"),
    ):
        with patch("src.utils.resource_monitor.PSUTIL_AVAILABLE", True):
            memory = estimate_memory_per_worker()
            assert memory > 0  # Should return fallback value


def test_memory_percentage_calculation() -> None:
    """Test memory percentage calculation."""
    with patch("src.utils.resource_monitor.PSUTIL_AVAILABLE", True):
        with patch(
            "src.utils.resource_monitor.psutil.virtual_memory",
        ) as mock_virtual_memory:
            # Mock 50% memory usage
            mock_memory = MagicMock()
            mock_memory.total = 100
            mock_memory.used = 50
            mock_memory.available = 50
            mock_memory.percent = 50.0
            mock_virtual_memory.return_value = mock_memory

            memory_info = get_memory_usage()
            assert memory_info["percent"] == 50.0


if __name__ == "__main__":
    pytest.main([__file__])
