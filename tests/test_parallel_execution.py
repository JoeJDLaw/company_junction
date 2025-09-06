"""Tests for parallel execution functionality.

This module tests:
- Determinism of parallel execution
- Worker count variations
- Backend comparisons
- Resource monitoring
"""

from typing import Any

import pytest

from src.utils.parallel_utils import (
    create_parallel_executor,
    ensure_deterministic_order,
)
from src.utils.resource_monitor import (
    calculate_optimal_workers,
    estimate_memory_per_worker,
    get_system_info,
)


def test_parallel_executor_initialization() -> None:
    """Test parallel executor initialization with different configurations."""
    # Test with default settings
    executor = create_parallel_executor()
    assert executor is not None
    assert hasattr(executor, "workers")
    assert hasattr(executor, "backend")
    assert hasattr(executor, "chunk_size")

    # Test with custom settings
    executor = create_parallel_executor(
        workers=2,
        backend="threading",
        chunk_size=500,
        disable_parallel=True,
    )
    assert executor.workers == 1  # Should be 1 when disabled
    assert executor.backend == "sequential"
    assert executor.chunk_size == 500


def test_should_use_parallel() -> None:
    """Test the should_use_parallel logic."""
    executor = create_parallel_executor(workers=2, disable_parallel=False)

    # Should use parallel for large inputs (if joblib is available)
    if executor.backend != "sequential":
        assert executor.should_use_parallel(15000)
    else:
        # If joblib not available, should not use parallel
        assert not executor.should_use_parallel(15000)

    # Should not use parallel for small inputs
    assert not executor.should_use_parallel(5000)

    # Should not use parallel when disabled
    executor.disable_parallel = True
    assert not executor.should_use_parallel(15000)


def test_deterministic_execution() -> None:
    """Test that parallel execution produces deterministic results."""
    # Create test data
    test_items = list(range(100))

    def test_function(x: int) -> dict[str, Any]:
        """Test function that returns a dictionary."""
        return {"value": x, "squared": x**2, "string": f"item_{x}"}

    # Test sequential execution
    executor_seq = create_parallel_executor(disable_parallel=True)
    results_seq = executor_seq.execute(test_function, test_items, "test_sequential")

    # Test parallel execution
    executor_par = create_parallel_executor(workers=2, disable_parallel=False)
    results_par = executor_par.execute(test_function, test_items, "test_parallel")

    # Results should be identical (after sorting)
    results_seq_sorted = sorted(results_seq, key=lambda x: x["value"])
    results_par_sorted = sorted(results_par, key=lambda x: x["value"])

    assert results_seq_sorted == results_par_sorted


def test_worker_count_variations() -> None:
    """Test execution with different worker counts."""
    test_items = list(range(50))

    def test_function(x: int) -> int:
        return x * 2

    # Test with different worker counts
    for workers in [1, 2, 4]:
        executor = create_parallel_executor(workers=workers)
        results = executor.execute(test_function, test_items, f"test_workers_{workers}")

        # Results should be the same regardless of worker count
        expected = [x * 2 for x in test_items]
        assert sorted(results) == sorted(expected)


def test_backend_comparison() -> None:
    """Test execution with different backends."""
    test_items = list(range(20))

    def test_function(x: int) -> dict[str, Any]:
        return {"input": x, "output": x * 3}

    # Test threading backend
    executor_thread = create_parallel_executor(backend="threading", workers=2)
    results_thread = executor_thread.execute(
        test_function,
        test_items,
        "test_threading",
    )

    # Test sequential execution
    executor_seq = create_parallel_executor(disable_parallel=True)
    results_seq = executor_seq.execute(test_function, test_items, "test_sequential")

    # Results should be identical (after sorting)
    results_thread_sorted = sorted(results_thread, key=lambda x: x["input"])
    results_seq_sorted = sorted(results_seq, key=lambda x: x["input"])

    assert results_thread_sorted == results_seq_sorted


def test_chunked_execution() -> None:
    """Test chunked parallel execution."""
    test_items = list(range(100))

    def test_function(chunk: list[int]) -> list[dict[str, Any]]:
        return [{"value": x, "processed": True} for x in chunk]

    executor = create_parallel_executor(workers=2, chunk_size=25)

    # If parallel execution is available, test chunked execution
    if executor.backend != "sequential":
        results = executor.execute_chunked(
            test_function,
            test_items,
            operation_name="test_chunked",
        )

        # Should have processed all items
        assert len(results) == 100

        # All items should be processed
        for result in results:
            assert result["processed"]
            assert "value" in result
    else:
        # If sequential, test that it still works
        results = executor.execute_chunked(
            test_function,
            test_items,
            operation_name="test_chunked",
        )
        assert len(results) == 100


def test_ensure_deterministic_order() -> None:
    """Test deterministic ordering of results."""
    # Test with simple list
    test_results = [3, 1, 4, 1, 5, 9, 2, 6]
    ordered = ensure_deterministic_order(test_results)
    assert ordered == [1, 1, 2, 3, 4, 5, 6, 9]

    # Test with dictionaries
    test_dicts = [
        {"id": 3, "name": "c"},
        {"id": 1, "name": "a"},
        {"id": 2, "name": "b"},
    ]
    ordered_dicts = ensure_deterministic_order(test_dicts, lambda x: x["id"])
    assert ordered_dicts == [
        {"id": 1, "name": "a"},
        {"id": 2, "name": "b"},
        {"id": 3, "name": "c"},
    ]


def test_resource_monitoring() -> None:
    """Test resource monitoring functions."""
    # Test system info
    system_info = get_system_info()
    assert "cpu_count" in system_info
    assert "psutil_available" in system_info
    assert system_info["cpu_count"] > 0

    # Test worker calculation
    workers = calculate_optimal_workers()
    assert workers > 0
    assert workers <= system_info["cpu_count"]

    # Test memory estimation
    memory_per_worker = estimate_memory_per_worker()
    assert memory_per_worker > 0


def test_error_handling() -> None:
    """Test error handling in parallel execution."""
    test_items = list(range(10))

    def failing_function(x: int) -> int:
        if x == 5:
            raise ValueError("Test error")
        return x * 2

    executor = create_parallel_executor(workers=2)

    # Should handle errors gracefully
    with pytest.raises(ValueError):
        executor.execute(failing_function, test_items, "test_error")


def test_small_input_guard() -> None:
    """Test that small inputs automatically use sequential execution."""
    test_items = list(range(5))  # Small input

    def test_function(x: int) -> int:
        return x * 2

    executor = create_parallel_executor(workers=4, small_input_threshold=10)
    results = executor.execute(test_function, test_items, "test_small_input")

    # Should use sequential execution for small inputs
    assert len(results) == 5
    assert results == [0, 2, 4, 6, 8]


def test_memory_guard() -> None:
    """Test memory-based worker reduction."""
    # This test is more of a smoke test since we can't easily control system memory
    workers = calculate_optimal_workers(requested_workers=100)
    assert workers > 0
    # Should not exceed reasonable limits
    assert workers <= 100


if __name__ == "__main__":
    pytest.main([__file__])
