"""
Parallel execution utilities for the pipeline.

This module provides parallel execution capabilities using joblib,
with support for different backends and resource monitoring.
"""

from typing import Any, Callable, List, Optional

from src.utils.logging_utils import get_logger
from src.utils.resource_monitor import (
    calculate_optimal_workers,
    monitor_parallel_execution,
)

logger = get_logger(__name__)

# Try to import joblib, but don't fail if not available
try:
    from joblib import Parallel, delayed  # type: ignore

    JOBLIB_AVAILABLE = True
except ImportError:
    JOBLIB_AVAILABLE = False
    logger.warning("joblib not available - parallel execution will be disabled")


class ParallelExecutor:
    """Parallel execution wrapper with resource monitoring and fallbacks."""

    def __init__(
        self,
        workers: Optional[int] = None,
        backend: str = "loky",
        chunk_size: int = 1000,
        small_input_threshold: int = 10000,
        disable_parallel: bool = False,
    ):
        """
        Initialize parallel executor.

        Args:
            workers: Number of workers (None for auto)
            backend: Backend to use ('loky' or 'threading')
            chunk_size: Chunk size for parallel processing
            small_input_threshold: Threshold for auto-switching to sequential
            disable_parallel: Force sequential execution
        """
        self.disable_parallel = disable_parallel
        self.small_input_threshold = small_input_threshold

        if not JOBLIB_AVAILABLE or disable_parallel:
            logger.warning(
                "joblib not available or parallel disabled - using sequential execution"
            )
            self.workers = 1
            self.backend = "sequential"
            self.chunk_size = chunk_size
            return

        # Calculate optimal workers
        self.workers = calculate_optimal_workers(workers)

        # Validate backend
        if backend not in ["loky", "threading"]:
            logger.warning(f"Invalid backend '{backend}', using 'loky'")
            backend = "loky"

        # Try to use requested backend, fallback if needed
        if backend == "loky" and not self._test_loky_backend():
            logger.warning("loky backend not available, falling back to threading")
            backend = "threading"

        self.backend = backend
        self.chunk_size = chunk_size

        logger.info(
            f"Parallel executor initialized: workers={self.workers}, backend={self.backend}"
        )

    def _test_loky_backend(self) -> bool:
        """Test if loky backend is available and working."""
        try:
            # Simple test to see if loky works
            result = Parallel(n_jobs=1, backend="loky")(delayed(lambda x: x + 1)(1))
            return isinstance(result, list) and len(result) == 1 and result[0] == 2
        except Exception as e:
            logger.debug(f"loky backend test failed: {e}")
            return False

    def should_use_parallel(self, input_size: int) -> bool:
        """
        Determine if parallel execution should be used.

        Args:
            input_size: Size of input data

        Returns:
            True if parallel execution should be used
        """
        if self.disable_parallel:
            return False

        if not JOBLIB_AVAILABLE:
            return False

        if input_size < self.small_input_threshold:
            logger.info(
                f"Input size {input_size} < threshold {self.small_input_threshold}, using sequential"
            )
            return False

        if self.workers <= 1:
            return False

        return True

    def execute(
        self,
        func: Callable[[Any], Any],
        items: List[Any],
        operation_name: str = "parallel_operation",
    ) -> List[Any]:
        """
        Execute function in parallel or sequentially.

        Args:
            func: Function to execute
            items: List of items to process
            operation_name: Name of operation for logging

        Returns:
            List of results
        """
        input_size = len(items)

        if not self.should_use_parallel(input_size):
            logger.info(f"Executing {operation_name} sequentially (size: {input_size})")
            return [func(item) for item in items]

        # Monitor parallel execution
        monitor_parallel_execution(self.workers, operation_name)

        logger.info(
            f"Executing {operation_name} in parallel: "
            f"workers={self.workers}, backend={self.backend}, "
            f"chunk_size={self.chunk_size}, items={input_size}"
        )

        try:
            results = Parallel(
                n_jobs=self.workers,
                backend=self.backend,
                batch_size=self.chunk_size,
                verbose=0,
            )(delayed(func)(item) for item in items)

            logger.info(f"Completed {operation_name}: {len(results)} results")
            return list(results)

        except Exception as e:
            logger.error(f"Parallel execution failed: {e}")
            logger.info(f"Falling back to sequential execution for {operation_name}")
            return [func(item) for item in items]

    def execute_chunked(
        self,
        func: Callable[[List[Any]], Any],
        items: List[Any],
        chunk_size: Optional[int] = None,
        operation_name: str = "parallel_operation",
    ) -> List[Any]:
        """
        Execute function in parallel with custom chunking.

        Args:
            func: Function to execute
            items: List of items to process
            chunk_size: Custom chunk size (None for default)
            operation_name: Name of operation for logging

        Returns:
            List of results
        """
        if chunk_size is None:
            chunk_size = self.chunk_size

        input_size = len(items)

        if not self.should_use_parallel(input_size):
            logger.info(f"Executing {operation_name} sequentially (size: {input_size})")
            # For sequential execution, process items in chunks to match parallel behavior
            chunks = [
                items[i : i + chunk_size] for i in range(0, len(items), chunk_size)
            ]
            results = []
            for chunk in chunks:
                chunk_result = func(chunk)
                if isinstance(chunk_result, list):
                    results.extend(chunk_result)
                else:
                    results.append(chunk_result)
            return results

        # Create chunks
        chunks = [items[i : i + chunk_size] for i in range(0, len(items), chunk_size)]

        logger.info(
            f"Executing {operation_name} in parallel: "
            f"workers={self.workers}, backend={self.backend}, "
            f"chunks={len(chunks)}, chunk_size={chunk_size}, items={input_size}"
        )

        # Monitor parallel execution
        monitor_parallel_execution(self.workers, operation_name)

        try:
            results = Parallel(n_jobs=self.workers, backend=self.backend, verbose=0)(
                delayed(func)(chunk) for chunk in chunks
            )

            # Flatten results
            flattened_results = []
            for chunk_result in results:
                if isinstance(chunk_result, list):
                    flattened_results.extend(chunk_result)
                else:
                    flattened_results.append(chunk_result)

            logger.info(f"Completed {operation_name}: {len(flattened_results)} results")
            return flattened_results

        except Exception as e:
            logger.error(f"Parallel execution failed: {e}")
            logger.info(f"Falling back to sequential execution for {operation_name}")
            # For error fallback, process items in chunks to match parallel behavior
            chunks = [
                items[i : i + chunk_size] for i in range(0, len(items), chunk_size)
            ]
            results = []
            for chunk in chunks:
                chunk_result = func(chunk)
                if isinstance(chunk_result, list):
                    results.extend(chunk_result)
                else:
                    results.append(chunk_result)
            return results


def create_parallel_executor(
    workers: Optional[int] = None,
    backend: str = "loky",
    chunk_size: int = 1000,
    small_input_threshold: int = 10000,
    disable_parallel: bool = False,
) -> ParallelExecutor:
    """
    Create a parallel executor with the specified configuration.

    Args:
        workers: Number of workers (None for auto)
        backend: Backend to use ('loky' or 'threading')
        chunk_size: Chunk size for parallel processing
        small_input_threshold: Threshold for auto-switching to sequential
        disable_parallel: Force sequential execution

    Returns:
        Configured ParallelExecutor instance
    """
    return ParallelExecutor(
        workers=workers,
        backend=backend,
        chunk_size=chunk_size,
        small_input_threshold=small_input_threshold,
        disable_parallel=disable_parallel,
    )


def ensure_deterministic_order(
    results: List[Any], sort_key: Optional[Callable] = None
) -> List[Any]:
    """
    Ensure deterministic ordering of parallel results.

    Args:
        results: List of results from parallel execution
        sort_key: Optional function to extract sort key

    Returns:
        Sorted results
    """
    if sort_key is None:
        # Default sort for common data types
        return sorted(results)
    else:
        return sorted(results, key=sort_key)


def log_parallel_config(
    workers: int, backend: str, chunk_size: int, input_size: int
) -> None:
    """Log parallel execution configuration."""
    logger.info("=== Parallel Configuration ===")
    logger.info(f"Workers: {workers}")
    logger.info(f"Backend: {backend}")
    logger.info(f"Chunk size: {chunk_size}")
    logger.info(f"Input size: {input_size}")
    logger.info(f"Joblib available: {JOBLIB_AVAILABLE}")
    logger.info("==============================")
