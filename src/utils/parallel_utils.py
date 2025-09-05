"""Parallel execution utilities for the pipeline.

This module provides parallel execution capabilities using joblib,
with support for different backends and resource monitoring.
"""

import os
import threading
from collections.abc import Iterable
from typing import Any, Callable, Dict, List, Optional, Tuple

from src.utils.logging_utils import get_logger
from src.utils.path_utils import get_config_path
from src.utils.resource_monitor import (
    calculate_optimal_workers,
    monitor_parallel_execution,
)

logger = get_logger(__name__)


def _load_parallelism_settings() -> Dict[str, Any]:
    """Load parallelism settings from config file."""
    try:
        import yaml

        config_path = get_config_path()
        with open(config_path) as f:
            config = yaml.safe_load(f)
            if isinstance(config, dict):
                parallelism_config = config.get("parallelism", {})
                return (
                    parallelism_config if isinstance(parallelism_config, dict) else {}
                )
            return {}
    except Exception:
        # Return default settings if config loading fails
        return {
            "backend": "loky",
            "chunk_size": 1000,
            "small_input_threshold": 10000,
        }


def get_optimal_workers() -> int:
    """Get optimal number of workers based on system resources."""
    import os

    # Get CPU count
    cpu_count = os.cpu_count() or 1

    # Default to min(8, cpu_count) for stability
    optimal = min(8, cpu_count)

    # Load from config if available
    try:
        settings = _load_parallelism_settings()
        if settings.get("workers") == "auto":
            return optimal
        if "workers" in settings and settings["workers"] is not None:
            workers = settings["workers"]
            if isinstance(workers, int):
                return max(1, min(workers, cpu_count))
    except Exception:
        pass

    return optimal


def get_chunk_size_pairs() -> int:
    """Get chunk size for pair processing."""
    try:
        settings = _load_parallelism_settings()
        chunk_size = settings.get("chunk_size_pairs", 300000)
        return chunk_size if isinstance(chunk_size, int) else 300000
    except Exception:
        return 300000


# Try to import joblib, but don't fail if not available
try:
    from joblib import Parallel, delayed  # type: ignore

    JOBLIB_AVAILABLE = True
except ImportError:
    JOBLIB_AVAILABLE = False
    logger.warning("joblib not available - parallel execution will be disabled")

# Cache for loky availability test
_LOKY_AVAILABLE: bool | None = None


def ensure_single_thread_blas() -> None:
    """Set BLAS environment variables to 1 if not already set by user.

    This prevents oversubscription on Apple Silicon and other multi-core systems
    when using parallel processing.
    """
    blas_vars = [
        "OMP_NUM_THREADS",
        "OPENBLAS_NUM_THREADS",
        "MKL_NUM_THREADS",
        "VECLIB_MAXIMUM_THREADS",
        "NUMEXPR_MAX_THREADS",
    ]

    for var in blas_vars:
        if var not in os.environ:
            os.environ[var] = "1"
            logger.debug(f"Set {var}=1 for single-threaded BLAS")


def parallel_map(
    func: Callable[[Any], Any],
    items: List[Any],
    workers: Optional[int] = None,
    backend: Optional[str] = None,
    chunk_size: Optional[int] = None,
) -> List[Any]:
    """Parallel map function using joblib with deterministic ordering.

    Args:
        func: Function to apply to each item
        items: List of items to process
        workers: Number of workers (None for auto-detection)
        backend: Backend to use ('loky' or 'threading')
        chunk_size: Chunk size for parallel processing

    Returns:
        List of results in the same order as input items

    """
    # Load settings if not provided
    if backend is None or chunk_size is None:
        settings = _load_parallelism_settings()
        if backend is None:
            backend = settings.get("backend", "loky")
        if chunk_size is None:
            chunk_size = settings.get("chunk_size", 1000)

    if not JOBLIB_AVAILABLE or workers is None or workers <= 1:
        # Sequential fallback
        return [func(item) for item in items]

    # Ensure BLAS is single-threaded
    ensure_single_thread_blas()

    # Use joblib parallel execution
    results = Parallel(n_jobs=workers, backend=backend, batch_size=chunk_size)(
        delayed(func)(item) for item in items
    )

    # Ensure we return a list (joblib Parallel returns Any)
    return list(results)


def execute_chunked(
    func: Callable[[List[Any]], List[Any]],
    items: List[Any],
    workers: Optional[int] = None,
    backend: Optional[str] = None,
    chunk_size: Optional[int] = None,
) -> List[Any]:
    """Execute function on chunks of items in parallel.

    This is optimized for processing large datasets where the function
    can handle batches efficiently.

    Args:
        func: Function to apply to chunks of items
        items: List of items to process
        workers: Number of workers (None for auto-detection)
        backend: Backend to use ('loky' or 'threading')
        chunk_size: Chunk size for parallel processing

    Returns:
        List of results in the same order as input items

    """
    if not items:
        return []

    # Load settings if not provided
    if workers is None:
        workers = get_optimal_workers()
    if backend is None:
        backend = _load_parallelism_settings().get("backend", "loky")
    if chunk_size is None:
        chunk_size = get_chunk_size_pairs()

    # Create chunks
    chunks = [items[i : i + chunk_size] for i in range(0, len(items), chunk_size)]

    if not JOBLIB_AVAILABLE or workers <= 1:
        # Sequential fallback
        results = []
        for chunk in chunks:
            results.extend(func(chunk))
        return results

    # Ensure BLAS is single-threaded
    ensure_single_thread_blas()

    # Use joblib parallel execution on chunks
    chunk_results = Parallel(n_jobs=workers, backend=backend)(
        delayed(func)(chunk) for chunk in chunks
    )

    # Flatten results
    results = []
    for chunk_result in chunk_results:
        results.extend(chunk_result)

    return results


def _try_import_joblib() -> bool:
    """Try to import joblib - separated for testing."""
    try:
        import importlib.util

        return importlib.util.find_spec("joblib") is not None
    except ImportError:
        return False


def is_loky_available() -> bool:
    """Test if loky backend is available and working.

    Returns:
        True if loky backend can be used, False otherwise

    """
    global _LOKY_AVAILABLE

    if _LOKY_AVAILABLE is not None:
        return _LOKY_AVAILABLE

    if not _try_import_joblib():
        _LOKY_AVAILABLE = False
        return _LOKY_AVAILABLE

    # Test loky backend
    loky_works = False
    try:
        # Simple test to see if loky works
        result = Parallel(n_jobs=1, backend="loky")(delayed(lambda: 1)() for _ in [0])
        # Ensure result is a list and contains expected value
        loky_works = bool(
            isinstance(result, list) and len(result) == 1 and result[0] == 1,
        )
    except Exception as e:
        logger.debug(f"loky backend test failed: {e}")
        loky_works = False

    _LOKY_AVAILABLE = loky_works
    return _LOKY_AVAILABLE


def select_backend(requested: str) -> Tuple[str, str]:
    """Select the appropriate backend based on availability and platform.

    Args:
        requested: Requested backend ('loky' or 'threading')

    Returns:
        Tuple of (chosen_backend, reason)

    """
    if not JOBLIB_AVAILABLE:
        return "sequential", "joblib_unavailable"

    if requested == "threading":
        return "threading", "requested"

    if requested == "loky":
        if is_loky_available():
            return "loky", "requested"
        return "threading", "fallback_unsupported"

    # Default case
    if is_loky_available():
        return "loky", "default"
    return "threading", "fallback_unsupported"


class ParallelExecutor:
    """Parallel execution wrapper with resource monitoring and fallbacks."""

    def __init__(
        self,
        workers: Optional[int] = None,
        backend: str = "loky",
        chunk_size: int = 1000,
        small_input_threshold: int = 10000,
        disable_parallel: bool = False,
        stop_flag: Optional[threading.Event] = None,
    ):
        """Initialize parallel executor.

        Args:
            workers: Number of workers (None for auto)
            backend: Backend to use ('loky' or 'threading')
            chunk_size: Chunk size for parallel processing
            small_input_threshold: Threshold for auto-switching to sequential
            disable_parallel: Force sequential execution
            stop_flag: Optional threading.Event for graceful interruption

        """
        self.disable_parallel = disable_parallel
        self.small_input_threshold = small_input_threshold
        self.stop_flag = stop_flag or threading.Event()

        if not JOBLIB_AVAILABLE or disable_parallel:
            logger.warning(
                "joblib not available or parallel disabled - using sequential execution",
            )
            self.workers = 1
            self.backend = "sequential"
            self.chunk_size = chunk_size
            self.backend_reason = (
                "joblib_unavailable" if not JOBLIB_AVAILABLE else "disabled"
            )
        else:
            # Calculate optimal workers
            self.workers = calculate_optimal_workers(workers)

            # Select backend with explicit logging
            chosen_backend, reason = select_backend(backend)
            self.backend = chosen_backend
            self.backend_reason = reason
            self.chunk_size = chunk_size

            logger.info(
                f"Parallel executor initialized | requested={backend}, chosen={chosen_backend}, "
                f"reason={reason}, workers={self.workers}",
            )

    def should_use_parallel(self, input_size: int) -> bool:
        """Determine if parallel execution should be used.

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
                f"Input size {input_size} < threshold {self.small_input_threshold}, using sequential",
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
        """Execute function in parallel or sequentially.

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
            results = []
            for item in items:
                if self.stop_flag.is_set():
                    logger.info(f"Stop flag set, interrupting {operation_name}")
                    break
                results.append(func(item))
            return results

        # Monitor parallel execution
        monitor_parallel_execution(self.workers, operation_name)

        logger.info(
            f"Executing {operation_name} in parallel: "
            f"workers={self.workers}, backend={self.backend}, "
            f"chunk_size={self.chunk_size}, items={input_size}",
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
            results = []
            for item in items:
                if self.stop_flag.is_set():
                    logger.info(f"Stop flag set, interrupting {operation_name}")
                    break
                results.append(func(item))
            return results

    def execute_chunked(
        self,
        func: Callable[[List[Any]], Any],
        items: List[Any],
        chunk_size: Optional[int] = None,
        operation_name: str = "parallel_operation",
    ) -> List[Any]:
        """Execute function in parallel with custom chunking.

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

        # Create balanced chunks: target ~N_workers × 2-4 chunks
        target_chunks = min(
            max(self.workers * 3, 1),
            max(1, (input_size + chunk_size - 1) // chunk_size),
        )
        balanced_chunk_size = max(1, (input_size + target_chunks - 1) // target_chunks)

        # Ensure minimum chunk size unless input is tiny
        if input_size > 100:  # Configurable minimum
            balanced_chunk_size = max(balanced_chunk_size, 100)

        # Create chunks with balanced sizing
        chunks = [
            items[i : i + balanced_chunk_size]
            for i in range(0, len(items), balanced_chunk_size)
        ]

        # Log parallel plan summary
        if self.should_use_parallel(input_size):
            logger.info(
                f"Parallel plan: N={input_size}, chunks={len(chunks)}, "
                f"avg_size={balanced_chunk_size}, strategy=parallel "
                f"(workers={self.workers}, backend={self.backend})",
            )
        else:
            logger.info(
                f"Sequential plan: N={input_size}, chunks={len(chunks)}, "
                f"avg_size={balanced_chunk_size}, strategy=sequential "
                f"(reason=input_size < {self.small_input_threshold})",
            )

        if not self.should_use_parallel(input_size):
            logger.info(f"Executing {operation_name} sequentially (size: {input_size})")
            # For sequential execution, process items in chunks to match parallel behavior
            results = []
            for chunk in chunks:
                if self.stop_flag.is_set():
                    logger.info(f"Stop flag set, interrupting {operation_name}")
                    break
                chunk_result = func(chunk)
                if isinstance(chunk_result, list):
                    results.extend(chunk_result)
                else:
                    results.append(chunk_result)
            return results

        # Monitor parallel execution
        monitor_parallel_execution(self.workers, operation_name)

        logger.info(
            f"Executing {operation_name} in parallel: "
            f"workers={self.workers}, backend={self.backend}, "
            f"chunks={len(chunks)}, chunk_size={balanced_chunk_size}, items={input_size}",
        )

        try:
            results = Parallel(
                n_jobs=self.workers,
                backend=self.backend,
                batch_size=balanced_chunk_size,
                verbose=0,
            )(delayed(func)(chunk) for chunk in chunks)

            logger.info(f"Completed {operation_name}: {len(results)} results")
            return list(results)

        except Exception as e:
            logger.error(f"Parallel execution failed: {e}")
            logger.info(f"Falling back to sequential execution for {operation_name}")
            results = []
            for chunk in chunks:
                if self.stop_flag.is_set():
                    logger.info(f"Stop flag set, interrupting {operation_name}")
                    break
                chunk_result = func(chunk)
                if isinstance(chunk_result, list):
                    results.extend(chunk_result)
                else:
                    results.append(chunk_result)
            return results

    def map(
        self,
        fn: Callable[[Any], Any],
        items: Iterable[Any],
        *,
        chunksize: Optional[int] = None,
    ) -> Iterable[Any]:
        """Apply function to items in parallel.

        This method implements the ExecutorLike protocol.

        Args:
            fn: Function to apply to each item
            items: Iterable of items to process
            chunksize: Optional chunk size for batching

        Returns:
            Iterable of results

        """
        # Convert items to list for processing
        items_list = list(items)

        if not items_list:
            return []

        # Use the existing execute method
        results = self.execute(fn, items_list)

        return results


def create_parallel_executor(
    workers: Optional[int] = None,
    backend: Optional[str] = None,
    chunk_size: Optional[int] = None,
    small_input_threshold: Optional[int] = None,
    disable_parallel: bool = False,
    stop_flag: Optional[threading.Event] = None,
) -> ParallelExecutor:
    """Create a parallel executor with the specified configuration.

    Args:
        workers: Number of workers (None for auto)
        backend: Backend to use ('loky' or 'threading')
        chunk_size: Chunk size for parallel processing
        small_input_threshold: Threshold for auto-switching to sequential
        disable_parallel: Force sequential execution
        stop_flag: Optional threading.Event for graceful interruption

    Returns:
        Configured ParallelExecutor instance

    """
    # Load settings if not provided
    if backend is None or chunk_size is None or small_input_threshold is None:
        settings = _load_parallelism_settings()
        if backend is None:
            backend = settings.get("backend", "loky")
        if chunk_size is None:
            chunk_size = settings.get("chunk_size", 1000)
        if small_input_threshold is None:
            small_input_threshold = settings.get("small_input_threshold", 10000)

    return ParallelExecutor(
        workers=workers,
        backend=backend,
        chunk_size=chunk_size,
        small_input_threshold=small_input_threshold,
        disable_parallel=disable_parallel,
        stop_flag=stop_flag,
    )


def ensure_deterministic_order(
    results: List[Any], sort_key: Optional[Callable] = None,
) -> List[Any]:
    """Ensure deterministic ordering of parallel results.

    Args:
        results: List of results from parallel execution
        sort_key: Optional function to extract sort key

    Returns:
        Sorted results

    """
    if sort_key is None:
        # Default sort for common data types
        return sorted(results)
    return sorted(results, key=sort_key)


def log_parallel_config(
    workers: int, backend: str, chunk_size: int, input_size: int,
) -> None:
    """Log parallel execution configuration."""
    logger.info("=== Parallel Configuration ===")
    logger.info(f"Workers: {workers}")
    logger.info(f"Backend: {backend}")
    logger.info(f"Chunk size: {chunk_size}")
    logger.info(f"Input size: {input_size}")
    logger.info(f"Joblib available: {JOBLIB_AVAILABLE}")
    logger.info("==============================")
