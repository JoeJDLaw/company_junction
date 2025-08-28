"""
Performance utilities for the company junction pipeline.
"""

import logging
import time
import tracemalloc
from contextlib import contextmanager


@contextmanager
def log_perf(label: str):
    """
    Context manager for performance logging with timing and memory metrics.

    Args:
        label: Label for the performance measurement

    Yields:
        None
    """
    logger = logging.getLogger(__name__)
    tracemalloc.start()
    t0 = time.time()
    try:
        yield
    finally:
        dt = time.time() - t0
        current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        logger.info(
            "%s: time=%.2fs, py_mem_current=%.1f MB, py_mem_peak=%.1f MB",
            label,
            dt,
            current / 1e6,
            peak / 1e6,
        )
