"""
Performance profiling utilities for the Company Junction pipeline.

This module provides tools for:
- Memory usage tracking and peak detection
- Stage timing with automatic logging
- Performance regression detection
- Baseline comparison utilities
"""

from __future__ import annotations

import json
import logging
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Dict, Optional, Iterator

try:
    import psutil  # type: ignore

    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False


def get_memory_usage() -> Dict[str, float]:
    """
    Get current memory usage in MB.

    Returns:
        Dictionary with memory usage metrics in MB
    """
    if not PSUTIL_AVAILABLE:
        # Fallback when psutil is not available
        return {"rss": 0.0, "vms": 0.0}

    try:
        process = psutil.Process()
        memory_info = process.memory_info()
        return {
            "rss": memory_info.rss / 1024 / 1024,  # Resident Set Size
            "vms": memory_info.vms / 1024 / 1024,  # Virtual Memory Size
        }
    except Exception:
        return {"rss": 0.0, "vms": 0.0}


def log_memory_usage(stage: str, logger: logging.Logger) -> None:
    """
    Log current memory usage for a stage.

    Args:
        stage: Name of the pipeline stage
        logger: Logger instance to use
    """
    memory = get_memory_usage()
    logger.info(
        f"Memory usage at '{stage}': RSS={memory['rss']:.1f}MB, VMS={memory['vms']:.1f}MB"
    )


@contextmanager
def track_memory_peak(stage: str, logger: logging.Logger) -> Iterator[None]:
    """
    Context manager to track peak memory usage during a stage.

    Args:
        stage: Name of the pipeline stage
        logger: Logger instance to use
    """
    start_memory = get_memory_usage()
    peak_memory = start_memory.copy()

    try:
        yield
    finally:
        end_memory = get_memory_usage()

        # Update peak if current usage is higher
        for key in peak_memory:
            peak_memory[key] = max(peak_memory[key], end_memory[key])

        memory_delta = end_memory["rss"] - start_memory["rss"]
        peak_delta = peak_memory["rss"] - start_memory["rss"]

        logger.info(
            f"Stage '{stage}' memory: start={start_memory['rss']:.1f}MB, "
            f"end={end_memory['rss']:.1f}MB, peak={peak_memory['rss']:.1f}MB, "
            f"delta={memory_delta:+.1f}MB, peak_delta={peak_delta:+.1f}MB"
        )


@contextmanager
def time_stage(stage: str, logger: logging.Logger) -> Iterator[None]:
    """
    Context manager for timing pipeline stages.

    Args:
        stage: Name of the pipeline stage
        logger: Logger instance to use
    """
    start_time = time.time()
    start_memory = get_memory_usage()

    try:
        yield
    finally:
        end_time = time.time()
        end_memory = get_memory_usage()

        duration = end_time - start_time
        memory_delta = end_memory["rss"] - start_memory["rss"]

        logger.info(
            f"Stage '{stage}' completed in {duration:.2f}s, "
            f"memory delta: {memory_delta:+.1f}MB"
        )


def get_stage_timing() -> Dict[str, float]:
    """
    Get timing data for all stages (placeholder for future implementation).

    Returns:
        Dictionary with stage timing data
    """
    # This would be populated by the timing context managers
    # For now, return empty dict
    return {}


def log_performance_summary(logger: logging.Logger) -> None:
    """
    Log a performance summary for the entire pipeline.

    Args:
        logger: Logger instance to use
    """
    final_memory = get_memory_usage()
    logger.info(
        f"Pipeline performance summary: "
        f"Final memory usage: RSS={final_memory['rss']:.1f}MB, "
        f"VMS={final_memory['vms']:.1f}MB"
    )


def compare_performance(
    baseline: Dict[str, float], current: Dict[str, float], threshold: float = 0.1
) -> bool:
    """
    Compare current performance against baseline.

    Args:
        baseline: Baseline performance metrics
        current: Current performance metrics
        threshold: Acceptable change threshold (default: 10%)

    Returns:
        True if no regression detected, False otherwise
    """
    for metric, baseline_value in baseline.items():
        if metric in current:
            current_value = current[metric]
            if baseline_value > 0:
                change = abs(current_value - baseline_value) / baseline_value
                if change > threshold:
                    return False  # Regression detected
    return True  # No regression


def save_performance_baseline(data: Dict[str, float], filepath: str) -> None:
    """
    Save performance baseline to file.

    Args:
        data: Performance data to save
        filepath: Path to save the baseline file
    """
    path = Path(filepath)
    path.parent.mkdir(parents=True, exist_ok=True)

    with open(path, "w") as f:
        json.dump(data, f, indent=2)


def load_performance_baseline(filepath: str) -> Optional[Dict[str, float]]:
    """
    Load performance baseline from file.

    Args:
        filepath: Path to the baseline file

    Returns:
        Performance baseline data or None if file doesn't exist
    """
    path = Path(filepath)
    if not path.exists():
        return None

    try:
        with open(path, "r") as f:
            data = json.load(f)
            if isinstance(data, dict):
                return data
            return None
    except Exception:
        return None


def detect_performance_regression(
    baseline_file: str,
    current_metrics: Dict[str, float],
    threshold: float = 0.1,
    logger: Optional[logging.Logger] = None,
) -> bool:
    """
    Detect performance regression by comparing against baseline.

    Args:
        baseline_file: Path to baseline file
        current_metrics: Current performance metrics
        threshold: Acceptable change threshold
        logger: Logger instance for warnings

    Returns:
        True if regression detected, False otherwise
    """
    baseline = load_performance_baseline(baseline_file)
    if baseline is None:
        if logger:
            logger.warning(f"No baseline file found at {baseline_file}")
        return False

    is_regression = not compare_performance(baseline, current_metrics, threshold)

    if is_regression and logger:
        logger.warning(
            f"Performance regression detected! "
            f"Current metrics: {current_metrics}, "
            f"Baseline: {baseline}, "
            f"Threshold: {threshold}"
        )

    return is_regression


# Legacy function for backward compatibility
@contextmanager
def log_perf(label: str) -> Iterator[None]:
    """
    Legacy context manager for performance logging (backward compatibility).

    Args:
        label: Label for the performance measurement
    """
    logger = logging.getLogger(__name__)
    start_time = time.time()
    start_memory = get_memory_usage()

    try:
        yield
    finally:
        end_time = time.time()
        end_memory = get_memory_usage()

        duration = end_time - start_time
        memory_delta = end_memory["rss"] - start_memory["rss"]

        logger.info(
            f"{label}: time={duration:.2f}s, memory_delta={memory_delta:+.1f}MB"
        )
