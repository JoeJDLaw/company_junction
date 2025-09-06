"""Resource monitoring utilities for the pipeline.

This module provides CPU, memory, and disk usage monitoring
with automatic worker count adjustment based on available resources.
"""

import os
from typing import Any, Optional

from src.utils.logging_utils import get_logger

logger = get_logger(__name__)

# Try to import psutil, but don't fail if not available
try:
    import psutil

    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False
    logger.warning("psutil not available - resource monitoring will be limited")


def get_system_info() -> dict[str, Any]:
    """Get basic system information."""
    info: dict[str, Any] = {
        "cpu_count": os.cpu_count() or 1,
        "psutil_available": PSUTIL_AVAILABLE,
    }

    if PSUTIL_AVAILABLE:
        try:
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage(".")
            info.update(
                {
                    "total_memory_gb": memory.total / (1024**3),
                    "available_memory_gb": memory.available / (1024**3),
                    "memory_percent": memory.percent,
                    "total_disk_gb": disk.total / (1024**3),
                    "free_disk_gb": disk.free / (1024**3),
                    "disk_percent": (disk.used / disk.total) * 100,
                },
            )
        except Exception as e:
            logger.warning(f"Failed to get system info with psutil: {e}")

    return info


def estimate_memory_per_worker() -> float:
    """Estimate memory usage per worker process in GB.

    This is a conservative estimate based on typical pandas operations.
    """
    if not PSUTIL_AVAILABLE:
        # Conservative fallback estimate
        return 2.0  # 2GB per worker

    try:
        # Get current process memory usage as baseline
        current_process = psutil.Process()
        current_memory = float(current_process.memory_info().rss) / (1024**3)  # GB

        # Estimate worker memory (current + overhead for data processing)
        worker_memory = max(current_memory * 1.5, 1.0)  # At least 1GB

        logger.info(f"Estimated memory per worker: {worker_memory:.2f} GB")
        return worker_memory
    except Exception as e:
        logger.warning(f"Failed to estimate worker memory: {e}")
        return 2.0  # Conservative fallback


def calculate_optimal_workers(
    requested_workers: Optional[int] = None,
    memory_cap_percent: float = 75.0,
) -> int:
    """Calculate optimal number of workers based on available resources.

    Args:
        requested_workers: User-requested worker count (None for auto)
        memory_cap_percent: Maximum memory usage percentage (default 75%)

    Returns:
        Optimal number of workers

    """
    cpu_count = os.cpu_count() or 1

    # Default formula from cursor_rules.md
    default_workers = min(cpu_count, max(1, cpu_count - 2))

    if requested_workers is not None:
        # User specified workers - validate against limits
        optimal_workers = min(requested_workers, cpu_count)
        logger.info(f"Using user-requested workers: {optimal_workers}")
        return optimal_workers

    if not PSUTIL_AVAILABLE:
        logger.info(f"psutil not available, using default workers: {default_workers}")
        return default_workers

    try:
        # Get system memory
        memory = psutil.virtual_memory()
        total_memory_gb = memory.total / (1024**3)

        # Estimate memory per worker
        memory_per_worker = estimate_memory_per_worker()

        # Calculate memory-limited workers
        memory_cap_gb = total_memory_gb * (memory_cap_percent / 100.0)
        memory_limited_workers = int(memory_cap_gb / memory_per_worker)

        # Use the more conservative limit
        optimal_workers = min(default_workers, memory_limited_workers, cpu_count)

        logger.info(
            f"Resource analysis: CPU={cpu_count}, "
            f"Memory={total_memory_gb:.1f}GB, "
            f"Memory/worker={memory_per_worker:.1f}GB, "
            f"Memory-limited={memory_limited_workers}, "
            f"Optimal={optimal_workers}",
        )

        return optimal_workers

    except Exception as e:
        logger.warning(f"Failed to calculate optimal workers: {e}")
        return default_workers


def check_disk_space(warning_threshold_percent: float = 20.0) -> bool:
    """Check available disk space and warn if below threshold.

    Args:
        warning_threshold_percent: Warning threshold for free space percentage

    Returns:
        True if disk space is adequate, False if warning threshold exceeded

    """
    if not PSUTIL_AVAILABLE:
        logger.warning("psutil not available - cannot check disk space")
        return True

    try:
        disk = psutil.disk_usage(".")
        free_percent = (disk.free / disk.total) * 100

        if free_percent < warning_threshold_percent:
            logger.warning(
                f"Low disk space: {free_percent:.1f}% free "
                f"({disk.free / (1024**3):.1f} GB)",
            )
            return False
        logger.info(
            f"Disk space OK: {free_percent:.1f}% free "
            f"({disk.free / (1024**3):.1f} GB)",
        )
        return True

    except Exception as e:
        logger.warning(f"Failed to check disk space: {e}")
        return True


def get_memory_usage() -> dict:
    """Get current memory usage statistics."""
    if not PSUTIL_AVAILABLE:
        return {"error": "psutil not available"}

    try:
        memory = psutil.virtual_memory()
        current_process = psutil.Process()
        process_memory = current_process.memory_info()

        return {
            "total_gb": memory.total / (1024**3),
            "available_gb": memory.available / (1024**3),
            "used_gb": memory.used / (1024**3),
            "percent": memory.percent,
            "process_rss_gb": process_memory.rss / (1024**3),
            "process_vms_gb": process_memory.vms / (1024**3),
        }
    except Exception as e:
        return {"error": str(e)}


def log_resource_summary() -> None:
    """Log a summary of current resource usage."""
    system_info = get_system_info()
    memory_info = get_memory_usage()

    logger.info("=== Resource Summary ===")
    logger.info(f"CPU cores: {system_info['cpu_count']}")
    logger.info(f"psutil available: {system_info['psutil_available']}")

    if "error" not in memory_info:
        logger.info(
            f"Memory: {memory_info['used_gb']:.1f}GB / {memory_info['total_gb']:.1f}GB ({memory_info['percent']:.1f}%)",
        )
        logger.info(f"Process memory: {memory_info['process_rss_gb']:.1f}GB RSS")

    if "total_disk_gb" in system_info:
        logger.info(
            f"Disk: {system_info['free_disk_gb']:.1f}GB free / {system_info['total_disk_gb']:.1f}GB total",
        )

    logger.info("========================")


def monitor_parallel_execution(worker_count: int, operation_name: str) -> None:
    """Monitor resource usage during parallel execution.

    Args:
        worker_count: Number of workers being used
        operation_name: Name of the operation being monitored

    """
    logger.info(f"=== Parallel Execution Monitor: {operation_name} ===")
    logger.info(f"Workers: {worker_count}")

    memory_info = get_memory_usage()

    if "error" not in memory_info:
        estimated_total_memory = estimate_memory_per_worker() * worker_count
        memory_percent = (estimated_total_memory / memory_info["total_gb"]) * 100

        logger.info(
            f"Estimated total memory usage: {estimated_total_memory:.1f}GB ({memory_percent:.1f}%)",
        )

        if memory_percent > 75:
            logger.warning(f"High estimated memory usage: {memory_percent:.1f}%")

    # Check disk space
    check_disk_space()

    logger.info("==========================================")
