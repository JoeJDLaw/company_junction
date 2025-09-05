"""Observability metrics for company junction operations.

This module provides Prometheus-style metrics for monitoring performance,
backend choices, and error rates across the application.
"""

from src.utils.logging_utils import get_logger
from src.utils.opt_deps import PROMETHEUS_AVAILABLE

logger = get_logger(__name__)

# Initialize metrics if Prometheus is available
if PROMETHEUS_AVAILABLE:
    try:
        from prometheus_client import Counter, Gauge, Histogram  # type: ignore[import-not-found]

        # Request counters
        groups_req = Counter(
            "groups_page_requests_total",
            "Groups page requests",
            ["backend", "source", "ok"],
        )

        details_req = Counter(
            "details_page_requests_total", "Details page requests", ["backend", "ok"],
        )

        # Latency histograms
        groups_lat = Histogram(
            "groups_page_latency_seconds",
            "Groups page latency",
            ["backend", "source"],
            buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0],
        )

        details_lat = Histogram(
            "details_page_latency_seconds",
            "Details page latency",
            ["backend"],
            buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0],
        )

        # Backend choice tracking
        backend_choice = Counter(
            "backend_choice_total", "Backend choice decisions", ["reason", "backend"],
        )

        # Error tracking
        timeouts = Counter(
            "timeouts_total", "Timeout occurrences", ["backend", "where"],
        )

        # Configuration tracking
        clamped = Counter("page_size_clamped_total", "Page size clamping occurrences")

        # Active connections gauge
        active_connections = Gauge(
            "duckdb_active_connections", "Number of active DuckDB connections",
        )

        logger.info("Prometheus metrics initialized successfully")

    except ImportError as e:
        logger.warning(f"Prometheus client not available: {e}")
        # Create no-op metrics
        groups_req = details_req = groups_lat = details_lat = None
        backend_choice = timeouts = clamped = active_connections = None
        PROMETHEUS_AVAILABLE = False

else:
    # No-op metrics when Prometheus is not available
    groups_req = details_req = groups_lat = details_lat = None
    backend_choice = timeouts = clamped = active_connections = None


def record_groups_request(
    backend: str, source: str, success: bool, duration: float,
) -> None:
    """Record a groups page request."""
    if not PROMETHEUS_AVAILABLE or groups_req is None:
        return

    ok_label = "true" if success else "false"
    groups_req.labels(backend=backend, source=source, ok=ok_label).inc()
    if groups_lat is not None:
        groups_lat.labels(backend=backend, source=source).observe(duration)


def record_details_request(backend: str, success: bool, duration: float) -> None:
    """Record a details page request."""
    if not PROMETHEUS_AVAILABLE or details_req is None:
        return

    ok_label = "true" if success else "false"
    details_req.labels(backend=backend, ok=ok_label).inc()
    if details_lat is not None:
        details_lat.labels(backend=backend).observe(duration)


def record_backend_choice(reason: str, backend: str) -> None:
    """Record a backend choice decision."""
    if not PROMETHEUS_AVAILABLE or backend_choice is None:
        return

    backend_choice.labels(reason=reason, backend=backend).inc()


def record_timeout(backend: str, where: str) -> None:
    """Record a timeout occurrence."""
    if not PROMETHEUS_AVAILABLE or timeouts is None:
        return

    timeouts.labels(backend=backend, where=where).inc()


def record_page_size_clamped() -> None:
    """Record a page size clamping occurrence."""
    if not PROMETHEUS_AVAILABLE or clamped is None:
        return

    clamped.inc()


def update_active_connections(count: int) -> None:
    """Update the active connections gauge."""
    if not PROMETHEUS_AVAILABLE or active_connections is None:
        return

    active_connections.set(count)


def get_metrics_summary() -> dict:
    """Get a summary of current metrics (for debugging)."""
    if not PROMETHEUS_AVAILABLE:
        return {"status": "prometheus_not_available"}

    return {
        "status": "active",
        "metrics_available": {
            "groups_req": groups_req is not None,
            "details_req": details_req is not None,
            "groups_lat": groups_lat is not None,
            "details_lat": details_lat is not None,
            "backend_choice": backend_choice is not None,
            "timeouts": timeouts is not None,
            "clamped": clamped is not None,
            "active_connections": active_connections is not None,
        },
    }
