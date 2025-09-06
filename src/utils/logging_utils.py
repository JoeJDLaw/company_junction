"""Logging utilities for the company junction pipeline."""

import logging


def setup_logging(level: str = "INFO") -> None:
    """Configure logging for the pipeline.

    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR)

    """
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(), logging.FileHandler("pipeline.log")],
    )


def get_logger(name: str) -> logging.Logger:
    """Get a logger instance for the specified module.

    Args:
        name: Logger name (typically __name__)

    Returns:
        Configured logger instance

    """
    return logging.getLogger(name)
