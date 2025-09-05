"""Protocol definitions for parallel execution interfaces.

This module provides type-safe contracts for parallel executors without
coupling to specific implementations.
"""

from collections.abc import Iterable
from typing import Callable, Optional, Protocol, TypeVar

T = TypeVar("T")
R = TypeVar("R")


class ExecutorLike(Protocol):
    """Protocol for parallel executors that support map operations.

    This protocol allows different executor implementations (multiprocessing,
    threading, etc.) to be used interchangeably while maintaining type safety.
    """

    @property
    def workers(self) -> int:
        """Number of worker processes/threads available."""
        ...

    def map(
        self,
        fn: Callable[[T], R],
        items: Iterable[T],
        *,
        chunksize: Optional[int] = None,
    ) -> Iterable[R]:
        """Apply function to items in parallel.

        Args:
            fn: Function to apply to each item
            items: Iterable of items to process
            chunksize: Optional chunk size for batching

        Returns:
            Iterable of results (may be list or iterator depending on implementation)

        """
        ...
