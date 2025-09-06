"""Union-Find data structure for efficient grouping operations.

This module provides a DisjointSet implementation with size tracking
for canopy bound checks in the grouping stage.
"""

import logging
from typing import Any

logger = logging.getLogger(__name__)


class DisjointSet:
    """Union-Find data structure with size tracking.

    This implementation provides O(1) find operations and O(α(n)) union operations,
    where α is the inverse Ackermann function (effectively constant).
    """

    def __init__(self) -> None:
        """Initialize an empty disjoint set."""
        self.parent: dict[Any, Any] = {}
        self.rank: dict[Any, int] = {}
        self.size: dict[Any, int] = {}
        self._count = 0

    def make_set(self, x: Any) -> None:
        """Create a new set containing element x.

        Args:
            x: Element to add to the disjoint set

        """
        if x not in self.parent:
            self.parent[x] = x
            self.rank[x] = 0
            self.size[x] = 1
            self._count += 1

    def find(self, x: Any) -> Any:
        """Find the representative (root) of the set containing x.

        Args:
            x: Element to find

        Returns:
            Representative element of the set containing x

        """
        if x not in self.parent:
            raise ValueError(f"Element {x} not found in disjoint set")

        # Path compression
        if self.parent[x] != x:
            self.parent[x] = self.find(self.parent[x])

        return self.parent[x]

    def union(self, x: Any, y: Any) -> bool:
        """Merge the sets containing x and y.

        Args:
            x: First element
            y: Second element

        Returns:
            True if the sets were merged, False if they were already in the same set

        """
        root_x = self.find(x)
        root_y = self.find(y)

        if root_x == root_y:
            return False  # Already in the same set

        # Union by rank
        if self.rank[root_x] < self.rank[root_y]:
            root_x, root_y = root_y, root_x

        self.parent[root_y] = root_x

        # Update size
        self.size[root_x] += self.size[root_y]

        # Update rank if necessary
        if self.rank[root_x] == self.rank[root_y]:
            self.rank[root_x] += 1

        self._count -= 1
        return True

    def get_size(self, x: Any) -> int:
        """Get the size of the set containing x.

        Args:
            x: Element to get size for

        Returns:
            Size of the set containing x

        """
        root = self.find(x)
        return self.size[root]

    def get_representative(self, x: Any) -> Any:
        """Get the representative element of the set containing x.

        Args:
            x: Element to get representative for

        Returns:
            Representative element of the set containing x

        """
        return self.find(x)

    def is_same_set(self, x: Any, y: Any) -> bool:
        """Check if x and y are in the same set.

        Args:
            x: First element
            y: Second element

        Returns:
            True if x and y are in the same set, False otherwise

        """
        return bool(self.find(x) == self.find(y))

    def get_set_count(self) -> int:
        """Get the total number of sets.

        Returns:
            Number of disjoint sets

        """
        return self._count

    def get_all_representatives(self) -> set:
        """Get all representative elements.

        Returns:
            Set of all representative elements

        """
        return {self.find(x) for x in self.parent}

    def get_set_members(self, x: Any) -> set:
        """Get all members of the set containing x.

        Args:
            x: Element to get set members for

        Returns:
            Set of all members in the same set as x

        """
        root = self.find(x)
        return {member for member in self.parent if self.find(member) == root}

    def reset(self) -> None:
        """Reset the disjoint set to empty state."""
        self.parent.clear()
        self.rank.clear()
        self.size.clear()
        self._count = 0

    def __len__(self) -> int:
        """Get the total number of elements."""
        return len(self.parent)

    def __contains__(self, x: Any) -> bool:
        """Check if element x is in the disjoint set."""
        return x in self.parent
