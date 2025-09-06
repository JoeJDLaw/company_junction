"""Similarity-based clustering for company junction deduplication.

This module provides clustering algorithms that group records based on pairwise
similarity scores, offering an alternative to edge-gated grouping.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass
from typing import Iterable, Mapping, Sequence

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Cluster:
    """Represents a cluster of similar records.
    
    Attributes:
        id: Unique cluster identifier
        members: List of record IDs in this cluster (stable ordering)
        min_pairwise_sim: Minimum pairwise similarity within the cluster [0,1]
        size: Number of members in the cluster
    """
    id: int
    members: list[str]
    min_pairwise_sim: float
    size: int

    def __post_init__(self):
        """Validate cluster data after initialization."""
        if self.size != len(self.members):
            raise ValueError(f"Size {self.size} doesn't match member count {len(self.members)}")
        if not 0 <= self.min_pairwise_sim <= 1:
            raise ValueError(f"min_pairwise_sim {self.min_pairwise_sim} must be in [0,1]")


@dataclass(frozen=True)
class ClusteringResult:
    """Result of similarity-based clustering.
    
    Attributes:
        clusters: List of clusters found
        outliers: List of record IDs that didn't meet clustering criteria
        policy: Clustering policy used ("complete" or "single")
        threshold: Similarity threshold used [0,1]
    """
    clusters: list[Cluster]
    outliers: list[str]
    policy: str
    threshold: float

    def __post_init__(self):
        """Validate clustering result after initialization."""
        if self.policy not in ("complete", "single"):
            raise ValueError(f"Policy must be 'complete' or 'single', got {self.policy}")
        if not 0 <= self.threshold <= 1:
            raise ValueError(f"Threshold {self.threshold} must be in [0,1]")


def build_similarity_clusters(
    all_ids: Sequence[str],
    pairs: Iterable[tuple[str, str, float]],
    threshold: float,
    policy: str = "complete",
    min_cluster_size: int = 2,
) -> ClusteringResult:
    """Build similarity-based clusters from pairwise similarity scores.
    
    Args:
        all_ids: Universe of record IDs to cluster
        pairs: Iterable of (id_a, id_b, similarity) tuples where similarity is in [0,1]
        threshold: Minimum similarity required for clustering [0,1]
        policy: Clustering policy ("complete" or "single")
        min_cluster_size: Minimum size for a valid cluster
        
    Returns:
        ClusteringResult with clusters and outliers
        
    Raises:
        ValueError: If parameters are invalid
    """
    if not 0 <= threshold <= 1:
        raise ValueError(f"Threshold {threshold} must be in [0,1]")
    if policy not in ("complete", "single"):
        raise ValueError(f"Policy must be 'complete' or 'single', got {policy}")
    if min_cluster_size < 1:
        raise ValueError(f"min_cluster_size must be >= 1, got {min_cluster_size}")
    
    # Convert to sets for efficient lookup
    all_ids_set = set(all_ids)
    
    # Build adjacency graph for pairs above threshold
    adjacency = defaultdict(set)
    similarities = {}
    
    for id_a, id_b, sim in pairs:
        # Validate IDs are in our universe
        if id_a not in all_ids_set or id_b not in all_ids_set:
            continue
            
        # Only include pairs above threshold
        if sim >= threshold:
            adjacency[id_a].add(id_b)
            adjacency[id_b].add(id_a)
            similarities[(id_a, id_b)] = sim
            similarities[(id_b, id_a)] = sim
    
    logger.info(f"Built adjacency graph with {len(adjacency)} nodes and {sum(len(neighbors) for neighbors in adjacency.values()) // 2} edges above threshold {threshold}")
    
    # Find connected components (single-linkage clusters)
    components = _find_connected_components(adjacency, all_ids)
    
    if policy == "single":
        # Single-linkage: use connected components directly
        clusters = _components_to_clusters(components, similarities, min_cluster_size)
    else:
        # Complete-linkage: refine components to ensure all pairs meet threshold
        clusters = _refine_to_complete_linkage(components, similarities, threshold, min_cluster_size)
    
    # Identify outliers (items not in any cluster)
    clustered_ids = set()
    for cluster in clusters:
        clustered_ids.update(cluster.members)
    
    outliers = [id_ for id_ in all_ids if id_ not in clustered_ids]
    
    logger.info(f"Found {len(clusters)} clusters and {len(outliers)} outliers using {policy}-linkage at threshold {threshold}")
    
    return ClusteringResult(
        clusters=clusters,
        outliers=outliers,
        policy=policy,
        threshold=threshold,
    )


def _find_connected_components(adjacency: dict[str, set[str]], all_ids: list[str]) -> list[set[str]]:
    """Find connected components in the adjacency graph.
    
    Args:
        adjacency: Adjacency list representation of the graph
        all_ids: All possible node IDs
        
    Returns:
        List of connected components (sets of node IDs)
    """
    visited = set()
    components = []
    
    for node in all_ids:
        if node in visited:
            continue
            
        # BFS to find connected component
        component = set()
        queue = [node]
        
        while queue:
            current = queue.pop(0)
            if current in visited:
                continue
                
            visited.add(current)
            component.add(current)
            
            # Add unvisited neighbors to queue
            for neighbor in adjacency.get(current, set()):
                if neighbor not in visited:
                    queue.append(neighbor)
        
        if component:
            components.append(component)
    
    return components


def _components_to_clusters(
    components: list[set[str]], 
    similarities: dict[tuple[str, str], float],
    min_cluster_size: int
) -> list[Cluster]:
    """Convert connected components to Cluster objects.
    
    Args:
        components: List of connected components
        similarities: Dict mapping (id_a, id_b) to similarity scores
        min_cluster_size: Minimum size for a valid cluster
        
    Returns:
        List of Cluster objects
    """
    clusters = []
    
    for i, component in enumerate(components):
        if len(component) < min_cluster_size:
            continue
            
        # Sort members for deterministic ordering
        members = sorted(list(component))
        
        # Calculate minimum pairwise similarity within the cluster
        min_sim = 1.0
        for j, id_a in enumerate(members):
            for id_b in members[j+1:]:
                sim = similarities.get((id_a, id_b), 0.0)
                min_sim = min(min_sim, sim)
        
        cluster = Cluster(
            id=i,
            members=members,
            min_pairwise_sim=min_sim,
            size=len(members)
        )
        clusters.append(cluster)
    
    return clusters


def _refine_to_complete_linkage(
    components: list[set[str]],
    similarities: dict[tuple[str, str], float],
    threshold: float,
    min_cluster_size: int
) -> list[Cluster]:
    """Refine connected components to ensure complete-linkage property.
    
    Complete-linkage requires that all pairs within a cluster have similarity >= threshold.
    This function splits components that violate this property.
    
    Args:
        components: List of connected components from single-linkage
        similarities: Dict mapping (id_a, id_b) to similarity scores
        threshold: Minimum similarity threshold
        min_cluster_size: Minimum size for a valid cluster
        
    Returns:
        List of Cluster objects satisfying complete-linkage
    """
    clusters = []
    
    for i, component in enumerate(components):
        if len(component) < min_cluster_size:
            continue
            
        # Try to find valid subclusters within this component
        subclusters = _find_complete_linkage_subclusters(component, similarities, threshold, min_cluster_size)
        
        for j, subcluster in enumerate(subclusters):
            # Sort members for deterministic ordering
            members = sorted(list(subcluster))
            
            # Calculate minimum pairwise similarity within the subcluster
            min_sim = 1.0
            for k, id_a in enumerate(members):
                for id_b in members[k+1:]:
                    sim = similarities.get((id_a, id_b), 0.0)
                    min_sim = min(min_sim, sim)
            
            cluster = Cluster(
                id=len(clusters),
                members=members,
                min_pairwise_sim=min_sim,
                size=len(members)
            )
            clusters.append(cluster)
    
    return clusters


def _find_complete_linkage_subclusters(
    component: set[str],
    similarities: dict[tuple[str, str], float],
    threshold: float,
    min_cluster_size: int
) -> list[set[str]]:
    """Find subclusters within a component that satisfy complete-linkage.
    
    This is a greedy approximation that tries to find the largest valid subclusters.
    
    Args:
        component: Set of node IDs in the component
        similarities: Dict mapping (id_a, id_b) to similarity scores
        threshold: Minimum similarity threshold
        min_cluster_size: Minimum size for a valid cluster
        
    Returns:
        List of subclusters (sets of node IDs)
    """
    if len(component) < min_cluster_size:
        return []
    
    # Sort nodes for deterministic results
    nodes = sorted(list(component))
    subclusters = []
    remaining = set(nodes)
    
    while remaining:
        # Start with the lexicographically first remaining node
        seed = min(remaining)
        subcluster = {seed}
        remaining.remove(seed)
        
        # Greedily add nodes that are similar to all nodes in the subcluster
        changed = True
        while changed and remaining:
            changed = False
            candidates = list(remaining)
            
            for candidate in candidates:
                # Check if candidate is similar to all nodes in subcluster
                can_join = True
                for member in subcluster:
                    sim = similarities.get((candidate, member), 0.0)
                    if sim < threshold:
                        can_join = False
                        break
                
                if can_join:
                    subcluster.add(candidate)
                    remaining.remove(candidate)
                    changed = True
                    break
        
        # Only keep subclusters that meet minimum size
        if len(subcluster) >= min_cluster_size:
            subclusters.append(subcluster)
    
    return subclusters
