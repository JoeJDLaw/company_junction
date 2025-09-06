"""Grouping algorithms for company junction deduplication.

This package provides different grouping strategies:
- Edge-gated grouping (existing)
- Similarity-based clustering (new)
"""

from .similarity_clusters import (
    Cluster,
    ClusteringResult,
    build_similarity_clusters,
)

__all__ = [
    "Cluster",
    "ClusteringResult", 
    "build_similarity_clusters",
]
