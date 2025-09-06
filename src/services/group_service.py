"""Group service for managing different grouping strategies.

This service provides access to both edge-gated groups and similarity-based clusters,
with caching and DuckDB integration.
"""

import hashlib
import logging
import os
from pathlib import Path
from typing import Any, Optional

import duckdb
import pandas as pd

from src.grouping.similarity_clusters import (
    ClusteringResult,
    build_similarity_clusters,
)
from src.utils.artifact_management import get_artifact_paths

logger = logging.getLogger(__name__)


class GroupService:
    """Service for managing grouping operations with caching."""
    
    def __init__(self, settings: dict[str, Any]):
        """Initialize the group service.
        
        Args:
            settings: Configuration settings
        """
        self.settings = settings
        self._cache: dict[str, ClusteringResult] = {}
        
    def get_similarity_clusters(
        self,
        run_id: str,
        threshold: float,
        policy: str = "complete",
        min_cluster_size: int = 2,
        account_ids: Optional[list[str]] = None,
    ) -> ClusteringResult:
        """Get similarity-based clusters for a run.
        
        Args:
            run_id: Pipeline run ID
            threshold: Similarity threshold [0,1]
            policy: Clustering policy ("complete" or "single")
            min_cluster_size: Minimum cluster size
            account_ids: Optional list of account IDs to filter to
            
        Returns:
            ClusteringResult with clusters and outliers
        """
        # Create cache key
        cache_key = self._create_cache_key(
            run_id, threshold, policy, min_cluster_size, account_ids
        )
        
        # Check cache first
        if cache_key in self._cache:
            logger.info(f"Returning cached clustering result for {cache_key}")
            return self._cache[cache_key]
        
        # Load candidate pairs from DuckDB
        pairs_df = self._load_candidate_pairs(run_id, threshold, account_ids)
        
        if pairs_df.empty:
            logger.warning(f"No candidate pairs found for run {run_id} at threshold {threshold}")
            return ClusteringResult(
                clusters=[],
                outliers=account_ids or [],
                policy=policy,
                threshold=threshold,
            )
        
        # Get all account IDs
        if account_ids is None:
            all_ids = sorted(set(pairs_df['id_a'].tolist() + pairs_df['id_b'].tolist()))
        else:
            all_ids = account_ids
        
        # Convert pairs to the format expected by clustering algorithm
        pairs = [
            (row['id_a'], row['id_b'], row['score'] / 100.0)  # Convert [0,100] to [0,1]
            for _, row in pairs_df.iterrows()
        ]
        
        # Build clusters
        result = build_similarity_clusters(
            all_ids=all_ids,
            pairs=pairs,
            threshold=threshold,
            policy=policy,
            min_cluster_size=min_cluster_size,
        )
        
        # Cache the result
        self._cache[cache_key] = result
        
        logger.info(f"Computed clustering: {len(result.clusters)} clusters, {len(result.outliers)} outliers")
        
        return result
    
    def _load_candidate_pairs(
        self, 
        run_id: str, 
        threshold: float,
        account_ids: Optional[list[str]] = None
    ) -> pd.DataFrame:
        """Load candidate pairs from DuckDB for a run.
        
        Args:
            run_id: Pipeline run ID
            threshold: Similarity threshold [0,1] (converted to [0,100] for query)
            account_ids: Optional list of account IDs to filter to
            
        Returns:
            DataFrame with candidate pairs
        """
        # Get artifact paths
        artifact_paths = get_artifact_paths(run_id)
        candidate_pairs_path = artifact_paths.get("candidate_pairs")
        
        if not candidate_pairs_path or not os.path.exists(candidate_pairs_path):
            logger.warning(f"Candidate pairs file not found: {candidate_pairs_path}")
            return pd.DataFrame()
        
        # Convert threshold from [0,1] to [0,100] for query
        threshold_100 = threshold * 100
        
        # Build DuckDB query
        query = f"""
        SELECT id_a, id_b, score
        FROM read_parquet('{candidate_pairs_path}')
        WHERE score >= {threshold_100}
        """
        
        # Add account ID filter if provided
        if account_ids:
            account_ids_str = "', '".join(account_ids)
            query += f" AND id_a IN ('{account_ids_str}') AND id_b IN ('{account_ids_str}')"
        
        query += " ORDER BY score DESC"
        
        try:
            # Execute query
            conn = duckdb.connect()
            pairs_df = conn.execute(query).df()
            conn.close()
            
            logger.info(f"Loaded {len(pairs_df)} candidate pairs from {candidate_pairs_path}")
            return pairs_df
            
        except Exception as e:
            logger.error(f"Error loading candidate pairs: {e}")
            return pd.DataFrame()
    
    def _create_cache_key(
        self,
        run_id: str,
        threshold: float,
        policy: str,
        min_cluster_size: int,
        account_ids: Optional[list[str]] = None,
    ) -> str:
        """Create a cache key for clustering parameters.
        
        Args:
            run_id: Pipeline run ID
            threshold: Similarity threshold
            policy: Clustering policy
            min_cluster_size: Minimum cluster size
            account_ids: Optional account ID filter
            
        Returns:
            Cache key string
        """
        # Create a hash of the parameters
        params = {
            "run_id": run_id,
            "threshold": threshold,
            "policy": policy,
            "min_cluster_size": min_cluster_size,
            "account_ids": sorted(account_ids) if account_ids else None,
        }
        
        # Create deterministic hash
        params_str = str(sorted(params.items()))
        return hashlib.md5(params_str.encode()).hexdigest()
    
    def clear_cache(self, run_id: Optional[str] = None) -> None:
        """Clear clustering cache.
        
        Args:
            run_id: Optional run ID to clear cache for specific run only
        """
        if run_id:
            # Clear cache entries for specific run
            keys_to_remove = [
                key for key in self._cache.keys()
                if run_id in key
            ]
            for key in keys_to_remove:
                del self._cache[key]
            logger.info(f"Cleared cache for run {run_id}")
        else:
            # Clear entire cache
            self._cache.clear()
            logger.info("Cleared entire clustering cache")
    
    def get_cluster_stats(self, result: ClusteringResult) -> dict[str, Any]:
        """Get statistics about clustering results.
        
        Args:
            result: ClusteringResult to analyze
            
        Returns:
            Dictionary with clustering statistics
        """
        if not result.clusters:
            return {
                "total_clusters": 0,
                "total_outliers": len(result.outliers),
                "avg_cluster_size": 0,
                "max_cluster_size": 0,
                "min_cluster_size": 0,
                "total_clustered_records": 0,
            }
        
        cluster_sizes = [cluster.size for cluster in result.clusters]
        
        return {
            "total_clusters": len(result.clusters),
            "total_outliers": len(result.outliers),
            "avg_cluster_size": sum(cluster_sizes) / len(cluster_sizes),
            "max_cluster_size": max(cluster_sizes),
            "min_cluster_size": min(cluster_sizes),
            "total_clustered_records": sum(cluster_sizes),
            "policy": result.policy,
            "threshold": result.threshold,
        }
