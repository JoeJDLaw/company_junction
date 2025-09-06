"""Unit tests for similarity-based clustering."""

import pytest

from src.grouping.similarity_clusters import (
    Cluster,
    ClusteringResult,
    build_similarity_clusters,
)


class TestCluster:
    """Test Cluster dataclass."""
    
    def test_cluster_creation(self):
        """Test basic cluster creation."""
        cluster = Cluster(
            id=1,
            members=["a", "b", "c"],
            min_pairwise_sim=0.85,
            size=3
        )
        
        assert cluster.id == 1
        assert cluster.members == ["a", "b", "c"]
        assert cluster.min_pairwise_sim == 0.85
        assert cluster.size == 3
    
    def test_cluster_validation_size_mismatch(self):
        """Test cluster validation with size mismatch."""
        with pytest.raises(ValueError, match="Size 3 doesn't match member count 2"):
            Cluster(
                id=1,
                members=["a", "b"],
                min_pairwise_sim=0.85,
                size=3
            )
    
    def test_cluster_validation_invalid_similarity(self):
        """Test cluster validation with invalid similarity."""
        with pytest.raises(ValueError, match="min_pairwise_sim 1.5 must be in \\[0,1\\]"):
            Cluster(
                id=1,
                members=["a", "b"],
                min_pairwise_sim=1.5,
                size=2
            )


class TestClusteringResult:
    """Test ClusteringResult dataclass."""
    
    def test_clustering_result_creation(self):
        """Test basic clustering result creation."""
        clusters = [
            Cluster(id=0, members=["a", "b"], min_pairwise_sim=0.9, size=2)
        ]
        result = ClusteringResult(
            clusters=clusters,
            outliers=["c"],
            policy="complete",
            threshold=0.8
        )
        
        assert len(result.clusters) == 1
        assert result.outliers == ["c"]
        assert result.policy == "complete"
        assert result.threshold == 0.8
    
    def test_clustering_result_validation_invalid_policy(self):
        """Test clustering result validation with invalid policy."""
        with pytest.raises(ValueError, match="Policy must be 'complete' or 'single'"):
            ClusteringResult(
                clusters=[],
                outliers=[],
                policy="invalid",
                threshold=0.8
            )
    
    def test_clustering_result_validation_invalid_threshold(self):
        """Test clustering result validation with invalid threshold."""
        with pytest.raises(ValueError, match="Threshold 1.5 must be in \\[0,1\\]"):
            ClusteringResult(
                clusters=[],
                outliers=[],
                policy="complete",
                threshold=1.5
            )


class TestBuildSimilarityClusters:
    """Test build_similarity_clusters function."""
    
    def test_empty_input(self):
        """Test clustering with empty input."""
        result = build_similarity_clusters(
            all_ids=[],
            pairs=[],
            threshold=0.8,
            policy="complete"
        )
        
        assert len(result.clusters) == 0
        assert len(result.outliers) == 0
        assert result.policy == "complete"
        assert result.threshold == 0.8
    
    def test_single_item(self):
        """Test clustering with single item."""
        result = build_similarity_clusters(
            all_ids=["a"],
            pairs=[],
            threshold=0.8,
            policy="complete",
            min_cluster_size=2
        )
        
        assert len(result.clusters) == 0
        assert result.outliers == ["a"]
    
    def test_single_item_min_size_one(self):
        """Test clustering with single item and min_cluster_size=1."""
        result = build_similarity_clusters(
            all_ids=["a"],
            pairs=[],
            threshold=0.8,
            policy="complete",
            min_cluster_size=1
        )
        
        assert len(result.clusters) == 1
        assert result.clusters[0].members == ["a"]
        assert result.clusters[0].size == 1
        assert len(result.outliers) == 0
    
    def test_single_linkage_basic(self):
        """Test single-linkage clustering with basic example."""
        all_ids = ["a", "b", "c", "d"]
        pairs = [
            ("a", "b", 0.9),  # a-b connected
            ("b", "c", 0.9),  # b-c connected (creates chain a-b-c)
            ("d", "a", 0.7),  # d-a below threshold
        ]
        
        result = build_similarity_clusters(
            all_ids=all_ids,
            pairs=pairs,
            threshold=0.8,
            policy="single"
        )
        
        # Should have one cluster with a, b, c and d as outlier
        assert len(result.clusters) == 1
        assert set(result.clusters[0].members) == {"a", "b", "c"}
        assert result.outliers == ["d"]
        assert result.policy == "single"
    
    def test_complete_linkage_basic(self):
        """Test complete-linkage clustering with basic example."""
        all_ids = ["a", "b", "c", "d"]
        pairs = [
            ("a", "b", 0.9),  # a-b connected
            ("b", "c", 0.9),  # b-c connected
            ("a", "c", 0.7),  # a-c below threshold (violates complete-linkage)
            ("d", "a", 0.7),  # d-a below threshold
        ]
        
        result = build_similarity_clusters(
            all_ids=all_ids,
            pairs=pairs,
            threshold=0.8,
            policy="complete"
        )
        
        # Complete-linkage should split the cluster since a-c < 0.8
        # Should have two clusters: {a,b} and {c} (if min_cluster_size=1)
        # or {a,b} and c as outlier (if min_cluster_size=2)
        assert len(result.clusters) >= 1
        
        # Find the cluster with a and b
        ab_cluster = None
        for cluster in result.clusters:
            if "a" in cluster.members and "b" in cluster.members:
                ab_cluster = cluster
                break
        
        assert ab_cluster is not None
        assert ab_cluster.min_pairwise_sim >= 0.8
    
    def test_complete_linkage_strict(self):
        """Test complete-linkage with strict requirements."""
        all_ids = ["a", "b", "c"]
        pairs = [
            ("a", "b", 0.9),  # a-b connected
            ("b", "c", 0.9),  # b-c connected
            ("a", "c", 0.7),  # a-c below threshold
        ]
        
        result = build_similarity_clusters(
            all_ids=all_ids,
            pairs=pairs,
            threshold=0.8,
            policy="complete",
            min_cluster_size=2
        )
        
        # Should have one cluster with a and b only
        assert len(result.clusters) == 1
        assert set(result.clusters[0].members) == {"a", "b"}
        assert "c" in result.outliers
    
    def test_threshold_monotonicity(self):
        """Test that lowering threshold merges/grows clusters."""
        all_ids = ["a", "b", "c", "d"]
        pairs = [
            ("a", "b", 0.9),
            ("b", "c", 0.85),
            ("c", "d", 0.75),
        ]
        
        # High threshold
        result_high = build_similarity_clusters(
            all_ids=all_ids,
            pairs=pairs,
            threshold=0.9,
            policy="single"
        )
        
        # Low threshold
        result_low = build_similarity_clusters(
            all_ids=all_ids,
            pairs=pairs,
            threshold=0.7,
            policy="single"
        )
        
        # Lower threshold should result in fewer, larger clusters
        assert len(result_low.clusters) <= len(result_high.clusters)
        
        # Total clustered records should be same or higher
        total_high = sum(cluster.size for cluster in result_high.clusters)
        total_low = sum(cluster.size for cluster in result_low.clusters)
        assert total_low >= total_high
    
    def test_determinism_ordering(self):
        """Test that results are deterministic with same inputs."""
        all_ids = ["c", "a", "b", "d"]  # Unsorted order
        pairs = [
            ("a", "b", 0.9),
            ("b", "c", 0.9),
            ("c", "d", 0.9),
        ]
        
        result1 = build_similarity_clusters(
            all_ids=all_ids,
            pairs=pairs,
            threshold=0.8,
            policy="single"
        )
        
        result2 = build_similarity_clusters(
            all_ids=all_ids,
            pairs=pairs,
            threshold=0.8,
            policy="single"
        )
        
        # Results should be identical
        assert len(result1.clusters) == len(result2.clusters)
        assert len(result1.outliers) == len(result2.outliers)
        
        # Cluster members should be in same order
        for cluster1, cluster2 in zip(result1.clusters, result2.clusters):
            assert cluster1.members == cluster2.members
            assert cluster1.min_pairwise_sim == cluster2.min_pairwise_sim
    
    def test_min_cluster_size_filtering(self):
        """Test that clusters below min_cluster_size become outliers."""
        all_ids = ["a", "b", "c", "d", "e"]
        pairs = [
            ("a", "b", 0.9),  # Cluster of size 2
            ("c", "d", 0.9),  # Cluster of size 2
            ("e", "a", 0.7),  # Below threshold
        ]
        
        result = build_similarity_clusters(
            all_ids=all_ids,
            pairs=pairs,
            threshold=0.8,
            policy="single",
            min_cluster_size=3  # Require clusters of size 3+
        )
        
        # All clusters should be size 3+ or become outliers
        for cluster in result.clusters:
            assert cluster.size >= 3
        
        # Items that can't form clusters of size 3+ should be outliers
        assert len(result.outliers) >= 1  # At least 'e' should be outlier
    
    def test_invalid_parameters(self):
        """Test validation of invalid parameters."""
        all_ids = ["a", "b"]
        pairs = [("a", "b", 0.9)]
        
        # Invalid threshold
        with pytest.raises(ValueError, match="Threshold 1.5 must be in \\[0,1\\]"):
            build_similarity_clusters(all_ids, pairs, threshold=1.5)
        
        # Invalid policy
        with pytest.raises(ValueError, match="Policy must be 'complete' or 'single'"):
            build_similarity_clusters(all_ids, pairs, threshold=0.8, policy="invalid")
        
        # Invalid min_cluster_size
        with pytest.raises(ValueError, match="min_cluster_size must be >= 1"):
            build_similarity_clusters(all_ids, pairs, threshold=0.8, min_cluster_size=0)
    
    def test_pairs_with_missing_ids(self):
        """Test that pairs with IDs not in all_ids are ignored."""
        all_ids = ["a", "b"]
        pairs = [
            ("a", "b", 0.9),  # Valid pair
            ("a", "c", 0.9),  # 'c' not in all_ids - should be ignored
            ("b", "d", 0.9),  # 'd' not in all_ids - should be ignored
        ]
        
        result = build_similarity_clusters(
            all_ids=all_ids,
            pairs=pairs,
            threshold=0.8,
            policy="single"
        )
        
        # Should only cluster a and b
        assert len(result.clusters) == 1
        assert set(result.clusters[0].members) == {"a", "b"}
        assert len(result.outliers) == 0
    
    def test_complex_complete_linkage(self):
        """Test complete-linkage with complex example."""
        all_ids = ["a", "b", "c", "d", "e", "f"]
        pairs = [
            # Triangle a-b-c where all pairs meet threshold
            ("a", "b", 0.9),
            ("b", "c", 0.9),
            ("a", "c", 0.9),
            
            # Chain d-e-f where d-f doesn't meet threshold
            ("d", "e", 0.9),
            ("e", "f", 0.9),
            ("d", "f", 0.7),  # Below threshold
        ]
        
        result = build_similarity_clusters(
            all_ids=all_ids,
            pairs=pairs,
            threshold=0.8,
            policy="complete"
        )
        
        # Should have cluster {a,b,c} and either {d,e} + {f} or {d,e} + f as outlier
        assert len(result.clusters) >= 2
        
        # Find the a-b-c cluster
        abc_cluster = None
        for cluster in result.clusters:
            if "a" in cluster.members and "b" in cluster.members and "c" in cluster.members:
                abc_cluster = cluster
                break
        
        assert abc_cluster is not None
        assert abc_cluster.size == 3
        assert abc_cluster.min_pairwise_sim >= 0.8
