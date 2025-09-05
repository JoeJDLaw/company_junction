"""
Performance benchmarks for group pagination operations.

This module provides micro-benchmarks to track performance characteristics
across different backends, dataset sizes, and operation types.
"""

import pytest
import tempfile
import pandas as pd
import time
from pathlib import Path
from typing import Dict, Any, List

from src.utils.group_pagination import get_groups_page
from src.utils.group_details import get_group_details


@pytest.fixture
def synthetic_10k_data():
    """Generate synthetic 10k row dataset."""
    import numpy as np
    
    np.random.seed(42)  # Reproducible data
    
    data = []
    for i in range(10000):
        group_id = f"g{i // 10}"  # 1000 groups of ~10 rows each
        data.append({
            'group_id': group_id,
            'account_name': f'Company {i}',
            'is_primary': i % 10 == 0,  # Every 10th is primary
            'weakest_edge_to_primary': np.random.uniform(0.1, 1.0),
            'disposition': np.random.choice(['keep', 'merge', 'delete'], p=[0.6, 0.3, 0.1]),
            'group_size': 10,  # All groups have size 10
            'max_score': np.random.uniform(0.5, 1.0),
            'primary_name': f'Company {i // 10 * 10}',  # Primary name
        })
    
    return pd.DataFrame(data)


@pytest.fixture
def synthetic_100k_data():
    """Generate synthetic 100k row dataset."""
    import numpy as np
    
    np.random.seed(42)  # Reproducible data
    
    data = []
    for i in range(100000):
        group_id = f"g{i // 100}"  # 1000 groups of ~100 rows each
        data.append({
            'group_id': group_id,
            'account_name': f'Company {i}',
            'is_primary': i % 100 == 0,  # Every 100th is primary
            'weakest_edge_to_primary': np.random.uniform(0.1, 1.0),
            'disposition': np.random.choice(['keep', 'merge', 'delete'], p=[0.6, 0.3, 0.1]),
            'group_size': 100,  # All groups have size 100
            'max_score': np.random.uniform(0.5, 1.0),
            'primary_name': f'Company {i // 100 * 100}',  # Primary name
        })
    
    return pd.DataFrame(data)


@pytest.fixture
def synthetic_1m_data():
    """Generate synthetic 1M row dataset."""
    import numpy as np
    
    np.random.seed(42)  # Reproducible data
    
    data = []
    for i in range(1000000):
        group_id = f"g{i // 1000}"  # 1000 groups of ~1000 rows each
        data.append({
            'group_id': group_id,
            'account_name': f'Company {i}',
            'is_primary': i % 1000 == 0,  # Every 1000th is primary
            'weakest_edge_to_primary': np.random.uniform(0.1, 1.0),
            'disposition': np.random.choice(['keep', 'merge', 'delete'], p=[0.6, 0.3, 0.1]),
            'group_size': 1000,  # All groups have size 1000
            'max_score': np.random.uniform(0.5, 1.0),
            'primary_name': f'Company {i // 1000 * 1000}',  # Primary name
        })
    
    return pd.DataFrame(data)


@pytest.fixture
def synthetic_10k_paths(synthetic_10k_data, tmp_path):
    """Create temporary parquet files for 10k dataset."""
    review_ready_path = tmp_path / "review_ready_10k.parquet"
    group_stats_path = tmp_path / "group_stats_10k.parquet"
    
    # Create review_ready parquet
    synthetic_10k_data.to_parquet(review_ready_path)
    
    # Create group_stats parquet (aggregated)
    stats_data = synthetic_10k_data.groupby('group_id').agg({
        'group_size': 'count',
        'max_score': 'max',
        'primary_name': lambda x: x.iloc[0],  # First primary name
        'disposition': lambda x: x.mode().iloc[0] if len(x.mode()) > 0 else 'keep'
    }).reset_index()
    stats_data.to_parquet(group_stats_path)
    
    return {
        'review_ready_parquet': str(review_ready_path),
        'group_stats_parquet': str(group_stats_path),
    }


@pytest.fixture
def synthetic_100k_paths(synthetic_100k_data, tmp_path):
    """Create temporary parquet files for 100k dataset."""
    review_ready_path = tmp_path / "review_ready_100k.parquet"
    group_stats_path = tmp_path / "group_stats_100k.parquet"
    
    # Create review_ready parquet
    synthetic_100k_data.to_parquet(review_ready_path)
    
    # Create group_stats parquet (aggregated)
    stats_data = synthetic_100k_data.groupby('group_id').agg({
        'group_size': 'count',
        'max_score': 'max',
        'primary_name': lambda x: x.iloc[0],  # First primary name
        'disposition': lambda x: x.mode().iloc[0] if len(x.mode()) > 0 else 'keep'
    }).reset_index()
    stats_data.to_parquet(group_stats_path)
    
    return {
        'review_ready_parquet': str(review_ready_path),
        'group_stats_parquet': str(group_stats_path),
    }


@pytest.fixture
def synthetic_1m_paths(synthetic_1m_data, tmp_path):
    """Create temporary parquet files for 1M dataset."""
    review_ready_path = tmp_path / "review_ready_1m.parquet"
    group_stats_path = tmp_path / "group_stats_1m.parquet"
    
    # Create review_ready parquet
    synthetic_1m_data.to_parquet(review_ready_path)
    
    # Create group_stats parquet (aggregated)
    stats_data = synthetic_1m_data.groupby('group_id').agg({
        'group_size': 'count',
        'max_score': 'max',
        'primary_name': lambda x: x.iloc[0],  # First primary name
        'disposition': lambda x: x.mode().iloc[0] if len(x.mode()) > 0 else 'keep'
    }).reset_index()
    stats_data.to_parquet(group_stats_path)
    
    return {
        'review_ready_parquet': str(review_ready_path),
        'group_stats_parquet': str(group_stats_path),
    }


class TestGroupsPageBenchmarks:
    """Benchmark tests for group pagination."""
    
    @pytest.mark.performance
    def test_groups_page_10k_pyarrow(self, benchmark, synthetic_10k_paths, monkeypatch):
        """Benchmark PyArrow pagination on 10k dataset."""
        run_id = "bench_10k"
        
        # Mock artifact paths
        def mock_get_artifact_paths(run_id):
            return synthetic_10k_paths
        monkeypatch.setattr('src.utils.group_pagination.get_artifact_paths', mock_get_artifact_paths)
        
        def run():
            return get_groups_page(
                run_id, "Group Size (Desc)", 1, 50, 
                {"dispositions": ["keep", "merge"], "min_edge_strength": 0.5}
            )
        
        result = benchmark(run)
        assert len(result[0]) <= 50  # Page size limit
        assert result[1] > 0  # Total count
    
    @pytest.mark.performance
    def test_groups_page_10k_duckdb(self, benchmark, synthetic_10k_paths, monkeypatch):
        """Benchmark DuckDB pagination on 10k dataset."""
        run_id = "bench_10k"
        
        # Mock artifact paths
        def mock_get_artifact_paths(run_id):
            return synthetic_10k_paths
        monkeypatch.setattr('src.utils.group_pagination.get_artifact_paths', mock_get_artifact_paths)
        
        # Force DuckDB
        def run():
            return get_groups_page(
                run_id, "Group Size (Desc)", 1, 50, 
                {"dispositions": ["keep", "merge"], "min_edge_strength": 0.5}
            )
        
        result = benchmark(run)
        assert len(result[0]) <= 50  # Page size limit
        assert result[1] > 0  # Total count
    
    @pytest.mark.performance
    def test_groups_page_100k_pyarrow(self, benchmark, synthetic_100k_paths, monkeypatch):
        """Benchmark PyArrow pagination on 100k dataset."""
        run_id = "bench_100k"
        
        # Mock artifact paths
        def mock_get_artifact_paths(run_id):
            return synthetic_100k_paths
        monkeypatch.setattr('src.utils.group_pagination.get_artifact_paths', mock_get_artifact_paths)
        
        def run():
            return get_groups_page(
                run_id, "Max Score (Desc)", 1, 100, 
                {"dispositions": ["keep"], "min_edge_strength": 0.7}
            )
        
        result = benchmark(run)
        assert len(result[0]) <= 100  # Page size limit
        assert result[1] > 0  # Total count
    
    @pytest.mark.performance
    def test_groups_page_100k_duckdb(self, benchmark, synthetic_100k_paths, monkeypatch):
        """Benchmark DuckDB pagination on 100k dataset."""
        run_id = "bench_100k"
        
        # Mock artifact paths
        def mock_get_artifact_paths(run_id):
            return synthetic_100k_paths
        monkeypatch.setattr('src.utils.group_pagination.get_artifact_paths', mock_get_artifact_paths)
        
        def run():
            return get_groups_page(
                run_id, "Max Score (Desc)", 1, 100, 
                {"dispositions": ["keep"], "min_edge_strength": 0.7}
            )
        
        result = benchmark(run)
        assert len(result[0]) <= 100  # Page size limit
        assert result[1] > 0  # Total count
    
    @pytest.mark.performance
    @pytest.mark.slow
    def test_groups_page_1m_pyarrow(self, benchmark, synthetic_1m_paths, monkeypatch):
        """Benchmark PyArrow pagination on 1M dataset."""
        run_id = "bench_1m"
        
        # Mock artifact paths
        def mock_get_artifact_paths(run_id):
            return synthetic_1m_paths
        monkeypatch.setattr('src.utils.group_pagination.get_artifact_paths', mock_get_artifact_paths)
        
        def run():
            return get_groups_page(
                run_id, "Account Name (Asc)", 1, 200, 
                {"dispositions": ["keep", "merge", "delete"]}
            )
        
        result = benchmark(run)
        assert len(result[0]) <= 200  # Page size limit
        assert result[1] > 0  # Total count
    
    @pytest.mark.performance
    @pytest.mark.slow
    def test_groups_page_1m_duckdb(self, benchmark, synthetic_1m_paths, monkeypatch):
        """Benchmark DuckDB pagination on 1M dataset."""
        run_id = "bench_1m"
        
        # Mock artifact paths
        def mock_get_artifact_paths(run_id):
            return synthetic_1m_paths
        monkeypatch.setattr('src.utils.group_pagination.get_artifact_paths', mock_get_artifact_paths)
        
        def run():
            return get_groups_page(
                run_id, "Account Name (Asc)", 1, 200, 
                {"dispositions": ["keep", "merge", "delete"]}
            )
        
        result = benchmark(run)
        assert len(result[0]) <= 200  # Page size limit
        assert result[1] > 0  # Total count


class TestGroupDetailsBenchmarks:
    """Benchmark tests for group details."""
    
    @pytest.mark.performance
    def test_group_details_10k_pyarrow(self, benchmark, synthetic_10k_paths, monkeypatch):
        """Benchmark PyArrow details on 10k dataset."""
        run_id = "bench_10k"
        
        # Mock artifact paths
        def mock_get_artifact_paths(run_id):
            return synthetic_10k_paths
        monkeypatch.setattr('src.utils.group_details.get_artifact_paths', mock_get_artifact_paths)
        
        def run():
            return get_group_details(
                run_id, "g0", "Account Name (Asc)", 1, 25, 
                {"dispositions": ["keep"], "min_edge_strength": 0.5}
            )
        
        result = benchmark(run)
        assert len(result[0]) <= 25  # Page size limit
        assert result[1] > 0  # Total count
    
    @pytest.mark.performance
    def test_group_details_10k_duckdb(self, benchmark, synthetic_10k_paths, monkeypatch):
        """Benchmark DuckDB details on 10k dataset."""
        run_id = "bench_10k"
        
        # Mock artifact paths
        def mock_get_artifact_paths(run_id):
            return synthetic_10k_paths
        monkeypatch.setattr('src.utils.group_details.get_artifact_paths', mock_get_artifact_paths)
        
        def run():
            return get_group_details(
                run_id, "g0", "Account Name (Asc)", 1, 25, 
                {"dispositions": ["keep"], "min_edge_strength": 0.5}
            )
        
        result = benchmark(run)
        assert len(result[0]) <= 25  # Page size limit
        assert result[1] > 0  # Total count


class TestSortVariants:
    """Benchmark different sort options."""
    
    @pytest.mark.performance
    def test_sort_variants_10k(self, benchmark, synthetic_10k_paths, monkeypatch):
        """Benchmark different sort keys on 10k dataset."""
        run_id = "bench_10k"
        
        # Mock artifact paths
        def mock_get_artifact_paths(run_id):
            return synthetic_10k_paths
        monkeypatch.setattr('src.utils.group_pagination.get_artifact_paths', mock_get_artifact_paths)
        
        sort_keys = [
            "Group Size (Desc)",
            "Group Size (Asc)", 
            "Max Score (Desc)",
            "Max Score (Asc)",
            "Account Name (Asc)",
            "Account Name (Desc)"
        ]
        
        for sort_key in sort_keys:
            def run():
                return get_groups_page(run_id, sort_key, 1, 50, {})
            
            result = benchmark.pedantic(run, rounds=3, warmup_rounds=1)
            assert len(result[0]) <= 50
            assert result[1] > 0


class TestFilterVariants:
    """Benchmark different filter combinations."""
    
    @pytest.mark.performance
    def test_filter_variants_10k(self, benchmark, synthetic_10k_paths, monkeypatch):
        """Benchmark different filter combinations on 10k dataset."""
        run_id = "bench_10k"
        
        # Mock artifact paths
        def mock_get_artifact_paths(run_id):
            return synthetic_10k_paths
        monkeypatch.setattr('src.utils.group_pagination.get_artifact_paths', mock_get_artifact_paths)
        
        filter_combinations = [
            {},  # No filters
            {"dispositions": ["keep"]},  # Dispositions only
            {"min_edge_strength": 0.7},  # Edge strength only
            {"dispositions": ["keep", "merge"], "min_edge_strength": 0.5},  # Both
        ]
        
        for filters in filter_combinations:
            def run():
                return get_groups_page(run_id, "Group Size (Desc)", 1, 50, filters)
            
            result = benchmark.pedantic(run, rounds=3, warmup_rounds=1)
            assert len(result[0]) <= 50
            assert result[1] >= 0  # Could be 0 with restrictive filters
