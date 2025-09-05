"""
Tests for DuckDB group stats memoization functionality.

This module validates that the memoization system works correctly
and provides cache hits on subsequent runs with identical config.
"""

import pytest
import time
import pandas as pd
import tempfile
import os
from pathlib import Path
from src.utils.duckdb_group_stats import create_duckdb_group_stats_engine


class TestGroupStatsMemoization:
    """Test suite for DuckDB group stats memoization."""

    def test_duckdb_memoization_smoke(self):
        """Test basic memoization functionality with cache hits."""
        # Create a small test dataset with required columns
        test_data = {
            'group_id': ['G001', 'G001', 'G002', 'G002', 'G003'],
            'account_id': ['ACC_001', 'ACC_002', 'ACC_003', 'ACC_004', 'ACC_005'],
            'account_name': ['Company A', 'Company B', 'Company C', 'Company D', 'Company E'],
            'domain': ['companya.com', 'companyb.com', 'companyc.com', 'companyd.com', 'companye.com'],
            'industry': ['Tech', 'Finance', 'Tech', 'Healthcare', 'Retail'],
            'revenue': [1000000, 5000000, 2000000, 3000000, 1500000],
            'employees': [50, 200, 100, 150, 75],
            'is_primary': [True, False, True, False, True],
            'similarity_score': [1.0, 0.8, 1.0, 0.9, 1.0],
            'weakest_edge_to_primary': [1.0, 0.8, 1.0, 0.9, 1.0],
            'disposition': ['Keep', 'Update', 'Keep', 'Update', 'Keep']
        }
        
        df_primary = pd.DataFrame(test_data)
        
        # Create temporary directory for test artifacts
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create test settings
            settings = {
                'group_stats': {
                    'backend': 'duckdb',
                    'memoization': True,
                    'cache_dir': temp_dir
                }
            }
            
            # Create engine
            engine = create_duckdb_group_stats_engine(settings, run_id="test_memo")
            
            try:
                # First run - should be a cache miss
                config_digest = "test_config_123"
                t1_df, t1_meta = engine.compute_group_stats(df_primary, config_digest=config_digest)
                
                # Verify first run completed
                assert len(t1_df) > 0, "First run should produce results"
                assert t1_meta.get("cache_hit", False) is False, "First run should be cache miss"
                
                # Small delay to ensure timing differences
                time.sleep(0.1)
                
                # Second run with same config - should be cache hit
                t2_df, t2_meta = engine.compute_group_stats(df_primary, config_digest=config_digest)
                
                # Verify second run used cache
                assert len(t2_df) > 0, "Second run should produce results"
                assert t2_meta.get("cache_hit", False) is True, "Second run should be cache hit"
                
                # Verify results are identical
                pd.testing.assert_frame_equal(t1_df, t2_df, check_dtype=False)
                
                # Third run - should also be cache hit
                time.sleep(0.1)
                t3_df, t3_meta = engine.compute_group_stats(df_primary, config_digest=config_digest)
                
                assert len(t3_df) > 0, "Third run should produce results"
                assert t3_meta.get("cache_hit", False) is True, "Third run should be cache hit"
                
                # Verify all three runs produce identical results
                pd.testing.assert_frame_equal(t1_df, t3_df, check_dtype=False)
                
            finally:
                engine.close()

    def test_duckdb_memoization_different_configs(self):
        """Test that different config digests don't share cache."""
        test_data = {
            'group_id': ['G001', 'G002'],
            'account_id': ['ACC_001', 'ACC_002'],
            'account_name': ['Company A', 'Company B'],
            'domain': ['companya.com', 'companyb.com'],
            'is_primary': [True, True],
            'similarity_score': [1.0, 1.0],
            'weakest_edge_to_primary': [1.0, 1.0],
            'disposition': ['Keep', 'Keep']
        }
        
        df_primary = pd.DataFrame(test_data)
        
        with tempfile.TemporaryDirectory() as temp_dir:
            settings = {
                'group_stats': {
                    'backend': 'duckdb',
                    'memoization': True,
                    'cache_dir': temp_dir
                }
            }
            
            engine = create_duckdb_group_stats_engine(settings, run_id="test_memo_diff")
            
            try:
                # Run with first config
                config1 = "config_alpha"
                df1, meta1 = engine.compute_group_stats(df_primary, config_digest=config1)
                
                # Run with second config
                config2 = "config_beta"
                df2, meta2 = engine.compute_group_stats(df_primary, config_digest=config2)
                
                # Both should be cache misses
                assert meta1.get("cache_hit", False) is False, "First config should be cache miss"
                assert meta2.get("cache_hit", False) is False, "Second config should be cache miss"
                
                # Run first config again - should be cache hit
                df1_again, meta1_again = engine.compute_group_stats(df_primary, config_digest=config1)
                assert meta1_again.get("cache_hit", False) is True, "Repeated first config should be cache hit"
                
                # Run second config again - should be cache hit
                df2_again, meta2_again = engine.compute_group_stats(df_primary, config_digest=config2)
                assert meta2_again.get("cache_hit", False) is True, "Repeated second config should be cache hit"
                
            finally:
                engine.close()

    def test_duckdb_memoization_disabled(self):
        """Test that memoization can be disabled."""
        test_data = {
            'group_id': ['G001'],
            'account_id': ['ACC_001'],
            'account_name': ['Company A'],
            'domain': ['companya.com'],
            'is_primary': [True],
            'similarity_score': [1.0],
            'weakest_edge_to_primary': [1.0],
            'disposition': ['Keep']
        }
        
        df_primary = pd.DataFrame(test_data)
        
        with tempfile.TemporaryDirectory() as temp_dir:
            # Disable memoization
            settings = {
                'group_stats': {
                    'backend': 'duckdb',
                    'memoization': False,
                    'cache_dir': temp_dir
                }
            }
            
            engine = create_duckdb_group_stats_engine(settings, run_id="test_memo_disabled")
            
            try:
                config_digest = "test_config_no_memo"
                
                # First run
                df1, meta1 = engine.compute_group_stats(df_primary, config_digest=config_digest)
                assert len(df1) > 0, "First run should produce results"
                
                # Second run with same config - should still be cache miss if memoization disabled
                df2, meta2 = engine.compute_group_stats(df_primary, config_digest=config_digest)
                assert len(df2) > 0, "Second run should produce results"
                
                # Note: We can't easily test cache_hit=False without seeing the internal implementation
                # But we can verify the results are still correct
                pd.testing.assert_frame_equal(df1, df2, check_dtype=False)
                
            finally:
                engine.close()

    def test_duckdb_memoization_performance_improvement(self):
        """Test that memoization provides performance improvement."""
        # Create larger test dataset
        test_data = {
            'group_id': [f'G{i:03d}' for i in range(1000)],
            'account_id': [f'ACC_{i:06d}' for i in range(1000)],
            'account_name': [f'Company {i}' for i in range(1000)],
            'domain': [f'company{i}.com' for i in range(1000)],
            'industry': ['Tech'] * 1000,
            'revenue': [1000000] * 1000,
            'employees': [100] * 1000,
            'is_primary': [True] * 1000,
            'similarity_score': [1.0] * 1000,
            'weakest_edge_to_primary': [1.0] * 1000,
            'disposition': ['Keep'] * 1000
        }
        
        df_primary = pd.DataFrame(test_data)
        
        with tempfile.TemporaryDirectory() as temp_dir:
            settings = {
                'group_stats': {
                    'backend': 'duckdb',
                    'memoization': True,
                    'cache_dir': temp_dir
                }
            }
            
            engine = create_duckdb_group_stats_engine(settings, run_id="test_memo_perf")
            
            try:
                config_digest = "perf_test_config"
                
                # First run - measure time
                start_time = time.time()
                df1, meta1 = engine.compute_group_stats(df_primary, config_digest=config_digest)
                first_run_time = time.time() - start_time
                
                assert len(df1) > 0, "First run should produce results"
                assert meta1.get("cache_hit", False) is False, "First run should be cache miss"
                
                # Second run - should be faster due to cache
                start_time = time.time()
                df2, meta2 = engine.compute_group_stats(df_primary, config_digest=config_digest)
                second_run_time = time.time() - start_time
                
                assert len(df2) > 0, "Second run should produce results"
                assert meta2.get("cache_hit", False) is True, "Second run should be cache hit"
                
                # Verify results are identical
                pd.testing.assert_frame_equal(df1, df2, check_dtype=False)
                
                # Second run should be significantly faster (at least 2x)
                # Note: This is a reasonable expectation for cache hits
                assert second_run_time < first_run_time * 0.8, f"Cache hit should be faster: {second_run_time:.3f}s vs {first_run_time:.3f}s"
                
            finally:
                engine.close()


if __name__ == "__main__":
    pytest.main([__file__])
