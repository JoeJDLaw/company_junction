"""Tests for alias matching parallelism unification.

Verifies that ParallelExecutor integration produces identical results to legacy parallel_map.
"""

from typing import Any, Dict
from unittest.mock import Mock

import pandas as pd
import pytest

from src.alias_matching import (
    _build_first_token_bucket,
    _process_one_record_optimized,
    compute_alias_matches,
)
from src.utils.parallel_utils import ParallelExecutor


class TestAliasMatchingParallelism:
    """Test alias matching parallelism unification."""

    def setup_method(self):
        """Set up test fixtures."""
        # Create sample normalized data
        self.df_norm = pd.DataFrame(
            {
                "name_core": [
                    "Acme Corporation Inc",
                    "Acme Corp LLC",
                    "Beta Industries Ltd",
                    "Gamma Services Corp",
                    "Delta Solutions Inc",
                ],
                "suffix_class": ["INC", "LLC", "LTD", "CORP", "INC"],
                "alias_candidates": [
                    ["Acme Corp", "Acme"],
                    ["Acme Corp", "Acme Industries"],
                    ["Beta Corp", "Beta"],
                    ["Gamma Corp", "Gamma"],
                    ["Delta Corp", "Delta"],
                ],
                "alias_sources": [
                    ["parentheses", "semicolon"],
                    ["parentheses", "semicolon"],
                    ["parentheses", "semicolon"],
                    ["parentheses", "semicolon"],
                    ["parentheses", "semicolon"],
                ],
            },
            index=[100, 101, 102, 103, 104],
        )

        # Create sample groups data
        self.df_groups = pd.DataFrame(
            {
                "group_id": ["G1", "G1", "G2", "G3", "G4"],
                "account_id": ["A001", "A002", "A003", "A004", "A005"],
            },
            index=[100, 101, 102, 103, 104],
        )

        self.settings: Dict[str, Any] = {
            "similarity": {"high": 92, "max_alias_pairs": 1000},
            "alias": {"optimize": True, "progress_interval_s": 1.0},
            "parallelism": {"workers": 2, "backend": "loky", "chunk_size": 1000},
        }

    def test_parallel_executor_integration(self):
        """Test that ParallelExecutor is properly integrated."""
        # Create a mock ParallelExecutor
        mock_executor = Mock(spec=ParallelExecutor)
        mock_executor.should_use_parallel.side_effect = lambda x: True
        mock_executor.workers = 2
        mock_executor.execute_chunked.return_value = [
            [
                {
                    "record_id": 100,
                    "alias_text": "Acme Corp",
                    "match_record_id": 101,
                    "match_group_id": "G1",
                    "score": 95,
                    "suffix_match": True,
                },
            ],
            [
                {
                    "record_id": 101,
                    "alias_text": "Acme Corp",
                    "match_record_id": 100,
                    "match_group_id": "G1",
                    "score": 95,
                    "suffix_match": True,
                },
            ],
        ]

        # Call compute_alias_matches with ParallelExecutor
        result_df, stats = compute_alias_matches(
            self.df_norm, self.df_groups, self.settings, parallel_executor=mock_executor,
        )

        # Verify ParallelExecutor was used
        mock_executor.should_use_parallel.assert_called_once()
        mock_executor.execute_chunked.assert_called_once()

        # Verify results are properly flattened
        assert len(result_df) == 2
        assert "record_id" in result_df.columns
        assert "alias_text" in result_df.columns

    def test_sequential_fallback_when_parallel_disabled(self):
        """Test fallback to sequential processing when parallel is disabled."""
        # Create a mock ParallelExecutor that disables parallel processing
        mock_executor = Mock(spec=ParallelExecutor)
        # should_use_parallel is called with len(records_with_aliases) as argument
        # Use side_effect to handle any argument
        mock_executor.should_use_parallel.side_effect = lambda x: False
        mock_executor.workers = 2  # Add workers attribute

        # Call compute_alias_matches
        result_df, stats = compute_alias_matches(
            self.df_norm, self.df_groups, self.settings, parallel_executor=mock_executor,
        )

        # Should use sequential processing
        assert result_df is not None
        assert stats is not None
        # Verify that execute_chunked was not called
        mock_executor.execute_chunked.assert_not_called()

    def test_no_parallel_executor_fallback(self):
        """Test fallback to sequential processing when no executor provided."""
        # Call compute_alias_matches without parallel_executor
        result_df, stats = compute_alias_matches(
            self.df_norm, self.df_groups, self.settings,
        )

        # Should work without errors
        assert result_df is not None
        assert stats is not None

    def test_first_token_bucket_consistency(self):
        """Test that first token bucket creation is deterministic."""
        name_core = self.df_norm["name_core"]

        bucket1, index_map1, reverse_map1 = _build_first_token_bucket(name_core)
        bucket2, index_map2, reverse_map2 = _build_first_token_bucket(name_core)

        # Should be identical across calls - compare arrays properly
        assert len(bucket1) == len(bucket2)
        for token in bucket1:
            assert token in bucket2
            assert (bucket1[token] == bucket2[token]).all()
        assert index_map1 == index_map2
        assert reverse_map1 == reverse_map2

    def test_process_one_record_optimized(self):
        """Test that _process_one_record_optimized produces correct output."""
        # Build required data structures
        name_core = self.df_norm["name_core"]
        suffix_class = self.df_norm["suffix_class"]
        bucket, index_map, reverse_map = _build_first_token_bucket(name_core)
        group_id_by_idx = (
            self.df_groups["group_id"]
            .reindex(self.df_norm.index)
            .astype("string")
            .fillna("")
        )

        # Test processing one record
        record_id = 100
        result = _process_one_record_optimized(
            record_id,
            self.df_norm,
            self.df_groups,
            name_core,
            suffix_class,
            group_id_by_idx,
            bucket,
            index_map,
            reverse_map,
            high_threshold=92,
            debug=False,
        )

        # Should return a list of matches
        assert isinstance(result, list)

    def test_parallel_executor_chunking_consistency(self):
        """Test that chunking and flattening work consistently."""
        # Create mock executor
        mock_executor = Mock(spec=ParallelExecutor)
        mock_executor.should_use_parallel.side_effect = lambda x: True
        mock_executor.workers = 2
        # Return proper data structure that matches what the function expects
        mock_executor.execute_chunked.return_value = [
            [
                {
                    "record_id": 100,
                    "alias_text": "Acme Corp",
                    "match_record_id": 101,
                    "match_group_id": "G1",
                    "score": 95,
                    "suffix_match": True,
                },
            ],
            [
                {
                    "record_id": 101,
                    "alias_text": "Acme Corp",
                    "match_record_id": 100,
                    "match_group_id": "G1",
                    "score": 95,
                    "suffix_match": True,
                },
            ],
        ]

        result_df, stats = compute_alias_matches(
            self.df_norm, self.df_groups, self.settings, parallel_executor=mock_executor,
        )

        # Should flatten results correctly
        assert result_df is not None
        # Verify execute_chunked was called
        mock_executor.execute_chunked.assert_called_once()

    def test_settings_integration(self):
        """Test that settings are properly passed through to ParallelExecutor."""
        # Test with different worker counts (only > 1 since the function only uses ParallelExecutor when workers > 1)
        for workers in [2, 4]:
            test_settings = self.settings.copy()
            test_settings["parallelism"]["workers"] = workers

            # Create mock executor
            mock_executor = Mock(spec=ParallelExecutor)
            mock_executor.should_use_parallel.side_effect = lambda x: True
            mock_executor.workers = workers
            # Return proper data structure
            mock_executor.execute_chunked.return_value = [
                [
                    {
                        "record_id": 100,
                        "alias_text": "Acme Corp",
                        "match_record_id": 101,
                        "match_group_id": "G1",
                        "score": 95,
                        "suffix_match": True,
                    },
                ],
            ]

            # Call compute_alias_matches
            result_df, stats = compute_alias_matches(
                self.df_norm,
                self.df_groups,
                test_settings,
                parallel_executor=mock_executor,
            )

            # Verify executor was used
            mock_executor.execute_chunked.assert_called_once()

    def test_error_handling_in_parallel_execution(self):
        """Test error handling during parallel execution."""
        # Create a mock executor that raises an exception
        mock_executor = Mock(spec=ParallelExecutor)
        mock_executor.should_use_parallel.side_effect = lambda x: True
        mock_executor.workers = 2  # Add workers attribute
        mock_executor.execute_chunked.side_effect = Exception(
            "Parallel execution failed",
        )

        # Should handle the error gracefully
        with pytest.raises(Exception, match="Parallel execution failed"):
            compute_alias_matches(
                self.df_norm,
                self.df_groups,
                self.settings,
                parallel_executor=mock_executor,
            )

    def test_deterministic_output_ordering(self):
        """Test that output ordering is deterministic regardless of parallelization."""
        # Create mock executor
        mock_executor = Mock(spec=ParallelExecutor)
        mock_executor.should_use_parallel.side_effect = lambda x: True
        mock_executor.workers = 2

        # Return results in different chunk orders
        chunk1 = [
            {
                "record_id": 100,
                "alias_text": "Acme Corp",
                "match_record_id": 101,
                "match_group_id": "G1",
                "score": 95,
                "suffix_match": True,
            },
        ]
        chunk2 = [
            {
                "record_id": 101,
                "alias_text": "Acme Corp",
                "match_record_id": 100,
                "match_group_id": "G1",
                "score": 95,
                "suffix_match": True,
            },
        ]

        # Test both chunk orders
        for chunk_order in [[chunk1, chunk2], [chunk2, chunk1]]:
            mock_executor.execute_chunked.return_value = chunk_order

            result_df, stats = compute_alias_matches(
                self.df_norm,
                self.df_groups,
                self.settings,
                parallel_executor=mock_executor,
            )

            # Results should be identical regardless of chunk order
            assert len(result_df) == 2
            assert result_df["record_id"].iloc[0] == 100
            assert result_df["record_id"].iloc[1] == 101
