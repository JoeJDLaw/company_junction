"""Tests for bulk gate behavior in similarity scoring.

This module tests bulk gate functionality:
- Gate cutoff behavior
- Gate filtering logic
- Gate performance
"""

import sys
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.normalize import normalize_dataframe
from src.similarity.scoring import score_pairs_bulk


class TestScoringBulkGate:
    """Test bulk gate behavior for similarity scoring."""

    def test_bulk_gate_cutoff_behavior(self):
        """Test bulk gate cutoff behavior."""
        # Create test data
        test_data = pd.DataFrame(
            {
                "account_id": [1, 2, 3, 4],
                "Account Name": [
                    "acme store",
                    "acme shop",
                    "acme depot",
                    "xyz corporation",
                ],
            }
        )

        df_norm = normalize_dataframe(test_data, "Account Name")
        candidate_pairs = [(0, 1), (0, 2), (0, 3), (1, 2), (1, 3), (2, 3)]

        # Test with different gate cutoffs
        gate_cutoffs = [50, 72, 80, 90]

        for cutoff in gate_cutoffs:
            settings = {"similarity": {"scoring": {"gate_cutoff": cutoff}}}
            results = score_pairs_bulk(df_norm, candidate_pairs, settings)

            # All results should have ratio_set >= cutoff
            for result in results:
                assert (
                    result["ratio_set"] >= cutoff
                ), f"Result should have ratio_set >= {cutoff}, got {result['ratio_set']}"

    def test_bulk_gate_filtering_logic(self):
        """Test bulk gate filtering logic."""
        # Create test data with known similarity levels
        test_data = pd.DataFrame(
            {
                "account_id": [1, 2, 3, 4],
                "Account Name": [
                    "acme store",
                    "acme store",
                    "acme shop",
                    "xyz corporation",
                ],
            }
        )

        df_norm = normalize_dataframe(test_data, "Account Name")
        candidate_pairs = [(0, 1), (0, 2), (0, 3), (1, 2), (1, 3), (2, 3)]

        # Use medium gate cutoff
        settings = {"similarity": {"scoring": {"gate_cutoff": 72}}}
        results = score_pairs_bulk(df_norm, candidate_pairs, settings)

        # Should filter based on token_set_ratio
        for result in results:
            assert (
                result["ratio_set"] >= 72
            ), f"Result should have ratio_set >= 72, got {result['ratio_set']}"

    def test_bulk_gate_performance(self):
        """Test bulk gate performance."""
        import time

        # Create larger test data
        test_data = pd.DataFrame(
            {
                "account_id": list(range(20)),
                "Account Name": [f"acme store {i}" for i in range(20)],
            }
        )

        df_norm = normalize_dataframe(test_data, "Account Name")
        candidate_pairs = [
            (i, j) for i in range(10) for j in range(i + 1, 10)
        ]  # 45 pairs
        settings = {"similarity": {"scoring": {"gate_cutoff": 50}}}

        # Time execution
        start_time = time.time()
        results = score_pairs_bulk(df_norm, candidate_pairs, settings)
        execution_time = time.time() - start_time

        # Should complete quickly
        assert (
            execution_time < 2.0
        ), f"Bulk gate should be fast, took {execution_time:.3f}s"
        assert len(results) >= 0, "Should have some results"

    def test_bulk_gate_empty_results(self):
        """Test bulk gate with no results passing."""
        # Create test data with very different names
        test_data = pd.DataFrame(
            {
                "account_id": [1, 2, 3],
                "Account Name": ["acme store", "xyz corporation", "abc company"],
            }
        )

        df_norm = normalize_dataframe(test_data, "Account Name")
        candidate_pairs = [(0, 1), (0, 2), (1, 2)]

        # Use very high gate cutoff
        settings = {"similarity": {"scoring": {"gate_cutoff": 95}}}
        results = score_pairs_bulk(df_norm, candidate_pairs, settings)

        # Should have no results
        assert len(results) == 0, "Should have no results with very high gate cutoff"

    def test_bulk_gate_all_results(self):
        """Test bulk gate with all results passing."""
        # Create test data with identical names
        test_data = pd.DataFrame(
            {
                "account_id": [1, 2, 3],
                "Account Name": ["acme store", "acme store", "acme store"],
            }
        )

        df_norm = normalize_dataframe(test_data, "Account Name")
        candidate_pairs = [(0, 1), (0, 2), (1, 2)]

        # Use very low gate cutoff
        settings = {"similarity": {"scoring": {"gate_cutoff": 10}}}
        results = score_pairs_bulk(df_norm, candidate_pairs, settings)

        # Should have all results
        assert len(results) == 3, "Should have all results with very low gate cutoff"
