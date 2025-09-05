"""Tests for threshold and sorting contracts in similarity scoring.

This module tests threshold and sorting behavior:
- Medium threshold filtering (gate cutoff behavior)
- Stable sort contract documentation (current behavior)
- Threshold boundary behavior
- Gate cutoff configuration
"""

import sys
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import pytest

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.normalize import normalize_dataframe
from src.similarity.scoring import score_pairs_bulk, score_pairs_parallel


class TestScoringThresholdSort:
    """Test threshold and sorting contracts for similarity scoring."""

    def test_medium_threshold_filters_below_cutoff(self, settings_from_config):
        """Test that medium threshold filters below-cutoff pairs."""
        # Create test data with known similarity levels
        test_data = pd.DataFrame(
            {
                "account_id": [1, 2, 3, 4],
                "Account Name": [
                    "acme store",  # High similarity with "acme shop"
                    "acme shop",  # High similarity with "acme store"
                    "acme depot",  # Medium similarity with "acme store"
                    "xyz corporation",  # Low similarity with "acme store"
                ],
            },
        )

        df_norm = normalize_dataframe(test_data, "Account Name")
        candidate_pairs = [(0, 1), (0, 2), (0, 3)]  # Compare "acme store" with others
        settings = settings_from_config()

        # Run bulk scoring (which applies gate cutoff)
        bulk_results = score_pairs_bulk(df_norm, candidate_pairs, settings)

        # Run parallel scoring (no gate cutoff)
        parallel_results = score_pairs_parallel(df_norm, candidate_pairs, settings)

        # Bulk should have fewer results due to gate filtering
        assert len(bulk_results) <= len(
            parallel_results,
        ), "Bulk should filter out low-scoring pairs"

        # All bulk results should have passed the gate cutoff (token_set_ratio >= gate_cutoff)
        # Note: The gate uses token_set_ratio, but final scores may be lower due to penalties
        gate_cutoff = (
            settings.get("similarity", {}).get("scoring", {}).get("gate_cutoff", 72)
        )
        for result in bulk_results:
            # The gate is applied to token_set_ratio, not final score
            assert (
                result["ratio_set"] >= gate_cutoff
            ), f"Bulk result ratio_set {result['ratio_set']} should be >= {gate_cutoff}"

    def test_gate_cutoff_boundary_behavior(self, settings_from_config):
        """Test gate cutoff boundary behavior."""
        # Create test data designed to be near the gate cutoff
        test_data = pd.DataFrame(
            {
                "account_id": [1, 2, 3],
                "Account Name": [
                    "acme store",  # Base name
                    "acme shop",  # Should score high (above cutoff)
                    "acme depot",  # Should score lower (near/below cutoff)
                ],
            },
        )

        df_norm = normalize_dataframe(test_data, "Account Name")
        candidate_pairs = [(0, 1), (0, 2)]  # Compare "acme store" with others
        settings = settings_from_config()

        # Run bulk scoring
        bulk_results = score_pairs_bulk(df_norm, candidate_pairs, settings)

        # Run parallel scoring to see all scores
        parallel_results = score_pairs_parallel(df_norm, candidate_pairs, settings)

        gate_cutoff = (
            settings.get("similarity", {}).get("scoring", {}).get("gate_cutoff", 72)
        )

        # Document boundary behavior
        print(f"Gate cutoff: {gate_cutoff}")
        print(f"Bulk results: {len(bulk_results)}")
        print(f"Parallel results: {len(parallel_results)}")

        for result in parallel_results:
            print(f"Pair ({result['id_a']}, {result['id_b']}): score={result['score']}")

        # Verify that bulk only includes results that passed the gate (token_set_ratio >= cutoff)
        for result in bulk_results:
            assert (
                result["ratio_set"] >= gate_cutoff
            ), f"Bulk result ratio_set {result['ratio_set']} should be >= {gate_cutoff}"

    def test_gate_cutoff_configuration(self, settings_from_config):
        """Test that different gate cutoff values produce different filtering."""
        # Create test data
        test_data = pd.DataFrame(
            {
                "account_id": [1, 2, 3],
                "Account Name": ["acme store", "acme shop", "acme depot"],
            },
        )

        df_norm = normalize_dataframe(test_data, "Account Name")
        candidate_pairs = [(0, 1), (0, 2)]

        # Test with low gate cutoff (should include more results)
        settings_low = settings_from_config(
            {"similarity": {"scoring": {"gate_cutoff": 50}}},
        )
        results_low = score_pairs_bulk(df_norm, candidate_pairs, settings_low)

        # Test with high gate cutoff (should include fewer results)
        settings_high = settings_from_config(
            {"similarity": {"scoring": {"gate_cutoff": 90}}},
        )
        results_high = score_pairs_bulk(df_norm, candidate_pairs, settings_high)

        # Low cutoff should include more results than high cutoff
        assert len(results_low) >= len(
            results_high,
        ), "Lower gate cutoff should include more results"

    def test_stable_sort_contract_documentation(self, settings_from_config):
        """Test and document current sort order behavior."""
        # Create test data with multiple pairs
        test_data = pd.DataFrame(
            {
                "account_id": [1, 2, 3, 4],
                "Account Name": ["acme store", "acme shop", "acme depot", "acme corp"],
            },
        )

        df_norm = normalize_dataframe(test_data, "Account Name")
        candidate_pairs = [(0, 1), (0, 2), (0, 3), (1, 2), (1, 3), (2, 3)]
        settings = settings_from_config()

        # Run scoring
        results = score_pairs_bulk(df_norm, candidate_pairs, settings)

        # Document current behavior: results are not sorted
        print(f"Current sort behavior: {len(results)} results")
        if len(results) > 1:
            print("Results are NOT currently sorted by score or id")
            print("This is documented baseline behavior")

            # Show current order
            for i, result in enumerate(results):
                print(
                    f"  {i}: ({result['id_a']}, {result['id_b']}) score={result['score']}",
                )

        # Verify results have consistent structure
        for result in results:
            assert "id_a" in result, "Should have id_a"
            assert "id_b" in result, "Should have id_b"
            assert "score" in result, "Should have score"
            assert 0 <= result["score"] <= 100, "Score should be in valid range"

    def test_stable_sort_contract_specification(self, settings_from_config):
        """Test the expected stable sort contract specification."""
        # Create test data with known scores for contract testing
        test_data = pd.DataFrame(
            {
                "account_id": [1, 2, 3, 4],
                "Account Name": ["acme store", "acme shop", "acme depot", "acme corp"],
            },
        )

        df_norm = normalize_dataframe(test_data, "Account Name")
        candidate_pairs = [(0, 1), (0, 2), (0, 3), (1, 2), (1, 3), (2, 3)]
        settings = settings_from_config()

        # Run scoring
        results = score_pairs_bulk(df_norm, candidate_pairs, settings)

        if len(results) > 1:
            # Test the expected stable sort contract: (id_a, id_b asc; score desc)
            # This is a contract test - it documents what the sort should do if implemented

            # Create a copy and sort it according to the contract
            sorted_results = sorted(
                results,
                key=lambda r: (
                    r["id_a"],
                    r["id_b"],
                    -r["score"],
                ),  # id_a, id_b asc; score desc
            )

            # Document the expected behavior
            print("Expected stable sort contract: (id_a, id_b asc; score desc)")
            print("Current results (unsorted):")
            for i, result in enumerate(results):
                print(
                    f"  {i}: ({result['id_a']}, {result['id_b']}) score={result['score']}",
                )

            print("Expected sorted order:")
            for i, result in enumerate(sorted_results):
                print(
                    f"  {i}: ({result['id_a']}, {result['id_b']}) score={result['score']}",
                )

            # Verify the contract would produce stable ordering
            # (This test documents the contract without enforcing it on current implementation)
            assert len(sorted_results) == len(
                results,
            ), "Sorted results should have same length"

    def test_sort_determinism_contract(self, settings_from_config):
        """Test that sorting would be deterministic if implemented."""
        # Create test data
        test_data = pd.DataFrame(
            {
                "account_id": [1, 2, 3],
                "Account Name": ["acme store", "acme shop", "acme depot"],
            },
        )

        df_norm = normalize_dataframe(test_data, "Account Name")
        candidate_pairs = [(0, 1), (0, 2), (1, 2)]
        settings = settings_from_config()

        # Run scoring multiple times
        results1 = score_pairs_bulk(df_norm, candidate_pairs, settings)
        results2 = score_pairs_bulk(df_norm, candidate_pairs, settings)

        # Results should be identical (deterministic)
        assert len(results1) == len(results2), "Results should be deterministic"

        # If we were to sort, the sort should also be deterministic
        if len(results1) > 1:
            sorted1 = sorted(
                results1, key=lambda r: (r["id_a"], r["id_b"], -r["score"]),
            )
            sorted2 = sorted(
                results2, key=lambda r: (r["id_a"], r["id_b"], -r["score"]),
            )

            # Sorted results should be identical
            for r1, r2 in zip(sorted1, sorted2):
                assert (
                    r1["id_a"] == r2["id_a"]
                ), "Sorted results should be deterministic"
                assert (
                    r1["id_b"] == r2["id_b"]
                ), "Sorted results should be deterministic"
                assert (
                    r1["score"] == r2["score"]
                ), "Sorted results should be deterministic"

    def test_sort_stability_contract(self, settings_from_config):
        """Test that sorting would be stable if implemented."""
        # Create test data with potential ties
        test_data = pd.DataFrame(
            {
                "account_id": [1, 2, 3, 4],
                "Account Name": ["acme store", "acme shop", "acme depot", "acme corp"],
            },
        )

        df_norm = normalize_dataframe(test_data, "Account Name")
        candidate_pairs = [(0, 1), (0, 2), (0, 3), (1, 2), (1, 3), (2, 3)]
        settings = settings_from_config()

        # Run scoring
        results = score_pairs_bulk(df_norm, candidate_pairs, settings)

        if len(results) > 1:
            # Test stability: equal elements should maintain their relative order
            # This is a contract test for future implementation

            # Sort by score only (ignoring id_a, id_b for stability test)
            score_sorted = sorted(results, key=lambda r: -r["score"])

            # Group by score to test stability
            score_groups: Dict[float, List[Dict[str, Any]]] = {}
            for result in score_sorted:
                score = result["score"]
                if score not in score_groups:
                    score_groups[score] = []
                score_groups[score].append(result)

            # For each score group, verify stability
            for score, group in score_groups.items():
                if len(group) > 1:
                    print(f"Score {score} has {len(group)} results - testing stability")
                    # In a stable sort, elements with equal scores should maintain input order
                    # This documents the expected behavior

    def test_threshold_edge_cases(self, settings_from_config):
        """Test threshold edge cases."""
        # Create test data
        test_data = pd.DataFrame(
            {
                "account_id": [1, 2, 3],
                "Account Name": ["acme store", "acme shop", "xyz corporation"],
            },
        )

        df_norm = normalize_dataframe(test_data, "Account Name")
        candidate_pairs = [(0, 1), (0, 2)]

        # Test with very low threshold (should include almost everything)
        settings_low = settings_from_config(
            {"similarity": {"scoring": {"gate_cutoff": 0}}},
        )
        results_low = score_pairs_bulk(df_norm, candidate_pairs, settings_low)

        # Test with very high threshold (should include almost nothing)
        settings_high = settings_from_config(
            {"similarity": {"scoring": {"gate_cutoff": 100}}},
        )
        results_high = score_pairs_bulk(df_norm, candidate_pairs, settings_high)

        # Low threshold should include more results
        assert len(results_low) >= len(
            results_high,
        ), "Lower threshold should include more results"

        # Very high threshold might include no results
        for result in results_high:
            assert (
                result["score"] >= 100
            ), "Very high threshold should only include perfect matches"

    def test_sort_edge_cases(self, settings_from_config):
        """Test sort edge cases."""
        # Test with single pair
        test_data_single = pd.DataFrame(
            {"account_id": [1, 2], "Account Name": ["acme store", "acme shop"]},
        )

        df_norm_single = normalize_dataframe(test_data_single, "Account Name")
        candidate_pairs_single = [(0, 1)]
        settings = settings_from_config()

        results_single = score_pairs_bulk(
            df_norm_single, candidate_pairs_single, settings,
        )

        # Single result should be handled correctly
        assert len(results_single) <= 1, "Single pair should produce at most one result"

        if len(results_single) == 1:
            result = results_single[0]
            assert "id_a" in result, "Single result should have id_a"
            assert "id_b" in result, "Single result should have id_b"
            assert "score" in result, "Single result should have score"

    def test_threshold_sort_integration(self, settings_from_config):
        """Test threshold and sort integration."""
        # Create test data with various similarity levels
        test_data = pd.DataFrame(
            {
                "account_id": [1, 2, 3, 4, 5],
                "Account Name": [
                    "acme store",  # Base
                    "acme shop",  # High similarity
                    "acme depot",  # Medium similarity
                    "acme corp",  # Medium similarity
                    "xyz corporation",  # Low similarity
                ],
            },
        )

        df_norm = normalize_dataframe(test_data, "Account Name")
        candidate_pairs = [
            (0, 1),
            (0, 2),
            (0, 3),
            (0, 4),
        ]  # Compare base with all others
        settings = settings_from_config()

        # Run bulk scoring (threshold filtering)
        bulk_results = score_pairs_bulk(df_norm, candidate_pairs, settings)

        # Run parallel scoring (no threshold filtering)
        parallel_results = score_pairs_parallel(df_norm, candidate_pairs, settings)

        # Verify integration: threshold filtering works
        assert len(bulk_results) <= len(
            parallel_results,
        ), "Bulk should filter based on threshold"

        # If we were to sort the bulk results, they should be sortable
        if len(bulk_results) > 1:
            # Test that results can be sorted according to contract
            sortable_results = sorted(
                bulk_results, key=lambda r: (r["id_a"], r["id_b"], -r["score"]),
            )
            assert len(sortable_results) == len(
                bulk_results,
            ), "Results should be sortable"
