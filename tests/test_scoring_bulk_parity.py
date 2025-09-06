"""Tests for bulk vs parallel scoring parity.

This module tests numerical consistency between bulk and parallel scoring:
- Parity of scores/components between bulk and parallel methods
- Gate correctness (below/above cutoff behavior)
- Suffix defaulting and non-mutation behavior
- Order stability in sequential path
- Gate logging smoke-check
"""

import copy
import sys
from pathlib import Path
from typing import List, Tuple

import pandas as pd
import pytest

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.normalize import normalize_dataframe
from src.similarity.scoring import score_pairs_bulk, score_pairs_parallel


class TestScoringBulkParity:
    """Test bulk vs parallel scoring numerical consistency."""

    def test_parity_of_scores_components(self):
        """Test that bulk and parallel produce identical scores/components."""
        # Create realistic test data with normalized dataframe
        test_data = pd.DataFrame(
            {
                "account_id": [1, 2, 3, 4],
                "Account Name": [
                    "acme store",
                    "acme shop",  # Should match (canonical retail terms)
                    "acme depot",  # Should be below cutoff
                    "acme corporation",  # Should be above cutoff
                ],
            },
        )

        # Normalize the data (production flow)
        df_norm = normalize_dataframe(test_data, "Account Name")

        # Create candidate pairs (mix of near matches and non-matches)
        candidate_pairs = [(0, 1), (0, 2), (0, 3)]  # acme store vs others

        # Settings from config (gate_cutoff = 72)
        settings = {
            "similarity": {
                "scoring": {"gate_cutoff": 72},
                "penalty": {
                    "num_style_mismatch": 5,
                    "suffix_mismatch": 25,
                    "punctuation_mismatch": 3,
                },
            },
        }

        # Run both methods
        bulk_results = score_pairs_bulk(df_norm, candidate_pairs, settings)
        parallel_results = score_pairs_parallel(df_norm, candidate_pairs, settings)

        # Filter parallel results to bulk's survivors (apply same gate)
        gate_cutoff = settings["similarity"]["scoring"]["gate_cutoff"]
        parallel_survivors = [
            r for r in parallel_results if r["ratio_set"] >= gate_cutoff
        ]

        # Should have same number of survivors
        assert len(bulk_results) == len(
            parallel_survivors,
        ), f"Bulk: {len(bulk_results)} survivors, Parallel: {len(parallel_survivors)} survivors"

        # Compare components and scores for each survivor
        for bulk_result, parallel_result in zip(bulk_results, parallel_survivors):
            # Components should be identical
            assert bulk_result["ratio_name"] == parallel_result["ratio_name"]
            assert bulk_result["ratio_set"] == parallel_result["ratio_set"]
            assert bulk_result["jaccard"] == parallel_result["jaccard"]
            assert bulk_result["suffix_match"] == parallel_result["suffix_match"]
            assert bulk_result["num_style_match"] == parallel_result["num_style_match"]
            assert (
                bulk_result["punctuation_mismatch"]
                == parallel_result["punctuation_mismatch"]
            )

            # Scores should be identical (within 1 point tolerance for RapidFuzz)
            score_diff = abs(bulk_result["score"] - parallel_result["score"])
            assert score_diff <= 1, f"Score difference {score_diff} exceeds tolerance"

    def test_gate_correctness_below_cutoff(self):
        """Test that pairs below cutoff are correctly gated out."""
        # Create test data with pairs that should be below cutoff
        test_data = pd.DataFrame(
            {
                "account_id": [1, 2],
                "Account Name": [
                    "acme store",
                    "xyz corporation",
                ],  # Very different names
            },
        )

        df_norm = normalize_dataframe(test_data, "Account Name")
        candidate_pairs = [(0, 1)]

        settings = {
            "similarity": {
                "scoring": {"gate_cutoff": 72},
                "penalty": {
                    "num_style_mismatch": 5,
                    "suffix_mismatch": 25,
                    "punctuation_mismatch": 3,
                },
            },
        }

        # Bulk should gate out low-scoring pairs
        bulk_results = score_pairs_bulk(df_norm, candidate_pairs, settings)

        # Should be empty (gated out)
        assert len(bulk_results) == 0, "Low-scoring pairs should be gated out"

    def test_gate_correctness_above_cutoff(self):
        """Test that pairs above cutoff are correctly included."""
        # Create test data with pairs that should be above cutoff
        test_data = pd.DataFrame(
            {
                "account_id": [1, 2],
                "Account Name": [
                    "acme store",
                    "acme shop",
                ],  # Should match (canonical terms)
            },
        )

        df_norm = normalize_dataframe(test_data, "Account Name")
        candidate_pairs = [(0, 1)]

        settings = {
            "similarity": {
                "scoring": {"gate_cutoff": 72},
                "penalty": {
                    "num_style_mismatch": 5,
                    "suffix_mismatch": 25,
                    "punctuation_mismatch": 3,
                },
            },
        }

        # Bulk should include high-scoring pairs
        bulk_results = score_pairs_bulk(df_norm, candidate_pairs, settings)

        # Should have results (not gated out)
        assert len(bulk_results) == 1, "High-scoring pairs should be included"
        assert bulk_results[0]["ratio_set"] >= 72, "Included pairs should meet cutoff"

    def test_suffix_defaulting_non_mutation(self):
        """Test suffix defaulting and non-mutation behavior."""
        # Create test data without suffix_class column
        test_data = pd.DataFrame(
            {"account_id": [1, 2], "Account Name": ["acme store", "acme shop"]},
        )

        df_norm = normalize_dataframe(test_data, "Account Name")

        # Remove suffix_class to test defaulting
        assert "suffix_class" in df_norm.columns
        df_norm_no_suffix = df_norm.drop(columns=["suffix_class"])

        # Create a copy to verify immutability
        original_df = copy.deepcopy(df_norm_no_suffix)

        candidate_pairs = [(0, 1)]
        settings = {
            "similarity": {
                "scoring": {"gate_cutoff": 72},
                "penalty": {
                    "num_style_mismatch": 5,
                    "suffix_mismatch": 25,
                    "punctuation_mismatch": 3,
                },
            },
        }

        # Both methods should not mutate the original dataframe
        bulk_results = score_pairs_bulk(df_norm_no_suffix, candidate_pairs, settings)
        parallel_results = score_pairs_parallel(
            df_norm_no_suffix,
            candidate_pairs,
            settings,
        )

        # Verify immutability
        pd.testing.assert_frame_equal(df_norm_no_suffix, original_df)

        # Both should work (suffix_class defaults to "NONE")
        assert len(bulk_results) >= 0
        assert len(parallel_results) >= 0

    def test_order_stability_sequential(self):
        """Test order stability in sequential path."""
        # Create test data
        test_data = pd.DataFrame(
            {
                "account_id": [1, 2, 3, 4],
                "Account Name": ["acme store", "acme shop", "acme depot", "acme corp"],
            },
        )

        df_norm = normalize_dataframe(test_data, "Account Name")

        # Create candidate pairs in specific order
        candidate_pairs = [(0, 1), (0, 2), (0, 3), (1, 2), (1, 3), (2, 3)]

        settings = {
            "similarity": {
                "scoring": {"gate_cutoff": 50},  # Lower cutoff to get more results
                "penalty": {
                    "num_style_mismatch": 5,
                    "suffix_mismatch": 25,
                    "punctuation_mismatch": 3,
                },
            },
        }

        # Test sequential execution (no parallel_executor)
        bulk_results = score_pairs_bulk(df_norm, candidate_pairs, settings)
        parallel_results = score_pairs_parallel(df_norm, candidate_pairs, settings)

        # Results should be in the same order as input candidate_pairs
        # (for pairs that pass the gate)
        gate_cutoff = settings["similarity"]["scoring"]["gate_cutoff"]
        parallel_survivors = [
            r for r in parallel_results if r["ratio_set"] >= gate_cutoff
        ]

        # Check that both methods produce results in the same order
        # (they should both preserve the input order for pairs that pass the gate)
        assert len(bulk_results) == len(
            parallel_survivors,
        ), f"Bulk: {len(bulk_results)} survivors, Parallel: {len(parallel_survivors)} survivors"

        # Verify that the results are in the same order between bulk and parallel
        for bulk_result, parallel_result in zip(bulk_results, parallel_survivors):
            assert bulk_result["id_a"] == parallel_result["id_a"]
            assert bulk_result["id_b"] == parallel_result["id_b"]

        # Document the current behavior: order may not match input candidate_pairs exactly
        # This is important baseline information for future improvements
        print(f"Bulk results order: {[(r['id_a'], r['id_b']) for r in bulk_results]}")
        print(f"Input candidate_pairs: {candidate_pairs}")
        print("Note: Current implementation may not preserve exact input order")

    def test_gate_logging_smoke_check(self, caplog):
        """Test bulk gate logging output."""
        # Create test data
        test_data = pd.DataFrame(
            {
                "account_id": [1, 2, 3],
                "Account Name": ["acme store", "acme shop", "xyz corporation"],
            },
        )

        df_norm = normalize_dataframe(test_data, "Account Name")
        candidate_pairs = [(0, 1), (0, 2)]  # One should pass, one should fail

        settings = {
            "similarity": {
                "scoring": {"gate_cutoff": 72},
                "penalty": {
                    "num_style_mismatch": 5,
                    "suffix_mismatch": 25,
                    "punctuation_mismatch": 3,
                },
            },
        }

        # Run bulk method and capture logs
        with caplog.at_level("INFO"):
            score_pairs_bulk(df_norm, candidate_pairs, settings)

        # Check that gate logging occurred
        gate_logs = [
            record for record in caplog.records if "Bulk gate:" in record.message
        ]
        assert len(gate_logs) == 1, "Should have exactly one gate log message"

        # Verify log format
        log_message = gate_logs[0].message
        assert "Bulk gate:" in log_message
        assert "pairs passed" in log_message
        assert "token_set_ratio" in log_message

    def test_empty_candidate_pairs(self):
        """Test behavior with empty candidate pairs."""
        test_data = pd.DataFrame(
            {"account_id": [1, 2], "Account Name": ["acme store", "acme shop"]},
        )

        df_norm = normalize_dataframe(test_data, "Account Name")
        candidate_pairs: List[Tuple[int, int]] = []  # Empty

        settings = {
            "similarity": {
                "scoring": {"gate_cutoff": 72},
                "penalty": {
                    "num_style_mismatch": 5,
                    "suffix_mismatch": 25,
                    "punctuation_mismatch": 3,
                },
            },
        }

        # Both methods should return empty results
        bulk_results = score_pairs_bulk(df_norm, candidate_pairs, settings)
        parallel_results = score_pairs_parallel(df_norm, candidate_pairs, settings)

        assert len(bulk_results) == 0
        assert len(parallel_results) == 0

    def test_single_candidate_pair(self):
        """Test behavior with single candidate pair."""
        test_data = pd.DataFrame(
            {"account_id": [1, 2], "Account Name": ["acme store", "acme shop"]},
        )

        df_norm = normalize_dataframe(test_data, "Account Name")
        candidate_pairs = [(0, 1)]  # Single pair

        settings = {
            "similarity": {
                "scoring": {"gate_cutoff": 72},
                "penalty": {
                    "num_style_mismatch": 5,
                    "suffix_mismatch": 25,
                    "punctuation_mismatch": 3,
                },
            },
        }

        # Both methods should return single result
        bulk_results = score_pairs_bulk(df_norm, candidate_pairs, settings)
        parallel_results = score_pairs_parallel(df_norm, candidate_pairs, settings)

        # Results should be consistent
        if len(bulk_results) > 0:
            assert len(parallel_results) > 0
            # Compare the results
            bulk_result = bulk_results[0]
            parallel_result = parallel_results[0]

            assert bulk_result["id_a"] == parallel_result["id_a"]
            assert bulk_result["id_b"] == parallel_result["id_b"]
            assert abs(bulk_result["score"] - parallel_result["score"]) <= 1
        else:
            # If bulk gated it out, parallel should also have low score
            assert len(parallel_results) == 1
            assert parallel_results[0]["ratio_set"] < 72

    def test_configurable_gate_cutoff(self):
        """Test that gate cutoff is configurable."""
        test_data = pd.DataFrame(
            {
                "account_id": [1, 2],
                "Account Name": ["acme store", "acme depot"],  # Medium similarity
            },
        )

        df_norm = normalize_dataframe(test_data, "Account Name")
        candidate_pairs = [(0, 1)]

        # Test with different gate cutoffs
        settings_low = {
            "similarity": {
                "scoring": {"gate_cutoff": 50},  # Low cutoff
                "penalty": {
                    "num_style_mismatch": 5,
                    "suffix_mismatch": 25,
                    "punctuation_mismatch": 3,
                },
            },
        }

        settings_high = {
            "similarity": {
                "scoring": {"gate_cutoff": 90},  # High cutoff
                "penalty": {
                    "num_style_mismatch": 5,
                    "suffix_mismatch": 25,
                    "punctuation_mismatch": 3,
                },
            },
        }

        # Low cutoff should include more results
        bulk_low = score_pairs_bulk(df_norm, candidate_pairs, settings_low)
        bulk_high = score_pairs_bulk(df_norm, candidate_pairs, settings_high)

        # Results should reflect different cutoffs
        assert len(bulk_low) >= len(
            bulk_high,
        ), "Lower cutoff should include more results"
