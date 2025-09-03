"""
Test file to verify the similarity fix works correctly.
Tests the canonical scorer and ensures bulk and parallel paths produce identical results.
"""

import pytest
import pandas as pd
from src.similarity import compute_score_components, _compute_similarity_scores_bulk, _compute_similarity_scores_parallel


def test_compute_score_components_golden():
    """Test the canonical scorer with known input/output pairs."""
    
    # Test case 1: Identical names
    result1 = compute_score_components(
        "apple inc", "apple inc", "INC", "INC", 
        {"num_style_mismatch": 5, "suffix_mismatch": 25}
    )
    
    assert result1["score"] == 100
    assert result1["ratio_name"] == 100
    assert result1["ratio_set"] == 100
    assert result1["jaccard"] == 1.0
    assert result1["num_style_match"] is True
    assert result1["suffix_match"] is True
    assert result1["base_score"] == 100.0
    
    # Test case 2: Similar names with penalties
    result2 = compute_score_components(
        "apple inc", "apple corp", "INC", "CORP", 
        {"num_style_mismatch": 5, "suffix_mismatch": 25}
    )
    
    assert result2["score"] < 100  # Should be penalized
    assert result2["suffix_match"] is False
    assert result2["base_score"] < 100.0
    
    # Test case 3: Numeric style mismatch penalty
    result3 = compute_score_components(
        "company 123 456", "company 789 012", "NONE", "NONE",
        {"num_style_mismatch": 5, "suffix_mismatch": 25}
    )
    
    # Should have numeric style penalty applied (different numeric patterns)
    assert result3["num_style_match"] is False
    assert result3["base_score"] < result3["ratio_name"] * 0.45 + result3["ratio_set"] * 0.35 + result3["jaccard"] * 20.0
    
    # Test case 4: Punctuation mismatch penalty (when enabled)
    result4 = compute_score_components(
        "company, inc", "company inc", "INC", "INC",
        {"num_style_mismatch": 5, "suffix_mismatch": 25, "punctuation_mismatch": 3},
        punctuation_mismatch=True
    )
    
    # Should have punctuation penalty applied
    assert result4["punctuation_mismatch"] is True
    assert result4["base_score"] < result4["ratio_name"] * 0.45 + result4["ratio_set"] * 0.35 + result4["jaccard"] * 20.0


def test_bulk_survivors_never_exceed_input():
    """Regression test: ensure bulk survivors count never exceeds input pairs."""
    
    # Create test data
    df = pd.DataFrame({
        "name_core": ["apple inc", "apple corp", "microsoft", "google"],
        "suffix_class": ["INC", "CORP", "NONE", "NONE"],
        "account_id": ["001", "002", "003", "004"]
    })
    
    # Test with different gate cutoffs
    candidate_pairs = [(0, 1), (2, 3)]  # 2 pairs
    
    penalties = {"num_style_mismatch": 5, "suffix_mismatch": 25}
    
    # Test bulk scoring
    results = _compute_similarity_scores_bulk(
        df, candidate_pairs, penalties, gate_cutoff=72
    )
    
    # Critical assertion: survivors should never exceed input pairs
    assert len(results) <= len(candidate_pairs), f"Survivors ({len(results)}) exceeded input pairs ({len(candidate_pairs)})"
    
    # Log the actual counts for debugging
    print(f"Input pairs: {len(candidate_pairs)}")
    print(f"Survivors: {len(results)}")
    print(f"Survival rate: {len(results)/len(candidate_pairs)*100:.1f}%")


def test_bulk_and_parallel_equivalence():
    """Test that bulk and parallel paths produce identical scores for the same pairs."""
    
    # Create test data
    df = pd.DataFrame({
        "name_core": ["apple inc", "apple corp", "microsoft", "google"],
        "suffix_class": ["INC", "CORP", "NONE", "NONE"],
        "account_id": ["001", "002", "003", "004"]
    })
    
    candidate_pairs = [(0, 1), (2, 3)]
    penalties = {"num_style_mismatch": 5, "suffix_mismatch": 25}
    
    # Test bulk scoring (applies gate cutoff)
    bulk_results = _compute_similarity_scores_bulk(
        df, candidate_pairs, penalties, gate_cutoff=72
    )
    
    # Test parallel scoring (scores all pairs, no gate)
    parallel_results = _compute_similarity_scores_parallel(
        df, candidate_pairs, penalties, parallel_executor=None
    )
    
    # Bulk applies gate, parallel scores all - so parallel should have more results
    assert len(parallel_results) >= len(bulk_results), f"Parallel should score all pairs: {len(parallel_results)} vs bulk survivors: {len(bulk_results)}"
    
    # For pairs that passed the gate in bulk, find corresponding scores in parallel
    # and verify they're identical
    for bulk_result in bulk_results:
        # Find matching pair in parallel results
        matching_parallel = None
        for parallel_result in parallel_results:
            if (parallel_result["id_a"] == bulk_result["id_a"] and 
                parallel_result["id_b"] == bulk_result["id_b"]):
                matching_parallel = parallel_result
                break
        
        assert matching_parallel is not None, f"Could not find matching pair for {bulk_result['id_a']} - {bulk_result['id_b']}"
        
        # Scores should be identical
        assert bulk_result["score"] == matching_parallel["score"], f"Score mismatch: {bulk_result['score']} vs {matching_parallel['score']}"
        assert bulk_result["ratio_name"] == matching_parallel["ratio_name"]
        assert bulk_result["ratio_set"] == matching_parallel["ratio_set"]
        assert bulk_result["jaccard"] == matching_parallel["jaccard"]
        assert bulk_result["num_style_match"] == matching_parallel["num_style_match"]
        assert bulk_result["suffix_match"] == matching_parallel["suffix_match"]
    
    print(f"✅ Equivalence test passed: {len(bulk_results)} bulk survivors match {len(parallel_results)} parallel results")


if __name__ == "__main__":
    # Run tests
    test_compute_score_components_golden()
    test_bulk_survivors_never_exceed_input()
    test_bulk_and_parallel_equivalence()
    print("✅ All tests passed!")
