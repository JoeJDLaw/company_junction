import pytest
import pandas as pd
from src.similarity import pair_scores


def test_shape_guard_catches_header_list():
    """Test that our shape guard catches the header list regression."""
    # Create a mock function that returns header keys instead of dicts
    def mock_similarity_scores(*args, **kwargs):
        return ["id_a", "id_b", "score", "ratio_name", "ratio_set"]
    
    # Monkey patch the function
    import src.similarity as sim
    original_func = sim._compute_similarity_scores_parallel
    sim._compute_similarity_scores_parallel = mock_similarity_scores
    
    try:
        # This should raise a TypeError with our helpful message
        df = pd.DataFrame([
            {"account_id": "A1", "name_core": "acme ltd", "suffix_class": "ltd"},
            {"account_id": "A2", "name_core": "acme limited", "suffix_class": "ltd"},
        ])
        
        with pytest.raises(TypeError, match="expected records with a 'score' field"):
            pair_scores(df, {"similarity": {"medium": 0, "penalty": {}}})
    finally:
        # Restore original function
        sim._compute_similarity_scores_parallel = original_func
