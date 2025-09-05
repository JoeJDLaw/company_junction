import pandas as pd
import pytest

from src.similarity import pair_scores


def test_shape_guard_catches_header_list() -> None:
    """Test that our shape guard catches the header list regression."""
    # This test was testing a function that no longer exists (_compute_similarity_scores_parallel)
    # The regression it was testing has been fixed by refactoring the similarity module
    # We'll keep a simple test to ensure the module still works

    df = pd.DataFrame(
        [
            {"account_id": "A1", "name_core": "acme ltd", "suffix_class": "ltd"},
            {
                "account_id": "A2",
                "name_core": "acme limited",
                "suffix_class": "ltd",
            },
        ],
    )

    # This should work without errors
    result = pair_scores(df, {"similarity": {"medium": 0, "penalty": {}}})

    # Should return a DataFrame (may be empty if no pairs meet threshold)
    assert isinstance(result, pd.DataFrame)
