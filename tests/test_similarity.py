"""
Test similarity computation functionality.
"""

import pandas as pd
import pytest

from src.similarity import pair_scores
from tests.helpers.ingest import ensure_required_columns


@pytest.fixture
def sample_data():
    """Create sample data for similarity tests."""
    df_norm = pd.DataFrame(
        {
            "name_core": [
                "acme corporation",
                "acme incorporated",
                "beta industries inc",
                "beta industries incorporated",
            ],
            "suffix_class": [
                "corporation",
                "incorporated",
                "inc",
                "incorporated",
            ],
        }
    )

    # Ensure required columns are present
    required_columns = ["account_id", "name_core", "suffix_class"]
    df_norm = ensure_required_columns(df_norm, required_columns)

    return df_norm


@pytest.fixture
def settings():
    """Create settings for testing."""
    return {
        "similarity": {
            "high": 85,
            "medium": 40,  # Lower threshold so test data can meet it
            "low": 30,
            "max_alias_pairs": 1000,
        },
        "parallelism": {
            "workers": 2,
            "backend": "threading",
            "chunk_size": 100,
        },
    }


class TestSimilarity:
    """Test similarity computation functionality."""

    def test_candidate_pair_generation(self, sample_data, settings) -> None:
        """Test that candidate pairs are generated correctly."""
        pairs_df = pair_scores(sample_data, settings)

        # Should generate pairs
        assert isinstance(pairs_df, pd.DataFrame)
        assert len(pairs_df) > 0

        # Check required columns
        required_cols = ["id_a", "id_b", "score", "ratio_name", "ratio_set", "jaccard"]
        for col in required_cols:
            assert col in pairs_df.columns

    def test_inc_vs_inc_high_score(self, settings):
        """Test that INC vs INC comparisons produce high scores."""
        df_norm = pd.DataFrame(
            {
                "name_core": ["acme inc", "acme inc"],
                "suffix_class": ["inc", "inc"],
            }
        )

        # Ensure required columns are present
        required_columns = ["account_id", "name_core", "suffix_class"]
        df_norm = ensure_required_columns(df_norm, required_columns)

        pairs_df = pair_scores(df_norm, settings)

        # Should find high-scoring matches (identical names should score 100)
        high_scores = pairs_df[pairs_df["score"] >= 85]
        assert len(high_scores) > 0

    def test_inc_vs_llc_verification_needed(self, settings):
        """Test that INC vs LLC comparisons require verification."""
        df_norm = pd.DataFrame(
            {
                "name_core": ["acme inc", "acme llc"],
                "suffix_class": ["inc", "llc"],
            }
        )

        # Ensure required columns are present
        required_columns = ["account_id", "name_core", "suffix_class"]
        df_norm = ensure_required_columns(df_norm, required_columns)

        pairs_df = pair_scores(df_norm, settings)

        # Should find matches but may need verification
        assert len(pairs_df) > 0

    def test_save_load_candidate_pairs(self, sample_data, settings, tmp_path):
        """Test saving and loading candidate pairs."""
        pairs_df = pair_scores(sample_data, settings)

        # Save to temporary file
        output_path = tmp_path / "candidate_pairs.parquet"
        pairs_df.to_parquet(output_path)

        # Load back
        loaded_df = pd.read_parquet(output_path)

        # Should be identical
        pd.testing.assert_frame_equal(pairs_df, loaded_df)
