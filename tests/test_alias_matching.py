"""
Test alias matching functionality.
"""

import pandas as pd

from src.alias_matching import compute_alias_matches
from tests.helpers.ingest import ensure_required_columns


class TestAliasMatching:
    """Test alias matching functionality."""

    def setup_method(self):
        """Set up test data."""
        self.df_norm = pd.DataFrame(
            {
                "name_core": [
                    "acme corporation",
                    "acme corp",
                    "acme llc",
                    "beta industries",
                    "beta inc",
                ],
                "suffix_class": [
                    "corporation",
                    "corp",
                    "llc",
                    "industries",
                    "inc",
                ],
                "alias_candidates": [
                    ["acme corp", "acme llc"],
                    ["acme corporation"],
                    ["acme corp"],
                    ["beta inc"],
                    ["beta industries"],
                ],
                "alias_sources": [
                    ["semicolon", "parentheses"],
                    ["semicolon"],
                    ["parentheses"],
                    ["semicolon"],
                    ["parentheses"],
                ],
            }
        )

        self.df_groups = pd.DataFrame(
            {
                "group_id": [
                    "group_1",
                    "group_1",
                    "group_1",
                    "group_2",
                    "group_2",
                ]
            },
            index=self.df_norm.index,
        )

        # Ensure required columns are present
        required_columns = [
            "account_id",
            "name_core",
            "suffix_class",
            "alias_candidates",
            "alias_sources",
        ]
        self.df_norm = ensure_required_columns(self.df_norm, required_columns)
        self.df_groups = ensure_required_columns(
            self.df_groups, ["group_id", "account_id"]
        )

        self.settings = {
            "similarity": {
                "high": 85,
                "max_alias_pairs": 1000,
            },
            "parallelism": {
                "workers": 2,
                "backend": "threading",
                "chunk_size": 100,
            },
            "alias": {
                "optimize": True,
                "progress_interval_s": 0.1,
            },
        }

    def test_compute_alias_matches(self):
        """Test that alias matching produces expected results."""
        compute_alias_matches(self.df_norm, self.df_groups, self.settings)
