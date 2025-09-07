"""Tests for P1 fixes in disposition module.

This module contains tests for the critical P1 fixes:
- P1 #1: Index alignment in vectorized disposition reasons
- P1 #2: Legacy blacklist path respects config
"""

import pandas as pd
import pytest

from src.disposition import apply_dispositions, DISPOSITION


class TestDispositionP1Fixes:
    """Test P1 fixes for disposition module."""

    def test_legacy_disposition_respects_blacklist_config(self):
        """Test that legacy disposition path respects blacklist config."""
        # Create test data with blacklisted term
        df = pd.DataFrame({
            "account_name": ["Acme zzzbrand", "Acme zzzbrand"],
            "group_id": [1, 1],
            "suffix_class": ["llc", "llc"],
            "is_primary": [True, False]
        })
        
        # Configure to use legacy path and blacklist the term
        settings = {
            "disposition": {
                "performance": {"vectorized": False},  # Force legacy path
                "blacklist": {"tokens": ["zzzbrand"], "phrases": []}
            }
        }
        
        # Apply dispositions
        result = apply_dispositions(df, settings)
        
        # Both rows should be classified as Delete due to blacklist
        assert set(result[DISPOSITION].unique()) == {"Delete"}
        assert len(result[result[DISPOSITION] == "Delete"]) == 2

    def test_vectorized_disposition_respects_blacklist_config(self):
        """Test that vectorized disposition path respects blacklist config."""
        # Create test data with blacklisted term
        df = pd.DataFrame({
            "account_name": ["Acme zzzbrand", "Acme zzzbrand"],
            "group_id": [1, 1],
            "suffix_class": ["llc", "llc"],
            "is_primary": [True, False]
        })
        
        # Configure to use vectorized path and blacklist the term
        settings = {
            "disposition": {
                "performance": {"vectorized": True},  # Force vectorized path
                "blacklist": {"tokens": ["zzzbrand"], "phrases": []}
            }
        }
        
        # Apply dispositions
        result = apply_dispositions(df, settings)
        
        # Both rows should be classified as Delete due to blacklist
        assert set(result[DISPOSITION].unique()) == {"Delete"}
        assert len(result[result[DISPOSITION] == "Delete"]) == 2

    def test_disposition_index_alignment_regression(self):
        """Temporary regression test: verify index alignment fix works with non-RangeIndex."""
        # Create test data with non-RangeIndex
        df = pd.DataFrame({
            "group_id": [1, 1, 2],
            "account_id": ["A1", "A2", "B1"],
            "account_name": ["Okay Co", "Okay Co", "Weird Co"],
            "suffix_class": ["llc", "llc", "inc"],
            "is_primary": [True, False, True]
        }).set_index(pd.Index(["x", "y", "z"], name="rid"))  # non-RangeIndex

        settings = {
            "disposition": {
                "performance": {"vectorized": True},
                "blacklist": {"tokens": [], "phrases": []}
            }
        }
        
        # Apply dispositions
        result = apply_dispositions(df, settings)

        # Behavior check: same length, aligned index, no broadcast weirdness
        assert list(result.index) == ["x", "y", "z"]
        assert len(result) == 3
        assert DISPOSITION in result.columns
        
        # Verify no index misalignment caused data corruption
        assert result.loc["x", "account_name"] == "Okay Co"
        assert result.loc["y", "account_name"] == "Okay Co"
        assert result.loc["z", "account_name"] == "Weird Co"

    def test_explicit_empty_blacklist_disables_builtins(self, monkeypatch):
        """Test that explicit empty blacklist config disables built-in terms."""
        # Make sure manual terms are empty
        import src.disposition as disp
        monkeypatch.setattr(disp, "_load_manual_blacklist", lambda: [])

        df = pd.DataFrame({
            "account_name": ["Test Co"],  # "test" is a built-in token
            "group_id": [1],
            "suffix_class": ["llc"],
            "is_primary": [True],
        })
        settings = {
            "disposition": {
                "performance": {"vectorized": True},
                "blacklist": {"tokens": [], "phrases": []}  # explicit empty
            }
        }
        out = apply_dispositions(df, settings)
        # Should NOT be deleted, since built-ins are disabled explicitly
        assert out[DISPOSITION].iloc[0] != "Delete"
