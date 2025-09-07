"""Tests for exact equals pairs emission modes."""

import pandas as pd
import pytest

from src.utils.exact_equals import find_exact_equals_groups


class TestExactEqualsPairs:
    """Test exact equals pairs emission modes."""

    def test_spanning_tree_emission(self):
        """Test that spanning tree mode generates n-1 pairs per group."""
        # Create test data with exact matches
        df = pd.DataFrame({
            "account_id": ["ID1", "ID2", "ID3", "ID4", "ID5"],
            "account_name": ["Company A", "Company A", "Company A", "Company B", "Company B"]
        })
        
        settings = {
            "pipeline": {
                "exact_equals_first_pass": {
                    "pairs_emission": "spanning_tree"
                }
            }
        }
        
        groups, mapping, pairs = find_exact_equals_groups(df, settings, "account_name")
        
        # Should have 2 groups: Company A (3 records) and Company B (2 records)
        # Spanning tree: 3-1 + 2-1 = 2 + 1 = 3 pairs total
        assert len(pairs) == 3
        
        # Verify spanning tree structure (all pairs should connect to first record)
        company_a_pairs = pairs[pairs["raw_exact_key"] == "Company A"]
        company_b_pairs = pairs[pairs["raw_exact_key"] == "Company B"]
        
        # Company A group: 2 pairs (ID1->ID2, ID1->ID3)
        assert len(company_a_pairs) == 2
        assert all(company_a_pairs["id_a"] == "ID1")
        
        # Company B group: 1 pair (ID4->ID5)
        assert len(company_b_pairs) == 1
        assert all(company_b_pairs["id_a"] == "ID4")

    def test_complete_emission(self):
        """Test that complete mode generates all possible pairs."""
        # Create test data with exact matches
        df = pd.DataFrame({
            "account_id": ["ID1", "ID2", "ID3", "ID4", "ID5"],
            "account_name": ["Company A", "Company A", "Company A", "Company B", "Company B"]
        })
        
        settings = {
            "pipeline": {
                "exact_equals_first_pass": {
                    "pairs_emission": "complete"
                }
            }
        }
        
        groups, mapping, pairs = find_exact_equals_groups(df, settings, "account_name")
        
        # Should have 2 groups: Company A (3 records) and Company B (2 records)
        # Complete graph: C(3,2) + C(2,2) = 3 + 1 = 4 pairs total
        assert len(pairs) == 4
        
        # Verify complete graph structure
        company_a_pairs = pairs[pairs["raw_exact_key"] == "Company A"]
        company_b_pairs = pairs[pairs["raw_exact_key"] == "Company B"]
        
        # Company A group: 3 pairs (all combinations of ID1, ID2, ID3)
        assert len(company_a_pairs) == 3
        
        # Company B group: 1 pair (ID4->ID5)
        assert len(company_b_pairs) == 1

    def test_spanning_tree_vs_complete_count(self):
        """Test that spanning tree generates fewer pairs than complete mode."""
        # Create test data with a larger group
        df = pd.DataFrame({
            "account_id": [f"ID{i}" for i in range(1, 6)],  # 5 records
            "account_name": ["Company A"] * 5  # All same company
        })
        
        # Test spanning tree
        settings_spanning = {
            "pipeline": {
                "exact_equals_first_pass": {
                    "pairs_emission": "spanning_tree"
                }
            }
        }
        _, _, pairs_spanning = find_exact_equals_groups(df, settings_spanning, "account_name")
        
        # Test complete
        settings_complete = {
            "pipeline": {
                "exact_equals_first_pass": {
                    "pairs_emission": "complete"
                }
            }
        }
        _, _, pairs_complete = find_exact_equals_groups(df, settings_complete, "account_name")
        
        # Spanning tree: 5-1 = 4 pairs
        # Complete: C(5,2) = 10 pairs
        assert len(pairs_spanning) == 4
        assert len(pairs_complete) == 10
        assert len(pairs_spanning) < len(pairs_complete)

    def test_default_emission_mode(self):
        """Test that default emission mode is spanning tree."""
        df = pd.DataFrame({
            "account_id": ["ID1", "ID2", "ID3"],
            "account_name": ["Company A", "Company A", "Company A"]
        })
        
        # No pairs_emission specified - should default to spanning_tree
        settings = {
            "pipeline": {
                "exact_equals_first_pass": {}
            }
        }
        
        _, _, pairs = find_exact_equals_groups(df, settings, "account_name")
        
        # Should use spanning tree by default: 3-1 = 2 pairs
        assert len(pairs) == 2

    def test_no_exact_matches(self):
        """Test behavior when no exact matches exist."""
        df = pd.DataFrame({
            "account_id": ["ID1", "ID2", "ID3"],
            "account_name": ["Company A", "Company B", "Company C"]
        })
        
        settings = {
            "pipeline": {
                "exact_equals_first_pass": {
                    "pairs_emission": "spanning_tree"
                }
            }
        }
        
        groups, mapping, pairs = find_exact_equals_groups(df, settings, "account_name")
        
        # No groups, no pairs
        assert groups.empty
        assert mapping.empty
        assert pairs.empty
