"""
Tests for grouping functionality with edge-gating and stable group IDs.
"""

import pandas as pd
import sys
from pathlib import Path

# Add src to path for imports
sys.path.append(str(Path(__file__).parent.parent / "src"))

from grouping import (
    can_join_group,
    apply_canopy_bound,
    create_groups_with_edge_gating,
    create_groups_standard,
)
from src.utils.hash_utils import config_hash, stable_group_id


class TestConfigHash:
    """Test config hash functionality."""

    def test_config_hash_deterministic(self):
        """Test that config hash is deterministic."""
        config1 = {"similarity": {"high": 92, "medium": 84}}
        config2 = {"similarity": {"high": 92, "medium": 84}}

        hash1 = config_hash(config1)
        hash2 = config_hash(config2)

        assert hash1 == hash2
        assert len(hash1) == 8  # 8-character hex

    def test_config_hash_different_for_different_configs(self):
        """Test that different configs produce different hashes."""
        config1 = {"similarity": {"high": 92, "medium": 84}}
        config2 = {"similarity": {"high": 90, "medium": 82}}

        hash1 = config_hash(config1)
        hash2 = config_hash(config2)

        assert hash1 != hash2


class TestStableGroupId:
    """Test stable group ID generation."""

    def test_stable_group_id_deterministic(self):
        """Test that stable group ID is deterministic."""
        member_ids = ["123", "456", "789"]
        config = {"similarity": {"high": 92}}

        id1 = stable_group_id(member_ids, config)
        id2 = stable_group_id(member_ids, config)

        assert id1 == id2
        assert len(id1) == 10  # 10-character hex

    def test_stable_group_id_order_independent(self):
        """Test that group ID is independent of member order."""
        member_ids1 = ["123", "456", "789"]
        member_ids2 = ["789", "123", "456"]
        config = {"similarity": {"high": 92}}

        id1 = stable_group_id(member_ids1, config)
        id2 = stable_group_id(member_ids2, config)

        assert id1 == id2

    def test_stable_group_id_config_dependent(self):
        """Test that group ID changes with config changes."""
        member_ids = ["123", "456", "789"]
        config1 = {"similarity": {"high": 92}}
        config2 = {"similarity": {"high": 90}}

        id1 = stable_group_id(member_ids, config1)
        id2 = stable_group_id(member_ids, config2)

        assert id1 != id2


class TestCanJoinGroup:
    """Test edge-gating logic."""

    def test_can_join_high_threshold(self):
        """Test joining with high threshold."""
        primary_id = "123"
        candidate_id = "456"
        edge_scores = {("123", "456"): 95.0}
        token_sets = {"123": {"company", "inc"}, "456": {"company", "inc"}}
        config = {"similarity": {"high": 92, "medium": 84}}
        stop_tokens = {"inc"}

        can_join, reason, score = can_join_group(
            primary_id, candidate_id, edge_scores, token_sets, config, stop_tokens
        )

        assert can_join is True
        assert reason == "edge>=high"
        assert score == 95.0

    def test_can_join_medium_with_shared_token(self):
        """Test joining with medium threshold and shared token."""
        primary_id = "123"
        candidate_id = "456"
        edge_scores = {("123", "456"): 85.0}
        token_sets = {"123": {"company", "inc"}, "456": {"company", "llc"}}
        config = {
            "similarity": {"high": 92, "medium": 84},
            "grouping": {"edge_gating": {"allow_medium_plus_shared_token": True}},
        }
        stop_tokens = {"inc", "llc"}

        can_join, reason, score = can_join_group(
            primary_id, candidate_id, edge_scores, token_sets, config, stop_tokens
        )

        assert can_join is True
        assert reason == "edge>=medium+shared_token"
        assert score == 85.0

    def test_cannot_join_insufficient_edge(self):
        """Test rejection with insufficient edge."""
        primary_id = "123"
        candidate_id = "456"
        edge_scores = {("123", "456"): 80.0}
        token_sets = {"123": {"company", "inc"}, "456": {"company", "llc"}}
        config = {"similarity": {"high": 92, "medium": 84}}
        stop_tokens = {"inc", "llc"}

        can_join, reason, score = can_join_group(
            primary_id, candidate_id, edge_scores, token_sets, config, stop_tokens
        )

        assert can_join is False
        assert reason == "insufficient_edge"
        assert score == 80.0

    def test_cannot_join_no_shared_tokens(self):
        """Test rejection with medium score but no shared tokens."""
        primary_id = "123"
        candidate_id = "456"
        edge_scores = {("123", "456"): 85.0}
        token_sets = {"123": {"company", "inc"}, "456": {"different", "llc"}}
        config = {
            "similarity": {"high": 92, "medium": 84},
            "grouping": {"edge_gating": {"allow_medium_plus_shared_token": True}},
        }
        stop_tokens = {"inc", "llc"}

        can_join, reason, score = can_join_group(
            primary_id, candidate_id, edge_scores, token_sets, config, stop_tokens
        )

        assert can_join is False
        assert reason == "insufficient_edge"
        assert score == 85.0


class TestCanopyBound:
    """Test canopy bound logic."""

    def test_canopy_bound_under_limit(self):
        """Test that groups under limit can add members."""
        group_members = ["123", "456", "789"]
        primary_id = "123"
        candidate_id = "999"
        edge_scores = {("123", "999"): 80.0}
        config = {
            "grouping": {
                "edge_gating": {
                    "canopy_bound": {"enabled": True, "max_without_high_edge": 8}
                }
            },
            "similarity": {"high": 92},
        }

        can_join = apply_canopy_bound(
            group_members, primary_id, candidate_id, edge_scores, config
        )

        assert can_join is True

    def test_canopy_bound_over_limit_with_high_edge(self):
        """Test that groups over limit can add members with high edge."""
        group_members = ["123", "456", "789", "111", "222", "333", "444", "555"]
        primary_id = "123"
        candidate_id = "999"
        edge_scores = {("123", "999"): 95.0}
        config = {
            "grouping": {
                "edge_gating": {
                    "canopy_bound": {"enabled": True, "max_without_high_edge": 8}
                }
            },
            "similarity": {"high": 92},
        }

        can_join = apply_canopy_bound(
            group_members, primary_id, candidate_id, edge_scores, config
        )

        assert can_join is True

    def test_canopy_bound_over_limit_without_high_edge(self):
        """Test that groups over limit cannot add members without high edge."""
        group_members = ["123", "456", "789", "111", "222", "333", "444", "555"]
        primary_id = "123"
        candidate_id = "999"
        edge_scores = {("123", "999"): 85.0}
        config = {
            "grouping": {
                "edge_gating": {
                    "canopy_bound": {"enabled": True, "max_without_high_edge": 8}
                }
            },
            "similarity": {"high": 92},
        }

        can_join = apply_canopy_bound(
            group_members, primary_id, candidate_id, edge_scores, config
        )

        assert can_join is False

    def test_canopy_bound_disabled(self):
        """Test that canopy bound can be disabled."""
        group_members = ["123", "456", "789", "111", "222", "333", "444", "555"]
        primary_id = "123"
        candidate_id = "999"
        edge_scores = {("123", "999"): 85.0}
        config = {
            "grouping": {
                "edge_gating": {
                    "canopy_bound": {"enabled": False, "max_without_high_edge": 8}
                }
            },
            "similarity": {"high": 92},
        }

        can_join = apply_canopy_bound(
            group_members, primary_id, candidate_id, edge_scores, config
        )

        assert can_join is True


class TestGroupCreation:
    """Test group creation with edge-gating."""

    def test_create_groups_with_edge_gating(self):
        """Test group creation with edge-gating enabled."""
        # Create test data
        accounts_df = pd.DataFrame(
            {
                "account_id": ["123", "456", "789"],
                "name_core": ["company inc", "company llc", "different corp"],
                "name_core_tokens": [
                    '["company", "inc"]',
                    '["company", "llc"]',
                    '["different", "corp"]',
                ],
            }
        )

        pairs_df = pd.DataFrame(
            {
                "account_id_1": ["123", "123"],
                "account_id_2": ["456", "789"],
                "score": [90.0, 85.0],
            }
        )

        config = {
            "similarity": {"high": 92, "medium": 84},
            "grouping": {
                "edge_gating": {"enabled": True, "allow_medium_plus_shared_token": True}
            },
        }

        stop_tokens = {"inc", "llc", "corp"}

        # Create groups
        groups_df = create_groups_with_edge_gating(
            accounts_df, pairs_df, config, stop_tokens
        )

        # Verify results
        assert len(groups_df) == 3
        assert "group_id" in groups_df.columns
        assert "group_join_reason" in groups_df.columns
        assert "weakest_edge_to_primary" in groups_df.columns
        assert "shared_tokens_count" in groups_df.columns

        # Check that 123 and 456 are grouped (shared token 'company')
        group_ids = groups_df["group_id"].unique()
        assert len(group_ids) == 2  # Two groups: one with 123+456, one with 789

    def test_create_groups_standard_fallback(self):
        """Test standard group creation as fallback."""
        # Create test data
        accounts_df = pd.DataFrame(
            {
                "account_id": ["123", "456", "789"],
                "name_core": ["company inc", "company llc", "different corp"],
            }
        )

        pairs_df = pd.DataFrame(
            {
                "account_id_1": ["123", "123"],
                "account_id_2": ["456", "789"],
                "score": [90.0, 85.0],
            }
        )

        config = {
            "similarity": {"medium": 84},
            "grouping": {"edge_gating": {"enabled": False}},
        }

        # Create groups
        groups_df = create_groups_standard(accounts_df, pairs_df, config)

        # Verify results
        assert len(groups_df) == 3
        assert "group_id" in groups_df.columns
        assert "group_join_reason" in groups_df.columns
        assert groups_df["group_join_reason"].iloc[0] == "standard_grouping"
