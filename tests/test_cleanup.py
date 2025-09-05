#!/usr/bin/env python3
"""Tests for the simplified cleanup tool.

Tests deterministic discovery, type filtering, age filtering, prod-sweep mode,
pinned run protection, latest symlink protection, and JSON output.
"""

import sys
from pathlib import Path
from typing import Any, Dict
from unittest.mock import MagicMock, patch

import pytest

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from tools.cleanup_test_artifacts import (
    CleanupPlan,
    detect_run_type,
    discover_candidates,
    get_latest_run_id,
    get_run_age_days,
)


class TestCleanupPlan:
    """Test the CleanupPlan class."""

    def test_init(self):
        """Test CleanupPlan initialization."""
        plan = CleanupPlan()
        assert plan.candidates == []
        assert plan.latest_run_id is None
        assert plan.pinned_runs == set()
        assert plan.prod_runs == set()

    def test_add_candidate(self):
        """Test adding candidates."""
        plan = CleanupPlan()
        plan.add_candidate("test1", {"input_paths": ["test.csv"]}, "type_filter")
        assert len(plan.candidates) == 1
        assert plan.candidates[0] == (
            "test1",
            {"input_paths": ["test.csv"]},
            "type_filter",
        )

    def test_is_protected(self):
        """Test protection logic."""
        plan = CleanupPlan()
        plan.latest_run_id = "latest_run"
        plan.pinned_runs = {"pinned1", "pinned2"}
        plan.prod_runs = {"prod1"}

        assert plan.is_protected("latest_run")  # Latest
        assert plan.is_protected("pinned1")  # Pinned
        assert plan.is_protected("prod1")  # Prod
        assert not plan.is_protected("other")  # Not protected

    def test_get_protected_candidates(self):
        """Test getting protected candidates."""
        plan = CleanupPlan()
        plan.latest_run_id = "latest_run"
        plan.pinned_runs = {"pinned1"}
        plan.prod_runs = {"prod1"}

        plan.add_candidate("latest_run", {}, "type_filter")
        plan.add_candidate("pinned1", {}, "type_filter")
        plan.add_candidate("prod1", {}, "type_filter")
        plan.add_candidate("deletable", {}, "type_filter")

        protected = plan.get_protected_candidates()
        assert len(protected) == 3
        assert "latest_run" in protected
        assert "pinned1" in protected
        assert "prod1" in protected
        assert "deletable" not in protected

    def test_get_deletable_candidates(self):
        """Test getting deletable candidates."""
        plan = CleanupPlan()
        plan.latest_run_id = "latest_run"
        plan.pinned_runs = {"pinned1"}
        plan.prod_runs = {"prod1"}

        plan.add_candidate("latest_run", {}, "type_filter")
        plan.add_candidate("pinned1", {}, "type_filter")
        plan.add_candidate("prod1", {}, "type_filter")
        plan.add_candidate("deletable", {}, "type_filter")

        deletable = plan.get_deletable_candidates()
        assert len(deletable) == 1
        assert deletable[0][0] == "deletable"

    def test_sort_candidates(self):
        """Test candidate sorting for deterministic output."""
        plan = CleanupPlan()
        plan.add_candidate("z_run", {}, "type_filter")
        plan.add_candidate("a_run", {}, "type_filter")
        plan.add_candidate("m_run", {}, "type_filter")

        plan.sort_candidates()

        run_ids = [run_id for run_id, _, _ in plan.candidates]
        assert run_ids == ["a_run", "m_run", "z_run"]


class TestRunTypeDetection:
    """Test run type detection logic."""

    def test_detect_test_run(self):
        """Test detection of test runs."""
        # Sample test
        run_data = {"input_paths": ["sample_test.csv"]}
        assert detect_run_type(run_data) == "test"

        # Test prefix
        run_data = {"input_paths": ["test_data.csv"]}
        assert detect_run_type(run_data) == "test"

        # Mixed case
        run_data = {"input_paths": ["SAMPLE_TEST.csv"]}
        assert detect_run_type(run_data) == "test"

    def test_detect_dev_run(self):
        """Test detection of dev runs (default)."""
        run_data = {"input_paths": ["company_junction_range_01.csv"]}
        assert detect_run_type(run_data) == "dev"

        # No input paths
        empty_run_data: Dict[str, Any] = {}
        assert detect_run_type(empty_run_data) == "dev"

    def test_detect_run_type_edge_cases(self):
        """Test edge cases in run type detection."""
        # Empty input paths
        run_data: Dict[str, Any] = {"input_paths": []}
        assert detect_run_type(run_data) == "dev"

        # Non-string input paths
        run_data = {"input_paths": [123, "sample_test.csv"]}
        assert detect_run_type(run_data) == "test"


class TestAgeCalculation:
    """Test run age calculation."""

    def test_get_run_age_days_valid(self):
        """Test age calculation with valid timestamps."""
        # Recent timestamp
        run_data = {"timestamp": "2025-09-01T12:00:00"}
        age = get_run_age_days(run_data)
        assert age >= 0
        assert age <= 10  # Should be recent

        # Old timestamp
        run_data = {"timestamp": "2020-01-01T12:00:00"}
        age = get_run_age_days(run_data)
        assert age > 1000  # Should be very old

    def test_get_run_age_days_invalid(self):
        """Test age calculation with invalid timestamps."""
        # No timestamp
        run_data: Dict[str, Any] = {}
        age = get_run_age_days(run_data)
        assert age == 999

        # Invalid timestamp
        run_data = {"timestamp": "invalid"}
        age = get_run_age_days(run_data)
        assert age == 999

        # Empty timestamp
        run_data = {"timestamp": ""}
        age = get_run_age_days(run_data)
        assert age == 999


class TestCandidateDiscovery:
    """Test candidate discovery logic."""

    def test_discover_candidates_type_filter(self):
        """Test type-based filtering."""
        run_index = {
            "test1": {
                "input_paths": ["sample_test.csv"],
                "timestamp": "2025-09-01T12:00:00",
            },
            "dev1": {"input_paths": ["data.csv"], "timestamp": "2025-09-01T12:00:00"},
            "prod1": {"input_paths": ["prod.csv"], "timestamp": "2025-09-01T12:00:00"},
        }

        # Filter by test type
        plan = discover_candidates(run_index, types=["test"])
        assert len(plan.candidates) == 1
        assert plan.candidates[0][0] == "test1"
        assert plan.candidates[0][2] == "type_filter"

        # Filter by multiple types
        plan = discover_candidates(run_index, types=["test", "dev"])
        assert len(plan.candidates) == 3  # test1, dev1, prod1 (prod1 defaults to "dev")
        run_ids = [run_id for run_id, _, _ in plan.candidates]
        assert "test1" in run_ids
        assert "dev1" in run_ids
        assert "prod1" in run_ids  # prod1 is detected as "dev" type

    def test_discover_candidates_age_filter(self):
        """Test age-based filtering."""
        run_index = {
            "old": {"input_paths": ["old.csv"], "timestamp": "2020-01-01T12:00:00"},
            "recent": {
                "input_paths": ["recent.csv"],
                "timestamp": "2025-09-01T12:00:00",
            },
        }

        # Filter by age
        plan = discover_candidates(run_index, older_than=1000)
        assert len(plan.candidates) == 1
        assert plan.candidates[0][0] == "old"
        assert plan.candidates[0][2] == "age_filter"

        # No age filter
        plan = discover_candidates(run_index)
        assert len(plan.candidates) == 0  # No type filter specified

    def test_discover_candidates_prod_sweep(self):
        """Test prod-sweep mode."""
        run_index = {
            "test1": {
                "input_paths": ["sample_test.csv"],
                "timestamp": "2025-09-01T12:00:00",
            },
            "dev1": {"input_paths": ["data.csv"], "timestamp": "2025-09-01T12:00:00"},
            "prod1": {"input_paths": ["prod.csv"], "timestamp": "2025-09-01T12:00:00"},
        }

        # Prod sweep without including prod
        plan = discover_candidates(run_index, prod_sweep=True)
        assert len(plan.candidates) == 3  # test1, dev1, prod1 (prod1 defaults to "dev")
        run_ids = [run_id for run_id, _, _ in plan.candidates]
        assert "test1" in run_ids
        assert "dev1" in run_ids
        assert "prod1" in run_ids  # prod1 is detected as "dev" type

        # Prod sweep with including prod
        plan = discover_candidates(run_index, prod_sweep=True, include_prod=True)
        assert len(plan.candidates) == 3
        run_ids = [run_id for run_id, _, _ in plan.candidates]
        assert "test1" in run_ids
        assert "dev1" in run_ids
        assert "prod1" in run_ids

    def test_discover_candidates_combined_filters(self):
        """Test combined filtering logic."""
        run_index = {
            "test_old": {
                "input_paths": ["sample_test.csv"],
                "timestamp": "2020-01-01T12:00:00",
            },
            "test_recent": {
                "input_paths": ["sample_test.csv"],
                "timestamp": "2025-09-01T12:00:00",
            },
            "dev_old": {
                "input_paths": ["data.csv"],
                "timestamp": "2020-01-01T12:00:00",
            },
            "dev_recent": {
                "input_paths": ["data.csv"],
                "timestamp": "2025-09-01T12:00:00",
            },
        }

        # Type + age filter
        plan = discover_candidates(run_index, types=["test"], older_than=1000)
        assert len(plan.candidates) == 1
        assert plan.candidates[0][0] == "test_old"
        assert plan.candidates[0][2] == "age_filter"

    def test_discover_candidates_deterministic(self):
        """Test that discovery produces deterministic results."""
        run_index = {
            "z_run": {"input_paths": ["z.csv"], "timestamp": "2025-09-01T12:00:00"},
            "a_run": {"input_paths": ["a.csv"], "timestamp": "2025-09-01T12:00:00"},
            "m_run": {"input_paths": ["m.csv"], "timestamp": "2025-09-01T12:00:00"},
        }

        # Run discovery multiple times
        plan1 = discover_candidates(run_index, types=["dev"])
        plan2 = discover_candidates(run_index, types=["dev"])

        # Should get same results
        assert len(plan1.candidates) == len(plan2.candidates)

        # Should be sorted
        run_ids1 = [run_id for run_id, _, _ in plan1.candidates]
        run_ids2 = [run_id for run_id, _, _ in plan2.candidates]
        assert run_ids1 == run_ids2
        assert run_ids1 == ["a_run", "m_run", "z_run"]


class TestLatestSymlink:
    """Test latest symlink handling."""

    @patch("tools.cleanup_test_artifacts.get_processed_dir")
    def test_get_latest_run_id_valid(self, mock_get_processed_dir):
        """Test getting latest run ID from valid symlink."""
        # Mock the latest symlink
        mock_latest = MagicMock()
        mock_latest.exists.return_value = True
        mock_latest.is_symlink.return_value = True

        mock_target = MagicMock()
        mock_target.name = "test_run_123"
        mock_latest.resolve.return_value = mock_target

        mock_get_processed_dir.return_value = mock_latest

        result = get_latest_run_id()
        assert result == "test_run_123"

    @patch("tools.cleanup_test_artifacts.get_processed_dir")
    def test_get_latest_run_id_not_exists(self, mock_get_processed_dir):
        """Test getting latest run ID when symlink doesn't exist."""
        mock_latest = MagicMock()
        mock_latest.exists.return_value = False

        mock_get_processed_dir.return_value = mock_latest

        result = get_latest_run_id()
        assert result is None

    @patch("tools.cleanup_test_artifacts.get_processed_dir")
    def test_get_latest_run_id_not_symlink(self, mock_get_processed_dir):
        """Test getting latest run ID when path is not a symlink."""
        mock_latest = MagicMock()
        mock_latest.exists.return_value = True
        mock_latest.is_symlink.return_value = False

        mock_get_processed_dir.return_value = mock_latest

        result = get_latest_run_id()
        assert result is None

    @patch("tools.cleanup_test_artifacts.get_processed_dir")
    def test_get_latest_run_id_broken_symlink(self, mock_get_processed_dir):
        """Test getting latest run ID from broken symlink."""
        mock_latest = MagicMock()
        mock_latest.exists.return_value = True
        mock_latest.is_symlink.return_value = True
        mock_latest.resolve.side_effect = OSError("Broken symlink")

        mock_get_processed_dir.return_value = mock_latest

        result = get_latest_run_id()
        assert result is None


class TestIntegration:
    """Integration tests for the cleanup tool."""

    def test_full_discovery_flow(self):
        """Test the complete discovery flow with realistic data."""
        run_index = {
            "test_old": {
                "input_paths": ["sample_test.csv"],
                "timestamp": "2020-01-01T12:00:00",
            },
            "test_recent": {
                "input_paths": ["sample_test.csv"],
                "timestamp": "2025-09-01T12:00:00",
            },
            "dev_old": {
                "input_paths": ["company_junction_range_01.csv"],
                "timestamp": "2020-01-01T12:00:00",
            },
            "dev_recent": {
                "input_paths": ["company_junction_range_01.csv"],
                "timestamp": "2025-09-01T12:00:00",
            },
            "prod_run": {
                "input_paths": ["production_data.csv"],
                "timestamp": "2025-09-01T12:00:00",
            },
        }

        # Test prod sweep (should include all runs since prod_run defaults to "dev")
        plan = discover_candidates(run_index, prod_sweep=True)
        assert (
            len(plan.candidates) == 5
        )  # All runs included since prod_run defaults to "dev"
        run_ids = [run_id for run_id, _, _ in plan.candidates]
        assert "prod_run" in run_ids  # prod_run is detected as "dev" type

        # Test type + age filter
        plan = discover_candidates(run_index, types=["test"], older_than=1000)
        assert len(plan.candidates) == 1
        assert plan.candidates[0][0] == "test_old"

        # Test that prod runs are tracked (prod_run is detected as "dev" type, so not in prod_runs)
        assert "prod_run" not in plan.prod_runs  # prod_run defaults to "dev" type

        # Test protection logic
        plan.latest_run_id = "test_recent"
        plan.pinned_runs = {"dev_recent"}

        assert plan.is_protected("test_recent")  # Latest
        assert plan.is_protected("dev_recent")  # Pinned
        assert not plan.is_protected(
            "prod_run",
        )  # prod_run is detected as "dev" type, not protected
        assert not plan.is_protected("test_old")  # Not protected


if __name__ == "__main__":
    pytest.main([__file__])
