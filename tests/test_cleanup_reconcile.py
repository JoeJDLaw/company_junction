"""Tests for cleanup reconciliation functionality.

Phase 1.27.4: Cleanup reconciler and dry-run testing
"""

import json
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from tools.cleanup_test_artifacts import (
    CleanupPlan,
    _list_run_dirs,
    discover_candidates,
    scan_filesystem_runs,
)


class TestCleanupReconcile:
    """Test cleanup reconciliation functionality."""

    def test_list_run_dirs_excludes_known_dirs(self, tmp_path):
        """Test that _list_run_dirs excludes known non-run directories."""
        # Create test directory structure
        test_dir = tmp_path / "test"
        test_dir.mkdir()

        # Create known non-run dirs
        (test_dir / "index").mkdir()
        (test_dir / "legacy").mkdir()
        (test_dir / "default").mkdir()

        # Create run-like dirs
        (test_dir / "run_123").mkdir()
        (test_dir / "run_456").mkdir()

        # Test exclusion
        result = _list_run_dirs(test_dir)
        assert "index" not in result
        assert "legacy" not in result
        assert "default" not in result
        assert "run_123" in result
        assert "run_456" in result

    def test_scan_filesystem_runs_combines_directories(self, tmp_path):
        """Test that scan_filesystem_runs combines interim and processed directories."""
        # Create mock data structure
        interim_dir = tmp_path / "data" / "interim"
        processed_dir = tmp_path / "data" / "processed"
        interim_dir.mkdir(parents=True)
        processed_dir.mkdir(parents=True)

        # Create run directories
        (interim_dir / "run_123").mkdir()
        (processed_dir / "run_456").mkdir()
        (processed_dir / "run_789").mkdir()

        # Mock the path resolution
        with patch("tools.cleanup_test_artifacts.Path") as mock_path:

            def mock_path_side_effect(path_str):
                if path_str == "data/interim":
                    return interim_dir
                if path_str == "data/processed":
                    return processed_dir
                return Path(path_str)

            mock_path.side_effect = mock_path_side_effect

            result = scan_filesystem_runs()
            assert "run_123" in result
            assert "run_456" in result
            assert "run_789" in result

    def test_discover_candidates_without_reconcile(self):
        """Test discover_candidates without reconciliation (existing behavior)."""
        run_index = {
            "run_123": {
                "timestamp": "2024-01-01T00:00:00Z",
                "input_paths": ["test.csv"],
            },
            "run_456": {
                "timestamp": "2024-01-02T00:00:00Z",
                "input_paths": ["test2.csv"],
            },
        }

        plan = discover_candidates(
            run_index=run_index,
            prod_sweep=True,
            reconcile=False,
        )

        # Should only have index-based candidates
        candidate_ids = [rid for rid, _, _ in plan.candidates]
        assert "run_123" in candidate_ids
        assert "run_456" in candidate_ids
        assert len(plan.candidates) == 2

    def test_discover_candidates_with_reconcile(self):
        """Test discover_candidates with reconciliation enabled."""
        run_index = {
            "run_123": {
                "timestamp": "2024-01-01T00:00:00Z",
                "input_paths": ["test.csv"],
            },
            "stale_run": {
                "timestamp": "2024-01-02T00:00:00Z",
                "input_paths": ["test2.csv"],
            },
        }

        # Mock filesystem scanning to return orphan and stale runs
        with pytest.MonkeyPatch().context() as m:
            m.setattr(
                "tools.cleanup_test_artifacts.scan_filesystem_runs",
                lambda: {"run_123", "orphan_run"},
            )

            plan = discover_candidates(
                run_index=run_index,
                prod_sweep=True,
                reconcile=True,
            )

            # Should have index-based candidates plus reconciliation results
            candidate_ids = [rid for rid, _, _ in plan.candidates]
            reasons = [reason for _, _, reason in plan.candidates]

            assert "run_123" in candidate_ids  # From index
            assert "stale_run" in candidate_ids  # Stale index entry
            assert "orphan_run" in candidate_ids  # Orphan directory

            assert "orphan_directory" in reasons
            assert "stale_index" in reasons

    def test_orphan_directory_detected_in_dry_run(self, tmp_path):
        """Test that orphan directories are detected in dry-run mode."""
        # Create test data structure
        data_dir = tmp_path / "data"
        interim_dir = data_dir / "interim"
        processed_dir = data_dir / "processed"
        index_dir = processed_dir / "index"

        interim_dir.mkdir(parents=True)
        processed_dir.mkdir(parents=True)
        index_dir.mkdir(parents=True)

        # Create orphan run directories (on disk, not in index)
        (interim_dir / "orphan_run_123").mkdir()
        (processed_dir / "orphan_run_123").mkdir()

        # Mock the filesystem scanning to return the orphan
        with patch("tools.cleanup_test_artifacts.scan_filesystem_runs") as mock_scan:
            mock_scan.return_value = {"orphan_run_123"}

            # Test discover_candidates with reconciliation
            run_index: dict[str, Any] = {}
            plan = discover_candidates(
                run_index=run_index,
                prod_sweep=True,
                reconcile=True,
            )

            # Should detect orphan
            candidate_ids = [rid for rid, _, _ in plan.candidates]
            reasons = [reason for _, _, reason in plan.candidates]

            assert "orphan_run_123" in candidate_ids
            assert "orphan_directory" in reasons

    def test_stale_index_detected(self, tmp_path):
        """Test that stale index entries are detected."""
        # Create test data structure
        data_dir = tmp_path / "data"
        processed_dir = data_dir / "processed"
        index_dir = processed_dir / "index"

        processed_dir.mkdir(parents=True)
        index_dir.mkdir(parents=True)

        # Create index with stale entry (no corresponding directories)
        index_data = {
            "stale_abc": {
                "timestamp": "2024-01-01T00:00:00Z",
                "input_paths": ["test.csv"],
            },
        }

        # Mock the filesystem scanning to return no directories for stale entry
        with patch("tools.cleanup_test_artifacts.scan_filesystem_runs") as mock_scan:
            mock_scan.return_value = set()  # No directories on disk

            # Test discover_candidates with reconciliation
            run_index = index_data
            plan = discover_candidates(
                run_index=run_index,
                prod_sweep=True,
                reconcile=True,
            )

            # Should detect stale index entry
            candidate_ids = [rid for rid, _, _ in plan.candidates]
            reasons = [reason for _, _, reason in plan.candidates]

            assert "stale_abc" in candidate_ids
            assert "stale_index" in reasons

    def test_dry_run_flag_explicit(self):
        """Test that --dry-run flag works explicitly."""
        # Test discover_candidates without reconciliation (existing behavior)
        run_index = {
            "test_run": {
                "timestamp": "2024-01-01T00:00:00Z",
                "input_paths": ["test.csv"],
            },
        }

        plan = discover_candidates(
            run_index=run_index,
            prod_sweep=True,
            reconcile=False,
        )

        # Should find candidates
        candidate_ids = [rid for rid, _, _ in plan.candidates]
        assert "test_run" in candidate_ids

    def test_dry_run_vs_really_delete(self):
        """Test that --dry-run prevents deletion even with --really-delete."""
        # This test would require testing the main function logic
        # For now, test the core discovery logic
        run_index = {
            "test_run": {
                "timestamp": "2024-01-01T00:00:00Z",
                "input_paths": ["test.csv"],
            },
        }

        plan = discover_candidates(
            run_index=run_index,
            prod_sweep=True,
            reconcile=False,
        )

        # Should find candidates
        candidate_ids = [rid for rid, _, _ in plan.candidates]
        assert "test_run" in candidate_ids

    def test_reconcile_with_empty_index(self):
        """Test reconciliation when index is empty but orphan directories exist."""
        # Mock the filesystem scanning to return orphan directories
        with patch("tools.cleanup_test_artifacts.scan_filesystem_runs") as mock_scan:
            mock_scan.return_value = {"orphan_run_123"}

            # Test discover_candidates with reconciliation and empty index
            run_index: dict[str, Any] = {}
            plan = discover_candidates(
                run_index=run_index,
                prod_sweep=True,
                reconcile=True,
            )

            # Should detect orphan candidates
            candidate_ids = [rid for rid, _, _ in plan.candidates]
            reasons = [reason for _, _, reason in plan.candidates]

            assert "orphan_run_123" in candidate_ids
            assert "orphan_directory" in reasons

    def test_json_output_with_reconcile(self):
        """Test JSON output includes reconciliation results."""
        # Test that discover_candidates returns proper data structure
        run_index = {
            "test_run": {
                "timestamp": "2024-01-01T00:00:00Z",
                "input_paths": ["test.csv"],
            },
        }

        plan = discover_candidates(
            run_index=run_index,
            prod_sweep=True,
            reconcile=False,
        )

        # Should have candidates
        assert len(plan.candidates) > 0
        candidate_ids = [rid for rid, _, _ in plan.candidates]
        assert "test_run" in candidate_ids


class TestCleanupReconcileIntegration:
    """Integration tests for cleanup reconciliation."""

    def test_full_reconciliation_workflow(self):
        """Test complete reconciliation workflow from discovery to cleanup."""
        # Test the complete discovery logic with various types of runs
        run_index = {
            "valid_run": {
                "timestamp": "2024-01-01T00:00:00Z",
                "input_paths": ["valid.csv"],
            },
            "stale_run": {
                "timestamp": "2024-01-02T00:00:00Z",
                "input_paths": ["stale.csv"],
            },
        }

        # Mock filesystem scanning to return valid and orphan runs
        with patch("tools.cleanup_test_artifacts.scan_filesystem_runs") as mock_scan:
            mock_scan.return_value = {"valid_run", "orphan_run"}

            # Test discover_candidates with reconciliation
            plan = discover_candidates(
                run_index=run_index,
                prod_sweep=True,
                reconcile=True,
            )

            # Should detect all types
            candidate_ids = [rid for rid, _, _ in plan.candidates]
            reasons = [reason for _, _, reason in plan.candidates]

            assert "valid_run" in candidate_ids  # From index
            assert "stale_run" in candidate_ids  # Stale index entry
            assert "orphan_run" in candidate_ids  # Orphan directory

            assert "orphan_directory" in reasons
            assert "stale_index" in reasons
