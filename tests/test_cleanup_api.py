"""Tests for cleanup API functionality."""

import json
import os
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from src.utils.cleanup_api import (
    DeleteResult,
    PreviewInfo,
    RunInfo,
    delete_runs,
    list_runs,
    preview_delete,
)


class TestCleanupAPI:
    """Test cleanup API functions."""

    def setup_method(self):
        """Set up test environment."""
        self.temp_dir = tempfile.mkdtemp()
        self.run_index_path = os.path.join(self.temp_dir, "run_index.json")

        # Mock the RUN_INDEX_PATH
        self.patcher = patch(
            "src.utils.cache_utils.RUN_INDEX_PATH", self.run_index_path
        )
        self.patcher.start()

        # Mock the path utilities to use temp directory
        self.path_patcher = patch("src.utils.cache_utils.get_processed_dir")
        self.mock_get_processed_dir = self.path_patcher.start()
        self.mock_get_processed_dir.return_value = Path(self.temp_dir)

    def teardown_method(self):
        """Clean up test environment."""
        self.patcher.stop()
        self.path_patcher.stop()
        # Clean up temp directory
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _create_test_run_index(self, runs_data):
        """Create a test run index file."""
        with open(self.run_index_path, "w") as f:
            json.dump(runs_data, f)

    def test_list_runs_empty(self):
        """Test list_runs with empty index."""
        self._create_test_run_index({})
        runs = list_runs()
        assert runs == []

    def test_list_runs_with_data(self):
        """Test list_runs with run data."""
        runs_data = {
            "run1": {
                "run_type": "test",
                "timestamp": "2025-01-01T10:00:00",
                "status": "complete",
                "input_paths": ["input1.csv"],
                "config_paths": ["config.yaml"],
            },
            "run2": {
                "run_type": "dev",
                "timestamp": "2025-01-01T11:00:00",
                "status": "complete",
                "input_paths": ["input2.csv"],
                "config_paths": ["config.yaml"],
            },
        }
        self._create_test_run_index(runs_data)

        runs = list_runs()
        assert len(runs) == 2

        # Should be sorted by timestamp (newest first)
        assert runs[0].run_id == "run2"
        assert runs[0].run_type == "dev"
        assert runs[1].run_id == "run1"
        assert runs[1].run_type == "test"

    def test_list_runs_legacy_missing_run_type(self):
        """Test list_runs handles legacy runs without run_type."""
        runs_data = {
            "legacy_run": {
                "timestamp": "2025-01-01T10:00:00",
                "status": "complete",
                "input_paths": ["input1.csv"],
                "config_paths": ["config.yaml"],
            },
        }
        self._create_test_run_index(runs_data)

        with patch("src.utils.cleanup_api.logger") as mock_logger:
            runs = list_runs()
            assert len(runs) == 1
            assert runs[0].run_type == "dev"  # Default for legacy runs
            mock_logger.warning.assert_called_once()

    def test_preview_delete_empty(self):
        """Test preview_delete with no runs."""
        self._create_test_run_index({})
        preview = preview_delete(["nonexistent"])

        assert len(preview.runs_to_delete) == 0
        assert len(preview.runs_not_found) == 1
        assert preview.runs_not_found[0] == "nonexistent"
        assert preview.total_bytes == 0

    def test_preview_delete_with_runs(self):
        """Test preview_delete with existing runs."""
        runs_data = {
            "run1": {
                "run_type": "test",
                "timestamp": "2025-01-01T10:00:00",
                "status": "complete",
                "input_paths": ["input1.csv"],
                "config_paths": ["config.yaml"],
            },
            "run2": {
                "run_type": "test",
                "timestamp": "2025-01-01T11:00:00",
                "status": "running",
                "input_paths": ["input2.csv"],
                "config_paths": ["config.yaml"],
            },
        }
        self._create_test_run_index(runs_data)

        # Mock directory size calculation by mocking os.walk and os.path.getsize
        with patch("os.walk") as mock_walk, patch("os.path.getsize", return_value=1024):
            mock_walk.return_value = [("/fake/dir", [], ["file1.txt"])]
            preview = preview_delete(["run1", "run2", "nonexistent"])

        assert len(preview.runs_to_delete) == 1
        assert preview.runs_to_delete[0].run_id == "run1"
        assert len(preview.runs_inflight) == 1
        assert preview.runs_inflight[0] == "run2"
        assert len(preview.runs_not_found) == 1
        assert preview.runs_not_found[0] == "nonexistent"

    def test_delete_runs_fuse_off(self):
        """Test delete_runs with fuse disabled."""
        with patch.dict(os.environ, {"PHASE1_DESTRUCTIVE_FUSE": "false"}):
            result = delete_runs(["run1"])

            assert len(result.deleted) == 0
            assert len(result.errors) == 1
            assert "fuse not enabled" in result.errors[0]

    def test_delete_runs_inflight_blocked(self):
        """Test delete_runs blocks inflight runs."""
        runs_data = {
            "run1": {
                "run_type": "test",
                "timestamp": "2025-01-01T10:00:00",
                "status": "running",
                "input_paths": ["input1.csv"],
                "config_paths": ["config.yaml"],
            },
        }
        self._create_test_run_index(runs_data)

        with patch.dict(os.environ, {"PHASE1_DESTRUCTIVE_FUSE": "true"}):
            with patch(
                "src.utils.cache_utils.is_run_truly_inflight", return_value=True
            ):
                result = delete_runs(["run1"])

                assert len(result.deleted) == 0
                assert len(result.inflight_blocked) == 1
                assert result.inflight_blocked[0] == "run1"

    def test_delete_runs_idempotent(self):
        """Test delete_runs is idempotent."""
        with patch.dict(os.environ, {"PHASE1_DESTRUCTIVE_FUSE": "true"}):
            # First delete
            result1 = delete_runs(["nonexistent"])
            assert len(result1.deleted) == 0
            assert len(result1.not_found) == 1

            # Second delete (should be no-op)
            result2 = delete_runs(["nonexistent"])
            assert len(result2.deleted) == 0
            assert len(result2.not_found) == 1
            assert result2.total_bytes_freed == 0

    def test_preview_delete_legacy_run_type(self):
        """Test preview_delete handles legacy runs without run_type."""
        runs_data = {
            "legacy_run": {
                "timestamp": "2025-01-01T10:00:00",
                "status": "complete",
                "input_paths": ["input1.csv"],
                "config_paths": ["config.yaml"],
            },
        }
        self._create_test_run_index(runs_data)

        with patch("os.walk") as mock_walk, patch("os.path.getsize", return_value=1024):
            mock_walk.return_value = [("/fake/dir", [], ["file1.txt"])]
            with patch("src.utils.cleanup_api.logger") as mock_logger:
                preview = preview_delete(["legacy_run"])

                assert len(preview.runs_to_delete) == 1
                assert preview.runs_to_delete[0].run_type == "dev"
                mock_logger.warning.assert_called_once()
