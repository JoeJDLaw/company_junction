"""Tests for pipeline cleanup CLI functionality."""

import json
import os
import subprocess
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from pipeline_cleanup import format_bytes, get_runs_by_type, main, print_runs_by_type


class TestPipelineCleanupCLI:
    """Test pipeline cleanup CLI functions."""

    def setup_method(self):
        """Set up test environment."""
        self.temp_dir = tempfile.mkdtemp()
        self.run_index_path = os.path.join(self.temp_dir, "run_index.json")
        
        # Mock the RUN_INDEX_PATH
        self.patcher = patch("src.utils.cache_utils.RUN_INDEX_PATH", self.run_index_path)
        self.patcher.start()
        
        # Mock the path utilities to use temp directory
        self.path_patcher_processed = patch("src.utils.cache_utils.get_processed_dir")
        self.mock_get_processed_dir = self.path_patcher_processed.start()
        self.mock_get_processed_dir.return_value = Path(self.temp_dir)
        
        self.path_patcher_interim = patch("src.utils.cache_utils.get_interim_dir")
        self.mock_get_interim_dir = self.path_patcher_interim.start()
        self.mock_get_interim_dir.return_value = Path(self.temp_dir)

    def teardown_method(self):
        """Clean up test environment."""
        self.patcher.stop()
        self.path_patcher_processed.stop()
        self.path_patcher_interim.stop()
        # Clean up temp directory
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _create_test_run_index(self, runs_data):
        """Create a test run index file."""
        with open(self.run_index_path, "w") as f:
            json.dump(runs_data, f)

    def test_format_bytes(self):
        """Test format_bytes function."""
        assert format_bytes(0) == "0 B"
        assert format_bytes(1024) == "1.0 KB"
        assert format_bytes(1024 * 1024) == "1.0 MB"
        assert format_bytes(1024 * 1024 * 1024) == "1.0 GB"
        assert format_bytes(1024 * 1024 * 1024 * 1024) == "1.0 TB"
        assert format_bytes(1024 * 1024 * 1024 * 1024 * 1024) == "1.0 PB"

    def test_get_runs_by_type(self):
        """Test get_runs_by_type function."""
        runs_data = {
            "test_run1": {
                "run_type": "test",
                "timestamp": "2025-01-01T10:00:00",
                "status": "complete",
                "input_paths": ["input1.csv"],
                "config_paths": ["config.yaml"],
            },
            "test_run2": {
                "run_type": "test",
                "timestamp": "2025-01-01T11:00:00",
                "status": "complete",
                "input_paths": ["input2.csv"],
                "config_paths": ["config.yaml"],
            },
            "dev_run": {
                "run_type": "dev",
                "timestamp": "2025-01-01T12:00:00",
                "status": "complete",
                "input_paths": ["input3.csv"],
                "config_paths": ["config.yaml"],
            },
        }
        self._create_test_run_index(runs_data)
        
        test_runs = get_runs_by_type("test")
        assert len(test_runs) == 2
        assert "test_run1" in test_runs
        assert "test_run2" in test_runs
        
        dev_runs = get_runs_by_type("dev")
        assert len(dev_runs) == 1
        assert "dev_run" in dev_runs

    def test_print_runs_by_type_empty(self, capsys):
        """Test print_runs_by_type with no runs."""
        self._create_test_run_index({})
        print_runs_by_type()
        
        captured = capsys.readouterr()
        assert "No runs found." in captured.out

    def test_print_runs_by_type_with_data(self, capsys):
        """Test print_runs_by_type with run data."""
        runs_data = {
            "test_run": {
                "run_type": "test",
                "timestamp": "2025-01-01T10:00:00",
                "status": "complete",
                "input_paths": ["input1.csv"],
                "config_paths": ["config.yaml"],
            },
            "dev_run": {
                "run_type": "dev",
                "timestamp": "2025-01-01T11:00:00",
                "status": "complete",
                "input_paths": ["input2.csv"],
                "config_paths": ["config.yaml"],
            },
        }
        self._create_test_run_index(runs_data)
        
        print_runs_by_type()
        
        captured = capsys.readouterr()
        assert "TEST (1 runs):" in captured.out
        assert "DEV (1 runs):" in captured.out
        assert "Total: 2 runs" in captured.out

    def test_cli_list_command(self, capsys):
        """Test CLI --list command."""
        runs_data = {
            "test_run": {
                "run_type": "test",
                "timestamp": "2025-01-01T10:00:00",
                "status": "complete",
                "input_paths": ["input1.csv"],
                "config_paths": ["config.yaml"],
            },
        }
        self._create_test_run_index(runs_data)
        
        with patch("sys.argv", ["pipeline_cleanup.py", "--list"]):
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 0
        
        captured = capsys.readouterr()
        assert "TEST (1 runs):" in captured.out

    def test_cli_dry_run_no_candidates(self, capsys):
        """Test CLI --dry-run with no candidates."""
        self._create_test_run_index({})
        
        with patch("sys.argv", ["pipeline_cleanup.py", "--delete-tests", "--dry-run"]):
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 0
        
        captured = capsys.readouterr()
        assert "No test runs found." in captured.out

    def test_cli_dry_run_with_candidates(self, capsys):
        """Test CLI --dry-run with candidates."""
        runs_data = {
            "test_run": {
                "run_type": "test",
                "timestamp": "2025-01-01T10:00:00",
                "status": "complete",
                "input_paths": ["input1.csv"],
                "config_paths": ["config.yaml"],
            },
        }
        self._create_test_run_index(runs_data)
        
        with patch("os.walk") as mock_walk, patch("os.path.getsize", return_value=1024):
            mock_walk.return_value = [("/fake/dir", [], ["file1.txt"])]
            with patch("sys.argv", ["pipeline_cleanup.py", "--delete-tests", "--dry-run"]):
                with pytest.raises(SystemExit) as exc_info:
                    main()
                assert exc_info.value.code == 2  # Candidates found
        
        captured = capsys.readouterr()
        assert "Would delete 1 test runs:" in captured.out

    def test_cli_error_handling(self, capsys):
        """Test CLI error handling."""
        # Create a directory that can't be read
        bad_dir = os.path.join(self.temp_dir, "bad_dir")
        os.makedirs(bad_dir, mode=0o000)  # No permissions
        
        runs_data = {
            "test_run": {
                "run_type": "test",
                "timestamp": "2025-01-01T10:00:00",
                "status": "complete",
                "input_paths": ["input1.csv"],
                "config_paths": ["config.yaml"],
            },
        }
        self._create_test_run_index(runs_data)
        
        with patch("sys.argv", ["pipeline_cleanup.py", "--delete-tests", "--dry-run"]):
            with pytest.raises(SystemExit) as exc_info:
                main()
            # Should exit with error code
            assert exc_info.value.code > 0
        
        # Clean up
        os.chmod(bad_dir, 0o755)
        os.rmdir(bad_dir)

    def test_cli_confirmation_cancelled(self, capsys):
        """Test CLI confirmation cancellation."""
        runs_data = {
            "test_run": {
                "run_type": "test",
                "timestamp": "2025-01-01T10:00:00",
                "status": "complete",
                "input_paths": ["input1.csv"],
                "config_paths": ["config.yaml"],
            },
        }
        self._create_test_run_index(runs_data)
        
        with patch("os.walk") as mock_walk, patch("os.path.getsize", return_value=1024):
            mock_walk.return_value = [("/fake/dir", [], ["file1.txt"])]
            with patch("builtins.input", return_value="n"):  # Cancel
                with patch("sys.argv", ["pipeline_cleanup.py", "--delete-tests"]):
                    with pytest.raises(SystemExit) as exc_info:
                        main()
                    assert exc_info.value.code == 0
        
        captured = capsys.readouterr()
        assert "Deletion cancelled." in captured.out
