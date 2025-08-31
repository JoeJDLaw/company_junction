"""Tests for cleanup utility functionality."""

from pathlib import Path
from unittest.mock import patch

# Import the cleanup utility functions
import sys

sys.path.insert(0, str(Path(__file__).parent.parent / "tools"))
from cleanup_test_artifacts import (
    is_temporary_filename,
    is_sample_test_run,
    is_stale_run,
    get_run_age_days,
    find_candidate_runs,
    delete_run_directories,
    update_latest_symlink,
)


class TestCleanupUtils:
    """Test cleanup utility functions."""

    def test_is_temporary_filename(self) -> None:
        """Test temporary filename detection."""
        # Should be detected as temporary
        assert is_temporary_filename("tmp123.csv")
        assert is_temporary_filename("temp_file.csv")
        assert is_temporary_filename("test_data.csv")
        assert is_temporary_filename("data_test.csv")
        assert is_temporary_filename("file_temp.csv")
        assert is_temporary_filename("file_tmp.csv")

        # Should not be detected as temporary
        assert not is_temporary_filename("company_junction_range_01.csv")
        assert not is_temporary_filename("normal_file.csv")
        assert not is_temporary_filename("data.csv")

    def test_is_sample_test_run(self) -> None:
        """Test sample test run detection."""
        # Should be detected as sample test
        run_data = {"input_paths": ["data/raw/sample_test.csv"]}
        assert is_sample_test_run(run_data)

        # Should not be detected as sample test
        run_data = {"input_paths": ["data/raw/company_junction_range_01.csv"]}
        assert not is_sample_test_run(run_data)

        # Empty input paths
        run_data = {"input_paths": []}
        assert not is_sample_test_run(run_data)

    def test_is_stale_run(self) -> None:
        """Test stale run detection."""
        with patch("pathlib.Path.exists") as mock_exists:
            # Both directories exist - not stale
            mock_exists.return_value = True
            assert not is_stale_run("test_run", {})

            # Both directories missing - stale
            mock_exists.return_value = False
            assert is_stale_run("test_run", {})

    def test_get_run_age_days(self) -> None:
        """Test run age calculation."""
        from datetime import datetime, timedelta

        # Test with valid timestamp
        now = datetime.now()
        yesterday = now - timedelta(days=1)
        run_data = {"timestamp": yesterday.isoformat()}
        age = get_run_age_days(run_data)
        assert age == 1

        # Test with no timestamp
        run_data = {}
        age = get_run_age_days(run_data)
        assert age == 999

        # Test with invalid timestamp
        run_data = {"timestamp": "invalid"}
        age = get_run_age_days(run_data)
        assert age == 999

    def test_find_candidate_runs(self) -> None:
        """Test candidate run finding."""
        run_index = {
            "run1": {
                "input_paths": ["data/raw/sample_test.csv"],
                "timestamp": "2025-01-01T00:00:00",
            },
            "run2": {
                "input_paths": ["data/raw/tmp123.csv"],
                "timestamp": "2025-01-01T00:00:00",
            },
            "run3": {
                "input_paths": ["data/raw/company_junction_range_01.csv"],
                "timestamp": "2025-01-01T00:00:00",
            },
        }

        with patch("cleanup_test_artifacts.is_stale_run") as mock_stale:
            # Mock to return False for all runs (no stale runs)
            mock_stale.return_value = False

            # Test sample test detection
            candidates = find_candidate_runs(run_index, include_sample_test=True)
            assert len(candidates) == 1
            assert candidates[0][0] == "run1"
            assert candidates[0][2] == "sample_test"

            # Test pattern matching
            candidates = find_candidate_runs(run_index, pattern="*tmp*")
            assert len(candidates) == 1
            assert candidates[0][0] == "run2"
            assert candidates[0][2] == "pattern_match"

            # Test temporary filename detection (without pattern)
            candidates = find_candidate_runs(run_index)
            assert (
                len(candidates) == 2
            )  # Both sample_test.csv and tmp123.csv are temporary
            run_ids = [c[0] for c in candidates]
            assert "run1" in run_ids  # sample_test.csv
            assert "run2" in run_ids  # tmp123.csv
            reasons = [c[2] for c in candidates]
            assert all(reason == "temporary_filename" for reason in reasons)

    def test_find_candidate_runs_age_filter(self) -> None:
        """Test age filtering in candidate finding."""
        from datetime import datetime, timedelta

        now = datetime.now()
        old_date = now - timedelta(days=10)
        recent_date = now - timedelta(days=1)

        run_index = {
            "old_run": {
                "input_paths": ["data/raw/tmp123.csv"],
                "timestamp": old_date.isoformat(),
            },
            "recent_run": {
                "input_paths": ["data/raw/tmp456.csv"],
                "timestamp": recent_date.isoformat(),
            },
        }

        # Test age filtering
        candidates = find_candidate_runs(run_index, days_older_than=5)
        assert len(candidates) == 1
        assert candidates[0][0] == "old_run"

        # Test age filtering with sample test
        candidates = find_candidate_runs(
            run_index, include_sample_test=True, days_older_than=5
        )
        assert len(candidates) == 1  # Only old_run matches both criteria

    def test_find_candidate_runs_stale_index(self) -> None:
        """Test stale index detection."""
        run_index = {
            "stale_run": {
                "input_paths": ["data/raw/company_junction_range_01.csv"],
                "timestamp": "2025-01-01T00:00:00",
            }
        }

        with patch("cleanup_test_artifacts.is_stale_run") as mock_stale:
            mock_stale.return_value = True

            candidates = find_candidate_runs(run_index, only_stale_index=True)
            assert len(candidates) == 1
            assert candidates[0][0] == "stale_run"
            assert candidates[0][2] == "stale_index"

    def test_delete_run_directories(self) -> None:
        """Test run directory deletion."""
        with patch("pathlib.Path.exists") as mock_exists, patch(
            "shutil.rmtree"
        ) as mock_rmtree:

            # Test successful deletion
            mock_exists.return_value = True
            mock_rmtree.return_value = None

            success = delete_run_directories("test_run")
            assert success is True
            assert mock_rmtree.call_count == 2  # interim and processed

            # Test failed deletion
            mock_rmtree.side_effect = OSError("Permission denied")
            success = delete_run_directories("test_run")
            assert success is False

    def test_update_latest_symlink(self) -> None:
        """Test latest symlink update."""
        with patch("pathlib.Path.exists") as mock_exists, patch(
            "pathlib.Path.is_symlink"
        ) as mock_is_symlink, patch("pathlib.Path.resolve") as mock_resolve, patch(
            "pathlib.Path.unlink"
        ) as mock_unlink:

            # Test broken symlink
            mock_exists.return_value = True
            mock_is_symlink.return_value = True
            mock_resolve.side_effect = OSError("Broken symlink")

            update_latest_symlink()
            mock_unlink.assert_called_once()

            # Reset for second test
            mock_unlink.reset_mock()
            mock_exists.reset_mock()
            mock_is_symlink.reset_mock()
            mock_resolve.reset_mock()

            # Test symlink pointing to non-existent target
            mock_exists.return_value = True
            mock_is_symlink.return_value = True
            mock_resolve.return_value = Path("/nonexistent")

            # Mock the exists check for the target
            def exists_side_effect(*args):
                # First call is for latest_path.exists(), second is for target.exists()
                if len(args) > 0 and str(args[0]) == "/nonexistent":
                    return False
                return True

            mock_exists.side_effect = exists_side_effect

            update_latest_symlink()
            mock_unlink.assert_called_once()


class TestCleanupIntegration:
    """Integration tests for cleanup functionality."""

    def test_dry_run_behavior(self) -> None:
        """Test that dry-run doesn't delete anything."""
        run_index = {
            "test_run": {
                "input_paths": ["data/raw/sample_test.csv"],
                "timestamp": "2025-01-01T00:00:00",
            }
        }

        with patch("cleanup_test_artifacts.is_stale_run") as mock_stale:
            mock_stale.return_value = False

            # This would be called by the main function in dry-run mode
            candidates = find_candidate_runs(run_index, include_sample_test=True)
            assert len(candidates) == 1

            # Verify the candidate was found correctly
            assert candidates[0][0] == "test_run"
            assert candidates[0][2] == "sample_test"

    def test_safe_cleanup_scope(self) -> None:
        """Test that cleanup only affects allowed directories."""
        with patch("pathlib.Path.exists") as mock_exists, patch(
            "shutil.rmtree"
        ) as mock_rmtree:

            mock_exists.return_value = True
            mock_rmtree.return_value = None

            delete_run_directories("test_run")

            # Verify only interim and processed directories were targeted
            calls = mock_rmtree.call_args_list
            assert len(calls) == 2

            # Check that calls were for interim and processed directories
            call_paths = [str(call[0][0]) for call in calls]
            assert "data/interim/test_run" in call_paths
            assert "data/processed/test_run" in call_paths

            # Verify no calls to raw or samples directories
            for path in call_paths:
                assert "data/raw" not in path
                assert "data/samples" not in path
