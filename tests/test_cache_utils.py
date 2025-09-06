"""Test cache utilities functionality."""

import json
import os
from typing import Any, Dict

import pytest

from src.utils.cache_utils import (
    cleanup_failed_runs,
    create_latest_pointer,
    delete_runs,
    get_latest_run_id,
    get_next_latest_run,
    preview_delete_runs,
    prune_old_runs,
    recompute_latest_pointer,
    remove_latest_pointer,
)


@pytest.fixture
def sample_runs(cache_utils_workspace):
    """Create sample run data for testing."""
    workspace = cache_utils_workspace

    # Create sample runs
    runs = [
        {
            "run_id": "test0_123_20231201120000",
            "status": "complete",
            "timestamp": "2023-12-01T12:00:00",
            "config_hash": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            "input_hash": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            "dag_version": "1.0.0",
            "config_files": ["config/settings.yaml"],
        },
        {
            "run_id": "test1_123_20231201120001",
            "status": "complete",
            "timestamp": "2023-12-01T12:00:01",
            "config_hash": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            "input_hash": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            "dag_version": "1.0.0",
            "config_files": ["config/settings.yaml"],
        },
        {
            "run_id": "test2_123_20231201120002",
            "status": "complete",
            "timestamp": "2023-12-01T12:00:02",
            "config_hash": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            "input_hash": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            "dag_version": "1.0.0",
            "config_files": ["config/settings.yaml"],
        },
        {
            "run_id": "test3_123_20231201120003",
            "status": "complete",
            "timestamp": "2023-12-01T12:00:03",
            "config_hash": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            "input_hash": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            "dag_version": "1.0.0",
            "config_files": ["config/settings.yaml"],
        },
        {
            "run_id": "test4_123_20231201120004",
            "status": "complete",
            "timestamp": "2023-12-01T12:00:04",
            "config_hash": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            "input_hash": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            "dag_version": "1.0.0",
            "config_files": ["config/settings.yaml"],
        },
    ]

    # Create run directories and metadata
    for run in runs:
        run_dir = workspace / "data" / "processed" / run["run_id"]
        run_dir.mkdir(parents=True, exist_ok=True)

        # Create pipeline_state.json
        pipeline_state = {
            "run_id": run["run_id"],
            "status": run["status"],
            "timestamp": run["timestamp"],
            "config_hash": run["config_hash"],
            "input_hash": run["input_hash"],
            "dag_version": run["dag_version"],
            "config_files": run["config_files"],
        }

        with open(run_dir / "pipeline_state.json", "w") as f:
            json.dump(pipeline_state, f)

    # Create run_index.json
    run_index = {run["run_id"]: run for run in runs}
    run_index_path = workspace / "data" / "run_index.json"
    with open(run_index_path, "w") as f:
        json.dump(run_index, f)

    return runs, workspace


@pytest.fixture
def failed_runs(cache_utils_workspace):
    """Create failed run data for testing."""
    workspace = cache_utils_workspace

    failed_run: Dict[str, Any] = {
        "run_id": "failed_456_20231201120001",
        "status": "failed",
        "timestamp": "2023-12-01T12:00:01",
        "config_hash": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        "input_hash": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        "dag_version": "1.0.0",
        "config_files": ["config/settings.yaml"],
    }

    # Create failed run directory
    run_dir = workspace / "data" / "processed" / failed_run["run_id"]
    run_dir.mkdir(parents=True, exist_ok=True)

    # Create pipeline_state.json
    pipeline_state = {
        "run_id": failed_run["run_id"],
        "status": failed_run["status"],
        "timestamp": failed_run["timestamp"],
        "config_hash": failed_run["config_hash"],
        "input_hash": failed_run["input_hash"],
        "dag_version": failed_run["dag_version"],
        "config_files": failed_run["config_files"],
    }

    with open(run_dir / "pipeline_state.json", "w") as f:
        json.dump(pipeline_state, f)

    # Add to run_index.json
    run_index_path = workspace / "data" / "run_index.json"
    run_index: Dict[str, Any]
    if run_index_path.exists():
        with open(run_index_path) as f:
            run_index = json.load(f)
    else:
        run_index = {}

    run_index[failed_run["run_id"]] = failed_run

    with open(run_index_path, "w") as f:
        json.dump(run_index, f)

    return failed_run, workspace


def test_latest_pointer_operations(sample_runs, cache_utils_workspace):
    """Test latest pointer creation and retrieval."""
    runs, workspace = sample_runs

    # Get the latest run ID
    run_id = runs[-1]["run_id"]  # test4_123_20231201120004

    # Create latest pointer
    create_latest_pointer(run_id)

    # Verify pointer was created
    latest_id = get_latest_run_id()
    assert latest_id == run_id

    # Remove pointer
    remove_latest_pointer()

    # Verify pointer was removed
    latest_id = get_latest_run_id()
    assert latest_id is None


@pytest.mark.skip(
    reason="TODO: Phase 1.26.1 - Update cache utils tests for new path utilities",
)
def test_prune_old_runs(sample_runs, cache_utils_workspace):
    """Test pruning old runs."""
    runs, workspace = sample_runs

    # Prune old runs, keeping only 2 most recent
    prune_old_runs(keep_runs=2)

    # Check that only 2 runs remain
    remaining_runs = list((workspace / "data" / "processed").iterdir())
    completed_runs = [run.name for run in remaining_runs if run.is_dir()]

    assert len(completed_runs) == 2

    # Should keep the most recent runs
    expected_kept = ["test3_123_20231201120003", "test4_123_20231201120004"]
    for expected in expected_kept:
        assert expected in completed_runs


@pytest.mark.skip(
    reason="TODO: Phase 1.26.1 - Update cache utils tests for new path utilities",
)
def test_cleanup_failed_runs(failed_runs, cache_utils_workspace):
    """Test cleanup of failed runs."""
    failed_run, workspace = failed_runs

    # Cleanup failed runs
    cleanup_failed_runs()

    # Check that failed run was removed
    failed_run_dir = workspace / "data" / "processed" / failed_run["run_id"]
    assert not failed_run_dir.exists()

    # Check run_index.json was updated
    run_index_path = workspace / "data" / "run_index.json"
    with open(run_index_path) as f:
        run_index = json.load(f)

    assert failed_run["run_id"] not in run_index


@pytest.mark.skip(
    reason="TODO: Phase 1.26.1 - Update cache utils tests for new path utilities",
)
def test_preview_delete_runs(sample_runs, cache_utils_workspace):
    """Test preview of run deletion."""
    runs, workspace = sample_runs

    # Set up latest pointer to the chronologically latest run
    latest_run = runs[-1]["run_id"]  # test4_123_20231201120004 (latest)
    create_latest_pointer(latest_run)

    # Preview deletion of latest run (to test latest_affected logic)
    run_to_delete = latest_run
    preview = preview_delete_runs([run_to_delete])

    # Check preview results
    assert any(run["run_id"] == run_to_delete for run in preview["runs_to_delete"])
    assert preview["latest_affected"] is True  # This affects the latest pointer


@pytest.mark.skip(
    reason="TODO: Phase 1.26.1 - Update cache utils tests for new path utilities",
)
def test_delete_runs(sample_runs, cache_utils_workspace):
    """Test actual run deletion."""
    runs, workspace = sample_runs

    # Create latest pointer first
    latest_run = runs[-1]["run_id"]
    create_latest_pointer(latest_run)

    # Delete specific run
    run_to_delete = runs[0]["run_id"]  # test0_123_20231201120000
    results = delete_runs([run_to_delete])

    # Check deletion results
    assert len(results["deleted"]) == 1
    assert run_to_delete in results["deleted"]

    # Verify run was actually deleted
    deleted_run_dir = workspace / "data" / "processed" / run_to_delete
    assert not deleted_run_dir.exists()


@pytest.mark.skip(
    reason="TODO: Phase 1.26.1 - Update cache utils tests for new path utilities",
)
def test_recompute_latest_pointer(sample_runs, cache_utils_workspace):
    """Test recomputing latest pointer."""
    runs, workspace = sample_runs

    # Create pointer to run1 (oldest run)
    run1 = runs[0]["run_id"]  # test0_123_20231201120000
    create_latest_pointer(run1)

    # Delete run1
    delete_runs([run1])

    # Recompute latest pointer - should point to chronologically latest completed run
    recompute_latest_pointer()

    # Should point to the chronologically latest completed run (test4)
    latest_id = get_latest_run_id()
    expected_latest = runs[-1]["run_id"]  # test4_123_20231201120004 (latest timestamp)
    assert latest_id == expected_latest


@pytest.mark.skip(
    reason="TODO: Phase 1.26.1 - Update cache utils tests for new path utilities",
)
def test_remove_latest_pointer(sample_runs, cache_utils_workspace):
    """Test removing latest pointer."""
    runs, workspace = sample_runs

    # Create latest pointer
    run_id = runs[-1]["run_id"]
    create_latest_pointer(run_id)

    # Verify pointer exists
    assert os.path.exists("data/processed/latest.json")

    # Remove pointer
    remove_latest_pointer()

    # Verify pointer was removed
    assert not os.path.exists("data/processed/latest.json")


@pytest.mark.skip(
    reason="TODO: Phase 1.26.1 - Update cache utils tests for new path utilities",
)
def test_get_next_latest_run(sample_runs, cache_utils_workspace):
    """Test getting next latest run."""
    runs, workspace = sample_runs

    # Create pointer to run1 (oldest run)
    run1 = runs[0]["run_id"]  # test0_123_20231201120000
    create_latest_pointer(run1)

    # Delete run1
    delete_runs([run1])

    # Get next latest run - should be the next chronologically latest completed run
    next_latest = get_next_latest_run()
    # After deleting test0, the next latest should be test3 (second most recent timestamp)
    expected_next = runs[-2]["run_id"]  # test3_123_20231201120003
    assert next_latest == expected_next


@pytest.mark.skip(
    reason="TODO: Phase 1.26.1 - Update cache utils tests for new path utilities",
)
def test_recompute_latest_pointer_empty(cache_utils_workspace):
    """Test recomputing latest pointer when no runs exist."""
    # Recompute with no runs
    recompute_latest_pointer()

    # Should return None
    latest_id = get_latest_run_id()
    assert latest_id is None


@pytest.mark.skip(
    reason="TODO: Phase 1.26.1 - Update cache utils tests for new path utilities",
)
def test_delete_runs_with_stuck_running_status(sample_runs, cache_utils_workspace):
    """Test deleting runs with stuck running status."""
    runs, workspace = sample_runs

    # Mark one run as running
    stuck_run = runs[0]["run_id"]
    stuck_run_dir = workspace / "data" / "processed" / stuck_run
    pipeline_state_path = stuck_run_dir / "pipeline_state.json"

    with open(pipeline_state_path) as f:
        pipeline_state = json.load(f)

    pipeline_state["status"] = "running"

    with open(pipeline_state_path, "w") as f:
        json.dump(pipeline_state, f)

    # Delete stuck running runs
    results = delete_runs([stuck_run])

    # Should delete the stuck run
    assert len(results["deleted"]) == 1  # Only the stuck run


@pytest.mark.skip(
    reason="TODO: Phase 1.26.1 - Update cache utils tests for new path utilities",
)
def test_delete_all_runs_scenarios(sample_runs, cache_utils_workspace):
    """Test various deletion scenarios."""
    runs, workspace = sample_runs

    # Create latest pointer
    latest_run = runs[-1]["run_id"]
    create_latest_pointer(latest_run)

    # Delete all runs except latest
    runs_to_delete = [run["run_id"] for run in runs[:-1]]
    results = delete_runs(runs_to_delete)

    # Should delete the specified runs (4 runs: test0, test1, test2, test3)
    assert len(results["deleted"]) == 4

    # Latest run should still exist
    latest_run_dir = workspace / "data" / "processed" / latest_run
    assert latest_run_dir.exists()
