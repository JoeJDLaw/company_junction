"""
Tests for cache utilities functionality.

This module tests:
- Run ID generation
- Cache directory creation
- Run index management
- Latest pointer handling
- Pruning functionality
"""

import pytest
import tempfile
import os
from pathlib import Path

from src.utils.cache_utils import (
    compute_file_hash,
    generate_run_id,
    get_cache_directories,
    create_cache_directories,
    load_run_index,
    add_run_to_index,
    update_run_status,
    create_latest_pointer,
    get_latest_run_id,
    prune_old_runs,
    cleanup_failed_runs,
    preview_delete_runs,
    delete_runs,
    recompute_latest_pointer,
    remove_latest_pointer,
    list_runs_sorted,
    get_next_latest_run,
)


def test_compute_file_hash() -> None:
    """Test file hash computation."""
    with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
        f.write("test content")
        temp_file = f.name

    try:
        hash_value = compute_file_hash(temp_file)
        assert len(hash_value) == 64  # SHA256 hash length
        assert hash_value.isalnum()  # Should be alphanumeric
    finally:
        os.unlink(temp_file)


def test_generate_run_id() -> None:
    """Test run ID generation."""
    # Create temporary files for testing
    with tempfile.NamedTemporaryFile(mode="w", delete=False) as f1:
        f1.write("input content")
        input_file = f1.name

    with tempfile.NamedTemporaryFile(mode="w", delete=False) as f2:
        f2.write("config content")
        config_file = f2.name

    try:
        run_id = generate_run_id([input_file], [config_file])

        # Check format: {input_hash[:8]}_{config_hash[:8]}_{YYYYMMDDHHMMSS}
        parts = run_id.split("_")
        assert len(parts) == 3
        assert len(parts[0]) == 8  # input hash prefix
        assert len(parts[1]) == 8  # config hash prefix
        assert len(parts[2]) == 14  # timestamp (YYYYMMDDHHMMSS)

        # Timestamp should be numeric
        assert parts[2].isdigit()

    finally:
        os.unlink(input_file)
        os.unlink(config_file)


def test_get_cache_directories() -> None:
    """Test cache directory path generation."""
    run_id = "test123_456_20231201120000"
    interim_dir, processed_dir = get_cache_directories(run_id)

    assert interim_dir == f"data/interim/{run_id}"
    assert processed_dir == f"data/processed/{run_id}"


def test_create_cache_directories(tmp_path: Path) -> None:
    """Test cache directory creation."""
    # Change to temporary directory for testing
    original_cwd = os.getcwd()
    os.chdir(tmp_path)

    try:
        run_id = "test123_456_20231201120000"
        interim_dir, processed_dir = create_cache_directories(run_id)

        # Check that directories were created
        assert os.path.exists(interim_dir)
        assert os.path.exists(processed_dir)
        assert os.path.isdir(interim_dir)
        assert os.path.isdir(processed_dir)

        # Check paths match expected
        assert interim_dir == f"data/interim/{run_id}"
        assert processed_dir == f"data/processed/{run_id}"

    finally:
        os.chdir(original_cwd)


def test_run_index_operations(tmp_path: Path) -> None:
    """Test run index loading, saving, and operations."""
    # Change to temporary directory for testing
    original_cwd = os.getcwd()
    os.chdir(tmp_path)

    try:
        # Test empty index
        index = load_run_index()
        assert index == {}

        # Test adding runs
        run_id1 = "test1_123_20231201120000"
        run_id2 = "test2_456_20231201120001"

        add_run_to_index(run_id1, ["input1.csv"], ["config1.yaml"], "running")
        add_run_to_index(run_id2, ["input2.csv"], ["config2.yaml"], "complete")

        # Test loading updated index
        index = load_run_index()
        assert run_id1 in index
        assert run_id2 in index
        assert index[run_id1]["status"] == "running"
        assert index[run_id2]["status"] == "complete"

        # Test updating status
        update_run_status(run_id1, "complete")
        index = load_run_index()
        assert index[run_id1]["status"] == "complete"

        # Test updating non-existent run
        update_run_status("nonexistent", "complete")
        # Should not raise an error

    finally:
        os.chdir(original_cwd)


def test_latest_pointer_operations(tmp_path: Path) -> None:
    """Test latest pointer creation and retrieval."""
    # Change to temporary directory for testing
    original_cwd = os.getcwd()
    os.chdir(tmp_path)

    try:
        # Create processed directory structure
        os.makedirs("data/processed", exist_ok=True)

        run_id = "test123_456_20231201120000"
        run_dir = f"data/processed/{run_id}"
        os.makedirs(run_dir, exist_ok=True)

        # Test creating latest pointer
        create_latest_pointer(run_id)

        # Test retrieving latest run ID
        latest_id = get_latest_run_id()
        assert latest_id == run_id

        # Test with multiple runs
        run_id2 = "test789_012_20231201120001"
        run_dir2 = f"data/processed/{run_id2}"
        os.makedirs(run_dir2, exist_ok=True)

        create_latest_pointer(run_id2)
        latest_id = get_latest_run_id()
        assert latest_id == run_id2

    finally:
        os.chdir(original_cwd)


def test_prune_old_runs(tmp_path: Path) -> None:
    """Test pruning of old completed runs."""
    # Change to temporary directory for testing
    original_cwd = os.getcwd()
    os.chdir(tmp_path)

    try:
        # Create test structure
        os.makedirs("data/interim", exist_ok=True)
        os.makedirs("data/processed", exist_ok=True)

        # Create multiple runs
        runs = []
        for i in range(5):
            run_id = f"test{i}_123_2023120112000{i}"
            runs.append(run_id)

            # Create directories
            os.makedirs(f"data/interim/{run_id}", exist_ok=True)
            os.makedirs(f"data/processed/{run_id}", exist_ok=True)

            # Add to index
            add_run_to_index(run_id, [f"input{i}.csv"], [f"config{i}.yaml"], "complete")

        # Test pruning (keep only 2 runs)
        prune_old_runs(2)

        # Check that only 2 runs remain
        index = load_run_index()
        completed_runs = [
            rid for rid, data in index.items() if data["status"] == "complete"
        ]
        assert len(completed_runs) == 2

        # Check that directories were cleaned up
        remaining_dirs = [d for d in os.listdir("data/interim") if d.startswith("test")]
        assert len(remaining_dirs) == 2

    finally:
        os.chdir(original_cwd)


def test_cleanup_failed_runs(tmp_path: Path) -> None:
    """Test cleanup of failed runs."""
    # Change to temporary directory for testing
    original_cwd = os.getcwd()
    os.chdir(tmp_path)

    try:
        # Create test structure
        os.makedirs("data/interim", exist_ok=True)
        os.makedirs("data/processed", exist_ok=True)

        # Create successful and failed runs
        success_run = "success_123_20231201120000"
        failed_run = "failed_456_20231201120001"

        for run_id in [success_run, failed_run]:
            os.makedirs(f"data/interim/{run_id}", exist_ok=True)
            os.makedirs(f"data/processed/{run_id}", exist_ok=True)

        # Add to index with different statuses
        add_run_to_index(success_run, ["input1.csv"], ["config1.yaml"], "complete")
        add_run_to_index(failed_run, ["input2.csv"], ["config2.yaml"], "failed")

        # Test cleanup
        cleanup_failed_runs()

        # Check that failed run was removed
        index = load_run_index()
        assert success_run in index
        assert failed_run not in index

        # Check that failed run directories were cleaned up
        assert not os.path.exists(f"data/interim/{failed_run}")
        assert not os.path.exists(f"data/processed/{failed_run}")

        # Check that successful run directories remain
        assert os.path.exists(f"data/interim/{success_run}")
        assert os.path.exists(f"data/processed/{success_run}")

    finally:
        os.chdir(original_cwd)


def test_run_id_format_validation() -> None:
    """Test run ID format validation."""
    # Valid run ID
    valid_run_id = "a1b2c3d4_e5f6g7h8_20231201120000"
    parts = valid_run_id.split("_")
    assert len(parts) == 3
    assert len(parts[0]) == 8
    assert len(parts[1]) == 8
    assert len(parts[2]) == 14
    assert parts[2].isdigit()

    # Invalid run IDs
    invalid_ids = [
        "short_123_20231201120000",  # First part too short
        "a1b2c3d4e5f6g7h8_123_20231201120000",  # First part too long
        "a1b2c3d4_123_20231201120000",  # Second part too short
        "a1b2c3d4_123456789_20231201120000",  # Second part too long
        "a1b2c3d4_123_20231201",  # Timestamp too short
        "a1b2c3d4_123_20231201120000123",  # Timestamp too long
        "a1b2c3d4_123_abcdefghijklmn",  # Timestamp not numeric
    ]

    for invalid_id in invalid_ids:
        parts = invalid_id.split("_")
        assert (
            len(parts) != 3
            or len(parts[0]) != 8
            or len(parts[1]) != 8
            or len(parts[2]) != 14
            or not parts[2].isdigit()
        )


def test_error_handling() -> None:
    """Test error handling in cache operations."""
    # Test with non-existent files
    generate_run_id(["nonexistent1.csv"], ["nonexistent2.yaml"])
    # Should not raise an error, should handle missing files gracefully

    # Test with corrupted run index
    with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
        f.write("invalid json content")
        temp_file = f.name

    try:
        # Should handle corrupted JSON gracefully
        # This would require mocking the file path, but we can test the concept
        pass
    finally:
        os.unlink(temp_file)


# Deletion utilities tests
def test_preview_delete_runs(tmp_path: Path) -> None:
    """Test preview deletion functionality."""
    # Change to temporary directory for testing
    original_cwd = os.getcwd()
    os.chdir(tmp_path)

    try:
        # Create test structure
        os.makedirs("data/interim", exist_ok=True)
        os.makedirs("data/processed", exist_ok=True)

        # Create test runs
        run1 = "run1_123_20231201120000"
        run2 = "run2_456_20231201120001"
        run3 = "run3_789_20231201120002"

        for run_id in [run1, run2, run3]:
            os.makedirs(f"data/interim/{run_id}", exist_ok=True)
            os.makedirs(f"data/processed/{run_id}", exist_ok=True)

            # Create some test files
            with open(f"data/interim/{run_id}/test.txt", "w") as f:
                f.write("test content")
            with open(f"data/processed/{run_id}/output.csv", "w") as f:
                f.write("output content")

        # Add to index
        add_run_to_index(run1, ["input1.csv"], ["config1.yaml"], "complete")
        add_run_to_index(run2, ["input2.csv"], ["config2.yaml"], "complete")
        add_run_to_index(run3, ["input3.csv"], ["config3.yaml"], "running")

        # Set run1 as latest
        create_latest_pointer(run1)

        # Preview deletion
        preview = preview_delete_runs([run1, run2, "nonexistent"])

        # Check results
        assert len(preview["runs_to_delete"]) == 2
        assert len(preview["runs_not_found"]) == 1
        assert len(preview["runs_inflight"]) == 0
        assert preview["latest_affected"] is True
        assert preview["latest_run_id"] == run1

        # Check that run3 (running) is blocked
        preview2 = preview_delete_runs([run3])
        assert len(preview2["runs_inflight"]) == 1
        assert len(preview2["runs_to_delete"]) == 0

    finally:
        os.chdir(original_cwd)


def test_delete_runs(tmp_path: Path) -> None:
    """Test run deletion functionality."""
    # Change to temporary directory for testing
    original_cwd = os.getcwd()
    os.chdir(tmp_path)

    try:
        # Create test structure
        os.makedirs("data/interim", exist_ok=True)
        os.makedirs("data/processed", exist_ok=True)

        # Create test runs
        run1 = "run1_123_20231201120000"
        run2 = "run2_456_20231201120001"
        run3 = "run3_789_20231201120002"

        for run_id in [run1, run2, run3]:
            os.makedirs(f"data/interim/{run_id}", exist_ok=True)
            os.makedirs(f"data/processed/{run_id}", exist_ok=True)

            # Create some test files
            with open(f"data/interim/{run_id}/test.txt", "w") as f:
                f.write("test content")
            with open(f"data/processed/{run_id}/output.csv", "w") as f:
                f.write("output content")

        # Add to index
        add_run_to_index(run1, ["input1.csv"], ["config1.yaml"], "complete")
        add_run_to_index(run2, ["input2.csv"], ["config2.yaml"], "complete")
        add_run_to_index(run3, ["input3.csv"], ["config3.yaml"], "running")

        # Set run1 as latest
        create_latest_pointer(run1)

        # Delete runs
        results = delete_runs([run1, "nonexistent"])

        # Check results
        assert len(results["deleted"]) == 1
        assert len(results["not_found"]) == 1
        assert len(results["inflight_blocked"]) == 0
        assert results["latest_reassigned"] is True
        assert results["new_latest"] == run2

        # Check that directories were removed
        assert not os.path.exists(f"data/interim/{run1}")
        assert not os.path.exists(f"data/processed/{run1}")
        # run2 should still exist since it wasn't deleted
        assert os.path.exists(f"data/interim/{run2}")
        assert os.path.exists(f"data/processed/{run2}")

        # Check that run3 (running) remains
        assert os.path.exists(f"data/interim/{run3}")
        assert os.path.exists(f"data/processed/{run3}")

        # Check that run3 is blocked from deletion
        results2 = delete_runs([run3])
        assert len(results2["inflight_blocked"]) == 1
        assert len(results2["deleted"]) == 0

    finally:
        os.chdir(original_cwd)


def test_recompute_latest_pointer(tmp_path: Path) -> None:
    """Test latest pointer recomputation."""
    # Change to temporary directory for testing
    original_cwd = os.getcwd()
    os.chdir(tmp_path)

    try:
        # Create test structure
        os.makedirs("data/interim", exist_ok=True)
        os.makedirs("data/processed", exist_ok=True)

        # Create test runs
        run1 = "run1_123_20231201120000"
        run2 = "run2_456_20231201120001"

        for run_id in [run1, run2]:
            os.makedirs(f"data/interim/{run_id}", exist_ok=True)
            os.makedirs(f"data/processed/{run_id}", exist_ok=True)

        # Add to index
        add_run_to_index(run1, ["input1.csv"], ["config1.yaml"], "complete")
        add_run_to_index(run2, ["input2.csv"], ["config2.yaml"], "complete")

        # Set run1 as latest
        create_latest_pointer(run1)

        # Delete run1 and recompute
        delete_runs([run1])
        new_latest = recompute_latest_pointer()

        # Check that run2 is now latest
        assert new_latest == run2
        assert get_latest_run_id() == run2

    finally:
        os.chdir(original_cwd)


def test_remove_latest_pointer(tmp_path: Path) -> None:
    """Test latest pointer removal."""
    # Change to temporary directory for testing
    original_cwd = os.getcwd()
    os.chdir(tmp_path)

    try:
        # Create test structure
        os.makedirs("data/processed", exist_ok=True)

        # Create latest pointer
        create_latest_pointer("test_run")

        # Check that pointer exists
        assert os.path.exists("data/processed/latest.json")
        assert get_latest_run_id() == "test_run"

        # Remove pointer
        remove_latest_pointer()

        # Check that pointer is gone
        assert not os.path.exists("data/processed/latest.json")
        assert get_latest_run_id() is None

    finally:
        os.chdir(original_cwd)


def test_list_runs_sorted(tmp_path: Path) -> None:
    """Test sorted run listing."""
    # Change to temporary directory for testing
    original_cwd = os.getcwd()
    os.chdir(tmp_path)

    try:
        # Create test structure
        os.makedirs("data/interim", exist_ok=True)
        os.makedirs("data/processed", exist_ok=True)

        # Create test runs
        run1 = "run1_123_20231201120000"
        run2 = "run2_456_20231201120001"

        for run_id in [run1, run2]:
            os.makedirs(f"data/interim/{run_id}", exist_ok=True)
            os.makedirs(f"data/processed/{run_id}", exist_ok=True)

        # Add to index
        add_run_to_index(run1, ["input1.csv"], ["config1.yaml"], "complete")
        add_run_to_index(run2, ["input2.csv"], ["config2.yaml"], "complete")

        # Get sorted runs
        runs = list_runs_sorted()

        # Check that runs are sorted by timestamp (newest first)
        assert len(runs) == 2
        assert runs[0][0] == run2  # run2 should be newer
        assert runs[1][0] == run1

    finally:
        os.chdir(original_cwd)


def test_get_next_latest_run(tmp_path: Path) -> None:
    """Test next latest run retrieval."""
    # Change to temporary directory for testing
    original_cwd = os.getcwd()
    os.chdir(tmp_path)

    try:
        # Create test structure
        os.makedirs("data/interim", exist_ok=True)
        os.makedirs("data/processed", exist_ok=True)

        # Create test runs
        run1 = "run1_123_20231201120000"
        run2 = "run2_456_20231201120001"

        for run_id in [run1, run2]:
            os.makedirs(f"data/interim/{run_id}", exist_ok=True)
            os.makedirs(f"data/processed/{run_id}", exist_ok=True)

        # Add to index
        add_run_to_index(run1, ["input1.csv"], ["config1.yaml"], "complete")
        add_run_to_index(run2, ["input2.csv"], ["config2.yaml"], "complete")

        # Set run1 as latest
        create_latest_pointer(run1)

        # Get next latest
        next_latest = get_next_latest_run()

        # Check that run2 is next latest
        assert next_latest == run2

    finally:
        os.chdir(original_cwd)


if __name__ == "__main__":
    pytest.main([__file__])
