"""Test that group artifacts are properly scoped under run-specific directories.

This test verifies that group_stats.parquet and group_details.parquet
are always written under data/processed/{run_id}/ and never to global paths.
"""

import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.utils.path_utils import get_artifact_path, get_processed_dir


def test_group_artifacts_scoped(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test that group artifacts are written under run-scoped directories."""
    run_id = "test123"

    # Mock the project root to be our temp directory
    monkeypatch.chdir(tmp_path)

    # Create the processed directory structure
    processed_dir = tmp_path / "data" / "processed" / run_id
    processed_dir.mkdir(parents=True, exist_ok=True)

    # Test that get_processed_dir returns the correct run-scoped path
    result = get_processed_dir(run_id)
    expected = Path("data") / "processed" / run_id
    assert result == expected

    # Test that get_artifact_path returns run-scoped paths for group artifacts
    group_stats_path = get_artifact_path(run_id, "group_stats.parquet")
    group_details_path = get_artifact_path(run_id, "group_details.parquet")

    # Both should be under the run-specific processed directory
    assert str(group_stats_path).startswith(f"data/processed/{run_id}/")
    assert str(group_details_path).startswith(f"data/processed/{run_id}/")

    # Verify the paths are relative and run-scoped
    assert group_stats_path.parent == Path("data") / "processed" / run_id
    assert group_details_path.parent == Path("data") / "processed" / run_id


def test_artifact_paths_never_global(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test that artifact paths never resolve to global processed directories."""
    run_id = "test456"

    # Mock the project root to be our temp directory
    monkeypatch.chdir(tmp_path)

    # Create both processed and interim directories
    processed_dir = tmp_path / "data" / "processed" / run_id
    interim_dir = tmp_path / "data" / "interim" / run_id
    processed_dir.mkdir(parents=True, exist_ok=True)
    interim_dir.mkdir(parents=True, exist_ok=True)

    # Test various artifact names to ensure they're all run-scoped
    artifacts = [
        "group_stats.parquet",
        "group_details.parquet",
        "review_ready.parquet",
        "schema_mapping.json",
        "perf_summary.json",
    ]

    for artifact in artifacts:
        artifact_path = get_artifact_path(run_id, artifact)

        # Should never be in a global processed directory (without run_id)
        assert not str(artifact_path).startswith("data/processed/") or str(
            artifact_path,
        ).startswith(f"data/processed/{run_id}/")
        assert not str(artifact_path).startswith("data/interim/") or str(
            artifact_path,
        ).startswith(f"data/interim/{run_id}/")

        # Should always be under the specific run_id
        assert str(artifact_path).startswith(f"data/processed/{run_id}/") or str(
            artifact_path,
        ).startswith(f"data/interim/{run_id}/")


def test_run_id_required_for_processed_paths() -> None:
    """Test that run_id is required for processed directory paths."""
    # Empty run_id should not be allowed
    with pytest.raises(ValueError, match="run_id cannot be empty"):
        get_processed_dir("")

    # None run_id should not be allowed
    with pytest.raises(ValueError, match="run_id cannot be empty"):
        get_processed_dir(None)

    # Valid run_id should work
    result = get_processed_dir("valid_run_123")
    assert result == Path("data") / "processed" / "valid_run_123"


def test_path_utils_consistency() -> None:
    """Test that path utility functions are consistent with each other."""
    run_id = "consistency_test"

    # get_processed_dir and get_artifact_path should be consistent
    processed_dir = get_processed_dir(run_id)
    artifact_path = get_artifact_path(run_id, "test.parquet")

    # The artifact path should be a child of the processed directory
    assert artifact_path.parent == processed_dir

    # Both should use the same run_id
    assert run_id in str(processed_dir)
    assert run_id in str(artifact_path)
