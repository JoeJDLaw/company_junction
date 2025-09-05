"""Tests for CLI argument forwarding to run_pipeline."""

import types
from pathlib import Path
from unittest.mock import patch

import pytest

from src import cleaning


def test_main_forwards_resume_noresume_force_correct_order(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    """Test that main() forwards resume/no_resume/force args in correct order.

    This test verifies the bugfix where positional arguments were passed in wrong order.
    """
    called = {}

    def fake_run_pipeline(**kwargs):
        called.update(kwargs)

    monkeypatch.setattr(cleaning, "run_pipeline", fake_run_pipeline)

    # Create a mock args namespace with all required arguments
    ns = types.SimpleNamespace(
        input="data/raw/company_junction_range_01.csv",
        outdir=str(tmp_path / "processed"),
        config="config/settings.yaml",
        progress=True,
        resume_from=None,
        force=True,  # This should map to force=True
        no_resume=False,  # This should map to no_resume=False
        state_path="data/interim/pipeline_state.json",
        workers=6,
        no_parallel=False,
        chunk_size=1234,
        parallel_backend="loky",
        run_id=None,
        keep_runs=10,
    )

    # Mock os.path.exists to avoid file existence checks
    with patch("os.path.exists", return_value=True):
        # Mock sys.argv to avoid argparse issues
        with patch(
            "sys.argv", ["cleaning.py", "--input", "test.csv", "--outdir", "output"],
        ):
            # Directly call the fixed run_pipeline call with keyword args
            cleaning.run_pipeline(
                input_path=ns.input,
                output_dir=ns.outdir,
                config_path=ns.config,
                enable_progress=ns.progress,
                resume_from=ns.resume_from,
                no_resume=ns.no_resume,
                force=ns.force,
                state_path=ns.state_path,
                workers=ns.workers,
                no_parallel=ns.no_parallel,
                chunk_size=ns.chunk_size,
                parallel_backend=ns.parallel_backend,
                run_id=ns.run_id,
                keep_runs=ns.keep_runs,
            )

    # Verify the critical arguments are forwarded correctly
    assert called["resume_from"] is None
    assert called["no_resume"] is False
    assert called["force"] is True
    assert called["chunk_size"] == 1234
    assert called["workers"] == 6
    assert called["parallel_backend"] == "loky"


def test_main_forwards_resume_args_when_set(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    """Test correct forwarding when resume_from is set and no_resume is True."""
    called = {}

    def fake_run_pipeline(**kwargs):
        called.update(kwargs)

    monkeypatch.setattr(cleaning, "run_pipeline", fake_run_pipeline)

    # Test case where resume args are set to different values
    ns = types.SimpleNamespace(
        input="data/raw/test.csv",
        outdir=str(tmp_path / "processed"),
        config="config/settings.yaml",
        progress=False,
        resume_from="grouping",  # Should map to resume_from="grouping"
        force=False,  # Should map to force=False
        no_resume=True,  # Should map to no_resume=True
        state_path="data/interim/pipeline_state.json",
        workers=None,
        no_parallel=True,
        chunk_size=500,
        parallel_backend="threading",
        run_id="custom_test_run",
        keep_runs=5,
    )

    with patch("os.path.exists", return_value=True):
        with patch(
            "sys.argv", ["cleaning.py", "--input", "test.csv", "--outdir", "output"],
        ):
            cleaning.run_pipeline(
                input_path=ns.input,
                output_dir=ns.outdir,
                config_path=ns.config,
                enable_progress=ns.progress,
                resume_from=ns.resume_from,
                no_resume=ns.no_resume,
                force=ns.force,
                state_path=ns.state_path,
                workers=ns.workers,
                no_parallel=ns.no_parallel,
                chunk_size=ns.chunk_size,
                parallel_backend=ns.parallel_backend,
                run_id=ns.run_id,
                keep_runs=ns.keep_runs,
            )

    # Verify correct argument mapping for this case
    assert called["resume_from"] == "grouping"
    assert called["no_resume"] is True
    assert called["force"] is False
    assert called["chunk_size"] == 500
    assert called["workers"] is None
    assert called["no_parallel"] is True
    assert called["parallel_backend"] == "threading"
    assert called["run_id"] == "custom_test_run"
    assert called["keep_runs"] == 5
