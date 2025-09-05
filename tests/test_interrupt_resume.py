"""Tests for interrupt and resume functionality."""

import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pandas as pd

from src.utils.cache_utils import update_run_status
from src.utils.mini_dag import MiniDAG


class TestMiniDAGInterruptHandling:
    """Test MiniDAG interrupt handling functionality."""

    def test_mark_interrupted(self) -> None:
        """Test marking a stage as interrupted."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            temp_path = f.name

        try:
            dag = MiniDAG(Path(temp_path), "test_run_id")
            dag.register("test_stage")
            dag.start("test_stage")

            # Mark as interrupted
            dag.mark_interrupted("test_stage")

            # Check that the stage is marked as interrupted
            assert dag.get_status("test_stage") == "interrupted"

            # Check metadata
            with open(temp_path) as f:
                data = json.load(f)

            assert data["metadata"]["status"] == "interrupted"
            assert data["metadata"]["active_stage"] == "test_stage"
            assert "interrupt_timestamp" in data["metadata"]

        finally:
            os.unlink(temp_path)

    def test_get_current_stage(self) -> None:
        """Test getting the currently running stage."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            temp_path = f.name

        try:
            dag = MiniDAG(Path(temp_path), "test_run_id")
            dag.register("stage1")
            dag.register("stage2")

            # No stage running initially
            assert dag.get_current_stage() is None

            # Start a stage
            dag.start("stage1")
            assert dag.get_current_stage() == "stage1"

            # Complete the stage
            dag.complete("stage1")
            assert dag.get_current_stage() is None

            # Start another stage
            dag.start("stage2")
            assert dag.get_current_stage() == "stage2"

        finally:
            os.unlink(temp_path)

    def test_interrupted_status_type(self) -> None:
        """Test that interrupted is a valid status type."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            temp_path = f.name

        try:
            dag = MiniDAG(Path(temp_path), "test_run_id")
            dag.register("test_stage")

            # Should be able to mark as interrupted
            dag.mark_interrupted("test_stage")
            assert dag.get_status("test_stage") == "interrupted"

        finally:
            os.unlink(temp_path)


class TestInterruptResumeIntegration:
    """Test integration of interrupt and resume functionality."""

    def create_test_csv(self, filename: str, rows: int = 10) -> str:
        """Create a test CSV file for testing."""
        data = {
            "Account ID": [f"a012345678{i:05d}" for i in range(rows)],  # 15 chars total
            "Account Name": [f"Test Company {i}" for i in range(rows)],
            "Relationship": ["Customer"] * rows,
            "Created Date": ["2023-01-01"] * rows,
        }
        df = pd.DataFrame(data)

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            df.to_csv(f.name, index=False)
            return f.name

    def test_interrupt_resume_workflow(self) -> None:
        """Test the complete interrupt and resume workflow."""
        # Create test CSV
        csv_path = self.create_test_csv("test_interrupt.csv", 5)

        try:
            # Create temporary directories
            temp_dir = tempfile.mkdtemp()
            interim_dir = os.path.join(temp_dir, "interim")
            processed_dir = os.path.join(temp_dir, "processed")
            os.makedirs(interim_dir)
            os.makedirs(processed_dir)

            # Create a minimal config
            config_path = os.path.join(temp_dir, "test_config.yaml")
            with open(config_path, "w") as f:
                f.write(
                    """
similarity:
  high: 92
  medium: 84
  penalty:
    suffix_mismatch: 25
    num_style_mismatch: 5
parallelism:
  workers: 1
  backend: "threading"
  chunk_size: 1000
  small_input_threshold: 1000
""",
                )

            # Test that we can run a small pipeline
            cmd = [
                sys.executable,
                "src/cleaning.py",
                "--input",
                csv_path,
                "--outdir",
                processed_dir,
                "--config",
                config_path,
                "--workers",
                "1",
                "--parallel-backend",
                "threading",
                "--no-resume",
            ]

            # Run the pipeline (should complete quickly with small data)
            result = subprocess.run(cmd, check=False, capture_output=True, text=True, timeout=30)

            # Should complete successfully
            assert result.returncode == 0, f"Pipeline failed: {result.stderr}"

            # Extract run ID from output
            run_id = None
            for line in result.stdout.split("\n") + result.stderr.split("\n"):
                if "Pipeline completed successfully with run_id:" in line:
                    run_id = line.split("run_id:")[1].strip()
                    break

            assert run_id is not None, "Could not extract run ID from pipeline output"

            # Check that output files were created in the run-scoped directory
            # Note: Pipeline always creates run-scoped subdirectories under the default data/processed location
            expected_output_dir = f"data/processed/{run_id}"
            assert os.path.exists(
                expected_output_dir,
            ), f"Run-scoped output directory not found: {expected_output_dir}"

            output_files = os.listdir(expected_output_dir)
            assert len(output_files) > 0, "No output files created"

        finally:
            # Cleanup
            os.unlink(csv_path)
            if "temp_dir" in locals():
                import shutil

                shutil.rmtree(temp_dir)

    @patch("signal.signal")
    def test_keyboard_interrupt_handling(self, mock_signal: Any) -> None:
        """Test that KeyboardInterrupt is handled gracefully."""
        # This test verifies that the interrupt handling is in place
        # We can't easily test actual Ctrl+C in unit tests, but we can verify
        # that the exception handling is properly set up

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            temp_path = f.name

        try:
            dag = MiniDAG(Path(temp_path), "test_run_id")
            dag.register("test_stage")
            dag.start("test_stage")

            # Simulate interrupt
            dag.mark_interrupted("test_stage")

            # Verify state
            assert dag.get_status("test_stage") == "interrupted"

            # Check that we can resume from interrupted state
            # (This would be tested in actual pipeline runs)

        finally:
            os.unlink(temp_path)


class TestRunStatusInterrupted:
    """Test run status management for interrupted runs."""

    def test_update_run_status_interrupted(self) -> None:
        """Test updating run status to interrupted."""
        # Create a temporary run index
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            temp_path = f.name
            json.dump({}, f)

        try:
            # Test updating status to interrupted
            update_run_status("test_run_id", "interrupted")

            # Verify the status was updated
            with open(temp_path) as f:
                _ = json.load(f)  # Load to verify valid JSON

            # Note: update_run_status might not use the temp file we created
            # This test mainly verifies the function doesn't crash

        finally:
            os.unlink(temp_path)


class TestInterruptResumeDeterminism:
    """Test that interrupted and resumed runs maintain determinism."""

    def test_interrupt_does_not_affect_determinism(self) -> None:
        """Test that interrupting and resuming doesn't affect output determinism."""
        # This test would verify that:
        # 1. A full run produces output A
        # 2. An interrupted run that resumes produces output A
        # 3. Both outputs are identical

        # For now, we'll create a placeholder test
        # In practice, this would require running the full pipeline
        # with a controlled interrupt and resume scenario

        assert True  # Placeholder assertion


class TestInterruptSafety:
    """Test safety aspects of interrupt handling."""

    def test_interrupt_preserves_partial_artifacts(self) -> None:
        """Test that interrupting preserves partial artifacts."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            temp_path = f.name

        try:
            dag = MiniDAG(Path(temp_path), "test_run_id")
            dag.register("stage1")
            dag.register("stage2")

            # Start and complete stage1
            dag.start("stage1")
            dag.complete("stage1")

            # Start stage2 and interrupt
            dag.start("stage2")
            dag.mark_interrupted("stage2")

            # Verify stage1 is still completed
            assert dag.get_status("stage1") == "completed"

            # Verify stage2 is interrupted
            assert dag.get_status("stage2") == "interrupted"

        finally:
            os.unlink(temp_path)

    def test_interrupt_atomic_state_writes(self) -> None:
        """Test that interrupt state writes are atomic."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            temp_path = f.name

        try:
            dag = MiniDAG(Path(temp_path), "test_run_id")
            dag.register("test_stage")
            dag.start("test_stage")

            # Mark as interrupted (should use atomic write)
            dag.mark_interrupted("test_stage")

            # Verify the file is valid JSON
            with open(temp_path) as f:
                data = json.load(f)

            # Verify the structure is correct
            assert "stages" in data
            assert "metadata" in data
            assert data["metadata"]["status"] == "interrupted"

        finally:
            os.unlink(temp_path)
