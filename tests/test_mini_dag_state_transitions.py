import json
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from src.utils.mini_dag import MiniDAG


class TestMiniDAGStateTransitions:
    """Test MiniDAG state transitions with temp state files (no heavy data)."""

    def test_state_transitions_with_temp_file(self):
        """Test that MiniDAG correctly transitions through states."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Create a MiniDAG instance with correct parameters
            mini_dag = MiniDAG(
                state_file=temp_path / "mini_dag_state.json", run_id="test_run_123",
            )

            # Register stages
            mini_dag.register("stage1")
            mini_dag.register("stage2")
            mini_dag.register("stage3")

            # Test initial state - should_run returns True when resume_from is None
            assert mini_dag.should_run("stage1", None)
            assert mini_dag.should_run("stage2", None)
            assert mini_dag.should_run("stage3", None)

            # Start and complete stage1
            mini_dag.start("stage1")
            mini_dag.complete("stage1")

            # Start and complete stage2
            mini_dag.start("stage2")
            mini_dag.complete("stage2")

            # Verify stage3 not started
            assert mini_dag._stages["stage3"].status == "pending"

            # Test state persistence
            state_file = temp_path / "mini_dag_state.json"
            assert state_file.exists()

            # Verify state file contents
            with open(state_file) as f:
                state_data = json.load(f)
                assert state_data["metadata"]["run_id"] == "test_run_123"
                assert "stages" in state_data

    def test_resume_from_specific_stage(self):
        """Test that resume logic correctly identifies starting point."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            mini_dag = MiniDAG(
                state_file=temp_path / "mini_dag_state.json", run_id="test_run_456",
            )

            # Register stages
            mini_dag.register("stage1")
            mini_dag.register("stage2")
            mini_dag.register("stage3")
            mini_dag.register("stage4")

            # Complete first two stages
            mini_dag.start("stage1")
            mini_dag.complete("stage1")
            mini_dag.start("stage2")
            mini_dag.complete("stage2")

            # Test resume logic - should_run returns True for resume scenarios
            assert mini_dag.should_run("stage3", "stage2")
            assert mini_dag.should_run("stage4", "stage2")

    def test_state_file_corruption_handling(self):
        """Test that MiniDAG handles corrupted state files gracefully."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            state_file = temp_path / "mini_dag_state.json"

            # Create corrupted state file
            with open(state_file, "w") as f:
                f.write("invalid json content")

            # MiniDAG should handle this gracefully
            mini_dag = MiniDAG(state_file=state_file, run_id="test_run_789")

            # Should start with empty state
            assert len(mini_dag._stages) == 0

    def test_stage_validation(self):
        """Test that MiniDAG validates stage names correctly."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            mini_dag = MiniDAG(
                state_file=temp_path / "mini_dag_state.json", run_id="test_run_999",
            )

            # Valid stage registration should work
            mini_dag.register("valid_stage1")
            mini_dag.register("valid_stage2")

            assert "valid_stage1" in mini_dag._stages
            assert "valid_stage2" in mini_dag._stages

            # Test stage lifecycle
            mini_dag.start("valid_stage1")
            assert mini_dag._stages["valid_stage1"].status == "running"

            mini_dag.complete("valid_stage1")
            assert mini_dag._stages["valid_stage1"].status == "completed"  # type: ignore[comparison-overlap]
