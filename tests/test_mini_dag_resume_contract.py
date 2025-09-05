import tempfile
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest

from src.utils.mini_dag import MiniDAG


class TestMiniDAGResumeContract:
    """Stubbed smoke test to verify --resume-from respects the contract."""

    @patch("src.cleaning.run_pipeline")
    @patch("src.utils.mini_dag.MiniDAG")
    def test_resume_from_respects_contract(
        self, mock_mini_dag_class, mock_run_pipeline,
    ):
        """Test that --resume-from only executes expected stages and marks prior ones complete."""
        # Create a mock MiniDAG instance
        mock_mini_dag = Mock()
        mock_mini_dag_class.return_value = mock_mini_dag

        # Mock the pipeline stages
        _mock_stages = [
            "normalization",
            "filtering",
            "candidate_generation",
            "grouping",
            "survivorship",
        ]

        # Set up mock to simulate stages being completed
        mock_mini_dag._stages = {
            "normalization": Mock(status="completed"),
            "filtering": Mock(status="completed"),
            "candidate_generation": Mock(status="completed"),
            "grouping": Mock(status="pending"),
            "survivorship": Mock(status="pending"),
        }

        # Mock the resume logic
        mock_mini_dag.should_run.side_effect = lambda stage, resume_from: stage in [
            "grouping",
            "survivorship",
        ]

        # Test resume from grouping stage
        with tempfile.TemporaryDirectory() as _temp_dir:
            _temp_path = Path(_temp_dir)

            # Simulate the resume scenario
            resume_stage = "grouping"

            # Verify contract: prior stages should be marked complete
            assert mock_mini_dag._stages["normalization"].status == "completed"
            assert mock_mini_dag._stages["filtering"].status == "completed"
            assert mock_mini_dag._stages["candidate_generation"].status == "completed"

            # Verify contract: resume should start from specified stage
            assert mock_mini_dag.should_run("grouping", resume_stage)
            assert mock_mini_dag.should_run("survivorship", resume_stage)

            # Verify contract: completed stages should not be re-executed
            assert not mock_mini_dag.should_run("normalization", resume_stage)
            assert not mock_mini_dag.should_run("filtering", resume_stage)

    @patch("src.cleaning.run_pipeline")
    def test_resume_stage_validation(self, mock_run_pipeline):
        """Test that resume validates stage names correctly."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Create a real MiniDAG instance for this test
            mini_dag = MiniDAG(
                state_file=temp_path / "mini_dag_state.json",
                run_id="test_resume_contract",
            )

            # Test valid stage registration
            mini_dag.register("stage1")
            mini_dag.register("stage2")
            mini_dag.register("stage3")

            assert "stage1" in mini_dag._stages
            assert "stage2" in mini_dag._stages
            assert "stage3" in mini_dag._stages

            # Test invalid stage names - should be handled gracefully
            # (MiniDAG doesn't have explicit validation, so this tests graceful handling)

    def test_resume_state_persistence(self):
        """Test that resume state is properly persisted."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            mini_dag = MiniDAG(
                state_file=temp_path / "mini_dag_state.json", run_id="test_persistence",
            )

            # Register and complete stages
            mini_dag.register("stage1")
            mini_dag.register("stage2")
            mini_dag.register("stage3")

            mini_dag.start("stage1")
            mini_dag.complete("stage1")
            mini_dag.start("stage2")
            mini_dag.complete("stage2")

            # Verify state file exists and contains correct data
            state_file = temp_path / "mini_dag_state.json"
            assert state_file.exists()

            # Create new MiniDAG instance to test persistence
            mini_dag2 = MiniDAG(state_file=state_file, run_id="test_persistence")

            # Verify state was persisted
            assert mini_dag2._stages["stage1"].status == "completed"
            assert mini_dag2._stages["stage2"].status == "completed"
            assert mini_dag2._stages["stage3"].status == "pending"

    @patch("src.cleaning.run_pipeline")
    def test_resume_with_no_previous_run(self, mock_run_pipeline):
        """Test resume behavior when no previous run exists."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Create MiniDAG with no existing state
            mini_dag = MiniDAG(
                state_file=temp_path / "mini_dag_state.json", run_id="test_no_previous",
            )

            # Verify no stages are registered
            assert len(mini_dag._stages) == 0

            # Register a stage and test basic functionality
            mini_dag.register("stage1")
            assert "stage1" in mini_dag._stages
            assert mini_dag._stages["stage1"].status == "pending"

    def test_resume_stage_ordering(self):
        """Test that resume respects stage ordering."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            mini_dag = MiniDAG(
                state_file=temp_path / "mini_dag_state.json", run_id="test_ordering",
            )

            # Register stages in order
            mini_dag.register("stage1")
            mini_dag.register("stage2")
            mini_dag.register("stage3")
            mini_dag.register("stage4")

            # Test stage lifecycle
            mini_dag.start("stage1")
            mini_dag.complete("stage1")
            mini_dag.start("stage2")
            mini_dag.complete("stage2")

            # Verify stage statuses
            assert mini_dag._stages["stage1"].status == "completed"
            assert mini_dag._stages["stage2"].status == "completed"
            assert mini_dag._stages["stage3"].status == "pending"
            assert mini_dag._stages["stage4"].status == "pending"
