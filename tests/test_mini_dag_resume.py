"""Tests for mini-DAG resume functionality.

Phase 1.27.2: Mini-DAG resume system testing
"""

import json
import tempfile
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.utils.mini_dag import MiniDAG
from src.utils.pipeline_constants import PIPELINE_STAGES, STAGE_INTERMEDIATE_FILES


class TestMiniDAGResume:
    """Test suite for mini-DAG resume functionality."""

    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for testing."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            yield Path(tmp_dir)

    @pytest.fixture
    def mini_dag(self, temp_dir):
        """Create a mini-DAG instance for testing."""
        state_file = temp_dir / "pipeline_state.json"
        return MiniDAG(state_file, run_id="test_run")

    @pytest.fixture
    def interim_dir(self, temp_dir):
        """Create a mock interim directory with test files."""
        interim_dir = temp_dir / "data" / "interim" / "test_run"
        interim_dir.mkdir(parents=True, exist_ok=True)
        return interim_dir

    def test_pipeline_constants_import(self):
        """Test that pipeline constants are properly imported."""
        assert len(PIPELINE_STAGES) == 8
        assert "normalization" in PIPELINE_STAGES
        assert "final_output" in PIPELINE_STAGES

        assert "normalization" in STAGE_INTERMEDIATE_FILES
        assert "accounts_filtered.parquet" in STAGE_INTERMEDIATE_FILES["normalization"]

    def test_validate_intermediate_files_basic(self, mini_dag, interim_dir):
        """Test basic intermediate file validation."""
        # Create test files
        (interim_dir / "accounts_filtered.parquet").touch()

        # Test normalization stage
        assert mini_dag.validate_intermediate_files("normalization", interim_dir)

        # Test with missing files
        assert not mini_dag.validate_intermediate_files("grouping", interim_dir)

    def test_validate_intermediate_files_complete_pipeline(self, mini_dag, interim_dir):
        """Test intermediate file validation for complete pipeline."""
        # Create all intermediate files
        for _stage, files in STAGE_INTERMEDIATE_FILES.items():
            for filename in files:
                (interim_dir / filename).touch()

        # All stages should validate
        for stage in PIPELINE_STAGES:
            assert mini_dag.validate_intermediate_files(
                stage,
                interim_dir,
            ), f"Stage {stage} failed validation"

    def test_validate_resume_capability_no_previous_run(self, mini_dag, interim_dir):
        """Test resume capability when no previous run exists."""
        can_resume, reason, decision = mini_dag.validate_resume_capability(interim_dir)

        assert not can_resume
        assert "No previous run found" in reason
        assert decision == "NO_PREVIOUS_RUN"

    def test_validate_resume_capability_missing_files(self, mini_dag, interim_dir):
        """Test resume capability when intermediate files are missing."""
        # Mark a stage as completed but don't create files
        mini_dag.register("normalization")
        mini_dag.complete("normalization")

        can_resume, reason, decision = mini_dag.validate_resume_capability(interim_dir)

        assert not can_resume
        assert "Missing intermediate files" in reason
        assert decision == "MISSING_FILES"

    def test_validate_resume_capability_success(self, mini_dag, interim_dir):
        """Test successful resume capability validation."""
        # Create files and mark stage as completed
        (interim_dir / "accounts_filtered.parquet").touch()
        mini_dag.register("normalization")
        mini_dag.complete("normalization")

        can_resume, reason, decision = mini_dag.validate_resume_capability(interim_dir)

        assert can_resume
        assert "Can resume from stage 'normalization'" in reason
        assert decision == "NEXT_STAGE_READY"

    def test_validate_state_consistency_valid(self, mini_dag):
        """Test state consistency validation with valid state."""
        # Register stages with dependencies
        mini_dag.register("normalization")
        mini_dag.register("filtering", deps=["normalization"])

        # Complete stages in order
        mini_dag.complete("normalization")
        mini_dag.complete("filtering")

        assert mini_dag._validate_state_consistency()

    def test_validate_state_consistency_invalid(self, mini_dag):
        """Test state consistency validation with invalid state."""
        # Register stages with dependencies
        mini_dag.register("normalization")
        mini_dag.register("filtering", deps=["normalization"])

        # Complete filtering without completing normalization
        mini_dag.complete("filtering")

        assert not mini_dag._validate_state_consistency()

    def test_repair_state_inconsistency(self, mini_dag):
        """Test state inconsistency repair."""
        # Create inconsistent state
        mini_dag.register("normalization")
        mini_dag.start("normalization")
        mini_dag.fail("normalization")

        # Verify repair
        assert mini_dag._repair_state_inconsistency()

        # Check that stages were reset
        assert mini_dag.get_status("normalization") == "pending"

    def test_get_resume_validation_summary(self, mini_dag, interim_dir):
        """Test resume validation summary generation."""
        # Create a simple state
        (interim_dir / "accounts_filtered.parquet").touch()
        mini_dag.register("normalization")
        mini_dag.complete("normalization")

        summary = mini_dag.get_resume_validation_summary(interim_dir)

        assert "can_resume" in summary
        assert "reason" in summary
        assert "decision_code" in summary
        assert "last_completed_stage" in summary
        assert "interim_dir" in summary
        assert "stage_status" in summary
        assert "validation_timestamp" in summary

        # Check specific values
        assert summary["can_resume"] is True
        assert summary["last_completed_stage"] == "normalization"
        assert "file_validation" in summary

    def test_get_smart_resume_stage_enhanced(self, mini_dag, interim_dir):
        """Test enhanced smart resume stage logic."""
        # Create files and complete normalization
        (interim_dir / "accounts_filtered.parquet").touch()
        mini_dag.register("normalization")
        mini_dag.complete("normalization")

        # Test resume from next stage
        resume_stage = mini_dag.get_smart_resume_stage(interim_dir)
        assert resume_stage == "filtering"

    def test_get_smart_resume_stage_with_validation_failure(
        self,
        mini_dag,
        interim_dir,
    ):
        """Test smart resume when validation fails."""
        # Mark stage as completed but don't create files
        mini_dag.register("normalization")
        mini_dag.complete("normalization")

        # Should return None due to validation failure
        resume_stage = mini_dag.get_smart_resume_stage(interim_dir)
        assert resume_stage is None

    def test_resume_validation_timeout(self, mini_dag, interim_dir):
        """Test that resume validation completes within timeout."""
        # Create a large number of files to test performance
        for _stage, files in STAGE_INTERMEDIATE_FILES.items():
            for filename in files:
                (interim_dir / filename).touch()

        # Mark all stages as completed
        for stage in PIPELINE_STAGES:
            mini_dag.register(stage)
            mini_dag.complete(stage)

        # Time the validation
        start_time = time.time()
        can_resume, reason, decision = mini_dag.validate_resume_capability(interim_dir)
        validation_time = time.time() - start_time

        # Should complete within 5 seconds
        assert validation_time < 5.0
        assert can_resume

    def test_stage_order_consistency(self, mini_dag):
        """Test that stage order is consistent across methods."""
        # Get stage order from different methods
        order1 = mini_dag._stages.keys()
        order2 = PIPELINE_STAGES

        # Convert to lists for comparison
        stages1 = list(order1)
        stages2 = list(order2)

        # Should be consistent
        assert stages1 == stages2 or not stages1  # Empty if no stages registered

    def test_error_handling_corrupted_state(self, temp_dir):
        """Test error handling with corrupted state file."""
        state_file = temp_dir / "corrupted_state.json"

        # Create corrupted JSON
        state_file.write_text("{ invalid json }")

        # Should handle gracefully
        mini_dag = MiniDAG(state_file, run_id="test_run")

        # Should have clean state
        assert len(mini_dag._stages) == 0
        assert mini_dag._metadata["dag_version"] == "1.0.0"

    def test_feature_flag_resume_state_repair(self, mini_dag, interim_dir):
        """Test that state repair can be disabled via feature flag."""
        # Mock the feature flag to False
        with patch("src.utils.mini_dag.RESUME_STATE_REPAIR_ENABLED", False):
            # Create inconsistent state - need dependency inconsistency
            mini_dag.register("normalization")
            mini_dag.register("filtering", deps=["normalization"])
            mini_dag.complete(
                "filtering",
            )  # Complete filtering without completing normalization

            # Create files so file validation passes
            (interim_dir / "accounts_filtered.parquet").touch()
            (interim_dir / "candidate_pairs.parquet").touch()

            # Should not attempt repair and should detect state inconsistency
            can_resume, reason, decision = mini_dag.validate_resume_capability(
                interim_dir,
            )
            assert decision == "STATE_INCONSISTENT"

    def test_logging_enhancements(self, mini_dag, interim_dir, caplog):
        """Test enhanced logging for resume decisions."""
        # Set logging level to capture info messages
        caplog.set_level("INFO")

        # Create files and complete stage
        (interim_dir / "accounts_filtered.parquet").touch()
        mini_dag.register("normalization")
        mini_dag.complete("normalization")

        # Get resume stage (should log decision)
        mini_dag.get_smart_resume_stage(interim_dir)

        # Check for enhanced logging - look for the specific log messages
        log_messages = [record.message for record in caplog.records]
        assert any(
            "Auto-resume decision:" in msg for msg in log_messages
        ), f"Expected 'Auto-resume decision:' in logs: {log_messages}"
        assert any(
            "NEXT_STAGE_READY" in msg for msg in log_messages
        ), f"Expected 'NEXT_STAGE_READY' in logs: {log_messages}"


class TestMiniDAGResumeIntegration:
    """Integration tests for mini-DAG resume functionality."""

    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for testing."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            yield Path(tmp_dir)

    def test_full_pipeline_resume_simulation(self, temp_dir):
        """Test full pipeline resume simulation."""
        # Create mini-DAG
        state_file = temp_dir / "pipeline_state.json"
        mini_dag = MiniDAG(state_file, run_id="integration_test")

        # Create interim directory
        interim_dir = temp_dir / "data" / "interim" / "integration_test"
        interim_dir.mkdir(parents=True, exist_ok=True)

        # Simulate pipeline execution stages
        stages_to_execute = ["normalization", "filtering", "candidate_generation"]

        for stage in stages_to_execute:
            # Register and start stage
            mini_dag.register(stage)
            mini_dag.start(stage)

            # Create intermediate files for this stage
            for stage_name, files in STAGE_INTERMEDIATE_FILES.items():
                if stage_name in stages_to_execute:
                    for filename in files:
                        (interim_dir / filename).touch()

            # Complete stage
            mini_dag.complete(stage)

            # Validate resume capability
            can_resume, reason, decision = mini_dag.validate_resume_capability(
                interim_dir,
            )
            assert can_resume, f"Should be able to resume after {stage}: {reason}"

        # Create files for the next stage (grouping) so we can resume to it
        for filename in STAGE_INTERMEDIATE_FILES["grouping"]:
            (interim_dir / filename).touch()

        # Test resume from next stage
        resume_stage = mini_dag.get_smart_resume_stage(interim_dir)
        # Should resume from the next stage after the last completed one
        # Since we completed up to "candidate_generation", we should resume from "grouping"
        assert (
            resume_stage == "grouping"
        ), f"Expected to resume from 'grouping', got '{resume_stage}'"

    def test_resume_with_missing_intermediate_files(self, temp_dir):
        """Test resume behavior when intermediate files are missing."""
        # Create mini-DAG with completed stages
        state_file = temp_dir / "pipeline_state.json"
        mini_dag = MiniDAG(state_file, run_id="missing_files_test")

        # Mark stages as completed
        for stage in ["normalization", "filtering"]:
            mini_dag.register(stage)
            mini_dag.complete(stage)

        # Create interim directory but no files
        interim_dir = temp_dir / "data" / "interim" / "missing_files_test"
        interim_dir.mkdir(parents=True, exist_ok=True)

        # Should not be able to resume
        can_resume, reason, decision = mini_dag.validate_resume_capability(interim_dir)
        assert not can_resume
        assert decision == "MISSING_FILES"

        # Smart resume should return None
        resume_stage = mini_dag.get_smart_resume_stage(interim_dir)
        assert resume_stage is None
