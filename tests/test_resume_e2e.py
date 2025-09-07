"""End-to-end tests for pipeline resume functionality."""

import json
import logging
import os
import re
import tempfile
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from src.cleaning import run_pipeline

# Ensure pyarrow is available for parquet tests
pytest.importorskip("pyarrow", reason="pyarrow required for parquet IO in tests")


class TestResumeE2E:
    """End-to-end tests for pipeline resume functionality."""

    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for testing."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            yield Path(tmp_dir)

    @pytest.fixture
    def sample_data(self, temp_dir):
        """Create sample data for testing."""
        # Create a small CSV file for testing with valid Salesforce IDs
        data = {
            "account_id": [
                "001Hs0000000001",  # 15-char Salesforce ID
                "001Hs0000000002", 
                "001Hs0000000003", 
                "001Hs0000000004", 
                "001Hs0000000005"
            ],
            "account_name": [
                "Acme Corporation",
                "Acme Corp", 
                "Beta Industries",
                "Microsoft Inc",
                "Microsoft Corporation"
            ]
        }
        df = pd.DataFrame(data)
        input_file = temp_dir / "test_input.csv"
        df.to_csv(input_file, index=False)
        return input_file

    @pytest.fixture
    def config_file(self, temp_dir):
        """Create a minimal config file for testing."""
        config = {
            "similarity": {
                "high": 85,
                "medium": 70,
                "low": 50
            },
            "parallelism": {
                "workers": 2,
                "backend": "threading",
                "chunk_size": 100
            }
        }
        config_file = temp_dir / "test_config.yaml"
        import yaml
        with open(config_file, 'w') as f:
            yaml.dump(config, f)
        return config_file

    def test_resume_e2e_full_pipeline(self, sample_data, config_file, temp_dir):
        """Test that resume functionality works end-to-end."""
        output_dir = temp_dir / "output"
        output_dir.mkdir()
        
        # Run pipeline to completion
        run_pipeline(
            input_path=str(sample_data),
            output_dir=str(output_dir),
            config_path=str(config_file),
            run_id="test_resume_e2e"
        )
        
        # Verify output files exist
        assert (output_dir / "test_resume_e2e").exists()
        assert (output_dir / "test_resume_e2e" / "review_ready.parquet").exists()
        assert (output_dir / "test_resume_e2e" / "group_stats.parquet").exists()

    def test_auto_resume_detection(self, sample_data, config_file, temp_dir, caplog):
        """Test that auto-resume detection works correctly."""
        caplog.set_level(logging.INFO, logger=None)
        
        output_dir = temp_dir / "output"
        output_dir.mkdir()
        
        # Run pipeline to completion first
        run_pipeline(
            input_path=str(sample_data),
            output_dir=str(output_dir),
            config_path=str(config_file),
            run_id="test_auto_resume"
        )
        
        # Clear logs
        caplog.clear()
        
        # Run again - should auto-resume
        run_pipeline(
            input_path=str(sample_data),
            output_dir=str(output_dir),
            config_path=str(config_file),
            run_id="test_auto_resume"
        )
        
        # Check for auto-resume log patterns
        log_text = caplog.text
        assert "Auto-resume decision:" in log_text
        assert "reason=SMART_DETECT" in log_text or "reason=NO_PREVIOUS_RUN" in log_text

    def test_resume_log_verification(self, sample_data, config_file, temp_dir, caplog):
        """Test that resume logs contain exact expected patterns."""
        caplog.set_level(logging.INFO, logger=None)
        
        output_dir = temp_dir / "output"
        output_dir.mkdir()
        
        # Run pipeline to completion first
        run_pipeline(
            input_path=str(sample_data),
            output_dir=str(output_dir),
            config_path=str(config_file),
            run_id="test_log_verification"
        )
        
        # Clear logs
        caplog.clear()
        
        # Run with manual resume
        run_pipeline(
            input_path=str(sample_data),
            output_dir=str(output_dir),
            config_path=str(config_file),
            run_id="test_log_verification",
            resume_from="grouping"
        )
        
        # Verify exact log patterns
        log_text = caplog.text
        
        # Check for manual resume pattern
        manual_resume_pattern = r"Auto-resume decision: resume_from='grouping' \| reason=MANUAL_SPECIFIED"
        assert re.search(manual_resume_pattern, log_text), f"Manual resume pattern not found in logs: {log_text}"
        
        # Check for stage skipping patterns
        stage_skip_pattern = r"Stage '(\w+)' already completed - skipping"
        stage_skips = re.findall(stage_skip_pattern, log_text)
        assert len(stage_skips) > 0, f"No stage skip patterns found in logs: {log_text}"

    def test_artifact_reuse_verification(self, sample_data, config_file, temp_dir):
        """Test that artifacts are reused, not regenerated."""
        output_dir = temp_dir / "output"
        output_dir.mkdir()
        
        run_id = "test_artifact_reuse"
        
        # Run pipeline to completion
        run_pipeline(
            input_path=str(sample_data),
            output_dir=str(output_dir),
            config_path=str(config_file),
            run_id=run_id
        )
        
        # Get artifact paths and their content hashes
        run_dir = output_dir / run_id
        artifacts = {
            "candidate_pairs.parquet": run_dir / "candidate_pairs.parquet",
            "groups.parquet": run_dir / "groups.parquet",
            "survivorship.parquet": run_dir / "survivorship.parquet",
            # Note: pipeline_state.json is excluded because it's a state tracking file
            # that should be updated when resuming, not reused
        }
        
        # Calculate original hashes
        original_hashes = {}
        for name, path in artifacts.items():
            if path.exists():
                original_hashes[name] = self._get_file_content_hash(path)
        
        # Run again with resume
        run_pipeline(
            input_path=str(sample_data),
            output_dir=str(output_dir),
            config_path=str(config_file),
            run_id=run_id,
            resume_from="grouping"
        )
        
        # Verify artifacts were reused (same content hash)
        for name, path in artifacts.items():
            if name in original_hashes and path.exists():
                new_hash = self._get_file_content_hash(path)
                assert original_hashes[name] == new_hash, f"Artifact {name} was regenerated instead of reused"

    def test_no_resume_flag_behavior(self, sample_data, config_file, temp_dir, caplog):
        """Test that --no-resume flag forces full run."""
        caplog.set_level(logging.INFO, logger=None)
        
        output_dir = temp_dir / "output"
        output_dir.mkdir()
        
        # Run pipeline to completion first
        run_pipeline(
            input_path=str(sample_data),
            output_dir=str(output_dir),
            config_path=str(config_file),
            run_id="test_no_resume"
        )
        
        # Clear logs
        caplog.clear()
        
        # Run with --no-resume flag
        run_pipeline(
            input_path=str(sample_data),
            output_dir=str(output_dir),
            config_path=str(config_file),
            run_id="test_no_resume",
            no_resume=True
        )
        
        # Check for no-resume log pattern
        log_text = caplog.text
        no_resume_pattern = r"Auto-resume decision: --no-resume specified - forcing full run \| reason=NO_RESUME_FLAG"
        assert re.search(no_resume_pattern, log_text), f"No-resume pattern not found in logs: {log_text}"

    def test_input_hash_mismatch_behavior(self, sample_data, config_file, temp_dir, caplog):
        """Test that input hash mismatch forces full run."""
        caplog.set_level(logging.INFO, logger=None)
        
        output_dir = temp_dir / "output"
        output_dir.mkdir()
        
        # Run pipeline to completion first
        run_pipeline(
            input_path=str(sample_data),
            output_dir=str(output_dir),
            config_path=str(config_file),
            run_id="test_hash_mismatch"
        )
        
        # Modify input file to change hash
        df = pd.read_csv(sample_data)
        df.loc[0, 'account_name'] = 'Modified Name'
        df.to_csv(sample_data, index=False)
        
        # Clear logs
        caplog.clear()
        
        # Run again - should detect hash mismatch
        run_pipeline(
            input_path=str(sample_data),
            output_dir=str(output_dir),
            config_path=str(config_file),
            run_id="test_hash_mismatch"
        )
        
        # Check that the pipeline ran from the beginning (not resumed)
        # This indicates that either hash mismatch was detected or no previous run was found
        log_text = caplog.text
        
        # The pipeline should have run all stages from the beginning
        # Look for evidence that it started fresh (not resumed)
        fresh_run_indicators = [
            "[stage:start] normalization",
            "[stage:start] filtering", 
            "[stage:start] exact_equals",
            "[stage:start] candidate_generation"
        ]
        
        for indicator in fresh_run_indicators:
            assert indicator in log_text, f"Expected fresh run indicator '{indicator}' not found in logs"
        
        # The pipeline should NOT have resumed from any stage
        resume_indicators = [
            "Stage 'normalization' already completed - skipping",
            "Stage 'filtering' already completed - skipping",
            "Stage 'exact_equals' already completed - skipping"
        ]
        
        for indicator in resume_indicators:
            assert indicator not in log_text, f"Unexpected resume indicator '{indicator}' found in logs - pipeline should have run fresh"

    def _get_file_content_hash(self, file_path: Path) -> str:
        """Get content hash of a file."""
        import hashlib
        with open(file_path, 'rb') as f:
            return hashlib.md5(f.read()).hexdigest()
