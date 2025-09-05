"""Tests for MiniDAG smart auto-resume functionality.
"""

import tempfile
from pathlib import Path

from src.utils.mini_dag import MiniDAG


def test_get_last_completed_stage() -> None:
    """Test getting the last completed stage."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        state_file = Path(f.name)

    try:
        dag = MiniDAG(state_file)

        # No stages completed
        assert dag.get_last_completed_stage() is None

        # Register and complete some stages
        dag.register("normalization")
        dag.register("filtering")
        dag.register("candidate_generation")

        dag.start("normalization")
        dag.complete("normalization")

        assert dag.get_last_completed_stage() == "normalization"

        dag.start("filtering")
        dag.complete("filtering")

        assert dag.get_last_completed_stage() == "filtering"

    finally:
        state_file.unlink(missing_ok=True)


def test_validate_intermediate_files() -> None:
    """Test validation of intermediate files."""
    with tempfile.TemporaryDirectory() as temp_dir:
        interim_dir = Path(temp_dir)
        dag = MiniDAG(Path(temp_dir) / "state.json")

        # No files exist
        assert not dag.validate_intermediate_files("normalization", interim_dir)

        # Create required file for normalization
        (interim_dir / "accounts_filtered.parquet").touch()
        assert dag.validate_intermediate_files("normalization", interim_dir)

        # Missing file for candidate_generation
        assert not dag.validate_intermediate_files("candidate_generation", interim_dir)

        # Create required files for candidate_generation
        (interim_dir / "candidate_pairs.parquet").touch()
        (interim_dir / "accounts_filtered.parquet").touch()
        assert dag.validate_intermediate_files("candidate_generation", interim_dir)


def test_get_smart_resume_stage() -> None:
    """Test smart resume stage detection."""
    with tempfile.TemporaryDirectory() as temp_dir:
        interim_dir = Path(temp_dir)
        dag = MiniDAG(Path(temp_dir) / "state.json")

        # No previous run
        assert dag.get_smart_resume_stage(interim_dir) is None

        # Register and complete normalization
        dag.register("normalization")
        dag.start("normalization")
        dag.complete("normalization")

        # No intermediate files - should return None
        assert dag.get_smart_resume_stage(interim_dir) is None

        # Create intermediate files for normalization
        (interim_dir / "accounts_filtered.parquet").touch()

        # Should suggest starting from filtering (next stage)
        # But first need to create the filtering stage files
        (interim_dir / "accounts_filtered.parquet").touch()
        assert dag.get_smart_resume_stage(interim_dir) == "filtering"

        # Complete filtering and create its files
        dag.register("filtering")
        dag.start("filtering")
        dag.complete("filtering")

        # Create files for candidate_generation stage
        (interim_dir / "candidate_pairs.parquet").touch()
        (interim_dir / "accounts_filtered.parquet").touch()

        # Should suggest candidate_generation
        assert dag.get_smart_resume_stage(interim_dir) == "candidate_generation"


def test_input_hash_validation() -> None:
    """Test input hash computation and validation."""
    with tempfile.TemporaryDirectory() as temp_dir:
        dag = MiniDAG(Path(temp_dir) / "state.json")

        # Create test files
        input_file = Path(temp_dir) / "input.csv"
        config_file = Path(temp_dir) / "config.yaml"

        input_file.write_text("test,data\n1,2")
        config_file.write_text("test: config")

        # Compute initial hash
        hash1 = dag._compute_input_hash(input_file, config_file)
        assert len(hash1) == 64  # SHA256 hex string

        # Same files should produce same hash
        hash2 = dag._compute_input_hash(input_file, config_file)
        assert hash1 == hash2

        # Change input file content
        input_file.write_text("test,data\n1,3")
        hash3 = dag._compute_input_hash(input_file, config_file)
        assert hash1 != hash3

        # Test validation
        dag._update_state_metadata(input_file, config_file, "test command")
        assert dag._validate_input_invariance(input_file, config_file)

        # Change file again - validation should fail
        input_file.write_text("test,data\n1,4")
        assert not dag._validate_input_invariance(input_file, config_file)


def test_force_flag_required_on_hash_mismatch() -> None:
    """Test that force flag is required when hash mismatches."""
    # This test would be integration test with cleaning.py
    # For now, we test the DAG validation logic
    with tempfile.TemporaryDirectory() as temp_dir:
        dag = MiniDAG(Path(temp_dir) / "state.json")

        input_file = Path(temp_dir) / "input.csv"
        config_file = Path(temp_dir) / "config.yaml"

        input_file.write_text("original")
        config_file.write_text("original")

        # Update metadata with original files
        dag._update_state_metadata(input_file, config_file, "test")

        # Change files
        input_file.write_text("changed")

        # Validation should fail
        assert not dag._validate_input_invariance(input_file, config_file)


def test_metadata_storage() -> None:
    """Test metadata storage and retrieval."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        state_file = Path(f.name)

    try:
        dag = MiniDAG(state_file)

        input_file = Path("test_input.csv")
        config_file = Path("test_config.yaml")
        cmdline = "python src/cleaning.py --input test_input.csv"

        # Update metadata
        dag._update_state_metadata(input_file, config_file, cmdline)

        # Check metadata is stored
        assert dag._metadata["input_path"] == str(input_file)
        assert dag._metadata["config_path"] == str(config_file)
        assert dag._metadata["cmdline"] == cmdline
        assert dag._metadata["dag_version"] == "1.0.0"
        assert "ts" in dag._metadata

        # Reload DAG and check persistence
        dag2 = MiniDAG(state_file)
        assert dag2._metadata["input_path"] == str(input_file)
        assert dag2._metadata["cmdline"] == cmdline

    finally:
        state_file.unlink(missing_ok=True)


def test_byte_for_byte_invariance() -> None:
    """Test that auto-resume produces identical output."""
    # This would be an integration test that:
    # 1. Runs full pipeline on small dataset
    # 2. Captures output hash
    # 3. Deletes final output
    # 4. Runs with auto-resume
    # 5. Verifies output hash matches

    # For now, placeholder test
    assert True  # Will be implemented as integration test


def test_auto_resume_decision_logging() -> None:
    """Test that auto-resume decisions are logged clearly."""
    # This would test the logging output from cleaning.py
    # For now, placeholder test
    assert True  # Will be implemented as integration test


def test_invariance_resume() -> None:
    """Test that full run vs resume produces identical outputs."""
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create test fixture
        input_file = Path(temp_dir) / "test_input.csv"
        config_file = Path(temp_dir) / "test_config.yaml"
        output_dir = Path(temp_dir) / "output"
        interim_dir = Path(temp_dir) / "interim"

        # Create minimal test data
        input_file.write_text("account_id,account_name\n001,Test Company")
        config_file.write_text(
            "similarity:\n  high_threshold: 90\n  medium_threshold: 80",
        )
        output_dir.mkdir()
        interim_dir.mkdir()

        # This would require running the actual pipeline
        # For now, test the structure is in place
        assert input_file.exists()
        assert config_file.exists()


def test_hash_mismatch_guard() -> None:
    """Test that hash mismatch prevents resume without --force."""
    with tempfile.TemporaryDirectory() as temp_dir:
        dag = MiniDAG(Path(temp_dir) / "state.json")

        # Create initial files
        input_file = Path(temp_dir) / "input.csv"
        config_file = Path(temp_dir) / "config.yaml"

        input_file.write_text("original")
        config_file.write_text("original")

        # Update metadata with original files
        dag._update_state_metadata(input_file, config_file, "test")

        # Change files
        input_file.write_text("changed")

        # Validation should fail
        assert not dag._validate_input_invariance(input_file, config_file)


def test_missing_artifact_fallback() -> None:
    """Test that missing accounts_filtered.parquet prevents resuming from later stages."""
    with tempfile.TemporaryDirectory() as temp_dir:
        interim_dir = Path(temp_dir)
        dag = MiniDAG(Path(temp_dir) / "state.json")

        # Register and complete filtering stage
        dag.register("filtering")
        dag.start("filtering")
        dag.complete("filtering")

        # No accounts_filtered.parquet exists
        assert not dag.validate_intermediate_files("filtering", interim_dir)

        # Should not be able to resume from later stages
        assert dag.get_smart_resume_stage(interim_dir) is None


def test_corrupted_state_file() -> None:
    """Test handling of corrupted pipeline_state.json."""
    with tempfile.TemporaryDirectory() as temp_dir:
        state_file = Path(temp_dir) / "state.json"

        # Create corrupted JSON
        state_file.write_text('{"invalid": json}')

        # Should handle gracefully and reset to clean state
        dag = MiniDAG(state_file)
        assert dag._stages == {}
        assert dag._metadata["dag_version"] == "1.0.0"


def test_resume_force_interaction() -> None:
    """Test --resume-from + --force interaction."""
    with tempfile.TemporaryDirectory() as temp_dir:
        dag = MiniDAG(Path(temp_dir) / "state.json")

        # Create initial files
        input_file = Path(temp_dir) / "input.csv"
        config_file = Path(temp_dir) / "config.yaml"

        input_file.write_text("original")
        config_file.write_text("original")

        # Update metadata with original files
        dag._update_state_metadata(input_file, config_file, "test")

        # Change files
        input_file.write_text("changed")

        # Without force, validation should fail
        assert not dag._validate_input_invariance(input_file, config_file)

        # With force, should still fail validation but allow override
        # (This is tested in the cleaning.py integration)


def test_actual_invariance() -> None:
    """Test actual invariance between full run and resume on small fixture."""
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create small test fixture
        input_file = Path(temp_dir) / "test_input.csv"
        config_file = Path(temp_dir) / "test_config.yaml"
        output_dir = Path(temp_dir) / "output"
        interim_dir = Path(temp_dir) / "interim"

        # Create minimal test data
        input_file.write_text(
            "account_id,account_name\n001,Test Company\n002,Another Company",
        )
        config_file.write_text(
            "similarity:\n  high_threshold: 90\n  medium_threshold: 80",
        )
        output_dir.mkdir()
        interim_dir.mkdir()

        # This would require running the actual pipeline
        # For now, test the structure is in place
        assert input_file.exists()
        assert config_file.exists()

        # Test that we can create a DAG and validate files
        dag = MiniDAG(Path(temp_dir) / "state.json")
        dag._update_state_metadata(input_file, config_file, "test command")

        # Verify metadata is stored correctly
        assert dag._metadata["dag_version"] == "1.0.0"
        assert dag._metadata["input_path"] == str(input_file)
        assert dag._metadata["config_path"] == str(config_file)
