"""Tests for CLI builder utilities."""

from pathlib import Path
from typing import Any
from unittest.mock import patch

from src.utils.cli_builder import (
    build_cli_command,
    get_available_config_files,
    get_available_input_files,
    get_known_run_ids,
    validate_cli_args,
)


class TestGetAvailableInputFiles:
    """Test input file discovery."""

    def test_get_available_input_files_empty_dir(self) -> None:
        """Test when data/raw directory doesn't exist."""
        with patch("pathlib.Path.exists") as mock_exists:
            mock_exists.return_value = False
            files = get_available_input_files()
            assert files == []

    def test_get_available_input_files_with_files(self) -> None:
        """Test when CSV files exist."""
        with patch("pathlib.Path.exists") as mock_exists:
            mock_exists.return_value = True

            with patch("pathlib.Path.glob") as mock_glob:
                mock_glob.return_value = [
                    Path("data/raw/file1.csv"),
                    Path("data/raw/file2.csv"),
                    Path("data/raw/file3.txt"),  # Non-CSV file
                ]

                files = get_available_input_files()
                assert files == ["file1.csv", "file2.csv"]


class TestGetAvailableConfigFiles:
    """Test config file discovery."""

    def test_get_available_config_files_empty_dir(self) -> None:
        """Test when config directory doesn't exist."""
        with patch("pathlib.Path.exists") as mock_exists:
            mock_exists.return_value = False
            files = get_available_config_files()
            assert files == []

    def test_get_available_config_files_with_files(self) -> None:
        """Test when YAML files exist."""
        with patch("pathlib.Path.exists") as mock_exists:
            mock_exists.return_value = True

            with patch("pathlib.Path.glob") as mock_glob:
                mock_glob.return_value = [
                    Path("config/settings.yaml"),
                    Path("config/test.yaml"),
                    Path("config/readme.txt"),  # Non-YAML file
                ]

                files = get_available_config_files()
                assert files == ["settings.yaml", "test.yaml"]


class TestValidateCliArgs:
    """Test CLI argument validation."""

    def test_validate_cli_args_valid(self) -> None:
        """Test valid arguments."""
        with patch("pathlib.Path.exists") as mock_exists:
            mock_exists.return_value = True

            errors = validate_cli_args(
                input_file="test.csv",
                config="settings.yaml",
                workers=4,
                chunk_size=1000,
            )
            assert errors == {}

    def test_validate_cli_args_missing_input(self) -> None:
        """Test missing input file."""
        errors = validate_cli_args(input_file="", config="settings.yaml")
        assert "input_file" in errors
        assert "required" in errors["input_file"]

    def test_validate_cli_args_missing_config(self) -> None:
        """Test missing config file."""
        errors = validate_cli_args(input_file="test.csv", config="")
        assert "config" in errors
        assert "required" in errors["config"]

    def test_validate_cli_args_nonexistent_input(self) -> None:
        """Test nonexistent input file."""
        with patch("pathlib.Path.exists") as mock_exists:
            mock_exists.return_value = False

            errors = validate_cli_args(
                input_file="nonexistent.csv", config="settings.yaml",
            )
            assert "input_file" in errors
            assert "does not exist" in errors["input_file"]

    def test_validate_cli_args_nonexistent_config(self) -> None:
        """Test nonexistent config file."""
        with patch("pathlib.Path.exists") as mock_exists:
            mock_exists.return_value = False

            errors = validate_cli_args(input_file="test.csv", config="nonexistent.yaml")
            assert "config" in errors
            assert "does not exist" in errors["config"]

    def test_validate_cli_args_invalid_workers_with_no_parallel(self) -> None:
        """Test invalid workers with no-parallel."""
        with patch("pathlib.Path.exists") as mock_exists:
            mock_exists.return_value = True

            errors = validate_cli_args(
                input_file="test.csv",
                config="settings.yaml",
                no_parallel=True,
                workers=4,
            )
            assert "workers" in errors
            assert "Cannot specify workers > 1" in errors["workers"]

    def test_validate_cli_args_invalid_workers_value(self) -> None:
        """Test invalid workers value."""
        with patch("pathlib.Path.exists") as mock_exists:
            mock_exists.return_value = True

            errors = validate_cli_args(
                input_file="test.csv",
                config="settings.yaml",
                workers=0,
            )
            assert "workers" in errors
            assert "must be >= 1" in errors["workers"]

    def test_validate_cli_args_invalid_backend(self) -> None:
        """Test invalid parallel backend."""
        with patch("pathlib.Path.exists") as mock_exists:
            mock_exists.return_value = True

            errors = validate_cli_args(
                input_file="test.csv",
                config="settings.yaml",
                parallel_backend="invalid",
            )
            assert "parallel_backend" in errors
            assert "must be 'loky' or 'threading'" in errors["parallel_backend"]

    def test_validate_cli_args_invalid_chunk_size(self) -> None:
        """Test invalid chunk size."""
        with patch("pathlib.Path.exists") as mock_exists:
            mock_exists.return_value = True

            errors = validate_cli_args(
                input_file="test.csv",
                config="settings.yaml",
                chunk_size=0,
            )
            assert "chunk_size" in errors
            assert "must be >= 1" in errors["chunk_size"]

    def test_validate_cli_args_invalid_keep_runs(self) -> None:
        """Test invalid keep runs."""
        with patch("pathlib.Path.exists") as mock_exists:
            mock_exists.return_value = True

            errors = validate_cli_args(
                input_file="test.csv",
                config="settings.yaml",
                keep_runs=0,
            )
            assert "keep_runs" in errors
            assert "must be >= 1" in errors["keep_runs"]

    def test_validate_cli_args_empty_run_id(self) -> None:
        """Test empty run ID."""
        with patch("pathlib.Path.exists") as mock_exists:
            mock_exists.return_value = True

            errors = validate_cli_args(
                input_file="test.csv",
                config="settings.yaml",
                run_id="   ",
            )
            assert "run_id" in errors
            assert "cannot be empty" in errors["run_id"]


class TestBuildCliCommand:
    """Test CLI command building."""

    def test_build_cli_command_basic(self) -> None:
        """Test basic command building."""
        cmd = build_cli_command(
            input_file="test.csv",
            config="settings.yaml",
        )
        expected = "python src/cleaning.py --input data/raw/test.csv --outdir data/processed --config config/settings.yaml"
        assert cmd == expected

    def test_build_cli_command_with_parallelism(self) -> None:
        """Test command with parallelism flags."""
        cmd = build_cli_command(
            input_file="test.csv",
            config="settings.yaml",
            workers=4,
            parallel_backend="threading",
            chunk_size=1000,
        )
        expected = "python src/cleaning.py --input data/raw/test.csv --outdir data/processed --config config/settings.yaml --workers 4 --parallel-backend threading --chunk-size 1000"
        assert cmd == expected

    def test_build_cli_command_no_parallel(self) -> None:
        """Test command with no-parallel flag."""
        cmd = build_cli_command(
            input_file="test.csv",
            config="settings.yaml",
            no_parallel=True,
        )
        expected = "python src/cleaning.py --input data/raw/test.csv --outdir data/processed --config config/settings.yaml --no-parallel"
        assert cmd == expected

    def test_build_cli_command_with_run_control(self) -> None:
        """Test command with run control flags."""
        cmd = build_cli_command(
            input_file="test.csv",
            config="settings.yaml",
            no_resume=True,
            run_id="custom_run_123",
            keep_runs=5,
        )
        expected = "python src/cleaning.py --input data/raw/test.csv --outdir data/processed --config config/settings.yaml --no-resume --run-id custom_run_123 --keep-runs 5"
        assert cmd == expected

    def test_build_cli_command_with_extra_args(self) -> None:
        """Test command with extra arguments."""
        cmd = build_cli_command(
            input_file="test.csv",
            config="settings.yaml",
            extra_args="--verbose --debug",
        )
        expected = "python src/cleaning.py --input data/raw/test.csv --outdir data/processed --config config/settings.yaml --verbose --debug"
        assert cmd == expected

    def test_build_cli_command_custom_outdir(self) -> None:
        """Test command with custom output directory."""
        cmd = build_cli_command(
            input_file="test.csv",
            config="settings.yaml",
            outdir="custom/output",
        )
        expected = "python src/cleaning.py --input data/raw/test.csv --outdir custom/output --config config/settings.yaml"
        assert cmd == expected


class TestGetKnownRunIds:
    """Test run ID discovery."""

    @patch("src.utils.cache_utils.load_run_index")
    def test_get_known_run_ids_success(self, mock_load_index: Any) -> None:
        """Test successful run ID retrieval."""
        mock_index = {
            "run_20230830_120000": {"status": "complete"},
            "run_20230830_110000": {"status": "complete"},
            "run_20230830_130000": {"status": "running"},
        }
        mock_load_index.return_value = mock_index

        run_ids = get_known_run_ids()
        assert run_ids == [
            "run_20230830_130000",
            "run_20230830_120000",
            "run_20230830_110000",
        ]

    @patch("src.utils.cache_utils.load_run_index")
    def test_get_known_run_ids_empty(self, mock_load_index: Any) -> None:
        """Test empty run index."""
        mock_load_index.return_value = {}

        run_ids = get_known_run_ids()
        assert run_ids == []

    @patch("src.utils.cache_utils.load_run_index")
    def test_get_known_run_ids_exception(self, mock_load_index: Any) -> None:
        """Test exception handling."""
        mock_load_index.side_effect = Exception("Test error")

        run_ids = get_known_run_ids()
        assert run_ids == []
