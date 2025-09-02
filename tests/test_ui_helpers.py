"""
Unit tests for UI helper functions.

This module tests the pure functions in src.utils.ui_helpers
without any Streamlit dependencies.
"""

import json
import tempfile
from pathlib import Path
from typing import Any
from unittest.mock import patch
from src.utils.ui_helpers import choose_backend


from src.utils.ui_helpers import (
    format_run_display_name,
    get_artifact_paths,
    get_default_run_id,
    get_run_metadata,
    get_run_status_icon,
    get_stage_status_icon,
    list_runs,
    load_stage_state,
    validate_run_artifacts,
)


class TestUIHelpers:
    """Test cases for UI helper functions."""

    def test_get_run_status_icon(self) -> None:
        """Test run status icon mapping."""
        assert get_run_status_icon("complete") == "✅"
        assert get_run_status_icon("running") == "⏳"
        assert get_run_status_icon("failed") == "❌"
        assert get_run_status_icon("unknown") == "❓"
        assert get_run_status_icon("invalid") == "❓"

    def test_get_stage_status_icon(self) -> None:
        """Test stage status icon mapping."""
        assert get_stage_status_icon("completed") == "✅"
        assert get_stage_status_icon("running") == "⏳"
        assert get_stage_status_icon("failed") == "❌"
        assert get_stage_status_icon("pending") == "⏸️"
        assert get_stage_status_icon("unknown") == "❓"
        assert get_stage_status_icon("invalid") == "❓"

    def test_get_artifact_paths(self) -> None:
        """Test artifact path generation."""
        run_id = "test_run_123"
        paths = get_artifact_paths(run_id)

        expected_keys = [
            "review_ready_csv",
            "review_ready_parquet",
            "review_meta",
            "pipeline_state",
            "candidate_pairs",
            "groups",
            "survivorship",
            "dispositions",
            "alias_matches",
            "block_top_tokens",
        ]

        for key in expected_keys:
            assert key in paths
            # Check that paths start with "data/" (relative paths as per cursor_rules.md)
            assert paths[key].startswith("data/")
            assert run_id in paths[key]

    def test_format_run_display_name_with_metadata(self) -> None:
        """Test run display name formatting with metadata."""
        metadata = {
            "input_paths": ["data/raw/test.csv"],
            "formatted_timestamp": "2025-08-30 10:43:43",
        }

        display_name = format_run_display_name("test_run_123", metadata)
        assert "test.csv" in display_name
        assert "2025-08-30 10:43:43" in display_name

    def test_format_run_display_name_without_metadata(self) -> None:
        """Test run display name formatting without metadata."""
        with patch("src.utils.ui_helpers.get_run_metadata") as mock_get_metadata:
            mock_get_metadata.return_value = {
                "input_paths": ["data/raw/another.csv"],
                "formatted_timestamp": "2025-08-30 11:00:00",
            }

            display_name = format_run_display_name("test_run_123")
            assert "another.csv" in display_name
            assert "2025-08-30 11:00:00" in display_name

    def test_format_run_display_name_fallback(self) -> None:
        """Test run display name formatting with fallback."""
        with patch("src.utils.ui_helpers.get_run_metadata") as mock_get_metadata:
            mock_get_metadata.return_value = None

            display_name = format_run_display_name("test_run_123")
            assert display_name == "test_run_123"

    def test_format_run_display_name_temp_file(self) -> None:
        """Test run display name formatting for temporary files."""
        mock_metadata = {
            "input_paths": ["/tmp/tmp123456.csv"],
            "formatted_timestamp": "2025-08-30 11:00:00",
        }

        with patch("src.utils.ui_helpers.get_run_metadata") as mock_get_metadata:
            mock_get_metadata.return_value = mock_metadata

            display_name = format_run_display_name("test_run_12345678")
            # Should use temp_file_ prefix for temporary files
            assert display_name.startswith("temp_file_")
            assert "2025-08-30 11:00:00" in display_name

    def test_format_run_display_name_unknown_path(self) -> None:
        """Test run display name formatting for unknown paths."""
        mock_metadata = {
            "input_paths": ["Unknown"],
            "formatted_timestamp": "2025-08-30 11:00:00",
        }

        with patch("src.utils.ui_helpers.get_run_metadata") as mock_get_metadata:
            mock_get_metadata.return_value = mock_metadata

            display_name = format_run_display_name("test_run_12345678")
            # Should use temp_file_ prefix for unknown paths
            assert display_name.startswith("temp_file_")
            assert "2025-08-30 11:00:00" in display_name

    @patch("src.utils.cache_utils.list_runs_deduplicated")
    def test_list_runs(self, mock_list_deduplicated: Any) -> None:
        """Test run listing functionality with deduplication."""
        mock_deduplicated_runs = [
            (
                "run_2",
                {
                    "timestamp": "2025-08-30T11:00:00",
                    "status": "complete",
                    "input_paths": ["data/raw/test2.csv"],
                    "config_paths": ["config/settings.yaml"],
                    "input_hash": "hash3",
                    "config_hash": "hash4",
                    "dag_version": "1.0.0",
                },
            ),
            (
                "run_1",
                {
                    "timestamp": "2025-08-30T10:00:00",
                    "status": "complete",
                    "input_paths": ["data/raw/test1.csv"],
                    "config_paths": ["config/settings.yaml"],
                    "input_hash": "hash1",
                    "config_hash": "hash2",
                    "dag_version": "1.0.0",
                },
            ),
        ]
        mock_list_deduplicated.return_value = mock_deduplicated_runs

        runs = list_runs()

        assert len(runs) == 2
        # Should be sorted by timestamp (newest first)
        assert runs[0]["run_id"] == "run_2"
        assert runs[1]["run_id"] == "run_1"
        assert runs[0]["timestamp"] == "2025-08-30T11:00:00"
        assert runs[1]["timestamp"] == "2025-08-30T10:00:00"

    @patch("src.utils.ui_helpers.load_run_index")
    def test_get_run_metadata(self, mock_load_index: Any) -> None:
        """Test run metadata retrieval."""
        mock_index = {
            "test_run": {
                "timestamp": "2025-08-30T10:43:43.123456",
                "status": "complete",
                "input_paths": ["data/raw/test.csv"],
                "config_paths": ["config/settings.yaml"],
                "input_hash": "hash1",
                "config_hash": "hash2",
                "dag_version": "1.0.0",
            }
        }
        mock_load_index.return_value = mock_index

        metadata = get_run_metadata("test_run")

        assert metadata is not None
        assert metadata["run_id"] == "test_run"
        assert metadata["status"] == "complete"
        # Accept the new timestamp format: YYYY-MM-DD HH:MM local
        assert metadata["formatted_timestamp"] == "2025-08-30 03:43 local"
        assert metadata["input_paths"] == ["data/raw/test.csv"]

    @patch("src.utils.ui_helpers.load_run_index")
    def test_get_run_metadata_not_found(self, mock_load_index: Any) -> None:
        """Test run metadata retrieval for non-existent run."""
        mock_load_index.return_value = {}

        metadata = get_run_metadata("nonexistent_run")

        assert metadata is None

    def test_validate_run_artifacts(self) -> None:
        """Test run artifact validation."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create test run directories
            run_id = "test_run_123"
            processed_dir = Path(temp_dir) / "data" / "processed" / run_id
            interim_dir = Path(temp_dir) / "data" / "interim" / run_id
            processed_dir.mkdir(parents=True, exist_ok=True)
            interim_dir.mkdir(parents=True, exist_ok=True)

            # Create some test files
            (processed_dir / "review_ready.csv").touch()
            (interim_dir / "pipeline_state.json").touch()

            with patch("src.utils.ui_helpers.get_run_metadata") as mock_get_metadata:
                mock_get_metadata.return_value = {
                    "run_id": run_id,
                    "status": "complete",
                }

                with patch("os.path.exists") as mock_exists:

                    def mock_exists_impl(path: str) -> bool:
                        # Convert Path objects to strings for comparison
                        path_str = str(path)
                        if "review_ready.csv" in path_str:
                            return True
                        elif "pipeline_state.json" in path_str:
                            return True
                        else:
                            return False

                    mock_exists.side_effect = mock_exists_impl

                    validation = validate_run_artifacts(run_id)

                    assert validation["run_exists"] is True
                    assert validation["status"] == "complete"
                    assert validation["has_review_ready_csv"] is True
                    assert validation["has_review_ready_parquet"] is False
                    assert validation["has_pipeline_state"] is True
                    assert validation["has_review_meta"] is False
                    assert "review_ready.parquet" in validation["missing_files"]
                    assert "review_meta.json" in validation["missing_files"]

    def test_load_stage_state(self) -> None:
        """Test stage state loading."""
        with tempfile.TemporaryDirectory() as temp_dir:
            state_path = Path(temp_dir) / "pipeline_state.json"

            # Create test state file
            test_state = {
                "stages": {
                    "normalization": {
                        "name": "normalization",
                        "status": "completed",
                        "start_time": 1756575823.499427,
                        "end_time": 1756575823.507553,
                        "deps": [],
                    },
                    "filtering": {
                        "name": "filtering",
                        "status": "completed",
                        "start_time": 1756575823.5084538,
                        "end_time": 1756575823.5334709,
                        "deps": [],
                    },
                },
                "metadata": {
                    "run_id": "test_run",
                    "dag_version": "1.0.0",
                },
            }

            with open(state_path, "w") as f:
                json.dump(test_state, f)

            with patch("os.path.exists") as mock_exists:
                mock_exists.return_value = True

                with patch("builtins.open") as mock_open:
                    mock_open.return_value.__enter__.return_value.read.return_value = (
                        json.dumps(test_state)
                    )

                    state = load_stage_state("test_run")

                    assert state is not None
                    assert len(state["stages"]) == 2
                    assert state["stages"][0]["name"] == "normalization"
                    assert state["stages"][0]["status"] == "completed"
                    assert state["stages"][0]["duration_str"] == "0.01s"
                    assert state["stages"][1]["name"] == "filtering"
                    assert state["stages"][1]["duration_str"] == "0.03s"

    def test_load_stage_state_missing_file(self) -> None:
        """Test stage state loading with missing file."""
        with patch("os.path.exists") as mock_exists:
            mock_exists.return_value = False

            state = load_stage_state("test_run")

            assert state is None

    @patch("src.utils.ui_helpers.get_latest_run_id")
    @patch("src.utils.ui_helpers.list_runs")
    def test_get_default_run_id_with_latest(
        self, mock_list_runs: Any, mock_get_latest: Any
    ) -> None:
        """Test default run ID with latest available."""
        mock_get_latest.return_value = "latest_run_123"

        default_run_id = get_default_run_id()

        assert default_run_id == "latest_run_123"
        mock_list_runs.assert_not_called()

    @patch("src.utils.ui_helpers.get_latest_run_id")
    @patch("src.utils.ui_helpers.list_runs")
    def test_get_default_run_id_fallback(
        self, mock_list_runs: Any, mock_get_latest: Any
    ) -> None:
        """Test default run ID fallback to most recent complete run."""
        mock_get_latest.return_value = None
        mock_list_runs.return_value = [
            {"run_id": "run_1", "status": "failed"},
            {"run_id": "run_2", "status": "complete"},
            {"run_id": "run_3", "status": "running"},
        ]

        default_run_id = get_default_run_id()

        assert default_run_id == "run_2"
        mock_list_runs.assert_called_once()

    @patch("src.utils.ui_helpers.get_latest_run_id")
    @patch("src.utils.ui_helpers.list_runs")
    def test_get_default_run_id_no_runs(
        self, mock_list_runs: Any, mock_get_latest: Any
    ) -> None:
        """Test default run ID when no runs are available."""
        mock_get_latest.return_value = None
        mock_list_runs.return_value = []

        default_run_id = get_default_run_id()

        assert default_run_id == ""


class TestBackendRouting:
    """Test backend routing logic."""

    def test_choose_backend_config_prefers_duckdb(self):
        """Test that config preference for DuckDB is respected."""
        config_flags = {
            "ui": {"use_duckdb_for_groups": True},
            "ui_perf": {
                "groups": {
                    "duckdb_prefer_over_pyarrow": False,
                    "rows_duckdb_threshold": 30000,
                }
            },
        }

        result = choose_backend("duckdb", True, 1000, config_flags, "test_context")

        assert result == "duckdb", "Should choose DuckDB when config prefers it"

    def test_choose_backend_threshold_exceeded(self):
        """Test that DuckDB is chosen when threshold is exceeded."""
        config_flags = {
            "ui": {"use_duckdb_for_groups": False},
            "ui_perf": {
                "groups": {
                    "duckdb_prefer_over_pyarrow": True,
                    "rows_duckdb_threshold": 1000,
                }
            },
        }

        result = choose_backend("duckdb", True, 5000, config_flags, "test_context")

        assert result == "duckdb", "Should choose DuckDB when threshold exceeded"

    def test_choose_backend_fallback_to_pyarrow(self):
        """Test that PyArrow is chosen as fallback."""
        config_flags = {
            "ui": {"use_duckdb_for_groups": False},
            "ui_perf": {
                "groups": {
                    "duckdb_prefer_over_pyarrow": False,
                    "rows_duckdb_threshold": 30000,
                }
            },
        }

        result = choose_backend("duckdb", True, 1000, config_flags, "test_context")

        assert (
            result == "pyarrow"
        ), "Should fallback to PyArrow when no config preference"

    def test_choose_backend_duckdb_not_available(self):
        """Test that PyArrow is chosen when DuckDB is not available."""
        config_flags = {
            "ui": {"use_duckdb_for_groups": True},
            "ui_perf": {
                "groups": {
                    "duckdb_prefer_over_pyarrow": True,
                    "rows_duckdb_threshold": 1000,
                }
            },
        }

        result = choose_backend("duckdb", False, 5000, config_flags, "test_context")

        assert result == "pyarrow", "Should choose PyArrow when DuckDB not available"
