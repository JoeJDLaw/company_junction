from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Literal, Any
from pathlib import Path
import json
import logging
import tempfile
import time
import os
import hashlib

from src.utils.path_utils import get_interim_dir

Status = Literal["pending", "running", "completed", "failed", "interrupted"]


@dataclass
class Stage:
    name: str
    status: Status = "pending"
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    deps: List[str] = field(default_factory=list)


class MiniDAG:
    def __init__(self, state_file: Path, run_id: str = "") -> None:
        """Initialize MiniDAG with state file path and run_id."""
        self.state_file = state_file
        self.run_id = run_id
        self._logger = logging.getLogger(__name__)
        self._stages: Dict[str, Stage] = {}
        self._metadata: Dict[str, Any] = {
            "dag_version": "1.0.0",  # Add version for future compatibility
            "input_path": "",
            "config_path": "",
            "input_hash": "",
            "config_hash": "",
            "run_id": run_id,
            "cmdline": "",
            "ts": "",
        }
        self._load()  # idempotent

    def register(self, name: str, deps: Optional[List[str]] = None) -> None:
        if name not in self._stages:
            self._stages[name] = Stage(name=name, deps=list(deps or []))
            self._save()

    def start(self, name: str) -> None:
        st = self._stages[name]
        st.status = "running"
        st.start_time = time.time()
        self._save()

    def complete(self, name: str) -> None:
        st = self._stages[name]
        st.status = "completed"
        st.end_time = time.time()
        self._save()

    def fail(self, name: str) -> None:
        st = self._stages[name]
        st.status = "failed"
        st.end_time = time.time()
        self._save()

    def mark_interrupted(self, stage: str) -> None:
        """Mark the pipeline as interrupted at the specified stage.

        Args:
            stage: Name of the stage that was interrupted
        """
        # Mark the current stage as interrupted
        if stage in self._stages:
            st = self._stages[stage]
            st.status = "interrupted"
            st.end_time = time.time()

        # Update metadata to indicate interruption
        self._metadata["status"] = "interrupted"
        self._metadata["active_stage"] = stage
        self._metadata["interrupt_timestamp"] = time.time()

        self._save()

    def should_run(self, name: str, resume_from: Optional[str]) -> bool:
        """Return True if this stage should execute, considering resume semantics."""
        if resume_from is None:
            return True
        # Run this stage and everything after it (caller decides ordering).
        return True

    def get_status(self, name: str) -> Optional[Status]:
        """Get the status of a stage."""
        if name in self._stages:
            return self._stages[name].status
        return None

    def is_completed(self, name: str) -> bool:
        """Check if a stage is completed."""
        return self.get_status(name) == "completed"

    def get_last_completed_stage(self) -> Optional[str]:
        """Get the name of the last completed stage, or None if no stages completed."""
        completed_stages = [
            name for name, stage in self._stages.items() if stage.status == "completed"
        ]
        if not completed_stages:
            return None

        # Define stage order for determining "last"
        stage_order = [
            "normalization",
            "filtering",
            "candidate_generation",
            "grouping",
            "survivorship",
            "disposition",
            "alias_matching",
            "final_output",
        ]

        # Find the highest-indexed completed stage
        last_completed = None
        for stage in stage_order:
            if stage in completed_stages:
                last_completed = stage

        return last_completed

    def get_current_stage(self) -> Optional[str]:
        """Get the name of the currently running stage, or None if no stage is running."""
        running_stages = [
            name for name, stage in self._stages.items() if stage.status == "running"
        ]
        if not running_stages:
            return None

        # Return the first running stage (should only be one)
        return running_stages[0] if running_stages else None

    def validate_intermediate_files(
        self, stage_name: str, interim_dir: Optional[Path] = None
    ) -> bool:
        """Check if intermediate files exist for a given stage."""
        if interim_dir is None:
            interim_dir = get_interim_dir("default")
        if not interim_dir.exists():
            return False

        # Define expected files for each stage
        stage_files = {
            "normalization": ["accounts_normalized.parquet"],
            "filtering": ["accounts_filtered.parquet"],
            "candidate_generation": [
                "accounts_normalized.parquet",
                "accounts_filtered.parquet",
                "candidate_pairs.parquet",
            ],
            "grouping": [
                "accounts_normalized.parquet",
                "accounts_filtered.parquet",
                "candidate_pairs.parquet",
                "groups.parquet",
            ],
            "survivorship": [
                "accounts_normalized.parquet",
                "accounts_filtered.parquet",
                "candidate_pairs.parquet",
                "groups.parquet",
                "survivorship.parquet",
            ],
            "disposition": [
                "accounts_normalized.parquet",
                "accounts_filtered.parquet",
                "candidate_pairs.parquet",
                "groups.parquet",
                "survivorship.parquet",
                "dispositions.parquet",
            ],
            "alias_matching": [
                "accounts_normalized.parquet",
                "accounts_filtered.parquet",
                "candidate_pairs.parquet",
                "groups.parquet",
                "survivorship.parquet",
                "dispositions.parquet",
                "alias_matches.parquet",
            ],
            "final_output": [
                "accounts_normalized.parquet",
                "accounts_filtered.parquet",
                "candidate_pairs.parquet",
                "groups.parquet",
                "survivorship.parquet",
                "dispositions.parquet",
                "alias_matches.parquet",
            ],
        }

        if stage_name not in stage_files:
            return False

        required_files = stage_files[stage_name]
        for filename in required_files:
            if not (interim_dir / filename).exists():
                return False

        return True

    def get_smart_resume_stage(
        self, interim_dir: Optional[Path] = None
    ) -> Optional[str]:
        """
        Intelligently determine where to resume from based on:
        1. Last completed stage in state file
        2. Existence of intermediate files
        3. Pipeline state consistency

        Returns tuple of (resume_stage, reason_code) for enhanced logging.
        """
        if interim_dir is None:
            interim_dir = get_interim_dir("default")
        last_completed = self.get_last_completed_stage()
        if not last_completed:
            self._logger.info("Auto-resume decision: NO_PREVIOUS_RUN - starting fresh")
            return None

        # Check if intermediate files exist for the last completed stage
        if not self.validate_intermediate_files(last_completed, interim_dir):
            self._logger.warning(
                f"Auto-resume decision: MISSING_FILES - last completed stage '{last_completed}' found but intermediate files missing"
            )
            return None

        # Check if we can resume from the next stage
        stage_order = [
            "normalization",
            "filtering",
            "candidate_generation",
            "grouping",
            "survivorship",
            "disposition",
            "alias_matching",
            "final_output",
        ]

        try:
            last_index = stage_order.index(last_completed)
            if last_index + 1 < len(stage_order):
                next_stage = stage_order[last_index + 1]
                if self.validate_intermediate_files(next_stage, interim_dir):
                    self._logger.info(
                        f"Auto-resume decision: NEXT_STAGE_READY - resuming from '{next_stage}'"
                    )
                    return next_stage
                else:
                    self._logger.info(
                        f"Auto-resume decision: NEXT_STAGE_MISSING - resuming from '{last_completed}'"
                    )
                    return last_completed  # Resume from last completed
            else:
                self._logger.info(
                    f"Auto-resume decision: FINAL_STAGE - already at final stage '{last_completed}'"
                )
                return last_completed  # Already at final stage
        except ValueError:
            self._logger.warning(
                f"Auto-resume decision: INVALID_STAGE_ORDER - resuming from '{last_completed}'"
            )
            return last_completed  # Stage not in order, resume from last completed

    def _compute_input_hash(self, input_path: Path, config_path: Path) -> str:
        """Compute hash of input file and config to detect changes."""
        hasher = hashlib.sha256()

        # Hash input file
        if input_path.exists():
            hasher.update(input_path.read_bytes())
            hasher.update(str(input_path.stat().st_size).encode())
            hasher.update(str(input_path.stat().st_mtime).encode())

        # Hash config file
        if config_path.exists():
            hasher.update(config_path.read_bytes())
            hasher.update(str(config_path.stat().st_size).encode())
            hasher.update(str(config_path.stat().st_mtime).encode())

        return hasher.hexdigest()

    def _validate_input_invariance(self, input_path: Path, config_path: Path) -> bool:
        """Check if inputs have changed since last run."""
        current_hash = self._compute_input_hash(input_path, config_path)
        stored_hash = self._metadata.get("input_hash")

        if stored_hash is None:
            return False  # No previous run to compare against

        return bool(current_hash == stored_hash)

    def _update_state_metadata(
        self, input_path: Path, config_path: Path, cmdline: str
    ) -> None:
        """Update state metadata with current run information."""
        self._metadata.update(
            {
                "input_path": str(input_path),
                "config_path": str(config_path),
                "input_hash": self._compute_input_hash(input_path, config_path),
                "dag_version": "1.0.0",
                "cmdline": cmdline,
                "ts": time.time(),
            }
        )
        self._save()

    def get_input_hash(self) -> Optional[str]:
        """Get the stored input hash from metadata."""
        return self._metadata.get("input_hash")

    def _save(self) -> None:
        # Ensure run_id is always populated in metadata
        if self.run_id and not self._metadata.get("run_id"):
            self._metadata["run_id"] = self.run_id

        data = {
            "stages": {
                k: {
                    "name": v.name,
                    "status": v.status,
                    "start_time": v.start_time,
                    "end_time": v.end_time,
                    "deps": v.deps,
                }
                for k, v in self._stages.items()
            },
            "metadata": self._metadata,
        }
        self._atomic_write(self.state_file, json.dumps(data, indent=2))

    def _load(self) -> None:
        """Load DAG state with error tolerance and version defaulting."""
        if not self.state_file.exists():
            return

        try:
            text = self.state_file.read_text()
            raw = json.loads(text)

            # Load stages with error tolerance
            stages = raw.get("stages", {})
            for name, d in stages.items():
                try:
                    self._stages[name] = Stage(
                        name=name,
                        status=d.get("status", "pending"),
                        start_time=d.get("start_time"),
                        end_time=d.get("end_time"),
                        deps=list(d.get("deps", [])),
                    )
                except Exception as e:
                    self._logger.warning("Failed to load stage %s: %s", name, e)
                    continue

            # Load metadata with version defaulting
            metadata = raw.get("metadata", {})
            # Default to current version if missing
            if "dag_version" not in metadata:
                metadata["dag_version"] = "1.0.0"
                self._logger.info("Defaulting missing dag_version to 1.0.0")

            self._metadata.update(metadata)

        except json.JSONDecodeError as e:
            self._logger.error("Corrupted state file %s: %s", self.state_file, e)
            # Reset to clean state
            self._stages = {}
            self._metadata = {
                "dag_version": "1.0.0",
                "input_path": "",
                "config_path": "",
                "input_hash": "",
                "cmdline": "",
                "ts": "",
            }
        except Exception as e:
            self._logger.warning("Failed to load DAG state %s: %s", self.state_file, e)

    @staticmethod
    def _atomic_write(path: Path, content: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(
            "w", delete=False, dir=str(path.parent)
        ) as tmp:
            tmp.write(content)
            tmp_path = Path(tmp.name)
        os.replace(tmp_path, path)
