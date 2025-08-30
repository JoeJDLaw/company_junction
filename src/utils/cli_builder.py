"""CLI Command Builder utilities for Streamlit UI."""

from pathlib import Path
from typing import Dict, List, Optional


def get_available_input_files() -> List[str]:
    """Get list of available CSV files in data/raw/ directory."""
    raw_dir = Path("data/raw")
    if not raw_dir.exists():
        return []

    csv_files = []
    for file_path in raw_dir.glob("*.csv"):
        if file_path.suffix.lower() == ".csv":
            csv_files.append(file_path.name)

    return sorted(csv_files)


def get_available_config_files() -> List[str]:
    """Get list of available YAML config files in config/ directory."""
    config_dir = Path("config")
    if not config_dir.exists():
        return []

    yaml_files = []
    for file_path in config_dir.glob("*.yaml"):
        if file_path.suffix.lower() in [".yaml", ".yml"]:
            yaml_files.append(file_path.name)

    return sorted(yaml_files)


def validate_cli_args(
    input_file: str,
    config: str,
    no_parallel: bool = False,
    workers: Optional[int] = None,
    parallel_backend: str = "loky",
    chunk_size: Optional[int] = None,
    no_resume: bool = False,
    run_id: Optional[str] = None,
    keep_runs: Optional[int] = None,
) -> Dict[str, str]:
    """Validate CLI arguments and return any validation errors.

    Returns:
        Dict mapping field names to error messages. Empty dict if valid.
    """
    errors: Dict[str, str] = {}

    # Required fields
    if not input_file:
        errors["input_file"] = "Input file is required"
    elif not Path(f"data/raw/{input_file}").exists():
        errors["input_file"] = f"Input file 'data/raw/{input_file}' does not exist"

    if not config:
        errors["config"] = "Config file is required"
    elif not Path(f"config/{config}").exists():
        errors["config"] = f"Config file 'config/{config}' does not exist"

    # Parallelism validation
    if no_parallel and workers is not None and workers > 1:
        errors["workers"] = "Cannot specify workers > 1 when --no-parallel is enabled"

    if workers is not None and workers < 1:
        errors["workers"] = "Workers must be >= 1"

    if parallel_backend not in ["loky", "threading"]:
        errors["parallel_backend"] = "Parallel backend must be 'loky' or 'threading'"

    if chunk_size is not None and chunk_size < 1:
        errors["chunk_size"] = "Chunk size must be >= 1"

    # Run management validation
    if keep_runs is not None and keep_runs < 1:
        errors["keep_runs"] = "Keep runs must be >= 1"

    # Run ID validation
    if run_id and not run_id.strip():
        errors["run_id"] = "Run ID cannot be empty"

    return errors


def build_cli_command(
    input_file: str,
    config: str,
    outdir: str = "data/processed",
    no_parallel: bool = False,
    workers: Optional[int] = None,
    parallel_backend: str = "loky",
    chunk_size: Optional[int] = None,
    no_resume: bool = False,
    run_id: Optional[str] = None,
    keep_runs: Optional[int] = None,
    extra_args: str = "",
) -> str:
    """Build a CLI command string from the given arguments.

    Args:
        input_file: Input CSV file name (from data/raw/)
        config: Config YAML file name (from config/)
        outdir: Output directory (default: data/processed)
        no_parallel: Whether to disable parallel execution
        workers: Number of workers (None for auto)
        parallel_backend: Backend for parallel execution
        chunk_size: Chunk size for parallel processing
        no_resume: Whether to disable resume functionality
        run_id: Custom run ID
        keep_runs: Number of runs to keep
        extra_args: Extra arguments to append verbatim

    Returns:
        Complete CLI command string
    """
    cmd_parts = ["python", "src/cleaning.py"]

    # Required arguments
    cmd_parts.extend(["--input", f"data/raw/{input_file}"])
    cmd_parts.extend(["--outdir", outdir])
    cmd_parts.extend(["--config", f"config/{config}"])

    # Parallelism flags
    if no_parallel:
        cmd_parts.append("--no-parallel")
    else:
        if workers is not None:
            cmd_parts.extend(["--workers", str(workers)])
        if parallel_backend != "loky":  # loky is default
            cmd_parts.extend(["--parallel-backend", parallel_backend])
        if chunk_size is not None:
            cmd_parts.extend(["--chunk-size", str(chunk_size)])

    # Run control flags
    if no_resume:
        cmd_parts.append("--no-resume")
    if run_id:
        cmd_parts.extend(["--run-id", run_id])
    if keep_runs is not None:
        cmd_parts.extend(["--keep-runs", str(keep_runs)])

    # Extra arguments
    if extra_args.strip():
        cmd_parts.extend(extra_args.strip().split())

    return " ".join(cmd_parts)


def get_known_run_ids() -> List[str]:
    """Get list of known run IDs from run_index.json."""
    try:
        from src.utils.cache_utils import load_run_index

        run_index = load_run_index()
        if not run_index:
            return []

        # Return run IDs sorted by timestamp (newest first)
        runs = list(run_index.keys())
        return sorted(runs, reverse=True)
    except Exception:
        return []
