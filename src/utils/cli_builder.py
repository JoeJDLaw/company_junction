"""CLI Command Builder utilities for Streamlit UI."""

from typing import Optional

from src.utils.path_utils import get_config_path


def get_available_input_files() -> list[str]:
    """Get list of available CSV files in data/raw/ directory."""
    raw_dir = get_config_path().parent.parent / "data" / "raw"
    if not raw_dir.exists():
        return []

    csv_files = []
    for file_path in raw_dir.glob("*.csv"):
        if file_path.suffix.lower() == ".csv":
            csv_files.append(file_path.name)

    return sorted(csv_files)


def get_available_config_files() -> list[str]:
    """Get list of available YAML config files in config/ directory."""
    config_dir = get_config_path().parent
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
    col_overrides: Optional[list[str]] = None,
) -> dict[str, str]:
    """Validate CLI arguments and return any validation errors.

    Returns:
        Dict mapping field names to error messages. Empty dict if valid.

    """
    errors: dict[str, str] = {}

    # Required fields
    if not input_file:
        errors["input_file"] = "Input file is required"
    elif not (get_config_path().parent.parent / "data" / "raw" / input_file).exists():
        errors["input_file"] = f"Input file 'data/raw/{input_file}' does not exist"

    if not config:
        errors["config"] = "Config file is required"
    elif not (get_config_path().parent / config).exists():
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

    # Column override validation
    if col_overrides:
        for col_override in col_overrides:
            if "=" not in col_override:
                errors["col_overrides"] = (
                    f"Column override '{col_override}' must use format 'canonical_name=actual_name'"
                )
            else:
                canonical_name, actual_name = col_override.split("=", 1)
                if not canonical_name.strip() or not actual_name.strip():
                    errors["col_overrides"] = (
                        f"Column override '{col_override}' has empty canonical or actual name"
                    )

    return errors


def build_cli_command(
    input_file: str,
    config: str,
    outdir: Optional[str] = None,
    no_parallel: bool = False,
    workers: Optional[int] = None,
    parallel_backend: str = "loky",
    chunk_size: Optional[int] = None,
    no_resume: bool = False,
    run_id: Optional[str] = None,
    keep_runs: Optional[int] = None,
    extra_args: str = "",
    col_overrides: Optional[list[str]] = None,
) -> str:
    """Build CLI command for running the pipeline.

    Args:
        input_file: Input file name
        config: Config file name
        outdir: Output directory (optional)
        no_parallel: Disable parallel processing
        workers: Number of workers
        parallel_backend: Parallel backend
        chunk_size: Chunk size for parallel processing
        no_resume: Disable resume functionality
        run_id: Custom run ID
        keep_runs: Number of runs to keep
        extra_args: Extra arguments to append
        col_overrides: Column overrides for schema resolution

    Returns:
        CLI command string

    """
    cmd_parts = ["python", "src/cleaning.py"]

    # Required arguments
    cmd_parts.extend(["--input", f"data/raw/{input_file}"])
    cmd_parts.extend(["--config", f"config/{config}"])

    # Optional arguments
    if outdir:
        cmd_parts.extend(["--outdir", outdir])

    # Parallelism options
    if no_parallel:
        cmd_parts.append("--no-parallel")
    elif workers:
        cmd_parts.extend(["--workers", str(workers)])

    if parallel_backend != "loky":
        cmd_parts.extend(["--parallel-backend", parallel_backend])

    if chunk_size:
        cmd_parts.extend(["--chunk-size", str(chunk_size)])

    # Resume options
    if no_resume:
        cmd_parts.append("--no-resume")

    if run_id:
        cmd_parts.extend(["--run-id", run_id])

    if keep_runs:
        cmd_parts.extend(["--keep-runs", str(keep_runs)])

    # Column overrides
    if col_overrides:
        for col_override in col_overrides:
            cmd_parts.extend(["--col", col_override])

    # Extra arguments
    if extra_args:
        cmd_parts.append(extra_args)

    return " ".join(cmd_parts)


def get_known_run_ids() -> list[str]:
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
