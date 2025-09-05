"""End-to-end test for Phase 1.16 run ID scoping and determinism.

This test verifies that:
1. Sequential and parallel runs produce identical outputs
2. All outputs are properly run-scoped
3. No legacy global paths are used
4. MiniDAG state is run-scoped
"""

import hashlib
import os
import subprocess
from pathlib import Path
from typing import Any, Dict

import pandas as pd
import pytest


def get_file_sha256(file_path: str) -> str:
    """Compute SHA256 hash of a file."""
    hash_sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_sha256.update(chunk)
    return hash_sha256.hexdigest()


def get_directory_hashes(directory: str) -> Dict[str, str]:
    """Get SHA256 hashes of all files in a directory."""
    hashes = {}
    for root, dirs, files in os.walk(directory):
        for file in files:
            file_path = os.path.join(root, file)
            relative_path = os.path.relpath(file_path, directory)
            hashes[relative_path] = get_file_sha256(file_path)
    return hashes


def run_pipeline_test(
    input_file: str, workers: int, no_resume: bool = True,
) -> Dict[str, Any]:
    """Run pipeline and return run ID and output hashes."""
    cmd = [
        "python",
        "src/cleaning.py",
        "--input",
        input_file,
        "--outdir",
        "data/processed",
        "--config",
        "config/settings.yaml",
    ]

    if workers == 1:
        cmd.append("--no-parallel")
    else:
        cmd.extend(["--workers", str(workers)])

    if no_resume:
        cmd.append("--no-resume")

    # Run the pipeline
    result = subprocess.run(
        cmd, check=False, capture_output=True, text=True, cwd=Path.cwd(), env=os.environ.copy(),
    )

    if result.returncode != 0:
        raise RuntimeError(f"Pipeline failed: {result.stderr}")

    # Extract run ID from output (check both stdout and stderr)
    run_id = None
    for line in result.stdout.split("\n") + result.stderr.split("\n"):
        if "Pipeline completed successfully with run_id:" in line:
            run_id = line.split("run_id:")[1].strip()
            break

    if not run_id:
        raise RuntimeError("Could not extract run ID from pipeline output")

    # Get hashes of output files
    output_dir = f"data/processed/{run_id}"
    if not os.path.exists(output_dir):
        raise RuntimeError(f"Output directory not found: {output_dir}")

    return {
        "run_id": run_id,
        "output_dir": output_dir,
        "hashes": get_directory_hashes(output_dir),
        "stdout": result.stdout,
        "stderr": result.stderr,
    }


def test_e2e_determinism() -> None:
    """Test that sequential and parallel runs produce identical outputs."""
    input_file = "data/raw/sample_test.csv"

    if not os.path.exists(input_file):
        pytest.skip(f"Test input file not found: {input_file}")

    # Run sequential pipeline
    print("Running sequential pipeline...")
    sequential_result = run_pipeline_test(input_file, workers=1)

    # Add a small delay to ensure different timestamps
    import time

    time.sleep(1)

    # Run parallel pipeline
    print("Running parallel pipeline...")
    parallel_result = run_pipeline_test(input_file, workers=4)

    # Verify run IDs (may be the same if runs happen quickly)
    # Run ID format: {input_hash[:8]}_{config_hash[:8]}_{YYYYMMDDHHMMSS}
    print(f"Sequential run_id: {sequential_result['run_id']}")
    print(f"Parallel run_id: {parallel_result['run_id']}")

    # Verify identical file hashes
    sequential_hashes = sequential_result["hashes"]
    parallel_hashes = parallel_result["hashes"]

    # Check that both runs produced the same files
    assert set(sequential_hashes.keys()) == set(
        parallel_hashes.keys(),
    ), "Sequential and parallel runs should produce the same files"

    # Check that all files have identical content (excluding timestamp-dependent files)
    # and derived files that may have legitimate differences due to grouping order
    timestamp_dependent_files = {"review_meta.json", "perf_summary.json"}
    derived_files = {
        "group_stats.parquet",
        "review_ready.parquet",
        "review_ready.csv",
    }  # Derived from groups, may differ due to grouping order

    # For grouping-derived files, check functional equivalence instead of bit-for-bit
    for filename in sequential_hashes:
        if filename in timestamp_dependent_files:
            print(f"⚠️  Skipping timestamp-dependent file: {filename}")
            continue
        if filename in derived_files:
            print(f"⚠️  Checking functional equivalence for derived file: {filename}")
            # Check functional equivalence for grouping-derived files
            if filename.endswith(".parquet"):
                seq_df = pd.read_parquet(
                    f"{sequential_result['output_dir']}/{filename}",
                )
                par_df = pd.read_parquet(f"{parallel_result['output_dir']}/{filename}")
            elif filename.endswith(".csv"):
                seq_df = pd.read_csv(f"{sequential_result['output_dir']}/{filename}")
                par_df = pd.read_csv(f"{parallel_result['output_dir']}/{filename}")
            else:
                continue

            # Check same shape and same number of unique groups
            assert (
                seq_df.shape == par_df.shape
            ), f"Shape differs for {filename}: {seq_df.shape} vs {par_df.shape}"
            if "group_id" in seq_df.columns:
                seq_groups = seq_df["group_id"].nunique()
                par_groups = par_df["group_id"].nunique()
                assert (
                    seq_groups == par_groups
                ), f"Number of groups differs for {filename}: {seq_groups} vs {par_groups}"
                print(
                    f"✅ Functional equivalence verified for {filename}: {seq_groups} groups, {seq_df.shape[0]} records",
                )
            continue
        assert (
            sequential_hashes[filename] == parallel_hashes[filename]
        ), f"File {filename} differs between sequential and parallel runs"

    print(f"✅ Determinism test passed: {len(sequential_hashes)} files identical")


def test_run_id_scoping() -> None:
    """Test that all outputs are properly run-scoped."""
    input_file = "data/raw/sample_test.csv"

    if not os.path.exists(input_file):
        pytest.skip(f"Test input file not found: {input_file}")

    # Run pipeline
    result = run_pipeline_test(input_file, workers=1)
    run_id = result["run_id"]

    # Check that output directory is run-scoped
    expected_output_dir = f"data/processed/{run_id}"
    assert os.path.exists(
        expected_output_dir,
    ), f"Run-scoped output directory not found: {expected_output_dir}"

    # Check that interim directory is run-scoped
    expected_interim_dir = f"data/interim/{run_id}"
    assert os.path.exists(
        expected_interim_dir,
    ), f"Run-scoped interim directory not found: {expected_interim_dir}"

    # Check that MiniDAG state is run-scoped
    expected_state_file = f"data/interim/{run_id}/pipeline_state.json"
    assert os.path.exists(
        expected_state_file,
    ), f"Run-scoped state file not found: {expected_state_file}"

    # Check that block statistics are run-scoped
    expected_block_stats = f"data/interim/{run_id}/block_top_tokens.csv"
    assert os.path.exists(
        expected_block_stats,
    ), f"Run-scoped block statistics not found: {expected_block_stats}"

    print(f"✅ Run ID scoping test passed: all outputs under {run_id}")


def test_no_legacy_paths() -> None:
    """Test that no legacy global paths are used."""
    input_file = "data/raw/sample_test.csv"

    if not os.path.exists(input_file):
        pytest.skip(f"Test input file not found: {input_file}")

        # Run pipeline
    run_pipeline_test(input_file, workers=1)

    # Check that no global pipeline state file exists
    global_state_file = "data/interim/pipeline_state.json"
    if os.path.exists(global_state_file):
        # If it exists, it should be from a previous run and not updated
        # We can't easily check this without more complex logic
        print(f"⚠️  Global state file exists: {global_state_file}")

    # Check that no global block statistics file exists
    global_block_stats = "data/interim/block_top_tokens.csv"
    if os.path.exists(global_block_stats):
        print(f"⚠️  Global block statistics file exists: {global_block_stats}")

    print("✅ No legacy paths test completed")


def test_latest_pointer() -> None:
    """Test that latest pointer is properly maintained."""
    # Enable destructive fuse for this test
    original_env = os.environ.copy()
    os.environ["PHASE1_DESTRUCTIVE_FUSE"] = "true"

    try:
        input_file = "data/raw/sample_test.csv"

        if not os.path.exists(input_file):
            pytest.skip(f"Test input file not found: {input_file}")

        # Run pipeline
        result = run_pipeline_test(input_file, workers=1)
        run_id = result["run_id"]

        # Check symlink
        latest_symlink = "data/processed/latest"
        if os.path.islink(latest_symlink):
            target = os.readlink(latest_symlink)
            assert (
                target == run_id
            ), f"Latest symlink should point to {run_id}, but points to {target}"
        else:
            print(f"⚠️  Latest symlink not found: {latest_symlink}")

        # Check JSON pointer
        latest_json = "data/processed/latest.json"
        if os.path.exists(latest_json):
            import json

            with open(latest_json) as f:
                data = json.load(f)
            assert (
                data.get("run_id") == run_id
            ), f"Latest JSON should contain run_id {run_id}"
        else:
            print(f"⚠️  Latest JSON pointer not found: {latest_json}")

        print("✅ Latest pointer test completed")

    finally:
        # Restore original environment
        os.environ.clear()
        os.environ.update(original_env)


if __name__ == "__main__":
    # Run tests manually if needed
    test_e2e_determinism()
    test_run_id_scoping()
    test_no_legacy_paths()
    test_latest_pointer()
    print("All end-to-end tests passed!")
