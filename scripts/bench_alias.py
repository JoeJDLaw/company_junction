#!/usr/bin/env python3
"""Benchmark alias matching performance between legacy and optimized paths.

This script runs the alias stage multiple times with different optimization settings
to measure performance improvements.
"""

import argparse
import subprocess
import sys
import time
from pathlib import Path

from src.utils.logging_utils import get_logger

logger = get_logger(__name__)


def run_pipeline_with_settings(
    input_path: Path,
    config_path: Path,
    optimize: bool,
    workers: int = 4,
    output_dir: Path | None = None,
) -> tuple[float, dict]:
    """Run pipeline with specific alias optimization settings.

    Args:
        input_path: Path to input CSV file
        config_path: Path to config YAML file
        optimize: Whether to enable alias optimization
        workers: Number of parallel workers
        output_dir: Output directory (auto-generated if None)

    Returns:
        Tuple of (elapsed_time, stats_dict)

    """
    if output_dir is None:
        # Create unique output directory
        timestamp = int(time.time())
        output_dir = Path(f"data/processed/benchmark_{timestamp}")

    output_dir.mkdir(parents=True, exist_ok=True)

    # Create temporary config with specific alias settings
    import yaml

    with open(config_path) as f:
        config = yaml.safe_load(f)

    # Set alias optimization
    if "alias" not in config:
        config["alias"] = {}
    config["alias"]["optimize"] = optimize

    # Set parallelism
    if "parallelism" not in config:
        config["parallelism"] = {}
    config["parallelism"]["workers"] = workers

    # Write temporary config
    temp_config = output_dir / "temp_config.yaml"
    with open(temp_config, "w") as f:
        yaml.dump(config, f)

    # Build pipeline command
    cmd = [
        sys.executable,
        "src/cleaning.py",
        "--input",
        str(input_path),
        "--outdir",
        str(output_dir),
        "--config",
        str(temp_config),
        "--progress",
        "--no-resume",
        "--workers",
        str(workers),
        "--chunk-size",
        "1000",
        "--parallel-backend",
        "threading",  # Use threading for benchmarking
    ]

    logger.info(f"Running pipeline with alias.optimize={optimize}, workers={workers}")
    logger.info(f"Command: {' '.join(cmd)}")

    # Run pipeline and measure time
    start_time = time.perf_counter()

    try:
        result = subprocess.run(
            cmd,
            check=False,
            capture_output=True,
            text=True,
            timeout=300,  # 5 minute timeout
            cwd=Path.cwd(),
        )
        elapsed_time = time.perf_counter() - start_time

        if result.returncode != 0:
            logger.error(f"Pipeline failed with return code {result.returncode}")
            logger.error(f"stdout: {result.stdout}")
            logger.error(f"stderr: {result.stderr}")
            return elapsed_time, {}

        # Parse logs for alias stage stats
        stats = parse_alias_stats(result.stdout)

        # Clean up temp config
        temp_config.unlink(missing_ok=True)

        return elapsed_time, stats

    except subprocess.TimeoutExpired:
        logger.error("Pipeline timed out after 5 minutes")
        return 300.0, {}
    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")
        return time.perf_counter() - start_time, {}


def parse_alias_stats(log_output: str) -> dict:
    """Parse pipeline logs for alias stage statistics.

    Args:
        log_output: Pipeline stdout/stderr

    Returns:
        Dictionary with parsed stats

    """
    stats = {}

    # Look for alias stage completion log
    lines = log_output.split("\n")
    for line in lines:
        if "Generated" in line and "alias matches in" in line:
            # Extract pairs_generated and elapsed_time
            try:
                parts = line.split()
                for i, part in enumerate(parts):
                    if part == "Generated":
                        stats["pairs_generated"] = int(parts[i + 1])
                    elif part == "in" and i + 2 < len(parts):
                        stats["elapsed_time"] = int(float(parts[i + 1].rstrip("s")))
                break
            except (ValueError, IndexError):
                continue

    return stats


def print_benchmark_results(results: list[tuple[str, float, dict]]) -> None:
    """Print benchmark results in a formatted table.

    Args:
        results: List of (name, elapsed_time, stats) tuples

    """
    logger.info("=" * 80)
    logger.info("ALIAS MATCHING BENCHMARK RESULTS")
    logger.info("=" * 80)

    # Print header
    print(f"{'Configuration':<25} {'Time (s)':<12} {'Pairs':<10} {'Throughput':<15}")
    print("-" * 80)

    # Print results
    for name, elapsed_time, stats in results:
        pairs = stats.get("pairs_generated", 0)
        throughput = f"{pairs/elapsed_time:.1f} pairs/s" if elapsed_time > 0 else "N/A"

        print(f"{name:<25} {elapsed_time:<12.2f} {pairs:<10} {throughput:<15}")

    print("-" * 80)

    # Calculate improvements
    if len(results) >= 2:
        legacy_time = results[0][1]
        optimized_time = results[1][1]

        if legacy_time > 0 and optimized_time > 0:
            speedup = legacy_time / optimized_time
            time_saved = legacy_time - optimized_time
            improvement_pct = (time_saved / legacy_time) * 100

            logger.info("Performance Summary:")
            logger.info(f"  Speedup: {speedup:.2f}x faster")
            logger.info(f"  Time saved: {time_saved:.2f}s ({improvement_pct:.1f}%)")

            if speedup >= 2.0:
                logger.info("  🚀 Excellent performance improvement!")
            elif speedup >= 1.5:
                logger.info("  ✅ Good performance improvement")
            elif speedup >= 1.1:
                logger.info("  📈 Modest performance improvement")
            else:
                logger.info("  ⚠️  Minimal performance improvement")


def main() -> None:
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Benchmark alias matching performance")
    parser.add_argument(
        "--input",
        type=Path,
        required=True,
        help="Path to input CSV file",
    )
    parser.add_argument(
        "--config",
        type=Path,
        required=True,
        help="Path to config YAML file",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Number of parallel workers (default: 4)",
    )
    parser.add_argument(
        "--runs",
        type=int,
        default=2,
        help="Number of runs per configuration (default: 2)",
    )

    args = parser.parse_args()

    # Validate inputs
    if not args.input.exists():
        logger.error(f"Input file not found: {args.input}")
        sys.exit(1)

    if not args.config.exists():
        logger.error(f"Config file not found: {args.config}")
        sys.exit(1)

    logger.info("Starting alias matching benchmark")
    logger.info(f"Input: {args.input}")
    logger.info(f"Config: {args.config}")
    logger.info(f"Workers: {args.workers}")
    logger.info(f"Runs per config: {args.runs}")

    results = []

    # Run legacy path
    for i in range(args.runs):
        logger.info(f"Run {i+1}/{args.runs}: Legacy alias matching")
        elapsed_time, stats = run_pipeline_with_settings(
            args.input,
            args.config,
            optimize=False,
            workers=args.workers,
        )
        results.append(("Legacy (optimize=false)", elapsed_time, stats))

    # Run optimized path
    for i in range(args.runs):
        logger.info(f"Run {i+1}/{args.runs}: Optimized alias matching")
        elapsed_time, stats = run_pipeline_with_settings(
            args.input,
            args.config,
            optimize=True,
            workers=args.workers,
        )
        results.append(("Optimized (optimize=true)", elapsed_time, stats))

    # Print results
    print_benchmark_results(results)

    logger.info("Benchmark completed successfully")


if __name__ == "__main__":
    main()
