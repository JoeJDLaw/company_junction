#!/usr/bin/env python3
"""Benchmark comparison between legacy and optimized paths for alias matching and group stats."""

import argparse
import json
import logging
import os
import time
import subprocess
from typing import Dict, Any, List

import psutil

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def get_memory_usage():
    """Get current memory usage in MB."""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024


def run_pipeline(input_file: str, run_id: str, use_legacy: bool = False) -> dict:
    """Run pipeline and capture metrics."""
    config = "config/settings_legacy.yaml" if use_legacy else "config/settings.yaml"
    outdir = f"data/interim/{run_id}"

    start_time = time.perf_counter()
    start_mem = get_memory_usage()

    # Run pipeline and capture output
    cmd = f"python -m src.cleaning --input {input_file} --outdir {outdir} --config {config} --run-id {run_id} --workers 4"
    result = os.system(cmd)

    end_time = time.perf_counter()
    end_mem = get_memory_usage()

    # Get match counts from alias_matches.parquet
    match_count = 0  # TODO: Read from parquet file

    return {
        "total_runtime": end_time - start_time,
        "memory_delta": end_mem - start_mem,
        "match_count": match_count,
        "exit_code": result,
    }


def run_group_stats_benchmark(dataset_size: str, run_id: str, backend: str = "duckdb") -> dict:
    """Run group stats benchmark with specified backend."""
    input_file = f"data/raw/company_junction_range_{dataset_size}.csv"
    
    # Set environment variable for backend
    env = os.environ.copy()
    env["CJ_GROUP_STATS_BACKEND"] = backend
    env["CJ_GROUP_STATS_PERSIST_ARTIFACTS"] = "true"
    
    start_time = time.perf_counter()
    start_mem = get_memory_usage()
    
    # Run the full pipeline to generate group_stats artifacts
    cmd = f"python -m src.cleaning --input {input_file} --outdir data/interim/{run_id} --config config/settings.yaml --run-id {run_id}"
    result = subprocess.run(cmd, shell=True, env=env, capture_output=True, text=True)
    
    end_time = time.perf_counter()
    end_mem = get_memory_usage()
    
    # Parse timing from logs
    timing = None
    if result.returncode == 0:
        # Look for timing in stdout
        for line in result.stdout.split('\n'):
            if 'group_stats | SUCCESS' in line and 'elapsed_sec=' in line:
                try:
                    # Extract elapsed time
                    elapsed_part = line.split('elapsed_sec=')[1].split()[0]
                    timing = float(elapsed_part)
                    break
                except (IndexError, ValueError):
                    continue
    
    return {
        "backend": backend,
        "runtime": timing if timing else (end_time - start_time),
        "memory_delta": end_mem - start_mem,
        "exit_code": result.returncode,
        "stdout": result.stdout,
        "stderr": result.stderr
    }

def generate_parquet_size_report(run_id: str) -> Dict[str, Any]:
    """Generate parquet size report for the run."""
    try:
        from src.utils.parquet_size_reporter import create_parquet_size_reporter
        
        size_reporter = create_parquet_size_reporter()
        
        # Analyze the generated parquet files
        duckdb_path = f"data/processed/{run_id}_duckdb/group_stats_duckdb.parquet"
        pandas_path = f"data/processed/{run_id}_pandas/group_stats_pandas.parquet"
        
        size_report = {
            "run_id": run_id,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "files": []
        }
        
        if os.path.exists(duckdb_path):
            duckdb_info = size_reporter.analyze_parquet_file(duckdb_path)
            size_report["files"].append({
                "path": duckdb_path,
                "size_mb": duckdb_info["size_mb"],
                "compression": duckdb_info.get("compression", "unknown"),
                "dict_encoding": duckdb_info.get("dictionary_encoding", False),
                "columns_pruned": 0
            })
        
        if os.path.exists(pandas_path):
            pandas_info = size_reporter.analyze_parquet_file(pandas_path)
            size_report["files"].append({
                "path": pandas_path,
                "size_mb": pandas_info["size_mb"],
                "compression": pandas_info.get("compression", "unknown"),
                "dict_encoding": pandas_info.get("dictionary_encoding", False),
                "columns_pruned": 0
            })
        
        # Save size report
        size_report_path = f"data/processed/{run_id}/parquet_size_report.json"
        os.makedirs(os.path.dirname(size_report_path), exist_ok=True)
        with open(size_report_path, 'w') as f:
            json.dump(size_report, f, indent=2)
        logger.info(f"Saved size report to {size_report_path}")
        
        return size_report
    except Exception as e:
        logger.warning(f"Failed to generate size report: {e}")
        return {"error": str(e)}

def create_settings_override(persist_artifacts: bool = True, run_parity: bool = False) -> Dict[str, Any]:
    """Create a transient settings override for benchmark runs."""
    import yaml
    
    # Load base settings
    with open("config/settings.yaml", 'r') as f:
        base_settings = yaml.safe_load(f)
    
    # Create override
    override_settings = base_settings.copy()
    
    # Override group_stats settings
    if "group_stats" not in override_settings:
        override_settings["group_stats"] = {}
    
    override_settings["group_stats"]["persist_artifacts"] = persist_artifacts
    override_settings["group_stats"]["run_parity_validation"] = run_parity
    
    return override_settings

def generate_benchmark_report(dataset_size: str, run_times: List[float], run_id: str, 
                            duckdb_settings: Dict[str, Any]) -> str:
    """Generate benchmark report markdown file."""
    median_time = sorted(run_times)[len(run_times) // 2]
    mean_time = sum(run_times) / len(run_times)
    
    # Get environment info
    env_info = {
        "duckdb_threads": duckdb_settings.get("threads", "auto"),
        "duckdb_memory": duckdb_settings.get("memory_limit"),
        "compression": "zstd",
        "dictionary_encoding": True,
        "row_group_size": duckdb_settings.get("row_group_size", 128000)
    }
    
    benchmark_content = f"""# Phase 1.35.4 Benchmark Report

**Generated**: {time.strftime("%Y-%m-%d %H:%M:%S")}  
**Dataset Size**: {dataset_size} ({len(run_times)} runs)  
**Backend**: DuckDB  
**Run ID**: {run_id}

## Performance Results

- **Run 1**: {run_times[0]:.3f}s
- **Run 2**: {run_times[1]:.3f}s
- **Run 3**: {run_times[2]:.3f}s
- **Median**: {median_time:.3f}s
- **Mean**: {mean_time:.3f}s
- **Target**: <50s (94K dataset)
- **Target Met**: {'✅ YES' if median_time < 50 else '❌ NO'}

## Environment

- **DuckDB Threads**: {env_info['duckdb_threads']}
- **DuckDB Memory**: {env_info['duckdb_memory']}
- **Compression**: {env_info['compression']}
- **Dictionary Encoding**: {env_info['dictionary_encoding']}
- **Row Group Size**: {env_info['row_group_size']}

## Memoization Performance

- **Cache Hit**: Run 1 (cold), Run 2+ (warm)
- **Speedup**: {((run_times[0] - median_time) / run_times[0] * 100):.1f}% improvement on subsequent runs
"""
    
    # Save benchmark report
    benchmark_path = "docs/reports/phase_1_35_4_benchmark.md"
    os.makedirs(os.path.dirname(benchmark_path), exist_ok=True)
    
    with open(benchmark_path, 'w') as f:
        f.write(benchmark_content)
    
    logger.info(f"Benchmark report saved to {benchmark_path}")
    return benchmark_path

def run_group_stats_parity(dataset_size: str, run_id: str) -> dict:
    """Run group stats parity validation (DuckDB vs pandas)."""
    logger.info(f"Starting parity validation for {dataset_size} dataset...")
    
    # Run DuckDB version
    logger.info("Running DuckDB backend...")
    duckdb_metrics = run_group_stats_benchmark(dataset_size, f"{run_id}_duckdb", "duckdb")
    
    # Run pandas version
    logger.info("Running pandas backend...")
    pandas_metrics = run_group_stats_benchmark(dataset_size, f"{run_id}_pandas", "pandas")
    
    # Generate parity report
    try:
        parity_report = generate_parity_report(run_id, dataset_size)
        logger.info(f"Parity report generated: {parity_report}")
    except Exception as e:
        logger.warning(f"Failed to generate parity report: {e}")
        parity_report = None

    # Generate size report for parity run
    try:
        size_report = generate_parquet_size_report(run_id)
        logger.info(f"Parity size report generated: {size_report}")
    except Exception as e:
        logger.warning(f"Failed to generate parity size report: {e}")
        size_report = None

    return {
        "duckdb": duckdb_metrics,
        "pandas": pandas_metrics,
        "parity_report": parity_report,  # This should be the actual report data
        "parity_report_path": f"data/processed/{run_id}_duckdb/parity_report_group_stats.json",
        "size_report": size_report
    }

def generate_parity_report(run_id: str, dataset_size: str) -> str:
    """Generate parity report comparing DuckDB and pandas outputs."""
    try:
        from src.utils.parity_validator import create_parity_validator
        
        # Check if both parquet files exist
        duckdb_path = f"data/processed/{run_id}_duckdb/group_stats_duckdb.parquet"
        pandas_path = f"data/processed/{run_id}_pandas/group_stats_pandas.parquet"
        
        if not os.path.exists(duckdb_path) or not os.path.exists(pandas_path):
            logger.warning(f"Parquet files not found for parity validation")
            return None
        
        # Load both files
        import pandas as pd
        df_duckdb = pd.read_parquet(duckdb_path)
        df_pandas = pd.read_parquet(pandas_path)
        
        # Create parity validator
        parity_validator = create_parity_validator()
        
        # Validate parity
        is_parity_valid, parity_report = parity_validator.validate_group_stats_parity(
            df_duckdb, df_pandas, run_id
        )
        
        # Save parity report
        parity_report_path = f"data/processed/{run_id}_duckdb/parity_report_group_stats.json"
        os.makedirs(os.path.dirname(parity_report_path), exist_ok=True)
        
        with open(parity_report_path, 'w') as f:
            json.dump(parity_report, f, indent=2)
        
        logger.info(f"Parity report saved to {parity_report_path}")
        return parity_report  # Return the actual report data, not the file path
        
    except Exception as e:
        logger.warning(f"Failed to generate parity report: {e}")
        return None

def main():
    parser = argparse.ArgumentParser(description="Run benchmark comparison")
    parser.add_argument("--dataset", choices=["1k", "5k", "10k", "94k"], required=True)
    parser.add_argument("--mode", choices=["alias", "group_stats", "parity"], default="alias", 
                       help="Benchmark mode: alias matching, group stats, or parity validation")
    parser.add_argument("--runs", type=int, default=3, help="Number of runs for group stats (default: 3)")
    parser.add_argument("--persist-group-stats-artifacts", choices=["true", "false"], default="true",
                       help="Whether to persist group stats artifacts (default: true)")
    args = parser.parse_args()

    if args.mode == "alias":
        # Original alias matching benchmark
        input_file = f"data/raw/company_junction_range_{args.dataset}.csv"

        # Run legacy pipeline
        logger.info(f"Running legacy pipeline on {args.dataset} dataset...")
        legacy_metrics = run_pipeline(
            input_file=input_file,
            run_id=f"{args.dataset}_bench_legacy",
            use_legacy=True,
        )

        # Run optimized pipeline
        logger.info(f"Running optimized pipeline on {args.dataset} dataset...")
        optimized_metrics = run_pipeline(
            input_file=input_file,
            run_id=f"{args.dataset}_bench_opt",
            use_legacy=False,
        )

        # Calculate speedup factors
        speedup = {
            "total_runtime": legacy_metrics["total_runtime"]
            / optimized_metrics["total_runtime"],
            "memory_ratio": (
                (optimized_metrics["memory_delta"] / legacy_metrics["memory_delta"])
                if legacy_metrics["memory_delta"] != 0
                else 1.0
            ),
        }

        # Combine results
        results = {
            "dataset_size": args.dataset,
            "mode": "alias",
            "legacy": legacy_metrics,
            "optimized": optimized_metrics,
            "speedup_factors": speedup,
        }

        # Save results
        output_file = f"data/benchmarks/comparison_{args.dataset}_alias.json"
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        with open(output_file, "w") as f:
            json.dump(results, f, indent=2)

        logger.info(f"Results saved to {output_file}")

        # Print summary
        print("\nBenchmark Results:")
        print(f"Dataset: {args.dataset}")
        print(f"Mode: {args.mode}")
        print("\nLegacy Pipeline:")
        print(f"  Total Runtime: {legacy_metrics['total_runtime']:.2f}s")
        print(f"  Memory Delta: {legacy_metrics['memory_delta']:.1f}MB")
        print(f"  Match Count: {legacy_metrics['match_count']}")
        print("\nOptimized Pipeline:")
        print(f"  Total Runtime: {optimized_metrics['total_runtime']:.2f}s")
        print(f"  Memory Delta: {optimized_metrics['memory_delta']:.1f}MB")
        print(f"  Match Count: {optimized_metrics['match_count']}")
        print("\nSpeedup Factors:")
        print(f"  Runtime: {speedup['total_runtime']:.1f}x")
        print(f"  Memory: {speedup['memory_ratio']:.1f}x")
    
    elif args.mode == "group_stats":
        # Group stats benchmark (multiple runs for median)
        logger.info(f"Running group stats benchmark on {args.dataset} dataset ({args.runs} runs)...")
        
        run_times = []
        run_id_base = f"{args.dataset}_group_stats_benchmark"
        
        for run_num in range(args.runs):
            run_id = f"{run_id_base}_run_{run_num + 1}"
            logger.info(f"Run {run_num + 1}/{args.runs}...")
            
            metrics = run_group_stats_benchmark(args.dataset, run_id, "duckdb")
            if metrics["runtime"]:
                run_times.append(metrics["runtime"])
                logger.info(f"Run {run_num + 1} completed in {metrics['runtime']:.3f}s")
            else:
                logger.warning(f"Run {run_num + 1} failed to capture timing")
        
        if run_times:
            median_time = sorted(run_times)[len(run_times) // 2]
            mean_time = sum(run_times) / len(run_times)
            
            results = {
                "dataset_size": args.dataset,
                "mode": "group_stats",
                "backend": "duckdb",
                "runs": args.runs,
                "timings": run_times,
                "median_time": median_time,
                "mean_time": mean_time,
                "target_seconds": 50 if args.dataset == "94k" else None,
                "target_met": median_time < 50 if args.dataset == "94k" else None
            }
            
            # Save results
            output_file = f"data/benchmarks/group_stats_{args.dataset}.json"
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            with open(output_file, "w") as f:
                json.dump(results, f, indent=2)
            
            logger.info(f"Results saved to {output_file}")
            
                    # Generate benchmark report
        try:
            # Get DuckDB settings from config
            import yaml
            with open("config/settings.yaml", 'r') as f:
                settings = yaml.safe_load(f)
            
            duckdb_settings = settings.get("engine", {}).get("duckdb", {})
            benchmark_path = generate_benchmark_report(args.dataset, run_times, run_id_base, duckdb_settings)
            logger.info(f"Benchmark report generated: {benchmark_path}")
        except Exception as e:
            logger.warning(f"Failed to generate benchmark report: {e}")
        
                # Generate parquet size report
        try:
            size_report = generate_parquet_size_report(run_id_base)
            logger.info(f"Size report generated: {size_report}")
        except Exception as e:
            logger.warning(f"Failed to generate size report: {e}")
            
            # Print summary
            print(f"\nGroup Stats Benchmark Results:")
            print(f"Dataset: {args.dataset}")
            print(f"Backend: DuckDB")
            print(f"Runs: {args.runs}")
            print(f"Timings: {[f'{t:.3f}s' for t in run_times]}")
            print(f"Median: {median_time:.3f}s")
            print(f"Mean: {mean_time:.3f}s")
            if args.dataset == "94k":
                print(f"Target: <50s")
                print(f"Target Met: {'✅ YES' if median_time < 50 else '❌ NO'}")
        else:
            logger.error("No successful runs completed")
            return 1
    
    elif args.mode == "parity":
        # Parity validation (DuckDB vs pandas)
        logger.info(f"Running group stats parity validation on {args.dataset} dataset...")
        
        # Create settings override for parity mode
        persist_artifacts = args.persist_group_stats_artifacts.lower() == "true"
        settings_override = create_settings_override(
            persist_artifacts=persist_artifacts,
            run_parity=True
        )
        
        # Set environment variables for the pipeline
        os.environ["CJ_GROUP_STATS_PERSIST_ARTIFACTS"] = str(persist_artifacts).lower()
        os.environ["CJ_GROUP_STATS_RUN_PARITY"] = "true"
        
        run_id = f"{args.dataset}_parity_validation"
        parity_results = run_group_stats_parity(args.dataset, run_id)
        
        # Save results
        output_file = f"data/benchmarks/parity_{args.dataset}.json"
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        with open(output_file, "w") as f:
            json.dump(parity_results, f, indent=2)
        
        logger.info(f"Parity results saved to {output_file}")
        
        # Print summary
        print(f"\nParity Validation Results:")
        print(f"Dataset: {args.dataset}")
        print(f"DuckDB Runtime: {parity_results['duckdb']['runtime']:.3f}s")
        print(f"Pandas Runtime: {parity_results['pandas']['runtime']:.3f}s")
        if parity_results['parity_report']:
            mismatches = parity_results['parity_report'].get('mismatches', 'unknown')
            print(f"Parity Mismatches: {mismatches}")
            print(f"Parity Status: {'✅ PASS' if mismatches == 0 else '❌ FAIL'}")
        else:
            print("Parity Report: Not generated")
    
    return 0


if __name__ == "__main__":
    main()
