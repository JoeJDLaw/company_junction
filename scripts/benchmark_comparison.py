#!/usr/bin/env python3
"""Benchmark comparison between legacy and optimized alias matching paths."""

import argparse
import json
import logging
import os
import time
from pathlib import Path

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

def main():
    parser = argparse.ArgumentParser(description="Run benchmark comparison")
    parser.add_argument("--dataset", choices=["1k", "5k", "10k"], required=True)
    args = parser.parse_args()
    
    # Define input file based on dataset size
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
        "total_runtime": legacy_metrics["total_runtime"] / optimized_metrics["total_runtime"],
        "memory_ratio": (optimized_metrics["memory_delta"] / legacy_metrics["memory_delta"]) 
            if legacy_metrics["memory_delta"] != 0 else 1.0,
    }
    
    # Combine results
    results = {
        "dataset_size": args.dataset,
        "legacy": legacy_metrics,
        "optimized": optimized_metrics,
        "speedup_factors": speedup,
    }
    
    # Save results
    output_file = f"data/benchmarks/comparison_{args.dataset}.json"
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, "w") as f:
        json.dump(results, f, indent=2)
    
    logger.info(f"Results saved to {output_file}")
    
    # Print summary
    print("\nBenchmark Results:")
    print(f"Dataset: {args.dataset}")
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

if __name__ == "__main__":
    main()
