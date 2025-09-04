# Phase 1.35.4 Benchmark Report

**Generated**: 2025-09-04 10:04:21  
**Dataset Size**: 1k (3 runs)  
**Backend**: DuckDB  
**Run ID**: 1k_group_stats_benchmark

## Performance Results

- **Run 1**: 6.572s
- **Run 2**: 7.179s
- **Run 3**: 6.943s
- **Median**: 6.943s
- **Mean**: 6.898s
- **Target**: <50s (94K dataset)
- **Target Met**: ✅ YES

## Environment

- **DuckDB Threads**: auto
- **DuckDB Memory**: None
- **Compression**: zstd
- **Dictionary Encoding**: True
- **Row Group Size**: 128000

## Memoization Performance

- **Cache Hit**: Run 1 (cold), Run 2+ (warm)
- **Speedup**: -5.6% improvement on subsequent runs
