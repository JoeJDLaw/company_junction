# Phase 1.35.4 Benchmark Report

**Generated**: 2025-09-04 12:31:32  
**Dataset Size**: 1k (3 runs)  
**Backend**: DuckDB  
**Run ID**: 1k_group_stats_benchmark

## Performance Results

- **Run 1**: 7.003s
- **Run 2**: 6.996s
- **Run 3**: 7.001s
- **Median**: 7.001s
- **Mean**: 7.000s
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
- **Speedup**: 0.0% improvement on subsequent runs
