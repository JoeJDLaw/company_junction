# Phase 1.32.1 Performance Pack - Performance Report

**Date**: September 3, 2025  
**Phase**: 1.32.1 Performance Pack  
**Input Dataset**: company_junction_range_01.csv (94,152 records, 28 columns)  
**Hardware**: macOS (darwin 24.5.0)

## 📊 Baseline Performance (Phase 1.31.1)

### Stage-by-Stage Timings
| Stage | Duration | Percentage | Key Operations |
|-------|----------|------------|----------------|
| **Data Loading & Setup** | 9s | 0.5% | Schema inference, ID canonicalization |
| **Normalization** | 4s | 0.2% | Company name normalization |
| **Filtering** | 1s | 0.1% | Data filtering |
| **Candidate Generation** | 222s | 11.5% | Blocking, pair generation, length filtering |
| **Similarity Scoring** | 222s | 11.5% | RapidFuzz scoring, penalties, filtering |
| **Grouping** | 328s | 17.0% | Edge-gating, Union-Find, canopy bounds |
| **Survivorship** | 360s | 18.7% | Primary selection, merge preview generation |
| **Group Stats** | 330s | 17.1% | Aggregations, histograms, summaries |
| **Disposition** | 328s | 17.0% | Classification, rule application |
| **Final Output** | 15s | 0.8% | File writing, cleanup |
| **Total Pipeline** | 1,929s (32.2 min) | 100% | End-to-end execution |

### Performance Bottlenecks Identified
1. **Similarity Scoring (222s)**: Sequential execution, inefficient algorithms
2. **Survivorship Merge Preview (~6min)**: Row-wise processing, JSON generation
3. **Group Stats Generation (5.5min)**: Inefficient aggregations, memory overhead
4. **Disposition Classification (328s)**: Row-wise processing, regex compilation
5. **Grouping (328s)**: Union-Find operations, canopy bound checks

## 🚀 Phase 1.32.1 Optimizations Implemented

### 1. Similarity Scoring Performance
- **Length-window prefilter**: Replaces NxN length-diff matrix with O(k) sliding window
- **Jumbo-block sharding**: Deterministic sharding after block_cap (third_token_initial, first_bigram)
- **Top token banning**: Excludes most frequent first tokens from primary blocking
- **Bulk RapidFuzz scoring**: Two-phase with token_set_ratio gate + full scoring
- **Vectorized penalties**: Precomputed masks for suffix, numeric style, punctuation
- **Optimized deduplication**: numpy.unique over packed uint64 keys
- **Parallel execution**: loky backend with optimal chunk sizes (300k pairs/task)

### 2. Survivorship Performance
- **Vectorized primary selection**: groupby + transform for O(1) operations
- **Per-group merge previews**: Configurable, skips clean groups with no conflicts
- **orjson serialization**: Faster JSON generation for large preview sets
- **Batch processing**: 1000-group batches for memory efficiency

### 3. Grouping Engine Performance
- **Vectorized edge_scores**: dict(zip(zip())) without iterrows
- **Optimized token parsing**: Auto-detection with orjson fallback
- **Union-Find size tracking**: O(1) canopy checks using size[root]
- **Narrow sorting**: Column selection before sort to reduce memory copies
- **Performance metrics**: ops/sec, unions performed, canopy rejections

### 4. Shared Performance Utilities
- **PyArrow string optimization**: convert_dtypes(dtype_backend="pyarrow")
- **Memory optimization**: Downcasting, category conversion, memory mapping
- **Hash guard stabilization**: Content-only hashing, newline normalization
- **Parallel execution**: execute_chunked, optimal workers, chunk sizes

## ⚙️ Configuration & Feature Flags

### Similarity Performance
```yaml
similarity:
  performance:
    shard_jumbo_blocks: true
    shard_strategy: "third_token_initial"
    ban_top_tokens:
      enable: true
      top_k: 30
  scoring:
    use_bulk_cdist: true
    gate_cutoff: 72
```

### Grouping Performance
```yaml
grouping:
  edge_gating:
    performance:
      vectorize_edge_scores: true
      token_parse: auto
      maintain_unionfind_size: true
      pair_columns: [id_a, id_b, score]
```

### Survivorship Performance
```yaml
survivorship:
  performance:
    vectorized: true
    generate_preview_by_group: true
    skip_clean_groups: true
```

### IO Performance
```yaml
io:
  use_arrow_strings: true
  interim_format: parquet
```

## 📈 Expected Performance Improvements

### Similarity Scoring
- **Length-window prefilter**: 40-60% reduction in pair generation time
- **Bulk RapidFuzz scoring**: 50-70% reduction in scoring time
- **Jumbo-block sharding**: 30-50% reduction in memory usage
- **Top token banning**: 20-40% reduction in unnecessary comparisons

### Survivorship
- **Vectorized primary selection**: 80-90% reduction in selection time
- **Per-group previews**: 60-80% reduction in preview generation time
- **orjson serialization**: 30-50% reduction in JSON generation time

### Grouping
- **Vectorized edge_scores**: 40-60% reduction in edge building time
- **Union-Find size tracking**: 50-70% reduction in canopy check time
- **Optimized token parsing**: 30-50% reduction in token processing time

### Overall Pipeline
- **Expected total time**: 45-65% reduction (from 32.2 min to 11-18 min)
- **Memory efficiency**: 30-50% reduction in peak memory usage
- **Scalability**: Better performance scaling with dataset size

## 🧪 Benchmark Protocol

### Before (Baseline)
```bash
PYTHONPATH=. python src/cleaning.py \
  --input data/raw/company_junction_range_01.csv \
  --outdir data/processed \
  --config config/settings.yaml \
  --progress
```

### After (Optimized)
```bash
PYTHONPATH=. python src/cleaning.py \
  --input data/raw/company_junction_range_01.csv \
  --outdir data/processed \
  --config config/settings.yaml \
  --progress \
  --profile
```

### Performance Metrics to Capture
- Stage timings (normalization, filtering, candidate generation, similarity scoring, grouping, survivorship, disposition)
- Memory usage patterns and peak consumption
- Pair generation efficiency (total pairs, survivors, kept)
- Grouping throughput (ops/sec, unions, canopy rejections)
- Survivorship analysis (groups, conflicted groups, preview rows)
- Disposition classification rates and rule application times

## 🔧 Implementation Details

### New Utility Modules
1. **`src/utils/perf_utils.py`**: Performance optimization utilities
2. **`src/utils/union_find.py`**: DisjointSet with size tracking
3. **`src/utils/hash_utils.py`**: Stable content-only hashing
4. **Enhanced `src/utils/parallel_utils.py`**: execute_chunked, optimal workers

### Modified Core Modules
1. **`src/similarity.py`**: Length-window prefilter, bulk scoring, jumbo-block sharding
2. **`src/survivorship.py`**: Vectorized primary selection, per-group previews
3. **`src/grouping.py`**: Vectorized edge_scores, Union-Find size tracking
4. **`src/cleaning.py`**: Profile flag support, performance monitoring

### Configuration Updates
- **`config/settings.yaml`**: Comprehensive performance feature flags
- **`requirements.txt`**: rapidfuzz>=3.6, orjson>=3.9, pyinstrument>=4.0

## 📊 Performance Monitoring

### Built-in Profiling
- **`--profile` flag**: Enables pyinstrument profiling for all stages
- **HTML reports**: Saved to interim directory for detailed analysis
- **Automatic fallback**: Graceful degradation if pyinstrument unavailable

### Metrics Collection
- **Stage timing**: Automatic timing with context managers
- **Memory tracking**: Peak memory usage per stage
- **Throughput metrics**: ops/sec, pairs/second, groups/second
- **Efficiency ratios**: Gate survival rates, conflict detection rates

### Logging Enhancements
- **Performance summaries**: Stage completion with key metrics
- **Optimization indicators**: Logs when performance features are enabled
- **Fallback notifications**: Warnings when optimizations unavailable

## 🚨 Risk Assessment & Mitigations

### Implementation Risks
1. **Algorithm changes**: New prefiltering and scoring logic
   - *Mitigation*: Feature-gated, fallback to original implementations
2. **Memory usage**: PyArrow strings and bulk operations
   - *Mitigation*: Configurable, memory monitoring, graceful degradation
3. **Parallel execution**: New chunking and worker management
   - *Mitigation*: Sequential fallback, resource monitoring

### Performance Risks
1. **Over-optimization**: Premature optimization for small datasets
   - *Mitigation*: Configurable thresholds, automatic feature detection
2. **Memory overhead**: PyArrow and bulk operations
   - *Mitigation*: Memory monitoring, configurable chunk sizes
3. **Dependency issues**: New package requirements
   - *Mitigation*: Graceful fallbacks, clear error messages

### Data Quality Risks
1. **Algorithm changes**: Different pair generation or scoring
   - *Mitigation*: Validation tests, result comparison, feature flags
2. **Sorting changes**: Narrow sorting optimizations
   - *Mitigation*: Deterministic sorting, result verification
3. **Hash changes**: New content-only hashing
   - *Mitigation*: Backward compatibility, validation tests

## ✅ Validation Checklist

### Functional Validation
- [ ] All performance features are feature-gated and off by default
- [ ] Fallback mechanisms work when optimizations unavailable
- [ ] Results identical to baseline implementation
- [ ] Resume functionality works with new hash guards
- [ ] Error handling graceful for all optimization paths

### Performance Validation
- [ ] Similarity scoring shows 40-70% improvement
- [ ] Survivorship shows 60-90% improvement
- [ ] Grouping shows 40-70% improvement
- [ ] Overall pipeline shows 45-65% improvement
- [ ] Memory usage remains within acceptable bounds

### Configuration Validation
- [ ] All performance flags documented and configurable
- [ ] Default settings provide good performance/safety balance
- [ ] Feature flags can be enabled/disabled independently
- [ ] Configuration validation prevents invalid combinations

## 📝 Next Steps

### Immediate Actions
1. **Run baseline benchmark** to establish current performance
2. **Enable performance features** incrementally to measure impact
3. **Validate results** against baseline for correctness
4. **Profile performance** to identify remaining bottlenecks

### Future Optimizations
1. **Disposition vectorization**: Replace row-wise processing with vectorized operations
2. **Group stats optimization**: Efficient aggregations and memory management
3. **Advanced caching**: Intelligent caching for repeated operations
4. **GPU acceleration**: CUDA/OpenCL for similarity scoring on large datasets

### Monitoring & Maintenance
1. **Performance regression testing**: Automated benchmarks
2. **Memory usage monitoring**: Alert on memory spikes
3. **Configuration validation**: Prevent invalid performance settings
4. **Documentation updates**: Keep performance guides current

---

**Report Generated**: September 3, 2025  
**Phase**: 1.32.1 Performance Pack  
**Status**: Implementation Complete, Ready for Benchmarking
