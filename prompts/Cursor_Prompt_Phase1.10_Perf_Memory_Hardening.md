# Cursor Prompt — Phase 1.10 Performance & Memory Hardening (please review critically)

> **Ask:** Apply the changes below with a strong focus on memory-safety and throughput. If you disagree with any item or know a better approach, **push back with rationale** before coding. Keep Phase 1 read-only.

## Context
We hit memory blow-ups due to giant blocks and index drift after filtering. We’ve already reduced pairs (≈1.6M ≥ medium) and fixed several issues. This phase makes the pipeline reliably fast and memory-tolerant at ~100k rows.

---

## Goals
1) **Safe, bounded blocking** with frequency caps and stop-token hygiene.  
2) **Index-robust grouping** using dense integer ids throughout union-find.  
3) **Streaming/scoped memory**: avoid holding unnecessary columns, spill debug artifacts for post-mortem.  
4) **Instrumentation**: log block size distribution, pair counts per strategy, peak memory & timings.

---

## A) Config additions (`config/settings.yaml`)
```yaml
blocking:
  strategies: ["first_two_tokens", "prefix_ngram"]   # keep existing defaults if already present
  prefix_len: 10
  max_pairs_per_block: 200000         # smaller cap per block to prevent blowups
  max_pairs_total: 5000000            # hard upper bound across all blocks
  max_block_size: 2500                # if a block exceeds, downsample or skip with warning
  min_token_len: 2                    # ignore tokens shorter than this (e.g., 'a')
  stop_tokens: ["inc", "llc", "ltd"]  # never use as first/second blocking tokens
  drop_top_freq_tokens: 5             # drop top-k most frequent first tokens from blocking (optional)
hygiene:
  drop_numeric_only: true             # ^\d+$
  drop_single_letter: true            # ^[a-zA-Z]$
  drop_placeholders: true             # test|sample|temp|unknown|n/?a|none|tbd
  drop_exact_1099: true               # ^1099$
logging:
  perf: true

B) Similarity — safer blocking & bounded pairs (src/similarity.py)

Changes:
	1.	Token hygiene: When deriving the first/second token from name_core:
	•	Lowercase, strip punctuation, collapse whitespace.
	•	Skip tokens in stop_tokens or with len < min_token_len.
	2.	Block frequency controls:
	•	Compute frequency of first and (first,second) tokens.
	•	If drop_top_freq_tokens>0, remove the top-K most frequent first tokens from blocking entirely.
	•	If a block’s size > max_block_size, downsample pairs from that block to not exceed max_pairs_per_block (record pairs_capped=1).
	3.	Global pair cap:
	•	Maintain a running counter of pairs; if max_pairs_total would be exceeded, stop adding more and log a warning.
	4.	Memory shaping:
	•	Generate pairs as np.ndarray of int32 ids (not Python tuples) to reduce overhead, then build a DataFrame once per strategy (or concatenate in chunks).
	•	Only carry the ids and needed score inputs forward; drop wide string columns until UI stage.
	5.	Stats:
	•	Write data/interim/block_stats.csv with: strategy, block_key, block_size, pairs_generated, pairs_capped.
	•	Log top-20 block keys by size.

Note: Keep suffix-match requirement and score cutoffs unchanged.

C) Grouping — dense ids & union-find optimization (src/grouping.py)

Changes:
	1.	Dense id mapping:
	•	Before building edges, create id_map: record_id -> 0..N-1 (np.int32) and rev_map array.
	•	Convert edge endpoints to these dense ints once (edges_src, edges_dst as np.int32).
	2.	Union-find arrays:
	•	Use parent = np.arange(N, dtype=np.int32), rank = np.zeros(N, dtype=np.int8); path compression + union by rank.
	•	Iterate over edges_src/dst arrays directly (vector-friendly loop).
	3.	Index safety:
	•	Validate that all edge ids exist in id_map; if not, drop with a warning counter (write grouping_dropped_edges.txt with first 100 examples).
	4.	Output:
	•	Build group_id via root parent; compress roots to 0..G-1 for compactness.
	•	Persist groups.parquet with record_id, group_id, group_size, max_score, etc.

⸻

D) Memory & scope hygiene

Changes across stages:
	•	At each step, select only required columns into new DataFrames; avoid dragging wide text columns.
	•	Explicit dtypes: use int32 for ids, uint8/float32 for small counts/scores where possible.
	•	After writing intermediates, del df; gc.collect() (import gc) to prompt memory return.
	•	Consider pd.Categorical for small discrete columns like suffix_class.

⸻

E) Instrumentation (src/utils.py + integrated)
	•	Add log_perf(label) context manager:

    from contextlib import contextmanager
import time, tracemalloc, logging
@contextmanager
def log_perf(label: str):
    logger = logging.getLogger(__name__)
    tracemalloc.start()
    t0 = time.time()
    try:
        yield
    finally:
        current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        dt = time.time() - t0
        logger.info("%s: time=%.2fs, mem_current=%.1f MB, mem_peak=%.1f MB",
                    label, dt, current/1e6, peak/1e6)
	•	Wrap major stages: blocking, scoring, grouping.
	•	Summarize in logs + write a compact data/interim/perf_summary.json.

⸻

F) Tests
	•	Block hygiene: Verify that numeric-only / single-character first tokens are filtered from blocking keys.
	•	Top-K drop: Craft a small fixture where one token dominates frequency and confirm it’s removed when enabled.
	•	Max caps: Ensure per-block and global caps are enforced (stats reflect capping).
	•	Union-find dense ids: Test non-sequential input ids; assert grouping matches expected components.
	•	Perf harness (optional lightweight): Run blocking on a synthetic 10k rows and assert runtime < configurable threshold (skippable on CI if flaky).

⸻

Acceptance Criteria
	•	Blocking never emits more than max_pairs_total pairs; large blocks are capped or downsampled with clear stats.
	•	Top-20 largest blocks logged; block_stats.csv exists.
	•	No index errors: grouping works with non-sequential record ids via dense mapping.
	•	Peak memory/time for blocking and grouping are logged via log_perf.
	•	Intermediates avoid dragging unused columns; memory releases between stages.
	•	All new tests pass.

⸻

Optional UI (very small)
	•	In “Rules & Settings”, display top-5 block keys and sizes (read block_stats.csv if present) and total pairs generated vs cap.
	•	This is read-only and helps reviewers understand why some names cluster heavily.

⸻
