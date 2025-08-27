# Cursor Prompt — Phase 1.3 Critical Enhancements & Streamlit UX (Please review critically)

> **Ask:** Apply the following improvements with a critical eye. If you disagree with any item or know a better approach, **push back with rationale and propose alternatives** before implementing.

This prompt targets: **blocking/recall**, **disposition clarity**, **Streamlit pagination & UX**, **consistency/DRY**, **observability**, and **tests** — while keeping Phase 1 read‑only.

---

## Goals

1) Increase recall of candidate pairs via broader, configurable **blocking** — while keeping cost manageable.  
2) Ensure **Update** records appear when appropriate (i.e., multi‑record groups form).  
3) Improve **Streamlit** as the main review surface: **pagination**, faster filtering, counters, and explicit **group rationale**.  
4) Centralize repeated logic into **utils** and config; reduce duplication.  
5) Strengthen **observability** (logging/metrics), **validation**, and **tests**.
6) Keep **Phase 1** read‑only and legally conservative (no merging, strict suffix rule).

---

## Patches

### Patch A — Similarity: broader blocking & explainability

**Files:** `src/similarity.py`, `config/settings.yaml`, `tests/test_similarity.py`

**Changes:**
1. Introduce **configurable blocking** strategies (union of keys):
   - `first_token` of `name_core`
   - `first_two_tokens` of `name_core`
   - `sorted_bigrams` of tokens from `name_core` (pairs of adjacent tokens, sorted)
   - `prefix_ngram` = first N non-space characters of `name_core` (default N=10)
   - Keep current strategy but make it **config-driven**; default enable: `first_token`, `first_two_tokens`, `prefix_ngram`.
2. Make **thresholds** tunable via config (already present) and add `max_pairs_per_block` (cap to avoid quadratic blowup; default 50k).
3. Add **pair diagnostics** (counts per strategy, dropped-by-cap) to logs and a small CSV `data/interim/pairing_stats.csv`.
4. Ensure we compute and persist a `why_score` string for top pairs: include major components (ratios, penalties applied).

**Config additions (append to `config/settings.yaml`):**
```yaml
blocking:
  strategies: ["first_token", "first_two_tokens", "prefix_ngram"]
  prefix_len: 10
  max_pairs_per_block: 50000
```

**Testing:** Update/create tests to verify that blocking produces more than the previous baseline on a crafted fixture and that performance caps apply.

---

### Patch B — Grouping: clearer rules & metadata

**File:** `src/grouping.py`

**Changes:**
- When constructing edges, continue to require `suffix_match=True` and `score >= medium`.
- Compute and persist per-group metadata to `data/interim/groups.parquet` and/or a companion JSON:
  - `group_id`, `size`, `max_score`, `min_score`, `avg_score`, `suffix_class`, `has_suffix_mismatch` (computed anyway), `reason_summary` (e.g., `"Edges based on score>=84 and matching suffix INC"`).

---

### Patch C — Survivorship: explicit rationale fields

**File:** `src/survivorship.py`

**Changes:**
- Add columns on the chosen primary and each non-primary:
  - `primary_reason` (e.g., `"lowest relationship rank (10)"` or `"tie on rank; earliest created date"`)
  - `tie_breaker_applied` (bool)
- Ensure `merge_preview_json` contains **only** field diffs (not entire rows) to keep the UI light.

---

### Patch D — Disposition: defensible outcomes

**File:** `src/disposition.py`, `tests/test_disposition.py`

**Changes:**
- Add a `disposition_reason` column with a compact, human-readable rationale (e.g., `"blacklist:pnc is not sure"`, `"group non-primary (suffix INC), score_to_primary=93"`,
  `"singleton clean"` , `"group suffix mismatch"`).
- Tighten blacklist heuristics: ensure tokens like `"temp"` / `"temporary"` are matched as **whole words** to avoid false positives (e.g., “Tempest”). Use a small regex word-boundary list.
- Keep LLM off; wire an **extension hook** `get_real_company_score(name)` that returns `None` when disabled (for Phase 2).

**Testing:** Add assertions around `disposition_reason` values.

---

### Patch E — Streamlit: pagination, counters, UX as primary review surface

**File:** `app/main.py`

**Changes:**
1. **Pagination** for group review:
   - Use `st.session_state` for `page`.
   - Page size selector (default 20). Buttons: **Prev/Next**, jump-to-page.
   - Show **"Displaying groups X–Y of Z"** label.
2. Fast filters:
   - Min score to primary (slider), disposition multi-select, suffix-mismatch toggle, group size range.
   - Text search over `Account Name` (simple `contains` filter).
3. Group header shows:
   - `group_id`, `size`, top score(s), suffix class, and a one-line `reason_summary`.
4. Rows show:
   - Badges for `suffix_mismatch`, `blacklist_hit` (if available), and display `disposition_reason`.
   - Pretty `merge_preview_json` collapsed by default.
5. Performance:
   - Lazy-load `data/processed/review_ready.csv` with a caching decorator.
   - Avoid re-computation in UI, operate on precomputed columns.
6. Export:
   - Export currently filtered rows to CSV (download button).

**Notes:** Keep Phase 1 read-only; no in-UI writebacks.

---

### Patch F — DRY & consistency

**Files:** `src/utils.py`, various

**Changes:**
- Centralize these helpers in `utils.py` (de-duplicate if found elsewhere):
  - `safe_read_csv`, `safe_write_csv`, `safe_read_parquet`, `safe_write_parquet` with schema enforcement.
  - `coerce_excel_serial_to_ts(val)` used by normalization & survivorship.
  - `slugify_key(s)` used by blocking.
  - `load_settings`, `load_relationship_ranks` (already exist; ensure single source).
- Add a `schemas.py` (optional) or constants in `utils.py` defining required and recommended columns for the pipeline; validate in `cleaning.py` and **fail fast** with clear messages.
- Validate that `settings.yaml` has all needed keys; fill with defaults as we already do.

---

### Patch G — Observability & run safety

**Files:** `src/cleaning.py`, `config/logging.conf`

**Changes:**
- Add a **run header** log with version (read from `CHANGELOG.md` top entry or a simple `__version__` constant), run timestamp, and selected thresholds.
- Log summary metrics at the end:
  - counts of groups, multi-record groups, disposition counts, candidate pairs above/below thresholds, pairs dropped by caps.
- On exceptions, write a `data/interim/last_error.txt` with a short traceback for triage.

---

## Code Diff Sketches

> These are targeted diffs; adjust surrounding context as needed.

### 1) `config/settings.yaml` additions
```diff
*** Begin Patch
*** Update File: config/settings.yaml
@@
 similarity:
   high: 92
   medium: 84
   penalty:
     suffix_mismatch: 25
     num_style_mismatch: 5
+
+blocking:
+  strategies: ["first_token", "first_two_tokens", "prefix_ngram"]
+  prefix_len: 10
+  max_pairs_per_block: 50000
*** End Patch
```

### 2) `src/similarity.py` — blocking strategies (excerpt)
```diff
*** Begin Patch
*** Update File: src/similarity.py
@@
-def _blocking_key_first_token(name_core: str) -> str:
-    return name_core.split()[0] if name_core else ""
+def _blocking_key_first_token(name_core: str) -> str:
+    return name_core.split()[0] if name_core else ""
+
+def _blocking_key_first_two_tokens(name_core: str) -> str:
+    toks = name_core.split()
+    return " ".join(toks[:2]) if len(toks) >= 2 else " ".join(toks)
+
+def _blocking_key_prefix_ngram(name_core: str, n: int) -> str:
+    return name_core.replace(" ", "")[:n] if name_core else ""
+
+def _blocking_keys_sorted_bigrams(name_core: str) -> list[str]:
+    toks = name_core.split()
+    bigrams = [" ".join(sorted([toks[i], toks[i+1]])) for i in range(len(toks)-1)]
+    return bigrams
@@
-def _generate_candidate_pairs(df_norm, cfg):
-    # existing: groupby first-token and make pairs
-    ...
+def _generate_candidate_pairs(df_norm, cfg):
+    strategies = set(cfg.get("blocking", {}).get("strategies", ["first_token"]))
+    prefix_len = int(cfg.get("blocking", {}).get("prefix_len", 10))
+    max_pairs = int(cfg.get("blocking", {}).get("max_pairs_per_block", 50000))
+
+    # Build blocks for each strategy and union the pairs
+    pair_set = set()
+    stats = []
+
+    def add_pairs_from_blocks(blocks, label):
+        nonlocal pair_set, stats
+        for key, idxs in blocks.items():
+            if len(idxs) < 2:
+                continue
+            # cap to avoid quadratic blow-ups
+            cap = max_pairs if len(idxs) * (len(idxs)-1) // 2 > max_pairs else None
+            pairs = []
+            count = 0
+            for i in range(len(idxs)):
+                for j in range(i+1, len(idxs)):
+                    pairs.append((idxs[i], idxs[j]))
+                    count += 1
+                    if cap and count >= cap:
+                        break
+                if cap and count >= cap:
+                    break
+            before = len(pair_set)
+            pair_set.update(pairs)
+            after = len(pair_set)
+            stats.append({"strategy": label, "block_key": key, "block_size": len(idxs), "pairs_added": after - before, "pairs_capped": 1 if cap else 0})
+
+    # Build blocks per strategy
+    if "first_token" in strategies:
+        blocks = df_norm.groupby(df_norm["name_core"].str.split().str[0].fillna("")).indices
+        add_pairs_from_blocks(blocks, "first_token")
+
+    if "first_two_tokens" in strategies:
+        blocks = df_norm.groupby(df_norm["name_core"].str.split().apply(lambda xs: " ".join(xs[:2]) if isinstance(xs, list) else "")).indices
+        add_pairs_from_blocks(blocks, "first_two_tokens")
+
+    if "prefix_ngram" in strategies:
+        blocks = df_norm.groupby(df_norm["name_core"].str.replace(" ", "", regex=False).str[:prefix_len]).indices
+        add_pairs_from_blocks(blocks, "prefix_ngram")
+
+    if "sorted_bigrams" in strategies:
+        # explode bigrams
+        bigrams = df_norm["name_core"].str.split().apply(lambda toks: [" ".join(sorted([toks[i], toks[i+1]])) for i in range(len(toks)-1)] if isinstance(toks, list) and len(toks) > 1 else [])
+        exploded = df_norm[["record_id"]].join(bigrams.rename("bg")).explode("bg")
+        blocks = exploded.groupby("bg").indices
+        add_pairs_from_blocks(blocks, "sorted_bigrams")
+
+    # Convert to DataFrame and continue with scoring as existing
+    # also write stats to data/interim/pairing_stats.csv
*** End Patch
```

### 3) `src/disposition.py` — reason strings and safer blacklist
```diff
*** Begin Patch
*** Update File: src/disposition.py
@@
-BLACKLIST = [...]
+BLACKLIST = [...]
+BLACKLIST_WORD_RE = re.compile(r"\b(pnc is not sure|pnc is unsure|unknown|no paystub|no paystubs|1099|none|n/a|tbd|test|sample|delete|do not use)\b", re.I)
@@
-def _is_blacklisted(name: str) -> bool:
-    s = name.lower()
-    return any(tok in s for tok in BLACKLIST)
+def _is_blacklisted(name: str) -> bool:
+    if not isinstance(name, str):
+        return False
+    return bool(BLACKLIST_WORD_RE.search(name))
@@
-# when assigning disposition:
-# return "Delete"/"Keep"/"Update"/"Verify"
+def _reason_delete(name: str) -> str:
+    m = BLACKLIST_WORD_RE.search(name or "")
+    return f"blacklist:{m.group(1) if m else 'rule'}"
+
+# when assigning disposition set a new column 'disposition_reason'
+
*** End Patch
```

### 4) `app/main.py` — pagination & UX (conceptual diff)
```diff
*** Begin Patch
*** Update File: app/main.py
@@
-import streamlit as st
+import streamlit as st
+from functools import lru_cache
@@
-# existing load
+# cached load
+@st.cache_data
+def load_review(path="data/processed/review_ready.csv"):
+    import pandas as pd
+    return pd.read_csv(path)
+
+df = load_review()
+
+# filters
+disp = st.multiselect("Disposition", options=sorted(df["Disposition"].unique().tolist()), default=None)
+min_score = st.slider("Min score to primary", 0, 100, 0)
+suffix_mismatch_only = st.checkbox("Suffix mismatch only", value=False)
+group_min, group_max = st.slider("Group size range", 1, int(df["group_size"].max() if "group_size" in df else 10), (1, int(df["group_size"].max() if "group_size" in df else 10)))
+search = st.text_input("Search Account Name contains")
+page_size = st.selectbox("Page size", [10,20,50,100], index=1)
+
+f = df.copy()
+if disp:
+    f = f[f["Disposition"].isin(disp)]
+if min_score:
+    f = f[f["score_to_primary"].fillna(0) >= min_score]
+if suffix_mismatch_only and "has_suffix_mismatch" in f.columns:
+    f = f[f["has_suffix_mismatch"]==True]
+if "group_size" in f.columns:
+    f = f[(f["group_size"]>=group_min) & (f["group_size"]<=group_max)]
+if search:
+    f = f[f["Account Name"].str.contains(search, case=False, na=False)]
+
+# pagination
+total = f["group_id"].nunique()
+st.write(f"Groups matching filters: {total}")
+if "page" not in st.session_state: st.session_state.page = 1
+max_page = max(1, (total + page_size - 1)//page_size)
+col1, col2, col3 = st.columns(3)
+with col1:
+    if st.button("Prev") and st.session_state.page > 1: st.session_state.page -= 1
+with col2:
+    st.write(f"Page {st.session_state.page} / {max_page}")
+with col3:
+    if st.button("Next") and st.session_state.page < max_page: st.session_state.page += 1
+
+start = (st.session_state.page-1)*page_size
+end = start + page_size
+page_group_ids = list(sorted(f["group_id"].unique()))[start:end]
+page_df = f[f["group_id"].isin(page_group_ids)]
+
+st.download_button("Export current view", data=page_df.to_csv(index=False).encode("utf-8"), file_name="review_filtered.csv")
+
+# render by group
+for gid, g in page_df.groupby("group_id"):
+    st.markdown(f"### Group {gid} (size={g.shape[0]})")
+    if "reason_summary" in g.columns:
+        st.caption(g["reason_summary"].iloc[0])
+    st.dataframe(g)
+    if "merge_preview_json" in g.columns:
+        with st.expander("Merge preview (diffs)"):
+            for _, row in g.iterrows():
+                st.json(row.get("merge_preview_json"))
*** End Patch
```

### 5) `src/cleaning.py` — metrics & error file
```diff
*** Begin Patch
*** Update File: src/cleaning.py
@@
 try:
     # existing pipeline
     ...
-    logger.info("Done.")
+    # summarize
+    import pandas as pd
+    disp = pd.read_parquet("data/interim/dispositions.parquet")
+    logger.info("Disposition counts: %s", disp["Disposition"].value_counts().to_dict())
+    logger.info("Wrote review file to data/processed/review_ready.csv")
 except Exception as e:
     logger.exception("Run failed")
+    import traceback, pathlib, time
+    pathlib.Path("data/interim").mkdir(parents=True, exist_ok=True)
+    with open("data/interim/last_error.txt", "w") as fh:
+        fh.write(time.strftime("%Y-%m-%d %H:%M:%S") + "\n")
+        fh.write(traceback.format_exc())
     raise
*** End Patch
```

---

## Acceptance Criteria

- **Recall**: On the existing fixture, blocking produces **≥ 25% more** candidate pairs while respecting `max_pairs_per_block` caps.  
- **Updates appear**: On `sample_test.csv`, at least one multi-record group yields non-primary records with `Update` (assuming data contains true duplicates with matching suffix).  
- **Explainability**: `candidate_pairs.parquet` includes `why_score`, and `dispositions.parquet` includes `disposition_reason`.  
- **Streamlit**: Pagination works; UI shows total groups, current page, and exports filtered view.  
- **DRY**: Common IO/helpers centralized; no duplicated implementations remain.  
- **Tests**: Updated unit tests pass; add a small test for pagination helper logic (pure function where possible) and blocking strategies.  
- **Observability**: `data/interim/pairing_stats.csv` written; logs contain summary metrics.

---

## Please review critically

If any of the above is non-idiomatic given our codebase, or you know a better pattern (e.g., using MinHash/LSH for blocking, or a different pagination approach), **propose the alternative** with pros/cons, estimated effort, and data impact before implementing.

