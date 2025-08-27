# Cursor Prompt — Phase 1.6 Pagination, Rules Panel, JSON Aliases, Perf Logging (Review critically)

> **Ask:** Implement the items below **conservatively**. If anything conflicts with current architecture or you know a better approach, **push back with rationale** before coding.

Scope focuses on:
- **A) Streamlit** pagination + **Rules & Settings** visibility
- **B) JSON alias metadata** end‑to‑end (pipeline write + UI read)
- **D) Performance logging** for alias matching
(*No change requested for parentheses gating in this phase.*)

---

## Acceptance Checklist

- Streamlit shows: **page size selector**, **Prev/Next**, **“Page X of Y”** (via `st.session_state`), and a top **“Rules & Settings”** expander that displays:
  - thresholds & penalties from `config/settings.yaml`
  - blacklist & suspect phrases from `disposition.py` getters
  - a short summary of alias extraction rules (semicolon & numbered; filtered parentheses if already implemented)
- `review_ready.csv` writes **valid JSON** for `alias_candidates`, `alias_sources`, `alias_cross_refs`. Streamlit **parses JSON** and renders badges/expanders reliably.
- Pipeline logs include alias perf stats, e.g.:
  - `Alias pairs generated: N (capped: M blocks)`
  - `Alias matches accepted (score ≥ high & suffix match): K`

---

## Patch A — Streamlit pagination + Rules & Settings panel

**File:** `app/main.py`

```diff
*** Begin Patch
*** Update File: app/main.py
@@
-import streamlit as st
+import streamlit as st
+import json
+from pathlib import Path
+
+try:
+    import yaml
+except Exception:
+    yaml = None
@@
-# existing data load
+# existing data load
 @st.cache_data
 def load_review(path="data/processed/review_ready.csv"):
     import pandas as pd
-    return pd.read_csv(path)
+    df = pd.read_csv(path)
+    # Parse JSON alias fields if present (written as JSON strings by pipeline)
+    for col in ["alias_candidates", "alias_sources", "alias_cross_refs"]:
+        if col in df.columns:
+            try:
+                df[col] = df[col].apply(lambda x: json.loads(x) if isinstance(x, str) and x.startswith("[") else (x if isinstance(x, list) else []))
+            except Exception:
+                # fall back to empty list if parsing fails
+                df[col] = [[] for _ in range(len(df))]
+    return df
+
+@st.cache_data
+def load_settings(path="config/settings.yaml") -> dict:
+    if yaml is None:
+        return {}
+    try:
+        with open(path, "r") as fh:
+            return yaml.safe_load(fh) or {}
+    except Exception:
+        return {}
+
+def _safe_get(d, *keys, default=None):
+    cur = d
+    for k in keys:
+        if not isinstance(cur, dict) or k not in cur:
+            return default
+        cur = cur[k]
+    return cur
+
+def _get_disposition_terms():
+    # Import lazily to avoid heavy imports at module load
+    try:
+        from src.disposition import get_blacklist_terms, get_suspect_patterns
+        bl = get_blacklist_terms() if callable(get_blacklist_terms) else []
+        sp = get_suspect_patterns() if callable(get_suspect_patterns) else []
+        return bl, sp
+    except Exception:
+        return [], []
@@
-df = load_review()
+df = load_review()
+settings = load_settings()
+blacklist_terms, suspect_terms = _get_disposition_terms()
+
+# --- Rules & Settings panel (top) ---
+with st.expander("Rules & Settings", expanded=False):
+    st.write("**Thresholds**") 
+    st.json({
+        "similarity": {
+            "high": _safe_get(settings, "similarity", "high", default=None),
+            "medium": _safe_get(settings, "similarity", "medium", default=None),
+            "penalty": _safe_get(settings, "similarity", "penalty", default={}),
+        },
+        "blocking": _safe_get(settings, "blocking", default=None),
+        "max_alias_pairs": _safe_get(settings, "max_alias_pairs", default=None)
+    })
+    st.write("**Blacklist terms** (delete heuristics)")
+    st.code(", ".join(sorted(set(blacklist_terms))) or "(not exposed)")
+    st.write("**Suspect phrases** (verify heuristics)")
+    st.code(", ".join(sorted(set(suspect_terms))) or "(not exposed)")
+    st.write("**Alias extraction summary**") 
+    st.markdown("- Semicolons split multiple entities\n- Numbered sequences like `(1)`, `(2)` denote separate entities\n- Parentheses may be evaluated (conservative gating rules applied in pipeline)")
@@
-# filters (existing)
+# filters (existing)
 disp = st.multiselect("Disposition", options=sorted(df.get("Disposition", pd.Series([])).dropna().unique().tolist()), default=None)
@@
-# pagination
-total = f["group_id"].nunique()
-st.write(f"Groups matching filters: {total}" )
-if "page" not in st.session_state: st.session_state.page = 1
-max_page = max(1, (total + page_size - 1)//page_size)
-col1, col2, col3 = st.columns(3)
-with col1:
-    if st.button("Prev") and st.session_state.page > 1: st.session_state.page -= 1
-with col2:
-    st.write(f"Page {st.session_state.page} / {max_page}" )
-with col3:
-    if st.button("Next") and st.session_state.page < max_page: st.session_state.page += 1
+# pagination
+total = f["group_id"].nunique() if "group_id" in f.columns else len(f)
+st.write(f"Groups matching filters: {total}" )
+if "page" not in st.session_state: st.session_state.page = 1
+max_page = max(1, (total + page_size - 1)//page_size)
+col1, col2, col3 = st.columns(3)
+with col1:
+    if st.button("Prev") and st.session_state.page > 1: st.session_state.page -= 1
+with col2:
+    st.write(f"Page {st.session_state.page} / {max_page}" )
+with col3:
+    if st.button("Next") and st.session_state.page < max_page: st.session_state.page += 1
*** End Patch
```

---

## Patch B — Ensure alias metadata is JSON at write time

**Files:** `src/cleaning.py` (final write), `app/main.py` (already parses), optional Parquet write

```diff
*** Begin Patch
*** Update File: src/cleaning.py
@@
-import pandas as pd
+import pandas as pd
+import json
@@
-# before writing review_ready.csv (df_final is the final table)
+# before writing review_ready.csv (df_final is the final table)
+# Ensure alias metadata columns are serialized as JSON strings for CSV
+for _col in ["alias_candidates", "alias_sources", "alias_cross_refs"]:
+    if _col in df_final.columns:
+        df_final[_col] = df_final[_col].apply(lambda x: json.dumps(x) if isinstance(x, (list, dict)) else (x if isinstance(x, str) else json.dumps([])))
@@
-df_final.to_csv(output_path, index=False)
+df_final.to_csv(output_path, index=False)
+
+# Optional: also write a Parquet version for richer types in UI if desired
+try:
+    parquet_out = output_path.replace(".csv", ".parquet")
+    df_final.to_parquet(parquet_out, index=False)
+    logger.info("Also wrote Parquet review file: %s", parquet_out)
+except Exception as _e:
+    logger.info("Parquet write skipped: %s", _e)
*** End Patch
```

---

## Patch D — Performance logging for alias matching

**Files:** `src/alias_matching.py` (or wherever alias pairs are generated), `src/cleaning.py` (summary log)

> If alias pair generation happens inside `src/alias_matching.py`, add counters and return them; otherwise, adapt to the current location.

```diff
*** Begin Patch
*** Update File: src/alias_matching.py
@@
-def compute_alias_matches(df_norm, df_groups, cfg):
+def compute_alias_matches(df_norm, df_groups, cfg):
     """Compute alias matches and persist to data/interim/alias_matches.parquet"""
-    # existing logic ...
+    # existing logic ...
+    # Track simple metrics
+    total_pairs_generated = 0
+    capped_blocks = 0
+    accepted_matches = 0
+
+    # whenever you generate candidate pairs per block:
+    # total_pairs_generated += num_pairs_in_block
+    # if capped: capped_blocks += 1
+
+    # when a match is accepted (suffix match + score >= high):
+    # accepted_matches += 1
+
+    # return matches_df, {"pairs_generated": total_pairs_generated, "capped_blocks": capped_blocks, "accepted": accepted_matches}
+    return matches_df, {"pairs_generated": total_pairs_generated, "capped_blocks": capped_blocks, "accepted": accepted_matches}
*** End Patch
```

```diff
*** Begin Patch
*** Update File: src/cleaning.py
@@
-# after computing alias matches
-matches_df = compute_alias_matches(...)
+# after computing alias matches
+res = compute_alias_matches(...)
+if isinstance(res, tuple) and len(res) == 2:
+    matches_df, alias_stats = res
+else:
+    matches_df, alias_stats = res, {"pairs_generated": None, "capped_blocks": None, "accepted": None}
@@
-logger.info("Wrote review file to %s", output_path)
+logger.info("Wrote review file to %s", output_path)
+if alias_stats:
+    logger.info("Alias pairs generated: %s (capped blocks: %s)", alias_stats.get("pairs_generated"), alias_stats.get("capped_blocks"))
+    logger.info("Alias matches accepted (score ≥ high & suffix match): %s", alias_stats.get("accepted"))
*** End Patch
```

---

## Notes
- Keep Phase 1 read‑only. No merge actions in UI.
- If `yaml` is not available, the Rules panel will gracefully degrade and still show blacklist/suspect phrases when exposed by `disposition.py` getters.
- If you already have pagination or JSON parsing utilities, reuse them and discard any duplicate code introduced above.

---

## Commit Message
```
ui+pipeline: Phase 1.6 — pagination, rules panel, JSON alias fields, alias perf logging
```

