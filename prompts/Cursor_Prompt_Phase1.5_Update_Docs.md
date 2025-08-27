# Cursor Patch — Update Docs for Phase 1.5 (Conservative Aliases & Minimal UI)

This applies documentation updates to reflect the refined **Phase 1.5** approach:
- Conservative alias extraction (semicolon, numbered sequences, **filtered** parentheses)
- High-confidence alias matching (≥ high threshold, suffix match)
- Performance caps
- Minimal UI additions (alias badge + filter), settings panel deferred to Phase 2

---

## Patch 1 — Update `cursor_rules.md`

```diff
*** Begin Patch
*** Update File: cursor_rules.md
@@
 ## Phase 1 Rules (Company Junction Deduplication)
@@
 - **Disposition (src/disposition.py):**
   - Values: `Keep`, `Update`, `Delete`, `Verify`.
   - `Delete` if name matches blacklist (`pnc is not sure`, `1099`, etc.).
   - Suffix mismatch within a group ⇒ `Verify`.
   - Primary in group ⇒ `Keep`; non-primary ⇒ `Update`.
   - Singleton clean ⇒ `Keep`; suspicious ⇒ `Verify`.
   - LLM gate is optional and **disabled by default** (Phase 2 consideration).
 - **Artifacts:**
   - `data/interim/*.parquet` and `data/processed/review_ready.csv`.
 - **Tests:**
   - Include unit tests for normalization, similarity, grouping/survivorship, and disposition. Match thresholds from config.
+
+---
+
+## Phase 1.5 Refinements (Conservative Aliases & Minimal UI)
+
+- **Normalization (src/normalize.py):**
+  - Underscore normalization: drop leading/trailing underscores; collapse multiple underscores to one space.
+  - Parentheses are **preserved** for display; flagged with `has_parentheses`.
+  - **Do not** strip commas or periods globally.
+  - Multi-name indicators:
+    - `has_semicolon`: raw name contains `;`
+    - `has_numbered_series`: numbered markers like `(1)`, `(2)`
+    - `has_multi_names`: union of the above; used to flag, not split.
+  - Parentheses **alias candidates** are created **only when** content contains a legal suffix token (INC/LLC/LTD/CORP/…) **or** multiple capitalized words.
+  - Parenthetical blacklist: phrases like `paystub`, `pay stubs`, `not sure`, `unsure`, `unknown`, `staffing agency`, numbers-only → **never** create aliases.
+
+- **Aliases (src/normalize.py, src/similarity.py):**
+  - Extract aliases from **semicolons** and **numbered sequences** by default; add filtered parentheses per rule above.
+  - Alias matching uses the **same similarity** function but requires:
+    - Suffix match
+    - Score ≥ **high** threshold
+  - Results are **cross-links only** (no regrouping/merging). Store in `data/interim/alias_matches.parquet` and surface minimal metadata in `review_ready.csv`.
+  - Performance guard: cap alias pair generation via config (e.g., `max_alias_pairs`).
+
+- **Disposition (src/disposition.py):**
+  - Records with one or more valid alias matches default to **`Verify`** with `disposition_reason="alias match"` (unless they are `Delete` from blacklist).
+  - Multi-name indicators alone (without valid alias matches) also bias to `Verify`.
+
+- **UI (app/main.py):**
+  - Minimal additions for Phase 1.5: show an **alias badge** with a simple expander, and a **“Has aliases”** filter.
+  - Defer the full settings/rules panel to Phase 2.
+
+- **Config (config/settings.yaml):**
+  - Keep existing thresholds.
+  - Ensure a small `similarity.penalty.punctuation_mismatch` (default `3`) remains conservative.
+  - Add `max_alias_pairs` guard if not present.
*** End Patch
```

---

## Patch 2 — Update `CHANGELOG.md`

```diff
*** Begin Patch
*** Update File: CHANGELOG.md
@@
 The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
 and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
 
+## [1.1.0] - 2025-08-27
+
+### Added
+- **Conservative alias extraction** (semicolon, numbered sequences; parentheses only when content contains a legal suffix or multiple capitalized words)
+- **Alias matching** with high-confidence gating (suffix match + score ≥ high), cross-links only; writes `alias_matches.parquet` and minimal metadata to `review_ready.csv`
+- **Minimal UI** support: alias badge/expander and “Has aliases” filter in Streamlit
+- **Performance safeguards** for alias comparisons (config cap)
+
+### Changed
+- Parentheses handling is **preserved and flagged** by default (no blanket alias creation)
+- Punctuation normalization remains conservative (no global comma/period stripping)
+
 ## [Unreleased]
*** End Patch
```

---

## Patch 3 — Update `README.md`

```diff
*** Begin Patch
*** Update File: README.md
@@
 ## Project Status
-**Phase 1 complete** ✅ — The system identifies likely duplicate Accounts, proposes a primary record using Relationship rank + Created Date, and assigns Disposition (`Keep`, `Update`, `Delete`, `Verify`). No Salesforce writes are performed in this phase.
+**Phase 1 complete** ✅ — The system identifies likely duplicate Accounts, proposes a primary record using Relationship rank + Created Date, and assigns Disposition (`Keep`, `Update`, `Delete`, `Verify`). No Salesforce writes are performed in this phase.
+
+**Phase 1.5 (refinements)**: Conservative alias extraction (semicolon & numbered sequences; parentheses only when content looks like a company), high-confidence alias matching (suffix match + score ≥ high), minimal Streamlit alias UI.
@@
 ### 4) Where to look
 - **Interim artifacts:** `data/interim/`
   - `accounts_normalized.parquet`
   - `candidate_pairs.parquet` (if created)
   - `groups.parquet`
   - `dispositions.parquet`
+  - `alias_matches.parquet` (Phase 1.5)
 - **Final review file:** `data/processed/review_ready.csv`
 - **Config:** `config/settings.yaml`, `config/relationship_ranks.csv`
@@
 ## Current Phase Summary
 - Legal-aware normalization preserves suffix differences (INC vs LLC, etc.)
 - Similarity scoring via RapidFuzz with configurable thresholds (`high=92`, `medium=84`)
 - Grouping with connected components (edges require suffix match + score ≥ medium)
 - Survivorship by Relationship rank → Created Date → Account ID
 - Disposition per record: `Keep`, `Update`, `Delete`, `Verify`
 - No Salesforce writes in Phase 1
+
+### Phase 1.5 Highlights
+- Conservative **alias extraction** (semicolon, numbered sequences; filtered parentheses)
+- **Alias matching** is cross-link only (no regrouping), requires high-confidence match
+- **Minimal Streamlit** updates to surface aliases without overwhelming the UI
*** End Patch
```

---

### Commit message suggestion

```
docs: update rules, changelog, and README for Phase 1.5 (conservative aliases & minimal UI)
```

