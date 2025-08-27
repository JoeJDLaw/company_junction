# Cursor Patch Instructions — Update README.md and cursor_rules.md (and CHANGELOG.md)

These patches update documentation to reflect **Phase 1 completion**, ensure `cursor_rules.md` stays the source of truth, and add a formal **Phase 1 release entry** to `CHANGELOG.md`.

---

## Patch 1: Update `README.md`

**Goal:** Add Phase 1 status, quick-start instructions, and core summary.

```diff
*** Begin Patch
*** Update File: README.md
@@
 # Company Junction Deduplication Pipeline
 
+## Project Status
+**Phase 1 complete** ✅ — The system identifies likely duplicate Accounts, proposes a primary record using Relationship rank + Created Date, and assigns Disposition (`Keep`, `Update`, `Delete`, `Verify`). No Salesforce writes are performed in this phase.
+
+**Docs:** see `docs/DLaw_Company_Junction_Dedup_Plan.md` for the detailed plan and acceptance criteria.
+
+**Next:** Phase 2 (future) will add Split detection & parsing, optional LLM "real-company" classifier, and Salesforce sync steps.
+
+---
+
+## Phase 1 Quick Start
+
+### 1) Install
+```bash
+pip install -r requirements.txt
+```
+
+### 2) Run the pipeline
+```bash
+python src/cleaning.py \
+  --input data/raw/company_junction_range_01.csv \
+  --outdir data/processed \
+  --config config/settings.yaml
+```
+
+### 3) Review results in Streamlit (read-only)
+```bash
+streamlit run app/main.py
+```
+
+### 4) Where to look
+- **Interim artifacts:** `data/interim/`
+  - `accounts_normalized.parquet`
+  - `candidate_pairs.parquet` (if created)
+  - `groups.parquet`
+  - `dispositions.parquet`
+- **Final review file:** `data/processed/review_ready.csv`
+- **Config:** `config/settings.yaml`, `config/relationship_ranks.csv`
+
+---
+
@@
 ## Contributing
 - Follow PEP 8.
 - Run tests with `pytest` before committing.
 - Update `CHANGELOG.md` for significant changes.
 
+## Current Phase Summary
+- Legal-aware normalization preserves suffix differences (INC vs LLC, etc.)
+- Similarity scoring via RapidFuzz with configurable thresholds (`high=92`, `medium=84`)
+- Grouping with connected components (edges require suffix match + score ≥ medium)
+- Survivorship by Relationship rank → Created Date → Account ID
+- Disposition per record: `Keep`, `Update`, `Delete`, `Verify`
+- No Salesforce writes in Phase 1
+
*** End Patch
```

---

## Patch 2: Update `cursor_rules.md`

**Goal:** Append Phase 1 rules to keep Cursor aligned with implementation plan.

```diff
*** Begin Patch
*** Update File: cursor_rules.md
@@
 - Keep file paths relative to project root
+
+---
+
+## Phase 1 Rules (Company Junction Deduplication)
+
+- **Scope:** Read-only first pass review. No Salesforce writes. Split detection is **deferred to Phase 2**.
+- **Normalization (src/normalize.py):**
+  - Preserve legal suffix differences (INC vs LLC etc.).
+  - Map symbols: `&→and`, `/→space`, `-→space`, `@→at`, `+→plus`; collapse whitespace.
+  - Numeric style unify: `20-20`, `20/20`, `20 20` → `20 20`.
+  - Extract trailing suffix into `suffix_class`; compute `name_core` without suffix for candidate generation.
+- **Similarity (src/similarity.py):**
+  - Use RapidFuzz (`token_sort_ratio`, `token_set_ratio`) + Jaccard on `name_core`.
+  - Composite score: `0.45*ratio_name + 0.35*ratio_set + 20*jaccard`, with penalties:
+    - `suffix_mismatch: 25`
+    - `num_style_mismatch: 5`
+  - Thresholds (from `config/settings.yaml`): `high=92`, `medium=84`.
+  - If `suffix_match=False`, do not auto-accept; mark for **Verify**.
+  - Build groups as connected components where `suffix_match=True` and `score ≥ medium`.
+- **Survivorship (src/survivorship.py):**
+  - Primary selection order:
+    1) Lowest Relationship rank (from `config/relationship_ranks.csv`)
+    2) Earliest Created Date (Excel serials supported)
+    3) Smallest Account ID (lexicographic)
+  - Provide a lightweight `merge_preview_json` (no writebacks).
+- **Disposition (src/disposition.py):**
+  - Values: `Keep`, `Update`, `Delete`, `Verify`.
+  - `Delete` if name matches blacklist (`pnc is not sure`, `1099`, etc.).
+  - Suffix mismatch within a group ⇒ `Verify`.
+  - Primary in group ⇒ `Keep`; non-primary ⇒ `Update`.
+  - Singleton clean ⇒ `Keep`; suspicious ⇒ `Verify`.
+  - LLM gate is optional and **disabled by default** (Phase 2 consideration).
+- **Artifacts:**
+  - `data/interim/*.parquet` and `data/processed/review_ready.csv`.
+- **Tests:**
+  - Include unit tests for normalization, similarity, grouping/survivorship, and disposition. Match thresholds from config.
+
*** End Patch
```

---

## Patch 3: Update `CHANGELOG.md`

**Goal:** Insert a Phase 1 release entry at the top, following Keep a Changelog and SemVer.

```diff
*** Begin Patch
*** Update File: CHANGELOG.md
@@
 All notable changes to this project will be documented in this file.
 
 The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
 and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
 
+## [1.0.0] - 2025-08-27
+
+### Added
+- Legal-aware normalization (`src/normalize.py`)
+- Similarity scoring (`src/similarity.py`)
+- Grouping & survivorship (`src/grouping.py`, `src/survivorship.py`)
+- Disposition logic (`src/disposition.py`)
+- CLI orchestrator (`src/cleaning.py`)
+- Streamlit review UI (`app/main.py`)
+- Config updates (`config/settings.yaml`, `config/relationship_ranks.csv`)
+- Unit tests across modules
+
+### Changed
+- README.md and cursor_rules.md updated to reflect Phase 1 rules
+
 ## [Unreleased]
 
 ### Added
 - Initial project scaffolding with Cookiecutter Data Science structure
 - Basic data cleaning pipeline with duplicate detection
*** End Patch
```

---

### How to use
1) Open this file in Cursor.
2) Run each patch block in order (Patch 1, Patch 2, Patch 3).
3) Commit with a message like: `docs: Phase 1 status + rules; changelog 1.0.0`.
