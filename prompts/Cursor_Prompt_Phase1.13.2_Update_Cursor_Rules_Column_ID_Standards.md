# Cursor_Prompt — Phase1.13.2 — Update `cursor_rules.md` with Column/ID Standards + QA Guardrails

**Goal:** Encode hard rules (column naming, Salesforce ID policy, utils locations, and code-quality gates) into `cursor_rules.md` so future work doesn’t rediscover them. Update the rules _safely_ (idempotent edits; no duplication), then run format/lint/type checks.

---

## 1) Critical Review / Pushback (quick)
Before editing, post a brief critique covering:
- Whether these rules are precise enough to prevent “Account ID” vs `account_id` churn.
- Any edge cases we missed (e.g., mixed 15/18-char IDs in the same dataset, case-only ID collisions in 15-char land).
- Confirmation that changes won’t conflict with existing tests/CI or Phase 1 acceptance criteria.

**Pause for my OK before writing to `cursor_rules.md`.**

---

## 2) Planned Edits to `cursor_rules.md` (append/update these sections)

> Perform **structure-aware** edits: if a section already exists, **update/merge** it; otherwise, **create** it. Avoid duplicate bullets/sections. Keep the doc’s tone/formatting consistent.

### A) **Canonical Column Naming Policy (Inputs vs Internal)**
- **Inputs (Salesforce exports)** may use **spaced, titled columns**: `Account ID`, `Account Name`, `Created Date`, `Relationship`.
- **Internal canonical columns** (snake_case, used throughout pipeline and tests):
  - `account_id` → **18-char canonical Salesforce ID** (see ID policy below)
  - `account_id_src` → original input ID (15 or 18) preserved for audit/UI
  - `account_name`
  - `created_date`
  - `relationship`
- **Renaming map** for ingestion (apply once, early in cleaning):
  - `{"Account ID": "account_id", "Account Name": "account_name", "Created Date": "created_date", "Relationship": "relationship"}`
- **Candidate pairs** must use: `id_a`, `id_b`, and `score` (plus any explain columns).
- **Never** use spaced/titled names inside `src/` modules; keep them at the ingestion boundary only.

### B) **Salesforce ID Canonicalization Policy**
- All IDs used in joins/grouping/survivorship must be **canonical 18-char Salesforce IDs**.
- Store original in `account_id_src` (trimmed string) for UI/audit; **never** use `account_id_src` for joins.
- 15→18 conversion uses the **Salesforce checksum algorithm** (3-char suffix, case-sensitive on the 15-char source).
- Canonicalization helper location: `src/utils/id_utils.py` with:
  - `sfid15_to_18(sfid15: str) -> str`
  - `normalize_sfid_series(series) -> pd.Series` (returns 18-char or validated 18-char pass-through).
- **Referential integrity** check before grouping: every `id_a`/`id_b` must exist in `accounts[account_id]` (canonical). Fail fast with helpful samples.
- **Uniqueness**: `accounts[account_id]` must be unique and non-null after canonicalization.

### C) **Utils Layout & Import Rules**
- All shared helpers live under `src/utils/` (no `src/utils.py` usage in code; the legacy file is archived under `deprecated/`).
- Modules: `path_utils.py`, `logging_utils.py`, `perf_utils.py`, `io_utils.py`, `validation_utils.py`, `dtypes.py`, `hash_utils.py`, `id_utils.py`.
- **Imports must be absolute and rooted at `src`**, e.g. `from src.utils.id_utils import normalize_sfid_series`.
- Do not introduce relative imports between top-level modules (`src/foo.py` ↔ `src/bar.py`) that create cycles; extract shared code into `src/utils/` instead.

### D) **Survivorship & Disposition (ID usage reminder)**
- Survivorship tie-breakers use canonical `account_id` for lexicographic comparison (after relationship rank and created date). Never compare using `account_id_src`.
- Disposition logic and UI explanations should reference canonical ID, with `account_id_src` shown as original where helpful.

### E) **QA Gates: Black / Ruff / MyPy (must run at phase end)**
- At the end of every Phase prompt/PR, run and require **all to pass**:
  - `black --check .`
  - `ruff .`
  - `mypy --config-file mypy.ini .`
  - `pytest -q`
- **MyPy configuration** (in `mypy.ini` or `pyproject.toml`):
  - `mypy_path = src`, `namespace_packages = True`, `ignore_missing_imports = False`
  - Prefer **absolute imports** (`from src...`) to avoid module path conflicts (e.g., `dtypes_map` vs `src.dtypes_map`).
- If third-party stubs are needed, add to `requirements-dev.txt` (e.g., `pandas-stubs`, `types-PyYAML`). “Stubs” supply type info only; they don’t affect runtime.

### F) **Logging, Perf, and Artifacts**
- Use `log_perf` (from `src/utils/perf_utils.py`) around each major stage.
- Keep run summaries clear: distinguish **hard duplicate rows removed** (same canonical `account_id`) vs **deduplication groups** (different `account_id`s believed to be same company).
- Artifacts:
  - `data/interim/*.parquet` (intermediate)
  - `data/processed/review_ready.*` (final for UI)
  - `data/processed/review_meta.json` (metadata)
  - `data/interim/block_top_tokens.csv` (blocking stats)

### G) **Edit Hygiene**
- Rules edits must be **idempotent**: if a rule exists, update/merge; do not duplicate.
- Keep terminology consistent with existing sections (Phase labels, bullets, formatting).

---

## 3) Implementation Steps
1. **Open** `cursor_rules.md` and apply the sections above, updating/merging where appropriate.
2. **Post a unified diff** (or quote the edited sections) for review.
3. After approval, run:
   - `black --check .`
   - `ruff .`
   - `mypy --config-file mypy.ini .`
   - `pytest -q`
4. If any gates fail, fix and re-run until green.
5. Update `CHANGELOG.md` under **Phase1.13.2 — Rules & Guardrails** summarizing the additions.

---

## 4) Deliverables
- Updated `cursor_rules.md` with the sections above (non-duplicative, consistent tone).
- Diff preview for my review before merge.
- All QA gates green (Black/Ruff/MyPy/PyTest).
- `CHANGELOG.md` entry for Phase1.13.2.

---

## 5) Communication Protocol
- Prefix all messages with **[Phase1.13.2]**.
- Pause after posting the **diff preview** for my approval before committing changes.
