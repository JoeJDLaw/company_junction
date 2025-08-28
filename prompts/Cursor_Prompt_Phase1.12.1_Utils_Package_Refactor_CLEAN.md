# Cursor_Prompt — Phase1.12 — Utils Package Refactor & Codebase Extraction

## 1) Critical Review / Pushback (before changes)
**Goal:** Provide a short critical review of the requested utils refactor. Include:
- Risks (import breakage, circular imports, accidental shadowing, test brittleness).
- Alternatives (e.g., rename-only vs. extraction-first; keep `common.py` now vs. split immediately).
- Scope suggestions:
  - Use a **single `src/utils/` package** as the long-term home for shared helpers.
  - Split utils logically into **small modules** to reduce coupling (e.g., `path_utils.py`, `logging_utils.py`, `perf_utils.py`, `io_utils.py`, `validation_utils.py`, `string_utils.py`).
- Industry best practices:
  - Avoid duplicate top-level names that collide with package names (no `utils.py` next to a `utils/` package).
  - Keep imports **explicit** (`from src.utils.path_utils import ensure_directory_exists`) to aid static analysis.
  - Avoid giant “kitchen sink” modules.
  - Add **tests** for moved functions to ensure behavior stability.
- **Wait for my approval**. Do not implement until I confirm.

---

## 2) Implementation Plan (after approval) — High-Level
**Objectives:**
- Eliminate the ambiguity of having both `src/utils.py` and `src/utils/` package.
- Extract reusable helpers from `src/utils.py` **and** identify other utility-like functions across the codebase for consolidation into `src/utils/` as appropriate.
- **Update all imports directly; no shims or backward-compatibility layers should be left in place.**
- Move obsolete/unused files to a top-level `./deprecated/` folder.

**Guardrails:**
- Keep changes **incremental**, **reviewable**, and **reversible** (copy → rewire → remove original).
- Preserve existing behavior (no semantic changes) unless explicitly approved.
- Avoid name collisions and circular imports; prefer smaller modules.

**Proposed module map inside `src/utils/` (adapt to reality):**
- `__init__.py` (explicit re-exports kept minimal)
- `path_utils.py` — project root, data paths, ensure dir exists
- `logging_utils.py` — logging setup/config
- `perf_utils.py` — `log_perf` context manager, timing & memory helpers (**coordinate with `performance.py` to avoid duplication**)
- `io_utils.py` — safe file I/O helpers (atomic writes/reads used by manual I/O)
- `validation_utils.py` — dataframe column checks, schema assertions
- `string_utils.py` — normalization-safe helpers, tokenization pieces (if any are generic)
- `dtypes.py` — dtype application & object-column validation (already present; integrate, don’t duplicate)
- (Optional) `hash_utils.py` — `config_hash`, `stable_group_id` (if introduced in Phase1.11)
- (Optional) `common.py` — temporary landing spot if a function doesn’t fit cleanly; add TODOs to split

---

## 3) Concrete Steps (after approval)
### 3.1 Inventory & Extraction
- Parse `src/utils.py` and produce a table: **function name** → **new home module** → **used-by (files)** → **notes** (e.g., side effects, logging, IO).
- Scan the codebase for **utility-like** functions that belong in `src/utils/` (e.g., generalized helpers currently living in `cleaning.py`, `manual_io.py`, etc.). Propose extractions, but **skip** anything tightly coupled to a single module’s domain logic.
- For each proposed move, state whether you’ll **copy** (first) then **replace imports** (preferred).

### 3.2 Create New Utility Modules
- Create modules (from §2) and copy functions from `src/utils.py` into their new homes.
- Ensure no circular imports occur; if detected, adjust responsibilities or create an internal `internal/` submodule for low-level helpers.

### 3.3 Update Imports
- Replace imports across the repo to point to the new modules (e.g., `from src.utils import ensure_directory_exists` → `from src.utils.path_utils import ensure_directory_exists`).  
- Prefer absolute imports rooted at `src` to avoid ambiguity.
- Run a repo-wide reference check (e.g., `rg -n "from src\.utils|import utils"` and `pytest -q`) to catch stragglers.

### 3.4 Remove the Old File
- Once imports compile and tests pass, **delete `src/utils.py`**.
- Record the removal in the changelog.

### 3.5 Deprecation Folder
- Create a top-level `./deprecated/` folder.
- Move fully **unused** files there (after confirming no imports reference them).  
- For any moved file, add a short markdown note in `deprecated/README.md` explaining why it was moved, and when it can be deleted.

### 3.6 Tests & CI
- Add/adjust tests:
  - Import tests: ensure the new import paths work.
  - Behavior tests for moved functions (e.g., `log_perf`, validators, path helpers).
- Run the full test suite; fix breakages.
- Ensure linters/type-checkers (ruff/black/mypy if configured) pass.

### 3.7 Documentation & Changelog
- Update `CHANGELOG.md` under **Phase1.12 — Utils Package Refactor**:
  - Summarize moves, new modules, and import path changes.
- Update `README.md` (developer section) with:
  - New utils module structure.
  - Guidance on where to add new helpers in the future.
  - Deprecation policy (`deprecated/` lifecycle).

### 3.8 Source-of-Truth Verification
- Re-open `cursor_rules.md` and explicitly confirm it still matches the new module/package rules.
- If discrepancies exist, propose **specific edits** or adjust implementation to comply.

---

## 4) Deliverables Checklist
- [ ] Critical review completed and **approved**.
- [ ] Inventory table for `src/utils.py` and other extracted utilities.
- [ ] New modules created under `src/utils/`; imports updated.
- [ ] `src/utils.py` **removed**.
- [ ] `./deprecated/` folder created with a `README.md`; unused files moved.
- [ ] Tests updated/passing; linters/type-checkers clean.
- [ ] `CHANGELOG.md` and `README.md` updated.
- [ ] `cursor_rules.md` alignment report posted.

---

## 5) Communication Protocol
- Prefix updates with **[Phase1.12]**.
- Pause after the **critical review** and **inventory & extraction plan** for my confirmation before altering imports/moves.

---

## 6) Acceptance Criteria
- No ambiguous imports (no conflict between `src/utils.py` and `src/utils/`), and `src/utils.py` has been fully removed.
- New utility modules are logically cohesive and free of circular imports.
- All tests pass; no runtime import errors.
- Docs updated; `cursor_rules.md` remains the source of truth or has an agreed patch.
- Deprecations are tracked and dated in `./deprecated/README.md`.

---

## Notes / Implementation Hints
- Use project-relative absolute imports (rooted at `src`) consistently.
- Prefer **copy-then-rewire** over move-first to keep diffs readable and allow easy rollback.
- If any function’s name is too generic, consider **namespacing** in the new module rather than renaming to avoid churn.
