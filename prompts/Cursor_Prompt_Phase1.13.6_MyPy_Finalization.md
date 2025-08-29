
# Cursor Prompt — Phase1.13.6 — MyPy Finalization & Residual Type Fixes

## Objective
Drive **MyPy to zero functional errors** (ok to keep *intentional* `# type: ignore` in tests for bad-input cases), while keeping **Black/Ruff/PyTest** green and preserving runtime behavior.

---

## Ground Rules (do not skip)
- **No functional changes**: only type annotations, safe casts, and typing-friendly refactors (e.g., `.loc` over tuple indexing).
- **Absolute imports only** for first-party code (`from src...`, `from app...`). No bare imports.
- **Keep CI gates green**: `black --check .`, `ruff check .`, `mypy --config-file mypy.ini src tests app`, `pytest -q`.
- After each module group fixed, **post a short diff summary** and **wait for my approval** before proceeding to the next group.

---

## Known Residual Errors (guide your fixes)
The latest run showed issues clustered in these areas. Tackle in the order below, pausing after each section.

### 1) `src/performance.py`
- Add `-> None` where missing.
- Fix numeric type conflicts: if `peak_mb` holds floats, **annotate as `float`**.
- Add parameter types for flagged functions.
- Prefer explicit types for accumulators and return values.

### 2) `src/utils/perf_utils.py`
- Annotate the context manager:
  ```py
  from contextlib import contextmanager
  from typing import Iterator

  @contextmanager
  def log_perf(label: str) -> Iterator[None]:
      ...
  ```

### 3) `src/utils/id_utils.py`
- Remove any **unused** `# type: ignore` and **unreachable** code flagged by MyPy.

### 4) `src/similarity.py`
- `_compute_pair_score` must receive **`pd.Series`** (not `DataFrame`). Adjust call sites.
- Add missing return type annotations (`-> bool` / `-> None`).
- Replace `Any` returns with concrete types.
- Where needed, use **TypedDict** or `Mapping[str, Any]` for config parameters to satisfy type checker.

### 5) `src/salesforce.py`
- Variables used with `.startswith` must be `str`; add `str(...)` or stricter typing upstream.
- List `.append` targets must be typed as `list[str]` or `list[dict[str, Any]]` as appropriate.
- Calls to `int(...)` must receive valid `str|int`; cast via `str(x)` if value may be `Hashable`.

### 6) `src/disposition.py` and `src/alias_matching.py`
- Replace tuple indexing (e.g., `df.loc[mask, "col"]`) to quiet `_LocIndexerFrame` errors.
- Ensure helpers have explicit return types.
- Fix list `.append` element types (unify list element type).

### 7) `src/survivorship.py`
- Avoid `int(Hashable)` — cast to `str` or use `SupportsInt` where valid.
- Add explicit types for dict/list accumulators.
- Ensure **declared return types** match actual returns.

### 8) `src/grouping.py`
- Add types to union-find structures:
  ```py
  parent: dict[str, str] = {}
  rank: dict[str, int] = {}
  group_members: dict[str, list[str]] = {}
  ```
- Add `-> None` to procedures; replace `Any` returns with concrete types.

### 9) `src/cleaning.py`
- Remove/rewrite **unreachable** sections.
- Add `-> None` where appropriate.

### 10) `app/main.py` and `app/manual_data.py`
- Ensure imports are `from app.manual_data import ...`, `from src.manual_io import ...`, `from src.disposition import ...`.
- Replace `Any` returns with concrete types (`list[str]`, `dict[str, Any] | None`, etc.).
- For NumPy/pandas arithmetic producing `Union` types, cast carefully or use `.astype(float)` before ops.

### 11) Tests
- Ensure **all test functions** have `-> None`.
- For tests that intentionally pass wrong types, add **narrow ignores**:
  ```py
  # type: ignore[arg-type]  # intentional invalid input test
  ```

---

## Implementation Plan
1. **Module-grouped passes**: Fix in this order → `performance` → `utils` → `similarity` → `salesforce` → `disposition/alias_matching` → `survivorship` → `grouping` → `cleaning` → `app` → `tests`.
2. After each group, run quick gates:
   ```bash
   ruff check . && black --check . && mypy --config-file mypy.ini src tests app
   ```
   Post a **delta count** of MyPy errors and a **high-level diff**; wait for approval.
3. When MyPy errors reach **≤ 10 (goal: 0)**, run the **full gates**:
   ```bash
   black --check .
   ruff check .
   mypy --config-file mypy.ini src tests app
   pytest -q
   ```

---

## Acceptance Criteria
- MyPy: **0 functional errors** (exceptions only for intentional negative tests).
- All gates green (Black/Ruff/PyTest).
- **CHANGELOG.md**: add “Phase1.13.6 — MyPy Finalization” section summarizing categories of fixes.
- Reply with a table summarizing: file → errors fixed → fix category (annotation, cast, indexer, signature, container typing).

---

## Pause Points (must wait for my approval)
- After finishing: `performance` + `utils`.
- After finishing: `similarity` + `salesforce`.
- After finishing: `disposition/alias_matching` + `survivorship`.
- After finishing: `grouping` + `cleaning`.
- Before touching `app/` and `tests/`.
- Before finalizing and updating CHANGELOG.md.

