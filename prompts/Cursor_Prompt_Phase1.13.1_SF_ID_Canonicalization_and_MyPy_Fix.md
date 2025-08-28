# Cursor_Prompt — Phase1.13.1 — Salesforce ID Canonicalization (15→18) + MyPy 100%

**Goal:** Canonicalize all Salesforce IDs to **18-character** form (exactly matching Salesforce’s algorithm) and reach **MyPy 100%** (no module path conflicts). This prompt assumes Phase1.12 utils refactor is complete.

---

## 1) Critical Review / Pushback (before changes)
Provide a brief critique that covers:
- Whether **`src/utils/id_utils.py`** is the right home (vs. `hash_utils.py`). (My position: **create a new `id_utils.py`**. Rationale: `hash_utils` is for config/group IDs; Salesforce ID conversion is a domain-specific utility and merits its own module.)
- Risks: double-conversion, mixed 15/18 inputs, case/whitespace issues, pandas NA pitfalls, and accidental misuse in tests.
- Alternatives: keep both 15- and 18-char columns vs. canonicalize one and preserve source in `*_src` audit columns.
- Plan to validate across the pipeline and UI without changing business semantics.

**Pause for approval before code changes.**

---

## 2) Pre-change Audit (discover all impact points)
Before implementation, produce a table enumerating all locations that touch Account IDs or generate/consume candidate pair IDs.

**Table format:**
| file | symbol / line ref | current behavior | required change | notes |
|---|---|---|---|---|

Search patterns to use (case-insensitive):
- `account_id`, `Account ID`, `id_a`, `id_b`, `accountid`
- places that **emit** pairs in `similarity.py`
- places that **join** pairs ↔ accounts (grouping, survivorship, UI)
- test fixtures referencing 15-char IDs

**Pause and post the audit table for approval.**

---

## 3) Implementation — ID Canonicalization (approved after §2)
### 3.1 Create `src/utils/id_utils.py`
Implement **Salesforce-standard** 15→18 conversion (3-char checksum) and a series helper:

```python
import re
from typing import Iterable
_BASE32 = "ABCDEFGHIJKLMNOPQRSTUVWXYZ012345"

def _chunk_checksum(chunk: str) -> str:
    bits = 0
    for i, c in enumerate(chunk):
        if "A" <= c <= "Z":
            bits |= (1 << i)
    return _BASE32[bits]

def sfid15_to_18(sfid15: str) -> str:
    if not isinstance(sfid15, str):
        raise TypeError("sfid15 must be a string")
    if len(sfid15) != 15 or not re.fullmatch(r"[A-Za-z0-9]{15}", sfid15):
        raise ValueError("sfid15 must be 15 alphanumeric chars")
    suffix = "".join(_chunk_checksum(sfid15[i:i+5]) for i in range(0, 15, 5))
    return sfid15 + suffix

def normalize_sfid_series(series):
    s = series.astype("string").str.strip()
    is18 = s.str.len() == 18
    is15 = s.str.len() == 15
    out = s.copy()
    out.loc[is15] = s.loc[is15].map(sfid15_to_18)
    bad = ~(is15 | is18)
    if bool(bad.any()):
        sample = s.loc[bad].head(5).tolist()
        raise ValueError(f"Found non 15/18-char Salesforce IDs: {sample}")
    return out
```

Add unit tests:
- 15→18 vectors (include mixed case inputs that differ only by case)
- pass-through for 18-char
- error on non 15/18
- pandas NA/whitespace safety

### 3.2 Wire into pipeline
- **`cleaning.py`** (right after column rename):
  - Preserve source in `account_id_src` (trimmed string)
  - Set canonical: `accounts_df["account_id"] = normalize_sfid_series(accounts_df["account_id_src"])`
  - Assert uniqueness on canonical `account_id`.
- **`similarity.py`**:
  - Ensure pairs emit **canonical** IDs as `id_a`/`id_b` (not row indices).
- **`grouping.py`**:
  - Keep `id_col="account_id"` param; call referential integrity on **canonical** IDs.
- **`app/main.py`** (UI):
  - Display canonical `account_id`; optionally show `account_id_src` in Explain panel / tooltips for transparency.
- **`tests/`**:
  - Update fixtures that used 15-char to include canonicalization path; add tests for both 15 and 18 inputs.

### 3.3 Logging / Reporting
Update run summary to distinguish:
- `data_hygiene.hard_duplicate_rows_removed_by_account_id18`
- `dedupe.groups_formed` with histogram

---

## 4) MyPy to 100% — fix module path conflicts
**Problem noted:** `dtypes_map.py` imported as both `dtypes_map` and `src.dtypes_map` → conflicts.

### 4.1 Standardize imports
- Use **absolute imports rooted at `src`** everywhere (no bare `dtypes_map`).
- Replace occurrences of `import dtypes_map` or `from dtypes_map` → `from src.dtypes_map import ...` (or move its content into `src/utils/dtypes.py` if appropriate; if you do so, replace all imports accordingly).
- Ensure tests also import via `src.`

### 4.2 MyPy configuration
Add/update `mypy.ini` (or `pyproject.toml` `[tool.mypy]`) with:

```
[mypy]
python_version = 3.12
warn_unused_ignores = True
warn_redundant_casts = True
warn_return_any = True
disallow_untyped_defs = True
disallow_incomplete_defs = True
check_untyped_defs = True
no_implicit_optional = True
ignore_missing_imports = False
namespace_packages = True

mypy_path = src
```

If using `pyproject.toml`:
```toml
[tool.mypy]
python_version = "3.12"
warn_unused_ignores = true
warn_redundant_casts = true
warn_return_any = true
disallow_untyped_defs = true
disallow_incomplete_defs = true
check_untyped_defs = true
no_implicit_optional = true
ignore_missing_imports = false
namespace_packages = true
mypy_path = "src"
```

### 4.3 CI gate & invocation
- Ensure `pytest`, `ruff`, `black`, and `mypy` run in CI with **non-zero** exit on failure.
- Command examples:
  - `black --check .`
  - `ruff .`
  - `mypy --config-file mypy.ini .` (or `pyproject.toml`)
  - `pytest -q`

**Deliverable:** MyPy returns **0 errors**. If third-party stubs are needed, add them to `requirements-dev.txt` (e.g., `pandas-stubs`, `types-PyYAML`) and commit.

**Note on “stubs”:** Stubs are `.pyi` (or typed) packages that provide type info for libraries that don’t ship types. They don’t change runtime, only static analysis.

---

## 5) Documentation & Rules
- `CHANGELOG.md`: Add **Phase1.13.1** section summarizing:
  - ID canonicalization (15→18), modules touched, tests added
  - MyPy 100% and import standardization
- `README.md` (developer notes):
  - Explain canonical ID policy (store original as `*_src`, operate on 18-char)
  - Where `id_utils.py` lives and how to use it
- `cursor_rules.md`: Confirm rules remain aligned (utils module structure, absolute imports, type-checking policy). Propose edits if drift exists.

---

## 6) Deliverables Checklist
- [ ] Critical review posted and approved
- [ ] Audit table of impacted locations approved
- [ ] `src/utils/id_utils.py` with tests
- [ ] Canonicalization wired into `cleaning.py`, `similarity.py`, `grouping.py`, and UI (as applicable)
- [ ] MyPy 100% clean; import paths standardized
- [ ] Docs updated (README, CHANGELOG); rules verified
- [ ] Final summary with sample inputs/outputs and run metrics

---

## 7) Communication Protocol
- Prefix updates with **[Phase1.13.1]**
- Pause after **§2 Audit** and after **tests go green** for my confirmation before merging
