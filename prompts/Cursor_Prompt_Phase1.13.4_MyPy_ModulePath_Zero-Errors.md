# Cursor_Prompt — Phase1.13.4 — MyPy Module-Path Conflict: Zero-Errors Finalization

**Objective:** Eliminate the remaining MyPy “module path conflict” (e.g., `dtypes_map` vs `src.dtypes_map`) and achieve **0 MyPy errors**. Enforce a single canonical package root (`src`), remove aliasing via `mypy_path`, fix any lingering bare imports, and verify with QA gates.

---

## 0) Quick Critical Review / Pushback
Before making changes, please post a short assessment of:
- Any code paths that still rely on bare imports of first‑party modules (e.g., `import dtypes_map`) and how you’ll adjust them.
- Whether removing `mypy_path` could affect local tooling, and why the proposed invocation `mypy src tests app` avoids that.
- Any test impacts (should be none) from the import path normalization.

**Pause for my approval** after posting the assessment.

---

## 1) MyPy Configuration — Replace `mypy.ini`
**Goal:** Remove `mypy_path` (which can create dual identities) and drive MyPy by targeting our packages explicitly.

**Replace the entire contents of `mypy.ini` with:**

```ini
[mypy]
python_version = 3.12

# Treat 'src' as the only canonical package root via package imports,
# not by aliasing the path with mypy_path.
explicit_package_bases = True
namespace_packages = True
ignore_missing_imports = False

# Tight typing (adjust if this is too strict in a future phase)
disallow_untyped_defs = True
disallow_incomplete_defs = True
check_untyped_defs = True
no_implicit_optional = True
strict_equality = True

# Hygiene
warn_return_any = True
warn_unused_configs = True
warn_redundant_casts = True
warn_unused_ignores = True
warn_no_return = True
warn_unreachable = True

# Third-party libs that don't ship types (keep minimal)
[mypy-pandas.*]
ignore_missing_imports = True

[mypy-yaml.*]
ignore_missing_imports = True

[mypy-streamlit.*]
ignore_missing_imports = True
```

**Invocation change (important):**
- Run MyPy only against our packages (prevents it from discovering alternate paths):
  ```bash
  mypy --config-file mypy.ini src tests app
  ```

---

## 2) Final Sweep — Replace Any Bare First‑Party Imports
Search for **any** bare import of local modules without the `src.` prefix:

```bash
# These should return ZERO results after the fix
rg -n '^(from|import)\s+(normalize|similarity|grouping|survivorship|disposition|alias_matching|performance|manual_io|dtypes_map)\b' src tests app
```

**For each hit you find, patch to absolute imports:**
- `import dtypes_map` → `from src import dtypes_map`
- `from dtypes_map import Foo` → `from src.dtypes_map import Foo`
- Likewise for `normalize`, `similarity`, `grouping`, `survivorship`, `disposition`, `alias_matching`, `performance`, `manual_io`.

> Post a small “patch list” (file:line → old → new) so I can review what changed.

---

## 3) Cache Clean & QA Gates
**Clean caches (important), then run the full gate:**
```bash
rm -rf .mypy_cache .pytest_cache
find . -name '*.py[co]' -delete

black --check .
ruff .
mypy --config-file mypy.ini src tests app
pytest -q
```

**Expected:** Black/Ruff/PyTest all pass; **MyPy: 0 errors**.

If any MyPy errors remain, post the **exact output** and the **offending import lines**.

---

## 4) Optional: CI Guardrail (recommended)
If we have a CI pipeline or pre-commit, add a simple check to prevent this from regressing:
- A script/step that fails if the grep in §2 finds any bare imports.
- A CI command that runs `mypy --config-file mypy.ini src tests app`.

(If you add CI changes, summarize them and include diffs.)

---

## 5) Deliverables
- Updated `mypy.ini` (no `mypy_path`, strict settings left in place).
- Patch list of import fixes (file:line → before/after).
- QA gate outputs with **MyPy: 0 errors**.
- Optional CI guardrail summary (if added).

---

## 6) Communication Protocol
- Prefix all updates with **[Phase1.13.4]**.
- Pause after the “Final Sweep” patch list and again after posting QA results, for my confirmation before merging.