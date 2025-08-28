# Cursor_Prompt — Phase1.13.3 — Type & Import Hygiene (MyPy path fix + `tests/test_imports.py` rewrite)

**Goal:** Eliminate the lingering MyPy module-path conflict and normalize the import test so it works cleanly under pytest. Apply the fixes below, verify with QA gates, and pause for review.

---

## 1) Critical Review / Pushback (brief)
Before changing files, post a short critique covering:
- Whether enforcing a single canonical package root (`src`) has any side-effects in this repo.
- Any places that still rely on bare/relative imports for first‑party modules (and how you’ll fix them).
- Whether the proposed `tests/test_imports.py` rewrite loses any meaningful coverage vs. the current script.

**Pause here for my OK.**

---

## 2) Planned Changes (exact, idempotent)

### A) Replace `mypy.ini` with a strict, src-rooted config
- Enforce resolution only through `src` (prevents duplicate module identities like `dtypes_map` vs `src.dtypes_map`).
- Keep third‑party stubs minimal.

**Target file:** `mypy.ini`  
**New contents (replace file):**
```ini
[mypy]
python_version = 3.12

# Resolution: only treat 'src' as the package base
mypy_path = src
explicit_package_bases = True
namespace_packages = True

# Tighten typing (tweak if too strict)
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

# Third-party libs that don’t ship types (keep minimal)
[mypy-pandas.*]
ignore_missing_imports = True

[mypy-yaml.*]
ignore_missing_imports = True

[mypy-streamlit.*]
ignore_missing_imports = True
```

Also perform a quick sweep to ensure **all first‑party imports are absolute and rooted at `src`** (no bare `import normalize`, etc.). If any violations are found, post a small patch list and fix them.

### B) Rewrite `tests/test_imports.py` as pure‑pytest
- No `sys.path` hacks, no `__main__` block.
- Validate imports via the canonical `src.` package path.
- Keep coverage for utils and cross‑module imports.

**Target file:** `tests/test_imports.py`  
**New contents (replace file):**
```python
import importlib
import pytest

# modules we want to be importable via the canonical package path
SRC_MODULES = [
    ("src.cleaning", "Main pipeline orchestration"),
    ("src.normalize", "Data normalization"),
    ("src.similarity", "Similarity computation"),
    ("src.grouping", "Group creation logic"),
    ("src.survivorship", "Primary record selection"),
    ("src.disposition", "Disposition classification"),
    ("src.alias_matching", "Alias matching"),
    ("src.manual_io", "Manual data I/O"),
    ("src.salesforce", "Salesforce utilities"),
    ("src.performance", "Performance tracking"),
    ("src.dtypes_map", "Data type mappings"),
]

UTILS_MODULES = [
    ("src.utils", "Main utils package"),
    ("src.utils.dtypes", "Data type utilities"),
    ("src.utils.logging_utils", "Logging utilities"),
    ("src.utils.path_utils", "Path utilities"),
    ("src.utils.validation_utils", "Validation utilities"),
    ("src.utils.io_utils", "I/O utilities"),
    ("src.utils.perf_utils", "Performance utilities"),
    ("src.utils.hash_utils", "Hash utilities"),
    ("src.utils.id_utils", "Salesforce ID utilities"),
]

CROSS_IMPORTS = [
    ("src.cleaning", "imports utils via absolute path"),
    ("src.grouping", "imports utils via absolute path"),
    ("src.survivorship", "imports src.normalize"),
    ("src.performance", "imports utils via absolute path"),
    ("src.utils.dtypes", "imports src.dtypes_map"),
]


@pytest.mark.parametrize("module_name, _desc", SRC_MODULES + UTILS_MODULES)
def test_importable(module_name, _desc):
    importlib.import_module(module_name)


@pytest.mark.parametrize("module_name, _desc", CROSS_IMPORTS)
def test_cross_importable(module_name, _desc):
    importlib.import_module(module_name)
```

---

## 3) Implementation Steps
1. Open a new ephemeral branch or continue on `phase-1.13-dev`.
2. Apply the `mypy.ini` replacement and the `tests/test_imports.py` rewrite.
3. **One-time sweep:** replace any lingering bare first‑party imports with absolute `from src....` imports, then list the changes you made (file:line).

**Cache cleanup before QA (important):**
```bash
rm -rf .mypy_cache .pytest_cache
find . -name '*.py[co]' -delete
```

---

## 4) QA Gates (must pass)
Run and post the results:
```bash
black --check .
ruff .
mypy --config-file mypy.ini .
pytest -q
```

Expected outcome:
- Black/Ruff: pass
- **MyPy: 0 errors** (module path conflict resolved)
- PyTest: still 100% passing

---

## 5) Documentation
- Append a short note in `CHANGELOG.md`:
  - **Phase1.13.3 — Type & Import Hygiene:** MyPy path fix (single `src` root), rewrite `tests/test_imports.py` to pytest-native, enforce absolute imports across codebase.

---

## 6) Deliverables
- Updated `mypy.ini`
- Updated `tests/test_imports.py`
- List of any import-path fixes applied across codebase
- QA gates output (all green)
- CHANGELOG updated

---

## 7) Communication Protocol
- Prefix updates with **[Phase1.13.3]**
- Pause after posting QA results for my approval before merging
