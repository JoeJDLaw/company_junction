<file name=0 path=/Users/joe.j/Documents/dev/salesforce/apps/company_junction/docs/plan_02_refactor_ui_helpers.md># Refactor Plan: ui_helpers.py Decomposition

## Overview

This document outlines the plan to safely decompose the monolithic `src/utils/ui_helpers.py` (2,214 lines) into smaller, focused modules while preserving behavior and ensuring reversibility.

## Core Principles

1. **No Behavioral Changes**
   - Copy literal code from ui_helpers.py
   - Preserve exact log messages and fields
   - Keep all type hints and optional dependency handling
   - Match current error handling (return empty vs raise)
   - **Logger identity parity**: preserve old `logger` names or at least keep log substrings consistent
   - **Schema constants**: all column references must come from `schema_utils`
   - **DuckDB query parameterization is a hard requirement**: all DuckDB queries must use parameter placeholders instead of manual string interpolation to prevent SQL injection and improve maintainability.

2. **Incremental Safety**
   - Use façade pattern to avoid big-bang changes
   - Feature flags for gradual migration
   - Golden tests for sort orders and cache keys
   - Performance baseline preservation

3. **Developer Experience**
   - Clear module boundaries and responsibilities
   - Centralized optional dependency handling
   - Unified models for sort/filter/pagination
   - Comprehensive documentation

## Module Structure

### Infrastructure Layer

1. **src/utils/opt_deps.py**
```python
from typing import Dict, Optional, NamedTuple, Any
import importlib

def try_import(module: str) -> Optional[Any]: ...
def try_import_many(modules: Dict[str, str]) -> Dict[str, Optional[Any]]: ...

# Centralized capability checks
DUCKDB = try_import("duckdb")
PYARROW = try_import_many({
    "pc": "pyarrow.compute",
    "ds": "pyarrow.dataset",
    "pq": "pyarrow.parquet"
})
STREAMLIT = try_import("streamlit")

# Export handles & flags
DUCKDB_AVAILABLE = DUCKDB is not None
PC, DS, PQ = PYARROW["pc"], PYARROW["ds"], PYARROW["pq"]
PYARROW_AVAILABLE = all(PYARROW.values())

# Detailed dependency status tracking
class DepStatus(NamedTuple):
    name: str
    available: bool
    version: Optional[str]

def get_dep_status() -> Dict[str, DepStatus]:
    """Return detailed status of optional dependencies."""
    deps = {
        "duckdb": DUCKDB,
        "pyarrow": PYARROW,
        "streamlit": STREAMLIT,
    }
    status = {}
    for name, mod in deps.items():
        if mod is None:
            status[name] = DepStatus(name, False, None)
        else:
            version = getattr(mod, "__version__", None)
            if isinstance(mod, dict):
                # For pyarrow dict, check one submodule version
                version = getattr(next(iter(mod.values())), "__version__", None)
            status[name] = DepStatus(name, True, version)
    return status
```

2. **src/utils/settings.py**
```python
from functools import lru_cache
from typing import Dict, Any

@lru_cache(maxsize=1)
def get_settings() -> Dict[str, Any]: ...

def get_ui_perf() -> Dict[str, Any]:
    """Helper to get ui.perf section with defaults."""
    ...

def validate_settings() -> List[str]:
    """Returns list of validation warnings."""
    ...
```

3. **src/utils/ui_session.py**
```python
from typing import Any, Optional, Dict
from .opt_deps import STREAMLIT

class SessionState:
    """Session state adapter with dict fallback for tests."""
    def __init__(self, use_streamlit: bool = True):
        self._use_streamlit = use_streamlit and STREAMLIT is not None
        self._fallback: Dict[str, Any] = {}

    def get(self, key: str, default: Any = None) -> Any: ...
    def set(self, key: str, value: Any) -> None: ...

# Global instance - configurable for tests
session = SessionState()

# Constants
BACKEND_KEY = "cj.backend.groups"
```

4. **src/utils/artifact_management.py**
```python
# Placeholder for artifact management utilities
# This module handles core path helpers for artifacts
```

### Data Model Layer

5. **src/utils/filtering.py**
```python
from dataclasses import dataclass
from typing import Literal

@dataclass(frozen=True)
class SortSpec:
    field: str
    direction: Literal["asc", "desc"]
    tie_breaker: tuple[str, Literal["asc", "desc"]] = ("group_id", "asc")

def resolve_sort(sort_key: str) -> SortSpec: ...
def to_duckdb_order_by(spec: SortSpec) -> str: ...
def to_pyarrow_sort_by(spec: SortSpec) -> list[tuple[str, str]]: ...
```

6. **src/utils/cache_keys.py**
```python
import hashlib
from typing import Any, Dict, Optional
from dataclasses import dataclass
from enum import Enum

class CacheKeyVersion(Enum):
    V1 = "CJCK1"  # Initial version
    # V2 = "CJCK2"  # Future: document changes that require version bump

@dataclass(frozen=True)
class CacheKey:
    version: CacheKeyVersion
    components: tuple[Any, ...]
    
    def compute(self) -> str:
        """Generate stable hash from components."""
        ...
    
    @classmethod
    def validate(cls, key: str) -> Optional[str]:
        """Returns warning if key version mismatch."""
        ...

def fingerprint(path: str) -> str:
    """Stable mtime+size fingerprint."""
    ...
```

### Business Logic Layer

7. **src/utils/group_stats.py**
```python
from typing import Union
import pandas as pd
from .opt_deps import DUCKDB, PC

def compute_group_stats(
    table: Union[pd.DataFrame, "pyarrow.Table"],
    backend: str = "auto"
) -> pd.DataFrame: ...
```

8. **src/utils/group_pagination.py**
```python
from dataclasses import dataclass
from typing import Dict, Any

@dataclass
class PaginationSpec:
    """Logical pagination model."""
    filters: Dict[str, Any]
    sort: SortSpec
    offset: int
    limit: int

def get_groups_page(run_id: str, spec: PaginationSpec) -> tuple[list[dict], int]: ...
```

### UI Layer

9. **src/utils/run_management.py**
```python
from typing import Dict, List, Optional
from .settings import get_settings
from .artifact_management import get_artifact_paths

def list_runs() -> List[Dict[str, Any]]: ...
def get_run_metadata(run_id: str) -> Optional[Dict[str, Any]]: ...
```

10. **src/utils/ui_helpers.py** (Façade)
```python
import os
import warnings
from typing import Any, Dict, List, Optional, Tuple

# Re-exports
from .artifact_management import get_artifact_paths
from .run_management import list_runs, get_run_metadata
# ... etc

# Default: PendingDeprecationWarning to nudge devs
if not os.getenv("CJ_UI_HELPERS_NO_WARN"):
    warnings.warn(
        "ui_helpers is pending deprecation; use new modules directly",
        PendingDeprecationWarning
    )

# Strong deprecation behind flag
if os.getenv("CJ_UI_HELPERS_DEPRECATE"):
    warnings.warn(
        "ui_helpers is deprecated; use new modules",
        DeprecationWarning
    )

__all__ = [
    "get_artifact_paths",
    "list_runs",
    # ... etc
]
```

## Migration Strategy

### Phase 1: Infrastructure (1-2 days)

1. Create core utilities:
   ```bash
   touch src/utils/{opt_deps,settings,ui_session}.py
   ```

2. Add import cycle detection:
   ```python
   # tests/test_import_cycles.py
   import pytest
   from importlib import import_module
   from pathlib import Path

   def collect_modules():
       """Find all .py files under src/utils."""
       root = Path("src/utils")
       return [
           f"src.utils.{p.stem}"
           for p in root.glob("*.py")
           if not p.name.startswith("_")
       ]

   @pytest.mark.parametrize("module", collect_modules())
   def test_no_import_cycles(module):
       """Verify each module can be imported cleanly."""
       import_module(module)
   ```

3. Add logging parity tests:
   ```python
   # tests/test_logging_parity.py
   import logging
   from io import StringIO

   def test_critical_log_fields():
       """Verify key log fields present."""
       log_buffer = StringIO()
       handler = logging.StreamHandler(log_buffer)
       
       # Capture logs
       with caplog.at_level(logging.INFO):
           get_groups_page(...)
       
       log_text = log_buffer.getvalue()
       
       # Check critical fields
       assert "groups_perf: backend=" in log_text
       assert "reason=" in log_text
       # etc
   ```

4. Parameterize DuckDB queries using placeholders rather than manual string interpolation. This is a **hard requirement** to prevent SQL injection and improve maintainability.

   **Before:**
   ```python
   query = f"SELECT * FROM groups WHERE run_id = '{run_id}'"
   result = con.execute(query).fetchdf()
   ```

   **After:**
   ```python
   query = "SELECT * FROM groups WHERE run_id = ?"
   result = con.execute(query, [run_id]).fetchdf()
   ```

5. Add static test refinement for DuckDB query parameterization:
   ```python
   # tests/test_duckdb_query_params.py
   import ast
   import pytest
   import re
   from pathlib import Path

   DUCKDB_QUERY_PATTERN = re.compile(r"\.execute\((?P<query>.+?)(,|\))")

   def test_duckdb_query_params():
       """Ensure all DuckDB queries use parameter placeholders, no f-strings."""
       src_dir = Path("src/utils")
       for py_file in src_dir.glob("*.py"):
           with open(py_file, "r") as f:
               source = f.read()
           tree = ast.parse(source, filename=str(py_file))
           for node in ast.walk(tree):
               if isinstance(node, ast.Call):
                   func = getattr(node.func, "attr", None)
                   if func == "execute":
                       query_arg = node.args[0]
                       # Check if query_arg is a JoinedStr (f-string)
                       if isinstance(query_arg, ast.JoinedStr):
                           pytest.fail(
                               f"Found f-string SQL query in {py_file} line {node.lineno}. "
                               "Use parameter placeholders instead."
                           )
   ```
   
### Phase 2: Core Models (2-3 days)

1. Create unified models with tests:
   ```python
   # tests/test_sort_spec.py
   def test_sort_spec_golden():
       """Verify sort orders match golden files."""
       spec = SortSpec(field="group_size", direction="desc")
       
       # Check DuckDB
       assert to_duckdb_order_by(spec) == "group_size DESC, group_id ASC"
       
       # Check PyArrow
       assert to_pyarrow_sort_by(spec) == [
           ("group_size", "descending"),
           ("group_id", "ascending")
       ]
   ```

2. Add performance baseline:
   ```python
   # scripts/benchmark_ui_helpers.py
   import time
   import pandas as pd
   
   def run_benchmarks():
       """Measure key operations."""
       results = []
       
       # Time page loads
       start = time.time()
       get_groups_page(...)
       results.append({
           "op": "page_load",
           "ms": (time.time() - start) * 1000
       })
       
       # Save report
       pd.DataFrame(results).to_markdown(
           "docs/reports/ui_helpers_refactor_benchmark.md"
       )

   def generate_trend_report(baseline_path: str, current_path: str) -> None:
       """
       Compare baseline and current benchmark results to detect regressions.
       Outputs a markdown report highlighting trends.
       """
       baseline = pd.read_markdown(baseline_path)
       current = pd.read_markdown(current_path)
       merged = baseline.merge(current, on="op", suffixes=("_baseline", "_current"))
       merged["diff_ms"] = merged["ms_current"] - merged["ms_baseline"]
       merged["pct_change"] = merged["diff_ms"] / merged["ms_baseline"] * 100
       report = merged[["op", "ms_baseline", "ms_current", "diff_ms", "pct_change"]]
       report.to_markdown("docs/reports/ui_helpers_trend_report.md", index=False)
   ```

### Phase 3: Business Logic (3-4 days)

1. Move core functions with tests
2. Validate performance baselines
3. Check logging parity

### Phase 4: UI Layer (2-3 days)

1. Create façade
2. Add deprecation warnings
3. Document new module map

### Phase 5: Migration (3-4 days)

1. Update imports behind flag
2. Archive original to .bak
3. Validate all changes

## Testing Strategy

### Automated Tests

1. Import cycles:
   - Use pytest-cyclic-imports
   - Import each module in isolation

2. Golden tests:
   - Sort orders match reference files
   - Cache keys stable across versions
   - Log messages contain critical fields

3. Performance tests:
   - Page load within 5% of baseline
   - Generate trend reports

4. Integration tests:
   - No-Streamlit environment works
   - Optional deps gracefully degrade

5. **Logger identity tests:**
   - Assert that logger names and key log substrings remain consistent to preserve log parsing and monitoring.

6. **Schema constants static analysis:**
   - Use static analysis tools or linters to verify all column references come from `schema_utils` constants rather than hardcoded strings.

7. **Pytest markers for optional dependencies:**
   - Mark tests that require optional dependencies (duckdb, pyarrow, streamlit) with custom pytest markers to allow selective test runs in different environments.

8. **DuckDB query parameterization static test:**
   - Add `test_duckdb_query_params` to ensure no f-string SQL queries exist and all DuckDB queries use parameter placeholders.

### Matrix CI for optional deps

- duckdb=yes/no
- pyarrow=yes/no
- streamlit=yes/no

### Logging parity tests

- Assert on substrings rather than full logger names

### Manual Verification

- [ ] Streamlit UI launches
- [ ] Group list renders
- [ ] Pagination works
- [ ] Details load
- [ ] Backend switching works
- [ ] Log messages unchanged

## Rollback Plan

1. Keep `deprecated/ui_helpers.py.bak`
2. Feature flags default to old paths
3. One-line rollback: `cp deprecated/ui_helpers.py.bak src/utils/ui_helpers.py`
4. Deprecated `.bak` files are write-protected in pre-commit
5. Rollback script: `scripts/rollback_ui_helpers.sh` with a one-liner to restore

6. Enhanced rollback script:
   ```bash
   #!/bin/bash
   BAK_FILE="deprecated/ui_helpers.py.bak"
   TARGET_FILE="src/utils/ui_helpers.py"

   if [ ! -f "$BAK_FILE" ]; then
       echo "Backup file $BAK_FILE does not exist. Abort rollback."
       exit 1
   fi

   # Copy backup to target
   cp "$BAK_FILE" "$TARGET_FILE"
   echo "Restored $TARGET_FILE from backup."

   # Set write protection on backup
   chmod 444 "$BAK_FILE"
   echo "Backup file write-protected."

   # Reset feature flags to disable refactor usage
   unset CJ_UI_HELPERS_DEPRECATE
   unset CJ_UI_HELPERS_NO_WARN
   echo "Feature flags reset."

   echo "Rollback complete."
   ```

## Public API Map

| Old Location (ui_helpers.py) | New Location | Notes |
|----------------------------|--------------|-------|
| `get_artifact_paths()` | artifact_management.py | Core path helper |
| `list_runs()` | run_management.py | Run lifecycle |
| `get_groups_page()` | group_pagination.py | Main pagination entry |
| `get_group_details_lazy()` | group_details.py | Details with caching |
| ... | ... | ... |

## API Map Maintenance

To maintain the Public API Map accurately:

- Always update this map when moving or renaming functions.
- Include clear notes about the purpose or changes.
- Use consistent formatting with columns: Old Location, New Location, Notes.
- Add entries in alphabetical order by the old function name for easy lookup.
- Example entry format:

| Old Location (ui_helpers.py) | New Location | Notes |
|------------------------------|--------------|-------|
| `fetch_user_data()`           | user_management.py | Handles user data retrieval |

- Review the map during code reviews to ensure it reflects the current codebase.
- Keep the map in sync with documentation and migration guides to assist developers during transition.

## Performance Baselines

Current metrics from production logs:
- Group page load: ~200-500ms typical
- Next page: ~100-200ms typical
- Group details: ~50-100ms typical
- Backend switch: ~10-20ms overhead

These will be preserved and monitored during migration.

## Future Considerations

1. **Cache Key Evolution**
   - Document when CJCK2 would be needed
   - Migration path for cache key versions
   - Validation and warnings

2. **Session State**
   - Test coverage for no-Streamlit case
   - Potential for other UI frameworks

3. **Performance Monitoring**
   - Regular benchmark runs
   - Trend analysis in reports
   - Alert on regressions

4. **Documentation**
   - Keep API map updated
   - Track performance trends
   - Document migration status

## Version Control Strategy

- Use descriptive branch names prefixed with `refactor/ui_helpers/` followed by the phase or module name.
- Keep pull requests small and focused, ideally one module or logical change per PR.
- Require at least two reviewers, including one familiar with the existing ui_helpers codebase and one with new module ownership.
- Enforce CI checks including import cycle detection, linting, and tests before merging.
- Use feature flags in code to toggle new vs old implementations, enabling safe rollback.

## Monitoring During Migration

- Continuously monitor performance metrics against baseline to detect regressions early.
- Track logging output for completeness and correctness, ensuring all critical fields and logger identities remain intact.
- Monitor error rates and exceptions in logs to catch any unexpected failures due to refactor.
- Set up alerting on key metrics such as page load time and backend switch latency to quickly respond to issues.

## Communication Plan

- Announce the refactor plan and timelines to the engineering team and relevant stakeholders ahead of starting work.
- Update internal documentation and API references to reflect new module structure and usage.
- Provide migration guides and examples for developers to update imports and usage.
- Offer support channels during migration phases for questions and troubleshooting.
- Communicate deprecation timelines clearly, including when old ui_helpers.py will be removed.
