# Mypy Difficult Errors Resolution Guide

**Date**: 2025-09-05  
**Purpose**: Focused troubleshooting guide for remaining difficult mypy errors  
**Target**: Systematic resolution of complex type issues  
**Working Document**: For collaboration between development team and AI assistants

**📋 LATEST UPDATE**: See `mypy_final_4_errors_report.md` for detailed analysis of the final 4 errors.

## **Current State**

- **Total Remaining Errors**: 4 mypy errors
- **Previous Progress**: Reduced from 263 to 4 errors (98.5% reduction)
- **Files Fixed**: 42+ files completely fixed, 1 partially fixed
- **Focus**: Final complex pandas read_csv overload issues in single file

## **🎉 Outstanding Progress Summary**

### **Error Reduction Journey**:
- **Starting Point**: 263 mypy errors
- **Current State**: 4 mypy errors
- **Total Fixed**: 259 errors (98.5% reduction)
- **Batches Completed**: 6 systematic batches
- **Files Completely Fixed**: 42+ files
- **Files Partially Fixed**: 1 file (`src/utils/io_utils.py`)

### **Key Achievements**:
- ✅ **Protocol-based typing**: Created `ExecutorLike` protocol for parallel executors
- ✅ **TypedDict approach**: Created `ScoreComponents` for structured returns
- ✅ **Context manager typing**: Fixed DuckDB connection typing
- ✅ **Import fixes**: Resolved third-party library import issues
- ✅ **Type casting**: Proper casting for complex type interactions
- ✅ **Unused ignore cleanup**: Removed masking type: ignore comments
- ✅ **Series/DataFrame fixes**: Resolved pandas type interaction issues

### **Remaining Work**:
- **Final 4 errors**: All in `src/utils/io_utils.py`
- **Root cause**: Complex pandas `read_csv` overload resolution
- **Solution**: Typed wrapper function (detailed in `mypy_final_4_errors_report.md`)

## **Error Categories by Difficulty**

### **Category 1: PyArrow Integration Issues (12 errors)**
**Difficulty**: High - Requires pyarrow type stubs or complex type definitions

#### **Files and Locations**:
1. `src/utils/parquet_size_reporter.py:58` - Skipping analyzing "pyarrow"
2. `src/utils/group_pagination.py:117` - Skipping analyzing "pyarrow" and "pyarrow.parquet"
3. `src/utils/group_details.py:76` - Skipping analyzing "pyarrow" and "pyarrow.parquet"
4. `src/utils/group_details.py:398` - Skipping analyzing "pyarrow.compute"
5. `src/utils/filtering.py:139-140` - Skipping analyzing "pyarrow" and "pyarrow.compute"
6. `tests/groups_pagination.py:12` - Skipping analyzing "pyarrow"
7. `tests/contracts/test_parquet_contracts.py:14-15` - Skipping analyzing "pyarrow"

**Root Cause**: PyArrow library lacks comprehensive type stubs

**Refined Fix Plan**:
1. **Install vetted stubs**:
   ```bash
   pip install pyarrow-stubs
   ```
2. **If residual issues remain**, create minimal local stubs:
   ```python
   # typings/pyarrow/__init__.pyi
   # typings/pyarrow/compute.pyi  
   # typings/pyarrow/parquet.pyi
   ```
3. **Ensure mypy.ini has**:
   ```ini
   mypy_path = typings
   ```
4. **Contain imports to TYPE_CHECKING blocks**:
   ```python
   from typing import TYPE_CHECKING
   if TYPE_CHECKING:
       import pyarrow as pa
       import pyarrow.parquet as pq
       import pyarrow.compute as pc

   def _to_table(...) -> "pa.Table":
       import pyarrow as pa  # Runtime import inside function
       ...
   ```
5. **Only if necessary**: Targeted ignore for pyarrow only:
   ```ini
   [mypy-pyarrow.*]
   ignore_missing_imports = True
   ```

**Safety Note**: Avoid blanket ignores; prefer precise stubs to maintain type safety. Document any ignores in working log.

---

### **Category 2: Pandas Type Overload Issues (3 errors)**
**Difficulty**: High - Complex pandas type system interactions

#### **Files and Locations**:
1. `src/utils/io_utils.py:293` - No overload variant of "read_csv" matches
2. `src/utils/io_utils.py:303` - No overload variant of "read_csv" matches  
3. `src/utils/io_utils.py:374` - Incompatible return value type

**Root Cause**: Pandas read_csv has complex overload signatures that mypy struggles with

**Refined Fix Plan**:
1. **Install pandas-stubs**:
   ```bash
   pip install pandas-stubs
   ```
2. **Create typed wrapper with kw-only signature**:
   ```python
   from pathlib import Path
   from typing import Dict, Mapping, Optional
   import pandas as pd

   def read_csv_typed(
       path: Path,
       *,
       dtype: Optional[Mapping[str, str]] = None,
       usecols: Optional[list[str]] = None,
       na_values: Optional[list[str]] = None,
       engine: str = "c",
   ) -> pd.DataFrame:
       # Narrow, kw-only signature keeps mypy happy
       return pd.read_csv(path, dtype=dtype, usecols=usecols, na_values=na_values, engine=engine)
   ```
3. **Replace only failing call sites**; keep others untouched
4. **If call site still noisy**, use surgical `# type: ignore[call-overload]  # rationale`
5. **Record each ignore** in working log with rationale

**Safety Note**: Prefer wrapper functions over blanket ignores. Do NOT ignore pandas.* globally - we rely on those types to catch regressions.

---

### **Category 3: Series/DataFrame Operations (4 errors)**
**Difficulty**: High - Complex pandas Series/DataFrame type interactions

#### **Files and Locations**:
1. `src/utils/group_stats.py:64` - "Series[Any]" not callable
2. `src/utils/group_stats.py:122` - "Series[Any]" not callable
3. `src/survivorship.py:146` - No overload variant of "__getitem__" of DataFrame
4. `src/survivorship.py:164` - Incompatible types in assignment (DataFrame indexing)

**Root Cause**: Complex pandas DataFrame/Series type inference issues

**Refined Fix Plan**:
1. **Eliminate callable pitfalls**:
   ```python
   # BAD: s(...)  # Series being called as function
   # GOOD: 
   s = df["col"]
   s2 = s.map(func)  # or .apply(func), prefer .map for elementwise
   ```
2. **Use explicit .loc/.iloc with narrow types**:
   ```python
   from pandas import Series, DataFrame
   col: Series = df.loc[:, "some_col"]
   df2: DataFrame = df.loc[df["flag"] == 1, :]
   ```
3. **Annotate multi-column selections**:
   ```python
   subset: DataFrame = df[["a", "b", "c"]]
   ```
4. **Avoid dtype reassignment without .astype()**:
   ```python
   df["col"] = df["col"].astype("string")  # Explicit conversion
   ```
5. **Use explicit selection with proper types**:
   ```python
   from pandas import DataFrame, Series
   col: Series = df.loc[:, "col_name"]
   sub: DataFrame = df.loc[df["flag"] == 1, ["a", "b"]]
   ```
6. **Add TypedDict/schema comments at boundaries** for known column structures
7. **Create tiny adapters if needed** (use sparingly):
   ```python
   def as_series(x: Any) -> "pd.Series":
       import pandas as pd
       return x  # type: ignore[return-value]
   ```

**Safety Note**: Prefer real type narrowing over adapters; use adapters only as last resort

---

### **Category 4: Unreachable Code (6 errors)**
**Difficulty**: Medium - Logic flow analysis issues

#### **Files and Locations**:
1. `src/utils/parallel_utils.py:235` - Statement is unreachable
2. `src/utils/perf_utils.py:82` - Statement is unreachable
3. `src/utils/exact_equals.py:32` - Statement is unreachable
4. `src/performance.py:42` - Statement is unreachable
5. `app/main.py:129` - Statement is unreachable

**Root Cause**: Mypy's control flow analysis detecting unreachable code

**Fix Plan**:
1. **Review each unreachable statement**:
   ```python
   # Check the logic flow leading to these lines
   # Determine if code is actually unreachable or if mypy is wrong
   ```
2. **Fix logic flow** if code should be reachable
3. **Remove unreachable code** if it's truly dead code
4. **Add type: ignore[unreachable]** if mypy is incorrect

**Recommended Approach**: Review each case individually and fix logic or add type ignore

---

### **Category 5: Missing Type Annotations (4 errors)**
**Difficulty**: Low-Medium - Straightforward type annotation fixes

#### **Files and Locations**:
1. `src/utils/duckdb_utils.py:8` - Function is missing a return type
2. `src/similarity/scoring.py:149` - Function is missing a type annotation
3. `src/utils/duckdb_utils.py:25` - Returning Any from function declared to return
4. `src/utils/duckdb_utils.py:35` - Returning Any from function declared to return

**Root Cause**: Missing or incomplete type annotations

**Fix Plan**:
1. **Add return type annotations**:
   ```python
   def function_name() -> ReturnType:
       # function body
   ```
2. **Add type casts for Any returns**:
   ```python
   return cast(ExpectedType, some_any_value)
   ```
3. **Import necessary types**:
   ```python
   from typing import cast, Any, Dict, List
   ```

**Recommended Approach**: Add proper type annotations and casts

---

### **Category 6: Import and Module Issues (4 errors)**
**Difficulty**: Medium - Module resolution and import conflicts

#### **Files and Locations**:
1. `setup.py:6` - Library stubs not installed for "setuptools"
2. `src/utils/metrics.py:16` - Cannot find implementation or library stub
3. `src/utils/__init__.py:30` - Incompatible import of "optimize_dataframe_memory"
4. `src/utils/fragment_utils.py:24` - Module has no attribute

**Root Cause**: Missing type stubs or import path issues

**Fix Plan**:
1. **Install missing stubs**:
   ```bash
   pip install types-setuptools
   ```
2. **Fix import paths**:
   ```python
   # Check if imports are correct
   # Update import statements if needed
   ```
3. **Add type: ignore comments** for problematic imports
4. **Create stub files** for missing modules

**Recommended Approach**: Install stubs and fix import paths

---

### **Category 7: Complex Type Assignments (3 errors)**
**Difficulty**: Medium-High - Complex type compatibility issues

#### **Files and Locations**:
1. `src/performance.py:20` - Incompatible types in assignment
2. `app/components/maintenance.py:262` - Incompatible types in assignment
3. `scripts/run_modes_benchmark.py:269` - Dict entry has incompatible type

**Root Cause**: Type system complexity in data structures

**Fix Plan**:
1. **Examine the specific assignments**:
   ```python
   # Check the exact types being assigned
   # Identify the type mismatch
   ```
2. **Add explicit type annotations**:
   ```python
   variable: ExpectedType = value
   ```
3. **Use type casts** if necessary:
   ```python
   variable = cast(ExpectedType, value)
   ```
4. **Create type-safe data structures**

**Recommended Approach**: Add explicit type annotations and casts

---

### **Category 8: Function Return Issues (3 errors)**
**Difficulty**: Low-Medium - Return type mismatches

#### **Files and Locations**:
1. `scripts/score_pair.py:170` - No return value expected
2. `scripts/score_pair.py:195` - "trace_scoring" does not return a value
3. `src/utils/ui_session.py:52` - Returning Any from function declared to return

**Root Cause**: Function signature and return type mismatches

**Fix Plan**:
1. **Fix return statements**:
   ```python
   # Add return statements where missing
   # Remove return statements where not expected
   ```
2. **Update function signatures**:
   ```python
   def function_name() -> None:  # or appropriate return type
   ```
3. **Add type casts** for Any returns

**Recommended Approach**: Fix return statements and update signatures

---

### **Category 9: Complex Logic Issues (4 errors)**
**Difficulty**: High - Complex business logic type issues

#### **Files and Locations**:
1. `tests/test_mini_dag_state_transitions.py:117` - Non-overlapping equality check
2. `src/grouping.py:335` - Returning Any from function declared to return
3. `src/grouping.py:596` - Function "optimize_dataframe_memory" could always return
4. `src/grouping.py:706` - Function "optimize_dataframe_memory" could always return

**Root Cause**: Complex business logic with type inference challenges

**Fix Plan**:
1. **Review business logic**:
   ```python
   # Understand the complex logic flow
   # Identify where type inference fails
   ```
2. **Add explicit type annotations**:
   ```python
   # Add type hints for complex variables
   # Use type guards for complex conditions
   ```
3. **Simplify complex expressions**:
   ```python
   # Break down complex expressions
   # Use intermediate variables with explicit types
   ```

**Recommended Approach**: Add explicit type annotations and simplify complex logic

---

### **Category 10: Attribute Access Issues (1 error)**
**Difficulty**: Medium - Object attribute access type issues

#### **Files and Locations**:
1. `src/similarity/scoring.py:166` - "ParallelExecutor" has no attribute "map"

**Root Cause**: Missing attribute in type definition

**Refined Fix Plan**:
1. **Create Protocol for contract** (match real usage to eliminate future churn):
   ```python
   # src/utils/parallel_protocols.py
   from typing import Protocol, Iterable, TypeVar, Callable, Iterator, Optional

   T = TypeVar("T")
   R = TypeVar("R")

   class ExecutorLike(Protocol):
       @property
       def workers(self) -> int: ...
       def map(
           self,
           fn: Callable[[T], R],
           items: Iterable[T],
           *,
           chunksize: Optional[int] = None,
       ) -> Iterator[R]: ...
   ```
2. **Update scoring module to use Protocol**:
   ```python
   from src.utils.parallel_protocols import ExecutorLike
   from typing import Optional

   def score_pairs_parallel(
       df_norm: pd.DataFrame,
       candidate_pairs: list[tuple[int, int]],
       settings: dict[str, Any],
       enable_progress: bool = False,
       parallel_executor: Optional[ExecutorLike] = None,
   ) -> list[dict[str, Any]]:
       ...
   ```
3. **If we control ParallelExecutor**, ensure it implements the same signature (including chunksize kw)

**Safety Note**: Protocol-based typing provides better contract enforcement than duck typing

---

## **Resolution Strategy**

### **Cross-Cutting Guardrails (Safety First)**
- **Focus filter**: Operate only on 5-10 files with highest error counts per `docs/reports/mypy_latest.txt`
- **Prefer type narrowing** and precise annotations over blanket `ignore_missing_imports`
- **Minimal local stubs** or Protocol/TypedDict wrappers over global ignores
- **Run from .venv**: All commands executed in virtual environment
- **Quality gates after each batch**: `ruff check . --fix && black . && pytest -q && mypy --show-error-codes --pretty`
- **Document each fix**: Record rationale in working log, avoid broad `# type: ignore`

### **Enhanced Configuration & CI Discipline**
- **Pin tool versions** in `tox.ini`/`pyproject.toml` to stabilize behavior:
  ```toml
  mypy==1.10.*
  pandas-stubs
  pyarrow-stubs
  ruff
  black
  ```
- **Staged mypy strict mode** (do NOT enable globally at once):
  ```ini
  [mypy]
  warn_unused_ignores = True
  warn_redundant_casts = True
  no_implicit_optional = True
  warn_return_any = True
  disallow_any_generics = True

  [mypy-src.utils.*]
  strict = True  # only after the file is green under base settings
  ```
- **Pre-commit hooks** for quick feedback: `ruff --fix`, `black`, `mypy -p src.utils`
- **Machine-readable snapshots**: `mypy --show-error-codes --pretty > docs/reports/mypy_latest.txt`

### **Phase 1: Quick Wins (Categories 5, 8) - 7 errors**
**Estimated Effort**: 1-2 hours
**Files to Fix**:
- `src/utils/duckdb_utils.py` (3 errors)
- `src/similarity/scoring.py` (1 error) 
- `scripts/score_pair.py` (2 errors)
- `src/utils/ui_session.py` (1 error)

**Refined Action Plan**:
1. **Add precise return type annotations**:
   ```python
   def connect() -> Any:  # Replace with proper return type
   def get_optimal_workers() -> int:  # Add explicit int return
   ```
2. **Fix function return statements**:
   ```python
   def trace_scoring(...) -> None:  # Set -> None, remove returns
   def main() -> int:  # CLI functions return exit codes
   ```
3. **Add type casts for Any returns**:
   ```python
   return cast(Optional[str], d.get("run_id"))
   ```

### **Phase 2: Medium Complexity (Categories 4, 6, 7) - 13 errors**
**Estimated Effort**: 3-4 hours
**Files to Fix**:
- `src/utils/parallel_utils.py` (1 error)
- `src/utils/perf_utils.py` (1 error)
- `src/utils/exact_equals.py` (1 error)
- `src/performance.py` (1 error)
- `app/main.py` (1 error)
- `setup.py` (1 error)
- `src/utils/metrics.py` (1 error)
- `src/utils/__init__.py` (1 error)
- `src/utils/fragment_utils.py` (1 error)
- `app/components/maintenance.py` (1 error)
- `scripts/run_modes_benchmark.py` (1 error)

**Refined Action Plan**:
1. **Audit unreachable code** (don't paper over):
   ```python
   # Review control flow, fix logic, only use # type: ignore[unreachable] if proven false-positive
   ```
2. **Install minimal stubs**:
   ```bash
   pip install types-setuptools
   ```
3. **Fix import paths and verify symbols**:
   ```python
   # Check __all__ exports, verify real symbol names
   ```
4. **Structure complex data with TypedDict**:
   ```python
   from typing import TypedDict, Optional

   class ScoringPenaltyCfg(TypedDict, total=False):
       suffix_mismatch: int
       num_style_mismatch: int
       punctuation_mismatch: int

   class ScoringCfg(TypedDict, total=False):
       use_bulk_cdist: bool
       gate_cutoff: int
       penalty: ScoringPenaltyCfg
   ```
5. **Single typed return pattern** for "could always return" issues:
   ```python
   def optimize_dataframe_memory(df: pd.DataFrame) -> pd.DataFrame:
       out = df.copy()  # Work on local copy
       # Apply typed steps; avoid conditional early returns
       return out  # Single, explicit return of DataFrame
   ```

### **Phase 3: High Complexity (Categories 1, 2, 3, 9, 10) - 23 errors**
**Estimated Effort**: 8-12 hours
**Files to Fix**:
- All PyArrow integration files (12 errors)
- `src/utils/io_utils.py` (3 errors)
- `src/utils/group_stats.py` (2 errors)
- `src/survivorship.py` (2 errors)
- `tests/test_mini_dag_state_transitions.py` (1 error)
- `src/grouping.py` (3 errors)

**Refined Action Plan**:
1. **PyArrow: Use vetted stubs + local shims**:
   ```bash
   pip install pyarrow-stubs
   # Create minimal local stubs in typings/pyarrow/ if needed
   ```
2. **Pandas: Create typed wrapper functions**:
   ```python
   def read_csv_typed(path: Path, *, dtype: Optional[Mapping[str, str]] = None) -> pd.DataFrame:
       return pd.read_csv(path, dtype=dtype)  # Narrow, kw-only signature
   ```
3. **Series/DataFrame: Eliminate callable pitfalls**:
   ```python
   # Replace s(...) with s[...] or s.map(func)
   # Use explicit .loc/.iloc with narrow types
   ```
4. **ParallelExecutor: Use Protocol for contract**:
   ```python
   class ExecutorLike(Protocol):
       def map(self, fn: Callable[[T], R], chunks: Iterable[T]) -> Iterator[R]: ...
   ```

## **First Batch Plan (5-File Focus)**

### **Current Top Error Files** (from `docs/reports/mypy_latest.txt`):
1. `src/utils/duckdb_utils.py` (3 errors) - Missing return types, Any returns
2. `scripts/score_pair.py` (2 errors) - Function return issues  
3. `src/utils/ui_session.py` (1 error) - Any return type
4. `src/similarity/scoring.py` (1 error) - Missing type annotation
5. `src/performance.py` (1 error) - Assignment type issue

### **New Helper Modules** (Add first):
1. **`src/utils/parallel_protocols.py`**:
   ```python
   from typing import Protocol, Iterable, TypeVar, Callable, Optional

   T = TypeVar("T")
   R = TypeVar("R")

   class ExecutorLike(Protocol):
       @property
       def workers(self) -> int: ...
       def map(
           self,
           fn: Callable[[T], R],
           items: Iterable[T],
           *,
           chunksize: Optional[int] = None,
       ) -> Iterable[R]: ...  # ← Iterable, not Iterator (some executors return list)
   ```

2. **`src/similarity/types.py`**:
   ```python
   from typing import TypedDict

   class ScoreComponents(TypedDict):
       score: int
       ratio_name: int
       ratio_set: int
       jaccard: float
       num_style_match: bool
       suffix_match: bool
       punctuation_mismatch: bool
       base_score: float
   ```

### **Batch 1 Fix Techniques** (Refined with ChatGPT feedback):
1. **`src/utils/duckdb_utils.py`**:
   - Fix context manager to return `Iterator["DuckDBPyConnection"]` (with TYPE_CHECKING gate)
   - Add `cast(pd.DataFrame, ...)` for duckdb .df() Any returns
   - Add narrow `# type: ignore[attr-defined]` for register method only
   - Use TYPE_CHECKING to avoid runtime import issues with DuckDBPyConnection

2. **`scripts/score_pair.py`**:
   - Set `trace_scoring()` to `-> int` (return the score)
   - Set `main()` to `-> int`, use `raise SystemExit(main())`

3. **`src/similarity/scoring.py`**:
   - Create Protocol for ExecutorLike to fix "map" attribute
   - Use TypedDict for ScoreComponents to eliminate Any propagation
   - Add proper type narrowing for enhanced tokens

4. **`src/grouping.py`**:
   - Fix Optional callable typing to eliminate "could always return"
   - Add proper type annotations for optional imports
   - Fix fallback size counter bug: `return sum(1 for node in parent.keys() if find(node) == root)`

5. **`src/performance.py`**:
   - Fix assignment type with explicit Optional type annotation

### **Expected Outcome**:
- **Before**: 43 errors
- **After**: ~35 errors (8 errors resolved)
- **Quality Gates**: All must pass (ruff, black, pytest, mypy)

### **Safety Notes**:
- **New modules are typing-only** - No functional changes, just type definitions
- **Protocol approach** - Better contract enforcement than concrete imports
- **TypedDict approach** - Eliminates Any propagation while maintaining runtime compatibility
- **Surgical type: ignore** - Only for specific duckdb stub gaps, documented with rationale
- **Context manager typing** - Proper Iterator[T] return type for mypy compliance

### **Pre-flight Checklist**:
- ✅ Update ExecutorLike to return `Iterable[R]` (not Iterator)
- ✅ Use TYPE_CHECKING gate for DuckDBPyConnection and annotate connect() as `Iterator["DuckDBPyConnection"]`
- ✅ `trace_scoring()` -> int, `main()` -> int, `raise SystemExit(main())`
- ✅ Optional-callable typing in grouping.py + fix get_group_size
- ✅ Remove unnecessary `# type: ignore[no-any-return]` in io_utils.py
- ✅ Keep all changes surgical; document each ignore/cast in the working log with a 1-liner rationale
- ✅ Run gates in order: `ruff check . --fix && black . && pytest -q && mypy --show-error-codes --pretty > docs/reports/mypy_latest.txt`

## **Tools and Resources**

### **Required Packages**:
```bash
pip install pyarrow-stubs pandas-stubs types-setuptools
```

### **Mypy Configuration**:
```ini
# mypy.ini additions for difficult errors
mypy_path = typings

[mypy]
warn_unused_ignores = True
warn_redundant_casts = True
no_implicit_optional = True
warn_return_any = True
disallow_any_generics = True

[mypy-src.utils.*]
strict = True  # only after the file is green under base settings

[mypy-pyarrow.*]
ignore_missing_imports = True  # only if absolutely necessary
```

### **Type Checking Commands**:
```bash
# Check specific files
mypy src/utils/io_utils.py --show-error-codes --pretty

# Check specific error types
mypy . --show-error-codes | grep "call-overload"

# Check with specific mypy version
mypy --version
```

## **Success Metrics**

- **Phase 1 Complete**: 7 errors resolved
- **Phase 2 Complete**: 20 errors resolved (7 + 13)
- **Phase 3 Complete**: 43 errors resolved (all remaining)
- **Final Goal**: 0 mypy errors

## **Batch Execution Template (Repeat Per Batch)**

### **1. Capture Baseline**:
```bash
mypy --show-error-codes --pretty > docs/reports/mypy_latest.txt
```

### **2. Fix Target Files** (5 highest error count files)

### **3. Run Quality Gates**:
```bash
ruff check . --fix && black . && pytest -q && mypy --show-error-codes --pretty
```

### **4. Update Documentation**:
Update `mypy_error_audit.md` with:
- Files touched
- Techniques used (Protocol, TypedDict, wrapper, cast, stub)
- Error deltas (before/after counts)
- Any `# type: ignore[...]` lines with one-line reasons

### **5. Verify File Paths**:
- Check actual file paths match documentation
- Verify `tests/test_groups_pagination.py` (not `tests/groups_pagination.py`)

## **Notes**

- This document serves as a working guide for systematic error resolution
- Each error should be addressed individually with proper testing
- Maintain code functionality while improving type safety
- Document any workarounds or type: ignore comments with explanations
- Update this document as errors are resolved
- **Do NOT ignore pandas.* globally** - we rely on those types to catch regressions

---

**Last Updated**: 2025-09-05  
**Next Review**: After Phase 1 completion
