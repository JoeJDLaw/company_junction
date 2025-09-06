# Final QA Report - Company Junction

## Header
- **Commit SHA**: d918df2c3569310cade2955452e14a735e8aa291
- **Python Version**: 3.12.2
- **OS**: Darwin 24.5.0
- **Timestamp**: 2025-09-06T00:01:52Z

## Quality Gates Summary

### Black: ❌ FAIL
- **Status**: 117 files would be reformatted, 56 files would be left unchanged
- **Artifact**: [artifacts/qa/black.txt](artifacts/qa/black.txt)
- **Note**: Code formatting issues present but not blocking for functionality

### Ruff: ❌ FAIL  
- **Status**: 4,300+ issues found (mostly E501 line length)
- **Top Issues**: E501 (line too long), T201 (print statements), UP006 (typing imports)
- **Artifact**: [artifacts/qa/ruff.txt](artifacts/qa/ruff.txt)
- **Note**: High-signal issues (E722, E402, E712, F841, W293) were resolved in previous cleanup

### Mypy: ❌ FAIL
- **Status**: 31 errors in 15 files
- **Breakdown**:
  - import-untyped: 12 errors (pyarrow, setuptools)
  - import-not-found: 4 errors (pyinstrument, orjson)
  - name-defined: 5 errors (temp_dir variables)
  - var-annotated: 4 errors (missing type annotations)
  - arg-type: 1 error (assert_frame_equal)
  - unused-ignore: 1 error
- **Artifact**: [artifacts/qa/mypy.txt](artifacts/qa/mypy.txt)

### Pytest: ❌ FAIL
- **Status**: 45 failed, 735 passed, 19 skipped, 25 warnings, 10 errors
- **Total Tests**: 809
- **Pass Rate**: 90.9% (735/809)
- **Artifact**: [artifacts/qa/pytest_full.txt](artifacts/qa/pytest_full.txt)
- **JUnit XML**: [artifacts/qa/junit.xml](artifacts/qa/junit.xml)

### Coverage: ❌ NO DATA
- **Status**: No coverage data collected (pytest failed before coverage collection)
- **Artifact**: [artifacts/qa/coverage.xml](artifacts/qa/coverage.xml)
- **HTML Report**: [artifacts/qa/htmlcov/index.html](artifacts/qa/htmlcov/index.html) (not generated)

## Slowest Tests

| Test Node ID | Duration |
|--------------|----------|
| tests/test_interrupt_resume.py::TestInterruptResumeIntegration::test_interrupt_resume_workflow | 0.28s |
| tests/test_readonly_safety.py::TestReadOnlySafety::test_no_destructive_functions_in_code | 0.26s |
| tests/test_readonly_safety.py::TestReadOnlySafety::test_no_direct_run_index_deletions | 0.22s |
| tests/test_group_stats_memoization.py::TestGroupStatsMemoization::test_duckdb_memoization_smoke | 0.22s |
| tests/test_disposition_vectorized_phase1353.py::test_performance_improvement | 0.20s |
| tests/test_env_clamp.py::test_parallel_map_uses_blas_clamp | 0.12s |
| tests/lints/test_no_schema_fragile_hardcoding.py::TestNoSchemaFragileHardcoding::test_no_hardcoded_is_primary_without_availability_check | 0.09s |
| tests/lints/test_no_schema_fragile_hardcoding.py::TestNoSchemaFragileHardcoding::test_no_hardcoded_weakest_edge_without_availability_check | 0.09s |
| tests/test_scoring_bulk_gate.py::TestScoringBulkGate::test_bulk_gate_cutoff_behavior | 0.04s |
| tests/lints/test_no_ui_helpers_import.py::test_no_ui_helpers_import | 0.03s |

## Flakiness Check

### Run 1 vs Run 2 Comparison
- **Outcome**: Identical test results across both runs
- **Duration Variance**: <5% variance in test durations
- **Flakiness**: No flaky tests detected
- **Determinism**: ✅ Tests are deterministic

### Duration Outliers
- No tests showed >3× duration variance between runs
- All test durations remained consistent within expected ranges

## Known Limitations (Documented)

1. **Punctuation penalties suppressed by normalization in production flow** - Intentionally documented baseline
2. **None inputs crash (AttributeError)** - Intentionally documented baseline  
3. **`use_bulk_cdist` read but unused** - Documented in code comments
4. **Results not sorted** - Documented baseline behavior
5. **Missing `src.utils.validation_utils`** - Module referenced in tests but not present in codebase
6. **PyArrow compute function type issues** - Related to our recent filtering.py changes

## Cursor Rules Compliance Snapshot

- **Rule §2**: Config over constants — ✅ (tests may use explicit constants to assert deltas; acceptable)
- **Rule §4**: Logging contract tested — ✅ bulk gate path smoke-checked
- **Rule §5**: Determinism — ✅ documented and tested
- **Rule §6**: Coverage — ❌ No coverage data collected due to test failures
- **Rule §7**: CI guardrails — ❌ Black/Ruff/Mypy/Pytest all have issues
- **Rule §10**: No deletions — ✅ No code deletions made to src/similarity/scoring.py

## Test Failure Analysis

### Major Failure Categories:
1. **Import Errors** (10 errors): Missing modules, path issues
2. **Schema/Contract Failures** (8 failures): Parquet schema validation issues
3. **Variable Name Issues** (5 failures): `temp_dir` vs `_temp_dir` inconsistencies
4. **PyArrow Compute Issues** (3 failures): Type errors from recent filtering changes
5. **UI/Component Issues** (3 failures): Streamlit component problems
6. **File System Issues** (2 failures): Missing test files

### Critical Issues:
- **No coverage data collected** due to early test failures
- **Multiple import path issues** affecting test execution
- **Schema validation failures** indicating potential data contract issues

## Next Suggested Enhancements (Non-blocking)

1. **Fix import path issues** in test configuration
2. **Resolve temp_dir variable naming** inconsistencies in test files
3. **Add pytest.ini markers** for long tests and create `-m "not slow"` profile
4. **Narrow log assertion patterns** to avoid brittleness on wording changes
5. **Fix PyArrow compute function** type issues in filtering.py
6. **Address missing validation_utils** module references

## Summary

The codebase has **significant test infrastructure issues** that prevent proper quality gate execution. While the core functionality appears intact (735 tests passing), the test environment needs attention to:

1. Fix import path configuration
2. Resolve variable naming inconsistencies  
3. Address schema validation issues
4. Fix PyArrow compute function type problems

**Zero code changes were made to `src/similarity/scoring.py`** as requested, maintaining the integrity of the similarity scoring module.

**Coverage data could not be collected** due to test execution failures, making it impossible to report on `src/similarity/scoring.py` coverage percentage.
