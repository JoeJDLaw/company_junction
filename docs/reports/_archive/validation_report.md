# Validation Report - Phase1.28.3 Company Junction Audit

**Date:** 2025-09-03  
**Validator:** AI Assistant  
**Status:** Critical Review Complete

---

## Agreement Matrix

| Item | Audit Claim | Your Finding | Verdict | Evidence |
|---|---|---|---|---|
| Processed scoping | group stats/details not run-scoped | **DISAGREE** - Already run-scoped | Disagree | `get_processed_dir(run_id)` returns `Path("data") / "processed" / run_id` |
| Run index path | should be data/run_index.json | **DISAGREE** - Currently `data/processed/index/run_index.json` | Disagree | `RUN_INDEX_PATH = str(get_processed_dir("index") / "run_index.json")` |
| Latest pointers | should be data/processed/latest(.json) | **AGREE** - Already correct | Agree | Hardcoded as `Path("data/processed/latest.json")` and `Path("data/processed/latest")` |
| UI flag drift | wrong key in controls.py | **AGREE** - Using legacy key | Agree | Line 42: `use_duckdb_for_groups` vs config has `ui_perf.groups.duckdb_prefer_over_pyarrow` |
| Cleanup guard | missing keep-at-least enforcement | **DISAGREE** - Guards exist but not enforced | Disagree | `--keep-at-least` arg exists but no enforcement logic found |
| Orphans | three files listed | **UNKNOWN** - Need deeper analysis | Unclear | Ruff found 10 unused imports, but not the specific modules mentioned |

---

## Risk Notes

### 🚨 **CRITICAL RISK: Path Scoping Already Correct**
The audit claims that processed directories are not run-scoped, but `get_processed_dir(run_id)` **already returns** `data/processed/{run_id}`. Changing this would break existing functionality.

**Evidence:**
```python
# src/utils/path_utils.py:69-75
def get_processed_dir(run_id: str) -> Path:
    return Path("data") / "processed" / run_id
```

**Risk:** This change would be **semantic drift** and potentially **breaking** for all consumers.

### ⚠️ **MEDIUM RISK: Run Index Path Inconsistency**
The audit wants `data/run_index.json` but current code uses `data/processed/index/run_index.json`. This affects:
- `src/utils/cache_utils.py:31`
- `tools/cleanup_test_artifacts.py:78,97`

**Risk:** Changing this path requires updating all consumers and could break existing tooling.

### ✅ **LOW RISK: UI Flag Fix**
The UI flag drift fix is safe - just changing which config key is read.

### ✅ **LOW RISK: Latest Pointers**
Latest pointers are already correctly implemented as `data/processed/latest(.json)`.

---

## Delta Recommendations

### **PR 1 (P0) - REQUIRES MAJOR REVISION**
**Current audit claim is INCORRECT.** The processed directories are already run-scoped.

**Proposed revision:**
- **Remove** the processed directory scoping changes
- **Keep only** the run index path standardization (if desired)
- **Keep** the latest pointer standardization (already correct)

### **PR 2 (P1) - SAFE TO PROCEED**
UI flag drift fix is correct and safe.

### **PR 3 (P1) - REQUIRES INVESTIGATION**
The cleanup tool already has `--keep-at-least` argument but enforcement logic is missing. Need to find where this should be implemented.

### **PR 4 (P2) - SAFE TO PROCEED**
Dead code cleanup and Ruff configuration are safe.

---

## Preconditions Checklist

- [ ] **CRITICAL**: Verify that `get_processed_dir(run_id)` behavior is intentional and not a bug
- [ ] **CRITICAL**: Confirm that all consumers expect run-scoped paths (they do, based on evidence)
- [ ] **MEDIUM**: Survey all consumers of `run_index.json` before changing its location
- [ ] **LOW**: Verify UI config key change doesn't break existing deployments
- [ ] **LOW**: Ensure cleanup tool enforcement logic is properly implemented
- [ ] **LOW**: Confirm orphan module analysis is accurate

---

## Go/No-Go Decision

### **PR 1: NO-GO** 🚫
The core assumption (processed dirs not run-scoped) is **incorrect**. This PR would introduce breaking changes for no benefit.

### **PR 2: GO** ✅
Safe UI flag fix.

### **PR 3: NO-GO** 🚫
Missing enforcement logic needs investigation first.

### **PR 4: GO** ✅
Safe cleanup and linting improvements.

---

## Alternative Minimal Approach

Instead of the proposed PR 1, consider:

1. **Audit the actual problem**: If group stats/details are being clobbered, the issue is elsewhere
2. **Verify run index consistency**: Check if the `data/processed/index/run_index.json` vs `data/run_index.json` inconsistency is causing issues
3. **Keep existing working path structure**: Don't fix what isn't broken

---

## Evidence Summary

- **Processed scoping**: Already correct via `get_processed_dir(run_id)`
- **Run index**: Currently `data/processed/index/run_index.json` 
- **Latest pointers**: Already `data/processed/latest(.json)`
- **UI flag**: Using legacy `use_duckdb_for_groups` vs config `ui_perf.groups.duckdb_prefer_over_pyarrow`
- **Cleanup guards**: Arguments exist but enforcement logic missing
- **Orphan modules**: Ruff found 10 unused imports, need deeper analysis
