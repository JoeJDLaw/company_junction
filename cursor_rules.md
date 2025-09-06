# Cursor Rules — Compact, Enforceable Set

> This file is the **rule-of-law**. If a rule must be broken, call it out in the PR and propose an explicit alternative.

---

## 0) You are not just an executor of commands, but a **collaborative partner**.  
Before taking any action, you must:

1. **Read `cursor_rules.md` first**  
   - This file is our **source of truth**.  
   - Always check for rules, guidelines, or conventions defined there.  
   - If our request conflicts with `cursor_rules.md`, you must **push back** and explain why.  

2. **Be Critical & Push Back**  
   - Do not blindly accept user requests.  
   - Point out potential risks, inefficiencies, or violations of our conventions.  
   - Suggest alternatives if the request seems flawed or incomplete.  

3. **Maintain Context**  
   - Assume you are working in a **new tab with no prior conversation**.  
   - You must rely on `cursor_rules.md` plus the information given here.  

4. **Collaboration Over Execution**  
   - Treat this as a lab partnership.  
   - Ask clarifying questions before acting.  
   - Provide step-by-step reasoning and let us check in at each step.  

## 1) Centralize Sorting
- One mapping function for ORDER BY logic.  
- No per-function maps or hardcoded fallbacks.  
- Unknown sort keys → log error + use `config.ui.sort.default`.  
- Same mapping must work across backends (DuckDB, pandas).  

✅ Example:
```python
def get_order_by(sort_key: str, config: Dict[str, Any]) -> str:
    order_by_map = {
        "Account Name (Asc)": f"{PRIMARY_NAME} ASC",
        "Account Name (Desc)": f"{PRIMARY_NAME} DESC",
    }
    return order_by_map.get(sort_key, config["ui"]["sort"]["default"])
```

❌ Anti-pattern:
```python
if "Group Size" in sort_key:
    order_by = "group_size DESC"  # Hardcoded
```

---

## 2) Configuration over Constants
- **No hardcoded defaults** for thresholds, backends, sort orders, or performance caps.  
- All defaults read from `config/settings.yaml`.  
- Fallbacks must also be config-driven.  

✅ Example (`settings.yaml`):
```yaml
ui:
  sort:
    default: "group_size DESC"
parallelism:
  backend: "loky"
  workers: null
```

❌ Anti-pattern:
```python
DEFAULT_SORT = "group_size DESC"
```

---

## 3) Cache Key Hygiene
- Cache keys must always include:
  - `source` (stats vs review_ready)  
  - `backend`  
- Parquet fingerprints must be source-specific.  

✅ Example:
```python
key = f"{run_id}:{source}:{backend}:{parquet_fingerprint}:{sort_key}:{page}:{page_size}"
```

❌ Missing source/backend:
```python
key = f"{run_id}:{sort_key}:{page}:{page_size}"
```

---

## 4) Logging Contract
- Each path logs:  
  `prefix | sort_key='...' | order_by='...' | backend=...`  
- Fallbacks log **reason** + chosen path.  
- Distinct log prefixes per function/backend.  

✅ Example:
```python
logger.info(f"groups_page_duckdb | sort_key='{sort_key}' | order_by='{order_by}' | backend=duckdb")
```

❌ Ambiguous:
```python
logger.info(f"DuckDB query built | order_by='{order_by}'")
```

---

## 5) Determinism & Safety
- Same inputs + run_id → identical outputs.  
- No nondeterministic ordering.  
- **Cleanup tools**:  
  - Deterministic discovery via `run_index.json`  
  - Protect latest & pinned runs by default  
  - Support empty-state (`latest.json` with `run_id: null`)  

---

## 6) Test Coverage
- **Sort Mapping**: unknown keys, cross-backend parity.  
- **Cache Keys**: must change with source/backend differences.  
- **Logging**: correct prefixes, includes sort_key/order_by/backend.  
- **Cleanup**: type/age filtering, prod sweep, JSON output, exit codes.  
- **Survivorship**: optimized flag vs baseline equivalence.  
- **Shape guards**: ensure survivors always 1D.  

---

## 7) Tooling & CI Guardrails
All new and modified code must pass:
- **Black** for formatting  
- **Ruff** for linting & unused imports  
- **Mypy** for static type checking  
- **Pytest** for all tests (unit + integration)  

> CI will block merges unless all four tools pass.

---

## 8) Change Management Philosophy
- PRs can include **multiple related changes** if they solve connected problems.  
- Scope creep is acceptable when it reduces overhead or improves coherence.  
- Rollback plans are optional — use Git history for recovery.  
- Large or multi-scope PRs must be clearly documented with reasoning.  

---

## 9) Cleanup Standards
- Deterministic candidate discovery from `run_index.json`.  
- Config-driven pinned runs and protections.  
- Always protect **latest symlink** and pinned runs unless explicitly overridden.  
- Double confirmation required for production deletions.  
- Exit codes standardized:  
  - `0 = no candidates`  
  - `2 = candidates found`  
  - `>0 = errors`  

---

## 10) Deprecation & File Preservation (No Deletions)

- **Never delete files** during refactors or sweeps.
- When a file is superseded or replaced, **move** it to the `deprecated/` folder, **prefix** the filename with a UTC timestamp, and **append** `.bak`.
- Preserve the original relative path *under* `deprecated/` when possible.

**Required format**
```
deprecated/{YYYYMMDD-HHMMSS}_&lt;original_relative_path_with_slashes_replaced_by_underscores&gt;.bak
```

**Examples**
- `src/utils/test.py` → `deprecated/20250905-1412_src_utils_test.py.bak`
- `tests/test_similarity.py` → `deprecated/20250905-1412_tests_test_similarity.py.bak`

**Git-aware move (preferred)**
- If the repo is git-tracked, use `git mv` so history is preserved.
- Create the `deprecated/` directory (and subdirs) if missing.

✅ Snippet:
```bash
ts=$(date -u +"%Y%m%d-%H%M%S")
safe_path=$(echo "src/utils/test.py" | tr '/' '_')
mkdir -p deprecated
git mv "src/utils/test.py" "deprecated/${ts}_${safe_path}.bak"
```

**Rationale**
- Maintains a human-auditable trail of removals.
- Allows fast rollback without digging through history.
- Keeps CI and local tooling from silently losing reference files.

**Absolutely do not**
- `rm`, `git rm`, or deleting within editor UI.
- Overwrite-in-place without preserving the original via the steps above.

---

## Known Limitations & Baseline Behavior

> **Important**: These are documented behaviors and limitations discovered through comprehensive testing. They represent the current system state and should be considered when making changes.

### Similarity Scoring System

**Gate Cutoff Behavior**:
- The bulk scoring gate uses `token_set_ratio` for initial filtering, not final composite scores
- This means pairs can pass the gate but have final scores below the gate cutoff due to penalties
- **Impact**: Gate cutoff of 72 filters on `token_set_ratio >= 72`, but final scores may be lower

**Punctuation Penalty Limitations**:
- Punctuation penalties work correctly in unit tests (direct `compute_score_components` calls)
- In production pipeline, punctuation penalties are largely ineffective because `normalize_dataframe` strips punctuation before scoring
- **Impact**: Punctuation mismatch penalties rarely fire in actual usage

**Sort Order Behavior**:
- Results are NOT currently sorted by the scoring functions
- **Impact**: Output order is not deterministic and may vary between runs
- **Expected Contract**: Future implementation should sort by (id_a, id_b asc; score desc)

**Input Validation Limitations**:
- `None` inputs to `compute_score_components` cause `AttributeError` crashes
- **Impact**: System is not resilient to None inputs from upstream processing
- **Recommendation**: Add input validation or handle gracefully

**Configuration Handling**:
- Config sections set to `None` cause `AttributeError` when accessing nested properties
- **Impact**: Invalid config structures crash the system rather than falling back to defaults
- **Recommendation**: Add defensive config parsing

### Usage Guidelines

When working with similarity scoring:
1. **Gate Cutoff**: Understand it filters on `token_set_ratio`, not final scores
2. **Punctuation**: Don't rely on punctuation penalties in production flows
3. **Sorting**: Don't assume results are sorted - implement sorting if needed
4. **Input Validation**: Ensure inputs are not None before calling scoring functions
5. **Config Validation**: Ensure config structures are valid dictionaries, not None

---

## Compliance Checklist
- [ ] Centralized sort mapping, no per-function maps  
- [ ] No hardcoded defaults (all from config)  
- [ ] Cache keys include `source` + `backend`  
- [ ] Logging includes sort_key/order_by/backend, with distinct prefixes  
- [ ] Deterministic outputs for same run_id  
- [ ] Cleanup protects latest + pinned, supports empty state  
- [ ] All code passes **Black, Ruff, Mypy, Pytest**  
- [ ] Single-scope PRs with rollback plan  
- [ ] No direct deletions — follow Rule 10 deprecations
- [ ] Deprecated files moved to `deprecated/` with UTC timestamp prefix and `.bak` suffix

---
