# Cursor Rules — Compact, Enforceable Set

> This file is the **rule-of-law**. If a rule must be broken, call it out in the PR and propose an explicit alternative.

---

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

## 8) Small, Reversible Changes
- Each PR = one focused change.  
- Rollback plan required for each optimization.  
- No scope creep (follow phase/version naming).  

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

## Compliance Checklist
- [ ] Centralized sort mapping, no per-function maps  
- [ ] No hardcoded defaults (all from config)  
- [ ] Cache keys include `source` + `backend`  
- [ ] Logging includes sort_key/order_by/backend, with distinct prefixes  
- [ ] Deterministic outputs for same run_id  
- [ ] Cleanup protects latest + pinned, supports empty state  
- [ ] All code passes **Black, Ruff, Mypy, Pytest**  
- [ ] Single-scope PRs with rollback plan  

---
