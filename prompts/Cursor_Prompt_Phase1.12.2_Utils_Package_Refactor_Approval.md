# Cursor_Prompt — Phase1.12.2 — Utils Package Refactor Approval & Execution Plan

**[Phase1.12] Approved — proceed with utils refactor now (to unblock Phase1.11)**

You’re approved to execute the **extraction-first with clean break** plan now so Phase1.11 can proceed without import conflicts.

---

## A. Before you move code — show me the mapping table
Generate a short table of what you’ll move and where. Use this exact format:

| function / object | current module | proposed new module | referenced by (paths) | notes |
|---|---|---|---|---|
| `setup_logging` | `src/utils.py` | `src/utils/logging_utils.py` | `src/*` (list) | no side effects beyond logging |
| `get_project_root` | `src/utils.py` | `src/utils/path_utils.py` | … | used by path helpers |
| `ensure_directory_exists` | `src/utils.py` | `src/utils/path_utils.py` | … | mkdir -p |
| `get_data_paths` | `src/utils.py` | `src/utils/path_utils.py` | … | relies on project root |
| `validate_dataframe` | `src/utils.py` | `src/utils/validation_utils.py` | … | required columns check |
| `get_file_info` | `src/utils.py` | `src/utils/io_utils.py` | … | path stat helper |
| `list_data_files` | `src/utils.py` | `src/utils/io_utils.py` | … | glob helper |
| `load_settings` | `src/utils.py` | `src/utils/io_utils.py` (or `config_utils.py` if you prefer) | … | YAML merge w/ defaults |
| `load_relationship_ranks` | `src/utils.py` | `src/utils/io_utils.py` | … | CSV → dict |
| `log_perf` (ctx mgr) | `src/utils.py` | `src/utils/perf_utils.py` | … | coordinates w/ `performance.py` |

> If you spot other generic helpers in `cleaning.py`, `manual_io.py`, etc., propose them in the same table but **skip** pulling anything that’s domain-specific.

**Pause and post the table for approval**. Once I confirm the mapping, proceed with the steps below.

---

## B. Implementation guardrails (no shims)
- **No shims** and no temporary `src/utils.py`. We will **delete** `src/utils.py` after imports compile and tests pass.
- Use **absolute imports** rooted at `src`.
- Keep modules small and cohesive (`path_utils.py`, `logging_utils.py`, `perf_utils.py`, `io_utils.py`, `validation_utils.py`, reuse existing `dtypes.py`; add `hash_utils.py` only if Phase1.11 introduced `config_hash`/`stable_group_id`).
- Avoid circular imports; if needed, adjust responsibilities instead of adding indirection.

---

## C. Steps to execute (after I approve the mapping)
1. **Create modules** under `src/utils/` per the mapping and **copy** functions from `src/utils.py`.
2. **Update imports** across the repo to the new modules. Prefer a deterministic sweep:
   - Search: `rg -n "from src\.utils|import utils\b" .`
   - Replace to explicit module paths, e.g.:
     - `from src.utils import ensure_directory_exists` → `from src.utils.path_utils import ensure_directory_exists`
     - `from src.utils import log_perf` → `from src.utils.perf_utils import log_perf`
3. **Run tests** and fix import fallout: `pytest -q`
4. **Delete `src/utils.py`** once tests pass.
5. **Create `./deprecated/`** (root) and move any now-unused files there. Add a short `deprecated/README.md` listing moved/retired files and planned deletion date.
6. **Docs**:
   - `CHANGELOG.md` → add “Phase1.12 — Utils Package Refactor” summary (what moved, new imports, no shims).
   - `README.md` (dev section) → document utils structure and where new helpers should live.
7. **Rules check**: re-read `cursor_rules.md`. Confirm it still matches the package layout or propose edits.

**Deliverables to post back:**
- The final **mapping table** (updated if anything changed during implementation)
- Confirmation that `src/utils.py` is **removed**
- Test results summary
- Diffs (or a short list) of the updated imports
- Notes on any functions you decided **not** to extract and why

---

## D. Ordering relative to Phase1.11
Proceed with Phase1.12 now to clear the import conflicts. When done, resume Phase1.11 from the last green point (QC memory + edge-gating work). The two tracks should not fight after imports are cleaned.

---

If you want me to sign off on the mapping quickly, post it as soon as it’s ready and I’ll confirm.
