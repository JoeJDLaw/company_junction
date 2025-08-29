# Cursor_Prompt — Phase1.13.7 — MyPy Finalization via Targeted Refactors

> Goal: Drive remaining MyPy errors (~46) to near‑zero without changing runtime behavior. Focus on **typed boundaries** and **small refactors** (not just annotations). Use `cursor_rules.md` as the source of truth for naming, IDs, imports, and QA gates.

---

## 1) Critical Review / Pushback (brief)
Before implementing, post a short critique covering:
- Risks: refactors in pandas indexing (tuple `.loc`), group union‑find, Series vs DataFrame parameters, Streamlit numeric ops.
- Alternatives considered: `# type: ignore` vs typed helpers vs light refactor. Prefer **typed helpers + light refactor**.
- Guardrails: no functional behavior change; keep diffs small; comprehensive tests at each step.

**Pause for my confirmation before code changes.**

---

## 2) Baseline & Inventory (post to thread)
1. Run baseline:  
   ```bash
   mypy --config-file mypy.ini src tests app | tee .mypy_report_before.txt
   ```
2. Produce a table of remaining errors grouped by **file → category → count** with top 10 offenders and representative examples.

**Wait for my “go”** after posting this inventory.

---

## 3) Implementation Plan (do in order, commit per step)
Follow these **modules & patterns**, one PR commit per numbered item. After each item: run `black --check . && ruff check . && pytest -q && mypy --config-file mypy.ini src tests app` and stop if anything regresses.

### 3.1 Grouping — typed Union‑Find & helpers
**File:** `src/grouping.py`  
**Why:** Unannotated `parent/rank`, nested functions, and `Any` returns.
**Actions:**
- Introduce a tiny, typed class:
  ```py
  class UnionFind:
      parent: dict[str, str]
      rank: dict[str, int]
      def __init__(self) -> None: ...
      def find(self, x: str) -> str: ...
      def union(self, a: str, b: str) -> bool: ...
  ```
- Replace raw dicts with `UnionFind` usage; annotate `group_members: dict[str, list[str]]`.
- Add explicit return types to any nested functions or lift them to top‑level helpers.

### 3.2 Similarity — Series vs DataFrame
**File:** `src/similarity.py`  
**Why:** `_compute_pair_score` expects `pd.Series` but receives 1‑col DataFrames / objects.
**Actions:**
- Ensure call sites pass **Series** (e.g., `df["name_core"]` not `df[["name_core"]]`).
- Tighten signature:  
  `def _compute_pair_score(a: pd.Series, b: pd.Series, cfg: dict[str, Any]) -> float:`
- Add typed local extracts before calling helpers.

### 3.3 Disposition & Alias matching — tuple `.loc` and indexing
**Files:** `src/disposition.py`, `src/alias_matching.py`  
**Why:** MyPy rejects tuple indexing on `_LocIndexerFrame` and mixed selectors.
**Actions:**
- Replace tuple‑style `.loc` with two‑step selection:
  ```py
  cols = ["c1", "c2"]
  df.loc[mask, cols] = ...
  ```
- When a **Series** is required, extract a named variable: `col = df["col"]`.
- Guard unions before attribute access; avoid chained complex indexing in a single expression.

### 3.4 Salesforce & Survivorship — boundary typing
**Files:** `src/salesforce.py`, `src/survivorship.py`  
**Why:** `Hashable` used like `str`, `int(Hashable)` issues, object‑typed mutation.
**Actions:**
- Normalize to `str` at boundaries (e.g., `sid: str = str(sid)`), then operate.
- For mutated collections, predeclare types: `winners: list[str] = []`.
- Convert before `int(...)` and annotate helper signatures accordingly.

### 3.5 Manual I/O — precise types
**File:** `src/manual_io.py`  
**Why:** `yaml.safe_load` returns `Any` → “Returning Any” errors.
**Actions:**
- Define `TypedDict` for manual files (overrides/blacklist), e.g.:
  ```py
  class ManualOverride(TypedDict, total=False):
      group_id: str
      disposition: str
      reason: str
  ```
- Cast once on load: `cast(dict[str, ManualOverride], raw or {})`.
- Keep the public function return types exact (no `Any`).

### 3.6 App (Streamlit) — numeric ops on ExtensionArray
**File:** `app/main.py`  
**Why:** Division/round on pandas `ExtensionArray` produces union types.
**Actions:**
- Coerce to numeric before math:  
  `pct = (counts.astype("float64") / float(counts.sum())).round(2)`
- Avoid arithmetic on nullable dtypes without cast.

### 3.7 ID utils — remove stale ignores / unreachable
**File:** `src/utils/id_utils.py`  
**Actions:**
- Remove any `# type: ignore` that are now unused, and simplify unreachable branches.

---

## 4) QA Gates (after each commit and at the end)
```bash
black --check .
ruff check .
pytest -q
mypy --config-file mypy.ini src tests app | tee .mypy_report_after.txt
```
- If MyPy remains > 0, post an updated error **heatmap** (file → category → count) and proceed to the next module.
- If a refactor would change behavior, stop and request explicit approval.

---

## 5) Documentation
- Update `CHANGELOG.md` under **Phase1.13.7 — MyPy Finalization** summarizing what changed (module by module) and the before/after MyPy counts.
- If we introduce `UnionFind`, add a short docstring block describing its invariants and usage.
- If we add `TypedDict`s for manual files, document their schema in `README.md` (developer section).

---

## 6) Deliverables Checklist
- [ ] Inventory table of remaining MyPy errors (before).
- [ ] Commits per module with brief rationale in each message.
- [ ] Grouping: `UnionFind` introduced & typed structures added.
- [ ] Similarity: Series vs DataFrame corrected; signatures tightened.
- [ ] Disposition/Alias: tuple `.loc` replaced; selectors typed.
- [ ] Salesforce/Survivorship: boundary types normalized to `str` and annotated.
- [ ] Manual I/O: `TypedDict` + cast on load; no `Any` returns.
- [ ] App: numeric coercion before arithmetic/round.
- [ ] ID utils: stale ignores removed.
- [ ] QA gates all green; final MyPy report attached.
- [ ] `CHANGELOG.md` + (if needed) `README.md` updated.

---

## 7) Communication Protocol
- Prefix updates with **[Phase1.13.7]**.
- **Pause** after posting the baseline inventory for my approval.
- After each module commit, post the MyPy diff (error count delta) and any notable code decisions.
