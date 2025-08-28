# Cursor_Prompt — Phase1.13.5 — MyPy Error Reduction (Targeted, No-Churn)

**Objective:** Drive MyPy from “many type errors” to a **substantially reduced** count through low-risk, high-yield typing improvements — without changing runtime behavior. Keep imports canonical (`from src...`), avoid refactors, and scope changes for fast review.

---

## 0) Critical Review / Pushback (post first)
Before making any edits, please post a short plan covering:
- The **top error categories** you expect (e.g., missing return types, untyped defs, pandas indexing).
- The **lowest-risk targets** (utils, configs, signatures) and what you’ll exclude for now.
- How you’ll avoid churn: no behavioral changes, minimal renames, smallest diffs.

**Pause for my approval** before proceeding.

---

## 1) Generate a MyPy Heatmap (evidence-driven targeting)
Produce a ranked view of where errors cluster to focus our effort.

```bash
mypy --config-file mypy.ini src tests app --no-color-output --show-error-codes | tee .mypy_report.txt

# Top files by error count
awk -F: '/^(src|tests|app)\// {print $1}' .mypy_report.txt | sort | uniq -c | sort -nr | head -30

# Top error codes by frequency
awk -F'\[|\]' '/error:/{print $2}' .mypy_report.txt | sort | uniq -c | sort -nr | head -20
```

**Post the two summaries** and propose a prioritized list of files to touch in this phase.

**Pause for my approval** before editing code.

---

## 2) Scope for This Phase (low risk only)
Focus on **non-controversial** changes that typically remove 50–70% of errors quickly:

1) **Utilities (`src/utils/*`)**
   - Add precise function signatures & return types.
   - Keep behavior identical; tests must remain green.

2) **Config Shapes**
   - Introduce `TypedDict`/`Protocol` for frequently accessed config dicts (e.g. similarity thresholds, penalties).
   - Use narrow keys that are actually accessed to avoid over-specifying.

3) **Pandas Typing Facades**
   - Where pandas indexing confuses MyPy, create tiny helper functions with typed inputs/outputs (e.g., `ensure_string_series(df[col]) -> pd.Series`).
   - Annotate parameters as `pd.DataFrame` / `pd.Series`; avoid exotic generics.

4) **Tests**
   - Add minimal annotations only when they unblock many errors.
   - Prefer **narrow line-level ignores** (`# type: ignore[specific-code]`) rather than file-wide ignores, and add a one-line rationale.

**Out of scope for this phase**
- Large refactors, renames, or logic changes.
- Heavy rework in core pipeline algorithms.
- Changing public function names or signatures used widely by tests unless annotation-only.

---

## 3) Implementation Guidance (patterns)

### A) Utils functions
- Add explicit parameters & return types.
- For small helpers, annotate local variables if it removes errors.
- Example:
```python
def load_relationship_ranks(path: str) -> dict[str, int]:
    ...
```

### B) Config `TypedDict`s (keep minimal)
```python
from typing import TypedDict

class SimilarityPenalty(TypedDict, total=False):
    suffix_mismatch: int
    num_style_mismatch: int

class SimilarityConfig(TypedDict, total=False):
    high: int
    medium: int
    penalty: SimilarityPenalty
```

### C) Pandas helpers (hide complex indexing)
```python
import pandas as pd

def ensure_string_series(s: pd.Series) -> pd.Series:
    return s.astype("string")

def dropna_series(s: pd.Series) -> pd.Series:
    return s.dropna()
```

Use these helpers inside modules instead of repeating complex chains that MyPy struggles to infer.

### D) Narrow ignores with rationale
```python
# mypy struggles with DataFrame __getitem__ here; value narrowed just below
val = df[col]  # type: ignore[index]
```

---

## 4) QA Gates (must pass at end of phase)
```bash
black --check .
ruff .
mypy --config-file mypy.ini src tests app
pytest -q
```

**Acceptance thresholds for Phase1.13.5:**
- Black/Ruff/PyTest: ✅ pass
- **MyPy errors reduced by ≥ 50%** from the pre-phase baseline posted in §1, or to **≤ 60 total errors**, whichever is lower.
- No new public API/behavior changes; diffs are annotation-focused.

---

## 5) Deliverables
- Heatmap outputs (top files + top error codes).
- A concise patch list: file → summary of changes (annotations added, helpers created).
- QA results showing improvement vs. baseline.
- Brief notes on remaining hotspots (what to tackle in Phase1.13.6).

---

## 6) Communication Protocol
- Prefix updates with **[Phase1.13.5]**.
- **Pause twice**: after the heatmap & plan, and after QA results for approval before merge.
