# Cursor Prompt — Phase 1.9 Stable Record IDs, Safer Blacklist, Centralized Manual I/O, and Streamlit “Run Pipeline” (please review critically)

> **Ask:** Implement the items below *conservatively*. If you disagree with any point or know a better approach, **push back with rationale** before coding. Please **pause for my confirmation** after proposing any schema changes. Keep Phase 1 read‑only for Salesforce.

This phase addresses three correctness issues (stable IDs, blacklist false positives, manual I/O duplication) and adds a small quality‑of‑life Streamlit launcher for running the pipeline without touching the terminal.

---

## Goals

1) **Stable record IDs** across runs for overrides & audit: derive a persistent `record_id` from configured key fields (not DataFrame row index).  
2) **Blacklist false‑positive reduction**: word‑boundary matching for short tokens, substring for multi‑word phrases; cache loads.  
3) **Centralize manual JSON I/O**: single module used by both pipeline and app.  
4) **(Optional) Group‑apply override**: checkbox to apply selected disposition to all records in a group.  
5) **Audit snapshot** per run for transparency.  
6) **Streamlit “Run Pipeline”**: choose an input CSV from `data/raw/`, run the pipeline, and auto‑reload results.

---

## A) Stable `record_id` — do not use DataFrame index

**Problem:** Manual overrides currently match on row index (`str(idx)`), which is unstable across runs/orderings.

**Changes:**

- **Config (`config/settings.yaml`):**
  ```yaml
  data:
    record_key_fields: ["Account ID", "Potential Case Employer Junction ID"]
    # If none present for a row, fallback to a stable hash built from a small set of fields.
    record_fallback_fields: ["Account Name", "Created Date", "LR ID"]
  ```

- **`src/utils.py` (new helpers):**
  - `build_record_id(df: pd.DataFrame, key_fields: list[str], fallback_fields: list[str]) -> pd.DataFrame`  
    Behavior:
    1. Use the **first** present field in `key_fields` per row as `record_id` (string).  
    2. If none exist (NaN/empty) → compute `record_id = sha1('|'.join(normalized fallback_fields)).hexdigest()[:16]`.  
    3. Ensure uniqueness; if duplicates are detected, append `-N` suffix to later duplicates (log a warning with counts).  
  - Return DF with a **new column** `record_id`. Do **not** silently overwrite any existing column; we own this name.
  - Add a tiny normalizer for fallback join (lowercase, strip, collapse whitespace).

- **`src/cleaning.py` (early in pipeline):**
  - Load config, call `build_record_id(...)`, and **set index to `record_id`** (or keep as column but ensure downstream writes include it).
  - Ensure all intermediate parquet files keep `record_id` and the final `review_ready.*` includes it.

- **`app/manual_data.py` & UI code:**
  - Wherever overrides are captured, write **the `record_id` field** (not row index).

- **`src/disposition.py`:**
  - Load overrides into a mapping `{record_id -> override}`, not `{str(index) -> override}`.
  - Apply overrides **last** in classification (unchanged order).

**Tests:**  
- New test to verify `record_id` is stable and **doesn’t change** if row order is shuffled.  
- Update existing tests that assumed `'record_id': '0'` to use actual keys/hashes.  

---

## B) Safer blacklist matching + caching

**Problems:** Short tokens (e.g., “temp”) cause false positives; manual blacklist JSON is re‑read per row.

**Changes (`src/disposition.py`):**
- Split **multi‑word phrases** vs **single‑word tokens**:
  - Phrases → case‑insensitive **substring** check.
  - Tokens → **word‑boundary regex** (compiled once): `\b(?:temp|temporary|unknown|n/?a|tbd|test|sample|paystub|employees)\b` (adjust list from current built‑ins).  
- Provide a public helper (if not present):  
  ```python
  def get_blacklist_terms() -> list[str]:
      """Return current built‑in blacklist (tokens + phrases)."""
  ```
- Load **manual blacklist** once per pipeline run (or cache with `functools.lru_cache()` and invalidate by file mtime).  
- `_is_blacklisted(name, manual_terms=None)` should accept the pre‑loaded `manual_terms` to avoid repeated IO.  
- Union built‑in + manual (dedupe) → build regex/phrase lists once and reuse for the entire DataFrame.

**Tests:**  
- Boundary test: “Tempest” **not** blacklisted; “temporary staffing” **is**.  
- Manual term present → row becomes `Delete` (respecting existing disposition logic).

---

## C) Centralize manual JSON I/O

**Change:** Introduce `src/manual_io.py` with the single source of truth for manual files used by *both* pipeline and app.

- `load_manual_blacklist(path="data/manual/manual_blacklist.json") -> set[str]`
- `save_manual_blacklist(terms: Iterable[str], path=...) -> None`
- `load_manual_overrides(path="data/manual/manual_dispositions.json") -> dict[str, dict]`  # keyed by record_id
- `upsert_manual_override(record_id: str, override: str, reason: str, reviewer: str, path=...) -> None`
- Use **atomic writes** (temp file + rename) to avoid corruption.
- All prior manual JSON reads/writes in app/pipeline should call these helpers.

**Tests:**  
- Round‑trip read/write (including empty/missing file).  
- Malformed JSON → raise or log + return empty (match current behavior), covered by a test.

---

## D) (Optional) Group‑apply override in Streamlit

- In the **group section** next to the existing dropdown, add a checkbox: “Apply to **all records** in this group”.  
- When checked, write one override entry **per `record_id`** in the group via `manual_io.upsert_manual_override(...)`.  
- Show a small success toast and a badge “Overridden” on affected rows upon reload.

**Note:** Keep group‑apply off by default. If you think this introduces risk or complexity, propose an alternative.

---

## E) Run audit snapshot

- At the end of `src/cleaning.py`, write `data/processed/review_meta.json` with:
  ```json
  {
    "run_ts": "2025-08-27T21:45:00Z",
    "thresholds": { "high": 92, "medium": 84, "penalties": {...} },
    "effective_blacklist_count": N,
    "manual_overrides_applied": M,
    "git_commit": "<short-sha or null>"
  }
  ```
- `git_commit`: best‑effort (subprocess `git rev-parse --short HEAD`, ignore on failure).

---

## F) Streamlit “Run Pipeline” launcher (app/main.py)

**Purpose:** Let me start Streamlit and select an input CSV in `data/raw/` to run the pipeline without the terminal.

**Changes:**  
- Add a sidebar “Run Pipeline” expander with:
  - A **selectbox** listing CSVs in `data/raw/` (e.g., `glob('data/raw/*.csv')`), default to the latest by modified time.
  - A **Run** button. On click:
    - Disable controls; show spinner.
    - Execute: `python src/cleaning.py --input <selected> --outdir data/processed --config config/settings.yaml` using `subprocess.run` (capture stdout/stderr).  
    - Stream a few log lines to the UI (tail `pipeline.log` if available).  
    - On success: reload `review_ready.parquet` if present (fallback to CSV).  
    - On error: show stderr in an expander.
- Add guardrails: validate selected path is under `data/raw/` and endswith `.csv`.  
- If the dataset is large, show a note that the run may take time and the UI will refresh when done.

**Out‑of‑scope:** No in‑app parameter editing; `outdir` and `config` remain static.

---

## Acceptance Criteria

- **Stable IDs**: `record_id` built from configured keys (or fallback hash) and used consistently in outputs, overrides, and tests. Shuffling input row order does **not** change a row’s `record_id`.  
- **Blacklist**: boundary‑safe matching; manual terms loaded once; unit tests cover “Tempest” vs “temporary”.  
- **Manual I/O**: one module (`src/manual_io.py`) used by both pipeline and app; atomic writes; malformed JSON handled gracefully.  
- **Overrides**: existing group dropdown still works; (optional) group‑apply checkbox writes overrides for all rows in group.  
- **Audit snapshot** is written on each run with counts + thresholds.  
- **Streamlit launcher**: I can pick a CSV in `data/raw/`, click **Run**, and the app executes the pipeline and refreshes results; errors are surfaced clearly.

---

## Notes

- Keep dependencies minimal; prefer stdlib (`hashlib`, `subprocess`, `json`, `tempfile`, `pathlib`, `re`, `functools`).  
- Do not change core dedup/grouping thresholds or legal suffix rule.  
- If any step introduces significant complexity, propose a simpler alternative and **pause for approval**.

---

## Commit Message

```
core+ui: Phase 1.9 — stable record_id; safer blacklist w/ boundary regex & caching; centralized manual I/O; Streamlit run-launcher; audit snapshot
```
