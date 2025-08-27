# Cursor Prompt — Phase 1.7 Review UX, Manual Overrides, and Sorting (Please review critically and pause for my approval)

> **Ask:** Implement the changes below **conservatively** and **pause for my confirmation** after proposing your approach. If you disagree with any point or know a better way, **push back with detailed rationale** before coding.

This phase focuses on **Streamlit review usability** and **lightweight human-in-the-loop controls** without changing our Salesforce read-only posture.

---

## Goals

1) Make the **review UI** more efficient: replace the disposition chart with a **table**, improve **group layout**, ensure **Account Name is fully visible**, and add **sorting controls**.  
2) Add **manual dispositions** (row-level overrides) and a **manual blacklist** editor — persisted to files under `data/manual/` so the pipeline can consume them next runs.  
3) Keep everything **non-destructive**: only write to `data/manual/*` and read them in the pipeline during disposition, no Salesforce writes.

---

## A) Streamlit UX changes (app/main.py)

### A1 — Replace the “Disposition Summary” bar chart with a compact table
- Show a small table with columns: `Disposition`, `Count`, `Percent` (of total).  
- Make each row **clickable filter** (or provide quick filter buttons right above).  
- Remove the chart to reduce vertical scroll.

### A2 — Group layout and name visibility
- In each group expander:
  - Move **Group Info** to the **top** (above the rows) with badges (e.g., suffix class, size).
  - Render the records table **below** it.
- Ensure **Account Name wraps** and is fully visible:
  - Use `st.dataframe(..., use_container_width=True)` with `column_config` to enable wrapping (e.g., `st.column_config.TextColumn(width="large", help="…", max_chars=None, disabled=True)`).
  - If needed, shorten ancillary columns or hide low-value columns by default (toggle in an expander).

### A3 — Sorting controls
- Add UI controls to **sort**:
  - **Group list** by **Total Records in Group** (asc/desc).
  - **Records table** by **Account Name** (asc/desc).
- Keep defaults sensible (e.g., groups sorted by size desc or by max score desc).

### A4 — Pagination remains
- Keep the Phase 1.6 pagination (Prev/Next + page size + indicator). Ensure page index resets when filters/sorts change to avoid empty pages.

---

## B) Manual review actions (front‑end only, persisted locally)

> **Storage:** create a new folder `data/manual/` (git-ignored).

### B1 — Row‑level disposition overrides
- In each row, add small action buttons (or a select) to mark: **Delete**, **Verify**, **Keep**, **Update**.
- On click:
  - Append (or upsert by `record_id`) a row into `data/manual/manual_dispositions.csv` with columns:  
    `record_id, account_id, account_name_raw, name_core, desired_disposition, reason, reviewer, ts_iso`
  - `reviewer` can be environment user or “streamlit_user”; `ts_iso` is current timestamp.
  - Show a small toast/snackbar “Override saved for {record_id} → Delete”.
- If an override exists for a row, render a small badge in the table (“Overridden: Delete”).

### B2 — Manual blacklist editor
- Add an expander “Manual Blacklist” with:
  - A table showing current manual terms loaded from `data/manual/manual_blacklist.csv` (column `term`), if file exists.
  - Input + “Add term” button to append a new row (lowercase, trimmed).
  - Optional: “Delete” button per term (soft delete → write a new file w/o the row).
- For **display only** (not editable here), also show the **built‑in** blacklist terms from `disposition.py` (`get_blacklist_terms()`) so reviewers understand the total set. Make clear only the *manual* file is editable via UI.

### B3 — Pipeline consumption (light wiring)
- Update `src/disposition.py` to **optionally** load:
  - `data/manual/manual_blacklist.csv` → **union** with built‑in blacklist (deduped).  
  - `data/manual/manual_dispositions.csv` → apply **final override** after normal classification (exact match by `record_id` if present; otherwise by `name_core` equality).  
- Make these reads **optional** (if file missing, skip). Log how many overrides/terms were applied.

> **Note:** No other behavior changes; this preserves our legal conservatism. Overrides are explicit, authored by a reviewer, and only affect the next pipeline run.

---

## C) Minimal rules visibility (top panel, already exists)
- Keep the Phase 1.6 panel but add a short static sentence noting:  
  “Manual overrides and manual blacklist (if present) are applied from `data/manual/` during pipeline runs.”  
- Provide a **download button** to export current `manual_dispositions.csv` and `manual_blacklist.csv` (if present) for audit.

---

## D) Non‑breaking guarantees
- All UI writes go to `data/manual/` only.  
- If those files are missing or invalid, pipeline behavior remains unchanged.  
- Existing tests continue to pass; add small unit tests only where necessary (see below).

---

## Tests (targeted, small)
- `tests/test_disposition.py`:
  - When a manual override file exists with a `record_id`, the final Disposition matches the override.
  - Manual blacklist term in `data/manual/manual_blacklist.csv` causes a `Delete` classification for a matching name.
- `tests/test_cleaning.py` (or a new small test):
  - Pipeline does **not fail** if `data/manual/*` files are absent.
- UI behavior will be verified manually; no Streamlit unit tests required in Phase 1.7.

---

## Acceptance Criteria
- **UI**: Disposition **table** replaces chart. Group info appears **above** the rows. Account Name is fully visible/wrapped. Sorting works for **Account Name** and **Total Records in Group**.
- **Manual overrides**: I can mark an individual row as Delete/Verify/Keep/Update; it persists to `data/manual/manual_dispositions.csv` and shows a badge upon reload.
- **Manual blacklist**: I can add/remove terms via UI; terms persist to `data/manual/manual_blacklist.csv` and are applied on next run (confirmed via pipeline log counts).
- **Non‑breaking**: If `data/manual/*` are missing, pipeline runs as before.
- **Pause for review**: Before implementing, propose your UI control layout (buttons vs select, where to place editors) and confirm file formats for the two manual files.

---

## Commit Message
```
ui+pipeline: Phase 1.7 — review UX improvements, manual overrides, manual blacklist, sorting
```
