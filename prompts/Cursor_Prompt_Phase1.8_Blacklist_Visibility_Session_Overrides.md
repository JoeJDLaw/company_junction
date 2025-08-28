# Cursor Prompt — Phase 1.7.1 Blacklist Visibility + Session Overrides, Filter Clarifications, Sorting (please review critically)

> **Ask:** Implement the items below *conservatively*. If you disagree with any point or know a better approach, **push back with rationale** before coding. Please **pause for my confirmation** after proposing the UI placement.

Scope:
- Make the **original built-in blacklist** clearly visible in the UI (read-only).
- Show the **manual blacklist** (editable) side-by-side.
- Add an optional **session-only blacklist sandbox**: start the app with built-in ∪ manual, let me temporarily remove/add terms for *this session only*, without touching files or pipeline. Include **Reset to defaults**.
- Clarify what the **“Show Suffix Mismatches Only”** and **“Has Aliases”** filters do, and ensure they actually filter the dataset.
- Add missing **sorting options** for Account Name (ascending and descending).
- No Salesforce writes; no changes to pipeline logic beyond existing manual file consumption.

---

## A) Blacklist visibility (app/main.py)

Add a new section in the sidebar (or enhance the existing “Manual Blacklist” expander) that displays three things:

1) **Built-in blacklist (read-only)**  
   - Pull from `src/disposition.get_blacklist_terms()` (or equivalent).  
   - Render a read-only list, with a small count.  
   - If the getter is missing, please add it and wire to the current built-in tokens.

2) **Manual blacklist (editable, persisted)**  
   - Already exists: stored at `data/manual/manual_blacklist.json`.  
   - Keep the add/remove UI.  
   - Show current terms and a count.

3) **Effective blacklist (built-in ∪ manual)**  
   - Show a deduped union with a count.  
   - This is what the pipeline will actually use on the next run.

UI Sketch (adjust to your current structure):
```python
with st.expander("Blacklist (Built-in, Manual, Effective)"):
    builtins = set(get_blacklist_terms())  # read-only
    manual = set(load_manual_blacklist_json())  # editable; you already have this

    st.caption(f"Built-in terms (read-only) — {len(builtins)}")
    st.write(", ".join(sorted(builtins)) or "— none —")

    st.caption(f"Manual terms (editable) — {len(manual)}")
    # keep existing add/remove UI here

    effective = sorted(builtins | manual)
    st.caption(f"Effective terms used by pipeline — {len(effective)}")
    st.write(", ".join(effective) or "— none —")
```

---

## B) Optional: session-only blacklist sandbox (no persistence)

Purpose: let me experiment during review without changing files or pipeline; useful for “what if” checks.

- Create a **“Session Blacklist Overrides (sandbox)”** sub-panel under the blacklist section.
- Initialize `st.session_state.session_blacklist` to the **effective** set at app start (built-in ∪ manual).
- Allow **temporary remove/add** of terms in this sandbox list via simple inputs/buttons.
- Provide **Reset to defaults** button to restore from the current effective set.
- This sandbox should **not** write to disk; it only affects **in-app filters** (checkbox: “Preview deletions using session blacklist”). Do **not** change pipeline outputs; this is only for review visibility.

If this is too large for 1.7.1, please implement **A)** fully and **defer B)** after proposing the minimal approach.

---

## C) Clarify and wire filters

1) **“Show Suffix Mismatches Only”**  
   - Tooltip: “Show groups where members disagree on legal suffix (e.g., INC vs LLC).”  
   - Behavior: When checked, filter groups where `has_suffix_mismatch == True` (or equivalent precomputed column).

2) **“Has Aliases”**  
   - Tooltip: “Show groups/records with alias candidates (semicolon/numbered/filtered parentheses).”  
   - Behavior: When checked, filter to rows/groups where `alias_candidates` is non-empty (in Parquet they are lists).

3) **Counters**  
   - After filters are applied, update the page header counters (Total Records, Groups, Primary Records).

If any of these columns don’t exist consistently, please add robust null-safe checks.

---

## D) Sorting Enhancements

- Add missing sorting options in the Streamlit sidebar:  
  - **Account Name (Asc)**  
  - **Account Name (Desc)**  
- Sorting should apply to groups and/or records consistently.  
- Ensure default behavior remains stable (e.g., Group Size Desc).

---

## E) Minimal code changes

- `src/disposition.py`: ensure a public helper exists:  
  - `def get_blacklist_terms() -> list[str]: ...` (returns the current built-in default set).  
- `app/main.py`: add the three-pane blacklist section. If the sandbox is implemented, use `st.session_state` and avoid any file writes.

No other pipeline changes are requested.

---

## Acceptance Criteria

- I can see **Built-in**, **Manual**, and **Effective** blacklist terms and counts in the UI.  
- Manual list remains editable; built-in list is read-only.  
- (Optional) Session sandbox exists: I can add/remove terms temporarily and reset, with a note that it doesn’t affect pipeline.  
- “Show Suffix Mismatches Only” and “Has Aliases” have tooltips and actually filter the results.  
- Sorting options include Account Name (Asc/Desc).  
- No errors if manual file is missing; graceful degradation.  
- No changes to pipeline outputs or persisted files except the existing manual blacklist behavior.

---

## Notes

- Keep dependencies minimal; no need for new packages.  
- If any part risks destabilizing performance, suggest a simpler alternative and pause.  
- Please propose the exact UI placement (sidebar vs expander positioning) before implementing, so I can confirm.

---

## Commit Message

```
ui: Phase 1.7.1 — blacklist visibility (built-in/manual/effective), optional session sandbox, filter clarifications, sorting by Account Name
```
