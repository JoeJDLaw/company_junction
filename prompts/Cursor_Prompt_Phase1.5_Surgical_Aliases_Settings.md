# Cursor Prompt — Phase 1.5 Surgical Parentheses & Multi‑Name Alias Matching + Settings Panel (Review critically)

> **Ask:** Implement the following targeted refinements. If anything conflicts with our current architecture or you know a better approach, **push back with rationale and propose alternatives** before coding.

## Intent (why this change)
- Be **surgical** about parentheses and numbering: `(1)`, `(2)` **are not part of legal names**; parenthetical content may indicate **other companies** that should be **evaluated** (not ignored).  
- Without actually splitting rows in Phase 1, generate **alias candidates** from clear separators to **evaluate & cross‑reference** matches across groups.  
- Keep Phase 1 conservative: no automatic merges; use aliases only to **flag** and **cross‑link** records/groups, and default such records to **Verify**.  
- Make Streamlit the **source of truth** UI by exposing the **rules and settings** used to reach decisions.

---

## Patch M — Normalization: numbered markers and alias candidate extraction

**Files:** `src/normalize.py`, `tests/test_normalize.py`

**Additions/Changes:**
1) **Numbered marker removal (not part of legal name)**  
   - Remove leading numbered tokens like `"(1)"`, `"(2)"` (pattern: `^\(\d+\)\s*`) from the **cleaned display** and the **scoring string**.
   - Example: `"(1)Don Roberto Jewelers"` → cleaned core starts with `"don roberto jewelers"`.
2) **Alias candidate extraction (no splitting yet)**  
   - From raw name, parse **alias segments**:
     - **Semicolons**: split on `;` → alias per segment (trim).
     - **Numbered sequence**: if multiple `"(\d+)"` labels appear, treat each following token span until the next label or end as a separate alias.
     - **Parentheses content**: capture inside `(...)` as an **alias candidate**, but **do not alter the base name** (we remain conservative).  
       - We will **always** create an alias candidate from parentheses content; label it `alias_source="parentheses"`. We’ll decide how to use it downstream.
   - For each alias candidate, produce a normalized alias string using the same rules as `name_core` (retain commas/periods; keep legal suffix detection).
   - Persist: `alias_candidates` as a **list[str]** plus a parallel **list[str] alias_sources** (e.g., `["semicolon","numbered","parentheses"]`).

**Tests:**
- `"(1)Don Roberto Jewelers; (2) BYD Auto"` → two alias candidates: `"don roberto jewelers"`, `"byd auto"`.
- `"BMW of Ontario (Penske Auto Group Ontario B1)"` → alias `"penske auto group ontario b1"` (source=parentheses).  
- Numbered markers stripped from scoring base.

---

## Patch N — Alias matching (cross‑links without changing groups)

**Files:** `src/similarity.py`, `src/grouping.py` (metadata only), `tests/test_similarity.py`

**Additions:**
- New function `compute_alias_matches(df_norm, df_groups, cfg) -> DataFrame` that:
  - Explodes `alias_candidates` and scores each alias against **other records' `name_core`** using the **same similarity** function.
  - Applies **same suffix rule** and thresholds for a **valid alias match** (suffix must match; score ≥ `medium`).  
  - Produces `data/interim/alias_matches.parquet` with columns:
    - `record_id`, `alias_text`, `alias_source`, `match_record_id`, `match_group_id`, `score`, `suffix_match`
- **Cross‑linking rule**: alias matches **do not** merge or reassign groups. Instead, they create **secondary references** used by the UI and disposition.
- Update `data/interim/groups.parquet` (or a companion) to include a JSON list per record: `alias_cross_refs=[{"alias":"...","group_id":G,"score":..,"source":"parentheses"}, ...]` (can be computed in `cleaning.py` after alias matching).

**Note:** If a record accumulates alias cross‑refs to **multiple other groups**, we do **not** union them. This is intentional to avoid over‑merging; it’s a *review cue*.  

**Tests:** Add a tiny fixture where one record with two aliases links to two different groups; ensure `alias_matches.parquet` reflects both and groups remain distinct.

---

## Patch O — Disposition adjustments for alias matches

**File:** `src/disposition.py`, `tests/test_disposition.py`

**Changes:**
- If a record has **one or more valid alias matches** (per N), set/ensure `Disposition="Verify"` (unless it is `Delete` by blacklist).  
- Set `disposition_reason` to include alias context: e.g., `"alias_matches: 2 groups via [semicolon,parentheses]"`.

---

## Patch P — Streamlit settings & cross‑link UI

**File:** `app/main.py`

**Additions:**
1) **Settings & Rules panel (top expander)**  
   - Load `config/settings.yaml` and show:
     - Similarity thresholds and penalties
     - Blocking strategies (if present)
     - Blacklist / suspect phrases (from code or config; if some are in code, surface them via a small API in `disposition.py`, e.g., `get_blacklist_terms()`)
     - Any parsing flags (e.g., “numbered markers removed from scoring”)
2) **Alias cross‑links in group view**  
   - For each record, if `alias_cross_refs` exists:
     - Show a badge: `Cross‑links: N`
     - In an expander, list each `alias → group_id (score, source)` with a “peek” (show target group’s primary name if available).  
   - Add a sidebar filter **“Has cross‑links”** (yes/no).
3) **Badges/flags**  
   - Show badges for `has_multiple_names`, `has_parentheses`, and **numbered markers present** (if you keep an explicit flag).

---

## Patch Q — Cleaning pipeline wiring

**File:** `src/cleaning.py`

**Additions:**
- After normalization and base candidate pairs/groups are built, **compute alias matches** (Patch N) and assemble `alias_cross_refs` per record.
- Add these to the `review_ready.csv` so Streamlit can render them without recomputing.
- Log summary counts: number of records with cross‑links, total alias matches, top sources.

---

## Config additions (if needed)

`config/settings.yaml` (only if not already present):
```yaml
similarity:
  high: 92
  medium: 84
  penalty:
    suffix_mismatch: 25
    num_style_mismatch: 5
    punctuation_mismatch: 3
# no new keys required; reuse existing thresholds for alias scoring
```

---

## Acceptance criteria

- Numbered markers `(1)`, `(2)` are **not** treated as part of the legal name for scoring.
- Alias candidates are extracted from **semicolons**, **numbered sequences**, and **parentheses** (source labeled).
- `alias_matches.parquet` is produced and **does not** change groups; it only **cross‑links** records to other groups.
- Records with any alias matches are set to **`Verify`** with a useful `disposition_reason`.
- Streamlit shows a **Settings & Rules** panel, **badges**, and an **alias cross‑links expander** with group peeks.
- All tests pass; include a small test that a record with alias `"BYD Auto"` cross‑links to a BYD group while remaining in its original group.

---

## Please review critically

If creating aliases from **all** parentheses content proves too noisy (e.g., “(Delaware)”), suggest a refinement such as:
- Only elevate a parentheses alias if it **contains a legal suffix** token or matches a **company‑like** pattern (`>=2 capitalized tokens`), or
- Use the LLM hook (Phase 2) to score parenthetical content and gate alias creation.
