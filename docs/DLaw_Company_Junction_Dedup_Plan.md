# Company Junction Deduplication — Implementation Plan (Phase 1)

> **Goal (Phase 1):** Produce a reviewable dataset that identifies likely duplicate Accounts originating from a junction export, proposes a primary “winner” per group using Relationship rank + Created Date, and assigns a **Disposition** of `Keep`, `Update`, `Delete`, or `Verify`. **No Salesforce writes** in this phase and **Split detection is deferred to Phase 2**.

---

## 0) Ground Rules

- **Project layout** (Cookiecutter DS):
  - `data/raw/`, `data/interim/`, `data/processed/`
  - `src/` (logic), `app/` (Streamlit UI), `tests/` (pytest), `docs/`, `config/`
- **Source-of-truth**: `cursor_rules.md` and this plan. Keep heavy logic in `src/`.
- **Reproducibility**: End-to-end runs idempotently from `data/raw/*.csv`.
- **Auditability**: Persist intermediate artifacts and decisions with clear columns.
- **Phase 1 scope**: Recommend only; do **not** merge/update/delete in Salesforce.

---

## 1) Inputs & Columns

Expect CSV rows coming from a **junction object** joined to **Account** fields. The following columns are typical (not exhaustive):

- Junction:
  - `Potential Case Employer Junction ID`, `Potential Case Employer Junction ID_6`
  - `Potential Case Name`, `Cleaned Potential Case Name`
  - `Relationship`, `Relationship (If Other)`, `Disposition` (to be populated by this pipeline)
  - `Stage`, `LR ID`, `Created Date`, `Last Modified Date`
- Account:
  - `Account ID`, `Account Name`, `Cleaned Account Name`
  - `Account Owner: Full Name`, `Account Record Type`
  - `Main Address`, `Main Country`

**Assumptions**
- `Account Name` is the canonical name field to clean/compare; if missing, fall back to `Employer Name` when available.
- `Created Date` may appear as Excel serial; normalize to datetime.

---

## 2) Legal‑Aware Name Normalization

Create normalization that **preserves legal suffix differences** (Inc vs LLC = different companies).

**Functions (in `src/normalize.py`):**
- `normalize_name(name: str) -> dict` returns:
  - `name_raw`: original
  - `name_base`: lowercased, punctuation trimmed (keep alnum & spaces), map symbols: `&→and`, `/→space`, `-→space`, `@→at`, `+→plus`; collapse whitespace
  - **numeric style unification**: `20-20`, `20/20`, `20 20` → `20 20`
  - `suffix_class`: one of `INC, LLC, LTD, CORP, LLP, LP, PLLC, PC, CO, GMBH, NONE` (detect trailing suffix tokens)
  - `name_core`: tokens with trailing suffix **removed** (for candidate generation only)
- `extract_suffix(name_base) -> suffix_class`
- **No Split detection in Phase 1.** (Will be added in Phase 2.)

**Tests (`tests/test_normalize.py`):**
- “20-20 Plumbing and Heating Inc” ≈ “20/20 Plumbing & Heating, Inc.” ≈ “20 20 Plumbing & Heating Inc”
- Ensure `suffix_class` distinguishes INC vs LLC.

---

## 3) Candidate Pairing & Similarity Scoring

Generate candidate pairs and compute a 1–100 composite score.

**Library**: `rapidfuzz`

**Signals (in `src/similarity.py`):**
- `ratio_name` = token_sort_ratio(`name_core`)
- `ratio_set` = token_set_ratio(`name_core`)
- `jaccard` = token set Jaccard on `name_core`
- `num_style_match`: boolean for equal numeric normalization
- `suffix_match`: boolean for identical `suffix_class`
- `phrase_penalties`: reduce if one side uniquely has tokens like `staffing`, `franchise`, `temp`

**Composite score:**
```
base = 0.45*ratio_name + 0.35*ratio_set + 20*jaccard
if not num_style_match: base -= 5
if not suffix_match:    base -= 25
score = clamp(round(base), 0, 100)
```

**Cutoffs (tunable in `config/settings.yaml`):**
- `high = 92` (strong candidate)
- `medium = 84` (review range)

**Rules:**
- If `suffix_match = False`, never auto‑accept → mark group as **Verify**, even if score high.
- Build a graph with edges where (`suffix_match=True` and `score≥medium`), then take connected components as duplicate groups.

**Artifacts:**
- `data/interim/accounts_normalized.parquet`
- `data/interim/candidate_pairs.parquet`
- `data/interim/groups.parquet` (record_id, group_id, best_score_to_primary, etc.)

**Tests (`tests/test_similarity.py`):**
- INC vs INC examples score ≥ `high`.
- INC vs LLC example stays below `high` or is forced to Verify due to suffix mismatch.

---

## 4) Survivorship (Primary Selection)

**Relationship ranking** (lower is better). Provide as `config/relationship_ranks.csv`:

```
Relationship,Rank
Company Name on 1099 form,10
Company Name on Paycheck,10
Company Name on Paystubs,10
Company Name on W-2,10
Company Logo on Paycheck,15
Company Logo on Paystubs,15
Franchise Name (Not a direct employee),15
Company Name on Arbitration,20
Company Name on Background Check,20
Company Name on EEHB,20
Company Name on New Hire Docs,20
Company Name on Separation Letter,20
Company Logo on EEHB,25
Company Logo on Emails,25
Company Logo on New Hire Docs,25
Company Name on Email Signature Lines,30
Company Name on LC 2810.5,30
Per PNC, Company Name on the Paystub,40
Per PNC, Staffing Agency,40
Per PNC, Company Name,45
Other/Miscellaneous,60
```

**Primary pick (in `src/survivorship.py`):**
1) Lowest `relationship_rank`
2) Tie → earliest `Created Date`
3) Tie → smallest `Account ID` (lexicographic)

Also compute a light **field merge preview** (no writes): for each group, show which fields differ and what the primary would carry vs alternatives. Emit as a JSON column for review.

**Tests (`tests/test_grouping.py`):**
- Correct primary chosen given ranks & dates.

---

## 5) Disposition Logic (Per Record)

**Disposition values (Phase 1):** `Keep`, `Update`, `Delete`, `Verify`  
(Split deferred to Phase 2)

**Heuristics (in `src/disposition.py`):**
- **Delete** if name hits a blacklist:
  - Substrings (case‑insensitive): `pnc is not sure`, `pnc is unsure`, `unsure`, `unknown`, `no paystub`, `no paystubs`, `1099`, `1099 pnc`, `none`, `n/a`, `tbd`, `test`, `sample`, `delete`, `do not use`
  - Very short (<3 chars), overly long (>100 chars), or mostly punctuation/stopwords
- **Verify** if:
  - In a group with **suffix mismatch**, or
  - No strong candidate (all scores < `medium`), or
  - Name is suspicious but not blacklisted (optional LLM gate, see below)
- **Keep/Update** within a group:
  - Primary ⇒ `Keep`
  - Non‑primary ⇒ `Update`
- **Singleton** (no group):
  - If clean and not suspicious ⇒ `Keep`
  - Else ⇒ `Verify`

**Optional LLM Gate (toggle off by default):**
- `src/llm_gate.py` with a `real_company_likelihood(name) -> 0..100` and a **SQLite cache** table `aux_llm_cache` to avoid duplicate calls.
- If enabled and score ≥ `delete_threshold` (e.g., 85) then `Delete`.

**Artifacts:**
- `data/interim/dispositions.parquet`
- `data/processed/review_ready.csv` (joined view for human review)

**Tests (`tests/test_disposition.py`):**
- Blacklisted examples → `Delete`
- Suffix mismatch groups → `Verify`
- Strong matches (same suffix) → primary=`Keep`, others=`Update`

---

## 6) CLI Orchestrator

**File:** `src/cleaning.py`

**CLI usage:**
```
python src/cleaning.py --input data/raw/my_export.csv --outdir data/processed --config config/settings.yaml
```
**Steps:**
1) Load CSV; coerce `Created Date` to datetime (handle Excel serials)
2) Build normalized table; persist
3) Generate candidate pairs + scores; persist
4) Build groups; compute primary; persist
5) Compute Disposition; join metadata; write `review_ready.csv`

**Exit code**: non‑zero on validation failures (e.g., missing required columns).

---

## 7) Streamlit Review UI (Read‑Only Actions)

**File:** `app/main.py`

- Sidebar Filters:
  - Min score (to primary), suffix mismatch toggle
  - Disposition filter (`Keep/Update/Delete/Verify`)
  - Group size range
- Group Panel:
  - Header: `group_id`, proposed primary with reason (rank/date)
  - Table: records + badges: `suffix_mismatch`, `blacklist_hit`
  - Show field‑merge preview JSON (pretty‑printed)
- Actions:
  - Export the current filtered view to CSV for offline review

---

## 8) Config & Thresholds

**File:** `config/settings.yaml`
```yaml
similarity:
  high: 92
  medium: 84
  penalty:
    suffix_mismatch: 25
    num_style_mismatch: 5
llm:
  enabled: false
  delete_threshold: 85
survivorship:
  tie_breakers: [created_date, account_id]
io:
  interim_format: parquet
```

**Relationship ranks:** `config/relationship_ranks.csv` (as above)

---

## 9) Tests & Fixtures

**Fixtures (`tests/fixtures/`):**
- Positives:
  - “20-20 Plumbing and Heating Inc”
  - “20/20 Plumbing & Heating, Inc.”
  - “20 20 Plumbing & Heating Inc”
- Negative (suffix differences):
  - “20/20 Plumbing & Heating, LLC”
- Junk:
  - “PNC is not sure”
  - “1099, no paystubs”
  - “N/A”

**Run:**
```
pytest -q
```

---

## 10) Milestones & Acceptance Criteria

- **M1 — Normalization & Tests**
  - `normalize.py` implemented; tests pass for numeric style & suffix parsing.
- **M2 — Similarity & Grouping**
  - Composite scores; groups persisted; cutoffs configurable.
- **M3 — Survivorship**
  - Primary chosen per group per rank/date; merge preview column.
- **M4 — Disposition**
  - Records labeled `Keep/Update/Delete/Verify` with rationale columns.
- **M5 — Review UX**
  - Streamlit lists groups, flags mismatches/blacklist; export works.

**Acceptance:** On provided sample, the three “20/20 … Inc” rows group together with `Inc` suffix; the `LLC` variant is **not** merged (grouped separately or marked Verify due to mismatch). Junk strings map to `Delete`.

---

## 11) Out‑of‑Scope (Phase 1)

- Split detection & parsing → **Phase 2**
- Any write‑back to Salesforce (update/delete) → future phase
- Golden name enforcement beyond survivorship suggestion

---

## 12) Setup & Commands

```
pip install -r requirements.txt
streamlit run app/main.py
python src/cleaning.py --input data/raw/sample.csv --outdir data/processed
pytest -q
```

> Keep `CHANGELOG.md` updated and commit small, focused PRs with tests. 
