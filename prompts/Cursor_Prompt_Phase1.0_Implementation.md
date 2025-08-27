# Cursor Prompt — Implement Phase 1 of Company Junction Deduplication

> **Objective:** Implement Phase 1 per `docs/DLaw_Company_Junction_Dedup_Plan.md` to produce a reviewable dataset that flags likely duplicate Accounts, selects a primary using Relationship rank + Created Date, and assigns Disposition (`Keep`, `Update`, `Delete`, `Verify`). **No Salesforce writes.** **Split detection deferred to Phase 2.**

---

## Preconditions

- Repo tree (confirmed):  
  - `config/settings.yaml` (updated), `config/relationship_ranks.csv` (present)  
  - Raw sample: `data/raw/company_junction_range_01.csv`  
  - Existing: `src/cleaning.py`, `src/utils.py`, `app/main.py`, tests scaffold

- If not already in `requirements.txt`, **add** (pin or loose as policy allows):  
  ```txt
  pandas
  pyarrow
  rapidfuzz
  python-dateutil
  streamlit
  ```
  (Only add missing ones; do **not** duplicate existing pins.)

---

## Tasks (do in order)

### 1) Create `config/settings.yaml` loader
**Edit:** `src/utils.py` (or create `src/configs.py` if preferred)

- Add:
  - `load_settings(path: str) -> dict`
  - `load_relationship_ranks(path: str) -> Dict[str, int]`

- Behavior:
  - YAML read; provide defaults for any missing keys per plan:
    ```python
    DEFAULTS = {
      "similarity": {"high": 92, "medium": 84, "penalty": {"suffix_mismatch": 25, "num_style_mismatch": 5}},
      "llm": {"enabled": False, "delete_threshold": 85},
      "survivorship": {"tie_breakers": ["created_date", "account_id"]},
      "io": {"interim_format": "parquet"},
    }
    ```
  - Merge user config over defaults (user wins).

### 2) Implement legal‑aware normalization
**Create:** `src/normalize.py`

- Expose:
  ```python
  from dataclasses import dataclass
  from typing import Optional, Dict

  @dataclass
  class NameNorm:
      name_raw: str
      name_base: str
      name_core: str
      suffix_class: str  # INC/LLC/LTD/CORP/LLP/LP/PLLC/PC/CO/GMBH/NONE

  LEGAL_SUFFIXES = {
      "inc": "INC", "inc.": "INC", "incorporated": "INC",
      "llc": "LLC", "l.l.c.": "LLC",
      "ltd": "LTD", "ltd.": "LTD",
      "corp": "CORP", "corp.": "CORP", "corporation": "CORP",
      "llp": "LLP", "lp": "LP", "pllc": "PLLC", "pc": "PC",
      "co": "CO", "co.": "CO", "gmbh": "GMBH"
  }

  def normalize_name(name: Optional[str]) -> NameNorm: ...
  def extract_suffix(tokens: list[str]) -> tuple[str, list[str]]: ...
  def excel_serial_to_datetime(val) -> Optional[pandas.Timestamp]: ...
  ```

- Rules:
  - `name_base`: lowercase; map `&->and`, `/->space`, `- -> space`, `@->at`, `+->plus`; collapse spaces; keep alnum + space.
  - Unify numeric style: `20-20`, `20/20`, `20 20` → `20 20`.
  - `suffix_class` recognized only as **trailing** suffix; `name_core` = tokens minus trailing suffix tokens.
  - Return `NameNorm` object.

- **Tests:** `tests/test_normalize.py`
  - Assert that the three “20/20 … Inc” variants share same `name_core` and `suffix_class == "INC"`.
  - “20/20 … LLC” yields `suffix_class == "LLC"`.

### 3) Candidate pairing & similarity
**Create:** `src/similarity.py`

- Use `rapidfuzz.fuzz` and `rapidfuzz.utils`:
  ```python
  from rapidfuzz import fuzz

  def pair_scores(df_norm):  # returns candidate_pairs DataFrame
      # generate pairs with blocking on first token(s) of name_core and/or account owner shard
      # compute:
      # ratio_name = fuzz.token_sort_ratio(a_core, b_core)
      # ratio_set  = fuzz.token_set_ratio(a_core, b_core)
      # jaccard on token sets
      # num_style_match boolean (derived during normalization)
      # suffix_match boolean
      # base = 0.45*ratio_name + 0.35*ratio_set + 20*jaccard
      # if not num_style_match: base -= penalty.num_style_mismatch
      # if not suffix_match: base -= penalty.suffix_mismatch
      # score = clip 0..100
  ```

- Emit to `data/interim/candidate_pairs.parquet` with cols:
  - `id_a`, `id_b`, `score`, `suffix_match`, `ratio_name`, `ratio_set`, `jaccard`

- **Tests:** `tests/test_similarity.py`
  - INC vs INC examples ≥ `high` (config).
  - INC vs LLC → either < `high` or flagged for verify due to `suffix_match=False`.

### 4) Grouping & primary selection
**Create:** `src/grouping.py` and `src/survivorship.py`

- `grouping.py`:
  - Build graph using edges where `suffix_match=True and score>=medium`.
  - Use simple Union‑Find / DSU to avoid new deps.
  - Return DataFrame with `record_id`, `group_id`, and best `score_to_primary` stub (compute after primary chosen).

- `survivorship.py`:
  - Load `config/relationship_ranks.csv` → dict.
  - Compute `relationship_rank` per record (default 60 when missing/unmapped).
  - Primary selection order:
    1) lowest `relationship_rank`
    2) earliest `Created Date` (handle Excel serial via `excel_serial_to_datetime`)
    3) smallest `Account ID` (string compare)
  - Provide a `merge_preview_json` per record comparing to primary (diff of selected fields).

- **Tests:** `tests/test_grouping.py`
  - Construct a tiny frame to validate primary choice and stable group ids.

### 5) Disposition logic
**Create:** `src/disposition.py`

- Implement:
  ```python
  BLACKLIST = [
    "pnc is not sure","pnc is unsure","unsure","unknown","no paystub","no paystubs",
    "1099","1099 pnc","none","n/a","tbd","test","sample","delete","do not use"
  ]

  def classify_disposition(row, group_meta, cfg) -> str: ...
  ```
- Rules:
  - In a group with suffix mismatch anywhere → mark all as `Verify`.
  - Strong group (primary + others) with same suffix → primary=`Keep`, others=`Update`.
  - Singleton: default `Keep` unless blacklisted/suspicious → `Delete`/`Verify`.
  - Optional LLM gate (respects `cfg["llm"]["enabled"]`) — **stub only** in phase 1.

- **Tests:** `tests/test_disposition.py`
  - Blacklisted → `Delete`
  - Suffix mismatch → `Verify`
  - Strong match same suffix → primary=`Keep`, others=`Update`

### 6) Wire the CLI orchestrator
**Edit:** `src/cleaning.py`

- Add arguments:
  ```bash
  python src/cleaning.py --input data/raw/company_junction_range_01.csv --outdir data/processed --config config/settings.yaml
  ```
- Steps:
  1) Load CSV and normalize key columns; coerce `Created Date` (Excel serials allowed).
  2) Write `data/interim/accounts_normalized.parquet`.
  3) Generate candidate pairs; write `data/interim/candidate_pairs.parquet`.
  4) Build groups; compute primary; write `data/interim/groups.parquet`.
  5) Compute Disposition per record; write `data/interim/dispositions.parquet`.
  6) Join into `data/processed/review_ready.csv` with columns:
     - `group_id`, `is_primary`, `score_to_primary`, `Disposition`, `relationship_rank`, `suffix_class`, `Account ID`, `Account Name`, `Created Date`, plus a `merge_preview_json`.

- Exit non‑zero if required columns are missing. Required minimum:
  - `Account ID`, `Account Name` (or `Employer Name` fallback), `Relationship`, `Created Date`.

### 7) Streamlit review (read‑only)
**Edit:** `app/main.py`

- Load `data/processed/review_ready.csv` if present; else show hint to run CLI.
- Filters: min score, disposition, suffix‑mismatch, group size.
- Show group header + table with badges (`suffix_mismatch`, `blacklist_hit`), pretty‑print `merge_preview_json`.
- Button to export filtered view.

### 8) Docs (light touch)
- **Do not** overhaul docs now; only ensure `docs/DLaw_Company_Junction_Dedup_Plan.md` is referenced from `README.md`.
- Note: Before Phase 2, we will update `README.md`, `CHANGELOG.md`, and `cursor_rules.md` with any deltas found during implementation.

---

## Acceptance checklist (have Cursor assert at the end)

- [ ] `src/normalize.py` with tests passing.
- [ ] `src/similarity.py` with tests passing.
- [ ] `src/grouping.py` + `src/survivorship.py` with tests passing.
- [ ] `src/disposition.py` with tests passing.
- [ ] `src/cleaning.py` runs end‑to‑end and produces `data/processed/review_ready.csv` on sample data.
- [ ] `app/main.py` loads review CSV and renders filters/tables.
- [ ] Intermediate Parquet files present under `data/interim/`.
- [ ] No Salesforce writes performed.
- [ ] Minimal doc reference updated; major doc updates deferred until pre‑Phase‑2 checkpoint.

---

## File scaffolds for Cursor to create

> **Create these files if missing, or extend if present.** Implement bodies as described above and make tests pass.

```
src/
  normalize.py
  similarity.py
  grouping.py
  survivorship.py
  disposition.py
tests/
  test_normalize.py
  test_similarity.py
  test_grouping.py
  test_disposition.py
```

Each test should use tiny DataFrames (or `tests/fixtures/sample_accounts.csv`) to validate the intended behavior and thresholds from `config/settings.yaml`.

---

## Notes for future Phase 2 (do **not** implement now)

- Add Split detection & parsing (multiple company names in one field) and a `Split` disposition.
- Optional LLM “real‑company” classifier with SQLite caching.
- Salesforce sync via CLI (staging fields then confirm).

---

**Run sequence after implementation:**

```bash
pip install -r requirements.txt
pytest -q
python src/cleaning.py --input data/raw/company_junction_range_01.csv --outdir data/processed --config config/settings.yaml
streamlit run app/main.py
```
