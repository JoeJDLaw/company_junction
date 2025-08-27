# Cursor Rules

## Project Structure
- Follow Cookiecutter Data Science foldering:
  - `data/raw` = raw exports from Salesforce
  - `data/interim` = temporary cleaning outputs
  - `data/processed` = final merged datasets
  - `src/` = pipeline code (cleaning, utils, salesforce CLI integration)
  - `app/` = Streamlit GUI
  - `tests/` = unit tests
  - `docs/` = additional documentation
- Keep heavy logic in `src/`, not in `app/`.
- Optional `config/` for `settings.yaml`, `logging.conf`.

## Code Standards
- Always create/update guardrail files: README.md, CHANGELOG.md
- Keep test files in `tests/` directory
- Use type hints in Python functions
- Add docstrings to all functions and classes
- Follow PEP 8 style guidelines

## Data Pipeline Principles
- Ensure reproducibility: pipeline always re-runs cleanly from `data/raw/`
- Maintain data lineage: track transformations from raw to processed
- Use logging for debugging and audit trails
- Validate data at each step of the pipeline
- Do **not** commit large data. Only small samples under `tests/fixtures/`.

## Development Workflow
- Install from `requirements.txt` only.
- All new features must include at least one test in `tests/`.
- Update `CHANGELOG.md` for any significant changes
- Keep dependencies minimal and well-documented
- Document any configuration requirements

## Critical Review
- Be critical of prompts: if unclear, provide feedback before executing
- Question assumptions about data formats and requirements
- Validate that solutions are maintainable and scalable
- Consider edge cases and error handling

## File Naming Conventions
- Use `snake_case` for Python files/functions.
- Use descriptive names that indicate purpose
- Include timestamps in processed data filenames
- Keep file paths relative to project root

---

## Phase 1 Rules (Company Junction Deduplication)

- **Scope:** Read-only first pass review. No Salesforce writes. Split detection is **deferred to Phase 2**.
- **Normalization (src/normalize.py):**
  - Preserve legal suffix differences (INC vs LLC etc.).
  - Map symbols: `&→and`, `/→space`, `-→space`, `@→at`, `+→plus`; collapse whitespace.
  - Numeric style unify: `20-20`, `20/20`, `20 20` → `20 20`.
  - Extract trailing suffix into `suffix_class`; compute `name_core` without suffix for candidate generation.
- **Similarity (src/similarity.py):**
  - Use RapidFuzz (`token_sort_ratio`, `token_set_ratio`) + Jaccard on `name_core`.
  - Composite score: `0.45*ratio_name + 0.35*ratio_set + 20*jaccard`, with penalties:
    - `suffix_mismatch: 25`
    - `num_style_mismatch: 5`
  - Thresholds (from `config/settings.yaml`): `high=92`, `medium=84`.
  - If `suffix_match=False`, do not auto-accept; mark for **Verify**.
  - Build groups as connected components where `suffix_match=True` and `score ≥ medium`.
- **Survivorship (src/survivorship.py):**
  - Primary selection order:
    1) Lowest Relationship rank (from `config/relationship_ranks.csv`)
    2) Earliest Created Date (Excel serials supported)
    3) Smallest Account ID (lexicographic)
  - Provide a lightweight `merge_preview_json` (no writebacks).
- **Disposition (src/disposition.py):**
  - Values: `Keep`, `Update`, `Delete`, `Verify`.
  - `Delete` if name matches blacklist (`pnc is not sure`, `1099`, etc.).
  - Suffix mismatch within a group ⇒ `Verify`.
  - Primary in group ⇒ `Keep`; non-primary ⇒ `Update`.
  - Singleton clean ⇒ `Keep`; suspicious ⇒ `Verify`.
  - LLM gate is optional and **disabled by default** (Phase 2 consideration).
- **Artifacts:**
  - `data/interim/*.parquet` and `data/processed/review_ready.csv`.
- **Tests:**
  - Include unit tests for normalization, similarity, grouping/survivorship, and disposition. Match thresholds from config.

---

## Phase 1.7 Review UX & Manual Controls

### Streamlit UX (`app/main.py`)
- **Disposition table**: Replace bar chart with compact table showing count/percent
- **Clickable filters**: Quick filter buttons for each disposition type
- **Group layout**: Move group info to top with badges (suffix class, size, blacklist hits)
- **Account Name wrapping**: Use `column_config` for full visibility and readability
- **Sorting controls**: Sort groups by size/score, records by name
- **Pagination**: Keep Phase 1.6 pagination with session state management

### Manual Overrides (`app/manual_data.py`)
- **Group-level dropdown**: Manual disposition override (Keep/Delete/Update/Verify)
- **JSON persistence**: Store in `data/manual/manual_dispositions.json`
- **Audit trail**: Include timestamps and reason fields
- **Pipeline integration**: Optional loading in `src/disposition.py` with graceful fallback

### Manual Blacklist (`app/manual_data.py`)
- **Pattern-based rules**: Add/remove terms for automatic Delete classification
- **JSON storage**: Store in `data/manual/manual_blacklist.json`
- **UI editor**: Sidebar expander with add/remove functionality
- **Built-in display**: Show built-in blacklist terms for reference

### Pipeline Integration (`src/disposition.py`)
- **Optional loading**: Load manual files if present, skip gracefully if missing
- **Override priority**: Manual overrides applied last in classification chain
- **Blacklist union**: Manual terms combined with built-in blacklist
- **Logging**: Report override and blacklist term counts

### File Management
- **Directory**: `data/manual/` with `.gitignore` to exclude JSON files
- **Export functionality**: Download buttons for audit trail
- **Error handling**: Graceful fallback if files are malformed

### Testing
- **Manual overrides**: Test that overrides are applied correctly
- **Manual blacklist**: Test that terms cause Delete classification
- **Pipeline robustness**: Test that missing files don't break pipeline

## Phase 1.5 Refinements (Conservative Aliases & Minimal UI)

- **Normalization (src/normalize.py):**
  - Underscore normalization: drop leading/trailing underscores; collapse multiple underscores to one space.
  - Parentheses are **preserved** for display; flagged with `has_parentheses`.
  - **Do not** strip commas or periods globally.
  - Multi-name indicators:
    - `has_semicolon`: raw name contains `;`
    - `has_multiple_names`: detects semicolons, numbered markers like `(1)`, `(2)`, or multiple "and" separators
  - Parentheses **alias candidates** are created **only when** content contains a legal suffix token (INC/LLC/LTD/CORP/…) **or** multiple capitalized words.
  - Parenthetical blacklist: phrases like `paystub`, `pay stubs`, `not sure`, `unsure`, `unknown`, `staffing agency`, numbers-only → **never** create aliases.

- **Aliases (src/normalize.py, src/alias_matching.py):**
  - Extract aliases from **semicolons** and **numbered sequences** by default; add filtered parentheses per rule above.
  - Alias matching uses the **same similarity** function but requires:
    - Suffix match
    - Score ≥ **high** threshold
  - Results are **cross-links only** (no regrouping/merging). Store in `data/interim/alias_matches.parquet` and surface minimal metadata in `review_ready.csv`.
  - Performance guard: cap alias pair generation via config (e.g., `max_alias_pairs`).

- **Disposition (src/disposition.py):**
  - Records with one or more valid alias matches default to **`Verify`** with `disposition_reason="alias_matches_N_groups_via_[sources]"` (unless they are `Delete` from blacklist).
  - Multi-name indicators alone (without valid alias matches) also bias to `Verify`.

- **UI (app/main.py):**
  - Minimal additions for Phase 1.5: show an **alias badge** with a simple expander, and a **"Has aliases"** filter.
  - Defer the full settings/rules panel to Phase 2.

- **Config (config/settings.yaml):**
  - Keep existing thresholds.
  - Ensure a small `similarity.penalty.punctuation_mismatch` (default `3`) remains conservative.
  - Add `max_alias_pairs` guard if not present.