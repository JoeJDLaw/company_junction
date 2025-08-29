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

## Phase 1.10 Performance & Memory Hardening

### Performance Logging Infrastructure (`src/utils/perf_utils.py`)
- **`log_perf` context manager**: Timing and memory tracking using `tracemalloc`
- **Integrated throughout pipeline**: All major stages (normalization, candidate generation, grouping, survivorship, disposition, alias matching)
- **Memory metrics**: Tracks both current and peak memory usage
- **Lightweight overhead**: Minimal performance impact with comprehensive visibility

### Enhanced Token Filtering (`src/cleaning.py`)
- **Improved problematic patterns**: Case-insensitive regex patterns for better edge case detection
- **Normalization-aware filtering**: Accounts for normalization changes (e.g., "n/a" → "n a")
- **Comprehensive test coverage**: Verifies filtering removes problematic records correctly
- **Memory safety**: Reduces memory usage by filtering problematic records early

### Stop Token Logic (`src/similarity.py`)
- **Smart blocking strategy**: Avoids common suffixes (`{"inc", "llc", "ltd"}`) as blocking keys
- **Fallback logic**: If all tokens are stop tokens, falls back to first token
- **Improved efficiency**: Reduces unnecessary candidate pairs and memory usage
- **Conservative approach**: Preserves existing blocking behavior while adding safety

### Block Visibility & Statistics (`src/similarity.py`)
- **Top token logging**: Logs top-10 most common first-token keys for visibility
- **Block statistics file**: Writes `data/interim/block_top_tokens.csv` with token distribution
- **Performance insights**: Helps identify problematic blocking patterns
- **Debugging capability**: Clear visibility into blocking strategy effectiveness

### Performance Summary Generation (`src/cleaning.py`)
- **`_create_performance_summary` function**: Generates `perf_summary.json` with key metrics
- **Pipeline metrics**: Total pairs generated, pairs above medium threshold, group statistics
- **Global cap detection**: Identifies when pair generation limits are hit
- **Disposition summary**: Complete disposition breakdown for analysis

### Code Quality & Testing
- **All linting issues fixed**: Ruff, Black, and MyPy compliance
- **Enhanced test coverage**: Added tests for new functionality
- **Pandas stubs installed**: Better type checking support
- **No regressions**: All existing tests continue to pass

## Phase 1.9 Blacklist Improvements & Centralized I/O

### Safer Blacklist Matching (`src/disposition.py`)
- **Word-boundary regex**: Single-word tokens use `\b(?:token1|token2)\b` pattern
- **Substring matching**: Multi-word phrases use case-insensitive substring check
- **Caching**: Manual blacklist terms loaded once per pipeline run
- **Performance**: Compiled regex patterns for efficient matching

### Centralized Manual I/O (`src/manual_io.py`)
- **Single source of truth**: All manual file operations in one module
- **Atomic writes**: Temporary file + rename to prevent corruption
- **Robust error handling**: Graceful fallback for malformed files
- **Backward compatibility**: Supports existing file formats

### Audit Snapshots (`src/cleaning.py`)
- **Run metadata**: Timestamps, thresholds, counts written to `review_meta.json`
- **Git tracking**: Best-effort git commit capture
- **Statistics**: Blacklist counts, override counts, alias stats
- **Lightweight**: Minimal overhead, comprehensive information

### Streamlit Pipeline Launcher (`app/main.py`)
- **Command generator**: Lists CSV files in `data/raw/`, generates pipeline commands
- **Copy functionality**: Copy-to-clipboard for easy terminal execution
- **File selection**: Dropdown with files sorted by modification time
- **No subprocess**: Avoids security risks, maintains UI responsiveness

## Phase 1.8 Blacklist Visibility & Filter Improvements

### Blacklist Transparency (`app/main.py`)
- **Three-pane view**: Built-in (read-only), Manual (editable), Effective (union) blacklist terms
- **Compact layout**: Single sidebar expander with clear sections and dividers
- **Counts displayed**: Show term counts for each blacklist type
- **Helper function**: `get_blacklist_terms()` in `src/disposition.py` for built-in terms

### Filter Improvements (`app/main.py`)
- **Tooltips added**: Clear explanations for "Show Suffix Mismatches Only" and "Has Aliases"
- **Robust functionality**: Enhanced alias filter with fallback to `alias_candidates` column
- **Null-safe checks**: Better handling of missing or empty columns

### Sorting Enhancements (`app/main.py`)
- **Account Name sorting**: New options for groups by primary record's Account Name (ascending/descending)
- **Enhanced logic**: Group statistics now include primary record's name for sorting
- **Consistent behavior**: Maintains existing sorting options while adding new ones

### Technical Implementation
- **Helper function**: `get_blacklist_terms()` returns copy of built-in BLACKLIST
- **Filter logic**: Improved alias detection with multiple column support
- **Group stats**: Enhanced calculation to include primary record information

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

---

## Phase 1.13 Column Naming & ID Standards

### Canonical Column Naming Policy (Inputs vs Internal)
- **Inputs (Salesforce exports)** may use **spaced, titled columns**: `Account ID`, `Account Name`, `Created Date`, `Relationship`.
- **Internal canonical columns** (snake_case, used throughout pipeline and tests):
  - `account_id` → **18-char canonical Salesforce ID** (see ID policy below)
  - `account_id_src` → original input ID (15 or 18) preserved for audit/UI
  - `account_name`
  - `created_date`
  - `relationship`
- **Renaming map** for ingestion (apply once, early in cleaning):
  - `{"Account ID": "account_id", "Account Name": "account_name", "Created Date": "created_date", "Relationship": "relationship"}`
- **Candidate pairs** must use: `id_a`, `id_b`, and `score` (plus any explain columns).
- **Never** use spaced/titled names inside `src/` modules; keep them at the ingestion boundary only.

### Salesforce ID Canonicalization Policy
- All IDs used in joins/grouping/survivorship must be **canonical 18-char Salesforce IDs**.
- Store original in `account_id_src` (trimmed string) for UI/audit; **never** use `account_id_src` for joins.
- 15→18 conversion uses the **Salesforce checksum algorithm** (3-char suffix, case-sensitive on the 15-char source).
- Canonicalization helper location: `src/utils/id_utils.py` with:
  - `sfid15_to_18(sfid15: str) -> str`
  - `normalize_sfid_series(series) -> pd.Series` (returns 18-char or validated 18-char pass-through).
- **Referential integrity** check before grouping: every `id_a`/`id_b` must exist in `accounts[account_id]` (canonical). Fail fast with helpful samples.
- **Uniqueness**: `accounts[account_id]` must be unique and non-null after canonicalization.

### Utils Layout & Import Rules
- All shared helpers live under `src/utils/` (no `src/utils.py` usage in code; the legacy file is archived under `deprecated/`).
- Modules: `path_utils.py`, `logging_utils.py`, `perf_utils.py`, `io_utils.py`, `validation_utils.py`, `dtypes.py`, `hash_utils.py`, `id_utils.py`.
- **Imports must be absolute and rooted at `src`**, e.g. `from src.utils.id_utils import normalize_sfid_series`.
- Do not introduce relative imports between top-level modules (`src/foo.py` ↔ `src/bar.py`) that create cycles; extract shared code into `src/utils/` instead.

### Survivorship & Disposition (ID usage reminder)
- Survivorship tie-breakers use canonical `account_id` for lexicographic comparison (after relationship rank and created date). Never compare using `account_id_src`.
- Disposition logic and UI explanations should reference canonical ID, with `account_id_src` shown as original where helpful.

### QA Gates: Black / Ruff / MyPy (must run at phase end)
- At the end of every Phase prompt/PR, run and require **all to pass**:
  - `black --check .`
  - `ruff .`
  - `mypy --config-file mypy.ini .`
  - `pytest -q`
- **MyPy configuration** (in `mypy.ini` or `pyproject.toml`):
  - `mypy_path = src`, `namespace_packages = True`, `ignore_missing_imports = False`
  - Prefer **absolute imports** (`from src...`) to avoid module path conflicts (e.g., `dtypes_map` vs `src.dtypes_map`).
- If third-party stubs are needed, add to `requirements-dev.txt` (e.g., `pandas-stubs`, `types-PyYAML`). "Stubs" supply type info only; they don't affect runtime.

### Logging, Perf, and Artifacts
- Use `log_perf` (from `src/utils/perf_utils.py`) around each major stage.
- Keep run summaries clear: distinguish **hard duplicate rows removed** (same canonical `account_id`) vs **deduplication groups** (different `account_id`s believed to be same company).
- Artifacts:
  - `data/interim/*.parquet` (intermediate)
  - `data/processed/review_ready.*` (final for UI)
  - `data/processed/review_meta.json` (metadata)
  - `data/interim/block_top_tokens.csv` (blocking stats)

### Edit Hygiene
- Rules edits must be **idempotent**: if a rule exists, update/merge; do not duplicate.
- Keep terminology consistent with existing sections (Phase labels, bullets, formatting).