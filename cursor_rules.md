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
- Install from `requirements.txt` and install package in development mode: `pip install -e .`
- All new features must include at least one test in `tests/`.
- Update `CHANGELOG.md` for any significant changes
- Keep dependencies minimal and well-documented
- Document any configuration requirements

## Environment & Execution (macOS + .venv is mandatory)

- **Always activate the project virtual environment before any command** (tests, type checks, running the pipeline, Streamlit, or ad‑hoc scripts). Do **not** install or run against the global interpreter.
- **Activation (bash/zsh):**
  - `source .venv/bin/activate`
  - Verify with: `which python` ⇒ should resolve inside `.venv/`
  - Install deps with: `python -m pip install -r requirements.txt`
- **Never** run `pip install` without the `python -m` prefix and **never** outside the venv.
- **Do not create or publish a distribution package** as part of any phase; this repository is executed directly via the venv.

### macOS hardware assumptions (for sensible defaults)
- Target machine: Apple Silicon (Apple M‑series), SSD storage, **14 logical cores**, **24 GB RAM** (auto-detected via `os.cpu_count()` and `psutil` where available).
- **Parallelism guideline** (when adding concurrency):
  - Default workers: `min(os.cpu_count(), max(1, os.cpu_count()-2))`
  - Memory guard: if `psutil` available, keep estimated **RSS per worker × workers ≤ 0.75 × total RAM**
  - Provide a CLI/config override: `--workers N`, `--no-parallel` to force single‑process.
  - Ensure all parallel code **degrades to sequential** if resources are constrained or library not available.

### Alias optimization activation (Phase 1.21.1+)
- **Default behavior**: `alias.optimize: true` enables optimized parallel alias matching
- **Worker requirement**: Optimization activates when `workers > 1`; falls back to sequential for single worker
- **CLI precedence**: CLI `--workers` flag has precedence over config file settings
- **Environment safety**: Automatic BLAS thread clamping (OMP_NUM_THREADS=1, OPENBLAS_NUM_THREADS=1, VECLIB_MAXIMUM_THREADS=1, NUMEXPR_NUM_THREADS=1)
- **Fallback behavior**: Graceful degradation to sequential execution when parallel resources unavailable

### Project path reference (source of truth)
- **Pipeline entrypoint:** `src/cleaning.py`
- **MiniDAG / Smart resume:** `src/utils/mini_dag.py`
- **Performance utilities:** `src/utils/perf_utils.py`
- **Progress logger:** `src/utils/progress.py`
- **Similarity / Grouping / Survivorship / Disposition / Aliases:**
  - `src/similarity.py`, `src/grouping.py`, `src/survivorship.py`, `src/disposition.py`, `src/alias_matching.py`
- **Streamlit app:** `app/main.py`
- **Config & ranks:** `config/settings.yaml`, `config/relationship_ranks.csv`
- **Interim & processed artifacts:** under `data/interim/` and `data/processed/`

### Commands (always inside .venv)
- **Run pipeline:**
  - `python src/cleaning.py --input data/raw/company_junction_range_01.csv --outdir data/processed --config config/settings.yaml`
- **Run Streamlit:**
  - `streamlit run app/main.py`
- **QA gates:**
  - `black --check .`
  - `ruff check .`
  - `mypy --config-file mypy.ini src tests app`
  - `pytest -q`
- **Import sanity (tests use absolute imports):**
  - `pytest tests/test_imports.py -q`

### Indexing & Cursor behavior
- Respect `.cursorignore` (authoritative for Cursor indexing). Current entries include:
  - `data/interim/`, `data/raw/company_junction_range_01.csv`, `*.tar.gz`, `.venv/`
- Do not propose edits that rely on files excluded from indexing.
- When searching the codebase, prefer the **paths listed above** to avoid scanning build artefacts or caches.

### Caching & artifact retention (Phase 1.16+ preparation)
- **Do not overwrite prior run outputs by default.** When adding caching or per-run outputs:
  - Use run-scoped directories: `data/interim/{run_id}/...` and `data/processed/{run_id}/...`
  - Maintain a stable `latest` symlink or copy for the most recent successful run
  - **MiniDAG state** should carry `run_id`, `input_hash`, `config_hash`, and `dag_version`
  - Provide `--keep-runs N` (GC policy) and `--run-id <str>` overrides
- All caching must preserve **artifact invariance** for fixed inputs/configs and pass all QA gates.

### Prohibited actions
- Do **not**:
  - Install requirements globally or modify the system Python
  - Introduce packaging/`setup.py` build & publish steps
  - Change CLI flags or defaults without updating README.md and CHANGELOG.md
  - Commit large datasets or intermediate artefacts (honor `.gitignore`)

## Critical Review
- Be critical of prompts: if unclear, provide feedback before executing
- Question assumptions about data formats and requirements
- Validate that solutions are maintainable and scalable
- Consider edge cases and error handling
- **Streamlit Debugging**: When troubleshooting UI issues, consider Streamlit's top-to-bottom rendering order
- **Evidence-Based Debugging**: Use controlled testing (like diagnostic modes) to isolate problematic sections
- **Simple Solutions First**: Prefer simple fixes (like reordering UI elements) over complex caching/optimization solutions

## File Naming Conventions
- Use `snake_case` for Python files/functions.
- Use descriptive names that indicate purpose
- Include timestamps in processed data filenames
- Keep file paths relative to project root

## Streamlit Debugging Best Practices (Phase 1.17.4+)

### Rendering Order Awareness
- **Top-to-bottom execution**: Streamlit renders UI elements in the order they appear in the script
- **Heavy operations placement**: Place computationally expensive operations at the end of sidebar or main content
- **Blocking prevention**: Heavy operations in early sections can block rendering of subsequent sections
- **Section isolation**: Use `st.expander()` to isolate heavy operations and prevent UI blocking

### Diagnostic Mode Implementation
- **Controlled testing**: Implement diagnostic mode to disable/enable problematic sections
- **Isolation testing**: Use diagnostic flags to isolate which section is causing issues
- **Evidence-based debugging**: Use controlled experiments rather than assumptions
- **Preserve diagnostic tools**: Keep diagnostic mode available for future troubleshooting

### Pandas Array Handling
- **Explicit length checks**: Use `len(array) > 0` instead of `if array:` for pandas Series
- **Truthiness errors**: Avoid `ValueError: The truth value of an array with more than one element is ambiguous`
- **Type safety**: Always validate DataFrame column types before operations
- **Array operations**: Use explicit boolean masks for DataFrame assignments

### UI Performance Optimization
- **Lazy loading**: Load heavy data only when needed (e.g., when expander is opened)
- **Session state caching**: Use `st.session_state` to cache expensive operations
- **Progressive disclosure**: Show essential UI first, load details on demand
- **Memory management**: Clear session state caches when switching between runs

### Error Handling in Streamlit
- **Section guards**: Wrap heavy sections in try/except blocks to prevent complete UI failure
- **Graceful degradation**: Provide fallback UI when operations fail
- **User feedback**: Show clear error messages and recovery options
- **Debug information**: Include diagnostic information in error messages

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

### UI Standards (Phase 1.17.1+)
- **Run Picker**: Implement in sidebar with run_id selection and metadata display
- **Stage Status**: Show pipeline execution status with ✅/❌/⏳ indicators and durations
- **Pure Helper Functions**: All UI logic must be in `src/utils/` with no Streamlit dependencies
- **Session State Caching**: Use Streamlit session state to avoid unnecessary reloading
- **Run-Scoped Only**: No global path fallbacks, all artifacts must be run-scoped
- **Error Handling**: Clear messages for missing runs, failed runs, or incomplete artifacts
- **Type Safety**: All helper functions must have comprehensive type annotations
- **Streamlit Rendering Order**: Heavy operations must be placed at the end of sidebar to avoid blocking main content
- **Diagnostic Mode**: Include diagnostic mode for troubleshooting UI rendering issues
- **Pandas Array Handling**: Use explicit length checks (`len(array) > 0`) instead of truthiness for pandas Series

### UI Safety Standards (Phase 1.17.2+)
- **Destructive Actions Fuse**: Require explicit enablement before any destructive operations
- **Preview Mode**: Show exactly what will be affected before confirming actions
- **Checkbox Confirmation**: Simple checkbox confirmation for all destructive operations
- **In-flight Protection**: Prevent deletion of running or active resources
- **Audit Logging**: Log all destructive operations with timestamps and details
- **Atomic Operations**: Use temporary files and atomic rename for critical updates
- **Latest Pointer Management**: Automatic recomputation and atomic updates of pointers
- **UI Performance**: Heavy operations must be placed at the end of sidebar to prevent blocking main content rendering

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

- At the end of every Phase prompt/PR, run and require **all to pass**:
  - `black --check .`
  - `ruff .`
  - `mypy --config-file mypy.ini src tests app`
  - `pytest -q`
- **MyPy configuration** (in `mypy.ini` or `pyproject.toml`):
  - `explicit_package_bases = True`, `namespace_packages = True`, `ignore_missing_imports = False`
  - Prefer **absolute imports** (`from src...`) to avoid module path conflicts (e.g., `dtypes_map` vs `src.dtypes_map`).
- If third-party stubs are needed, add to `requirements-dev.txt` (e.g., `pandas-stubs`, `types-PyYAML`). "Stubs" supply type info only; they don't affect runtime.

- Use `log_perf` (from `src/utils/perf_utils.py`) around each major stage.
- Keep run summaries clear: distinguish **hard duplicate rows removed** (same canonical `account_id`) vs **deduplication groups** (different `account_id`s believed to be same company).
- Artifacts:
  - `data/interim/*.parquet` (intermediate)
  - `data/processed/review_ready.*` (final for UI)
  - `data/processed/review_meta.json` (metadata)
  - `data/interim/block_top_tokens.csv` (blocking stats)
  - Performance profiling hooks must **not** change outputs; they are strictly observational.
  - Parallel execution must be **opt-in** via config/CLI and **auto-cap** workers per the Environment & Execution rules above.

### Edit Hygiene
- Rules edits must be **idempotent**: if a rule exists, update/merge; do not duplicate.
- Keep terminology consistent with existing sections (Phase labels, bullets, formatting).

---

## Phase 1.13.7 Type Safety & Code Quality Standards

### Zero MyPy Errors Requirement
- **Mandatory**: All code changes must maintain **0 MyPy errors** across the entire codebase
- **Strict enforcement**: No exceptions or type ignores unless absolutely necessary
- **Comprehensive coverage**: All 33 source files must pass MyPy validation
- **Test functions**: All test methods must have explicit `-> None` return type annotations

### Pandas DataFrame Operations
- **Boolean masking**: Use boolean masks instead of tuple indexing for DataFrame assignments
  - ✅ `mask = df.index == idx; df.loc[mask, "column"] = value`
  - ❌ `df.loc[idx, "column"] = value` (causes Invalid index type errors)
- **Arithmetic operations**: Cast pandas arrays to float before arithmetic operations
  - ✅ `(values.astype(float) / total * 100).round(1)`
  - ❌ `(values / total * 100).round(1)` (causes operator errors)
- **Type safety**: Always validate DataFrame column types before operations
- **Parquet hygiene**: Only write scalar, parquet-friendly columns to parquet files
  - ✅ Sanitize DataFrames to scalar columns only before parquet write
  - ✅ Force proper dtypes: string for IDs/text, float32 for scores
  - ❌ Write list/dict/object columns to parquet (causes pyarrow errors)

### JSON and AST Operations
- **Type validation**: Always validate return types from `json.load()` and `ast.literal_eval()`
  - ✅ `data = json.load(f); return data if isinstance(data, list) else []`
  - ❌ `return json.load(f)` (returns Any, causes no-any-return errors)
- **Explicit casting**: Use explicit type casting for Hashable types
  - ✅ `str(col).startswith("_")` for DataFrame column names
  - ❌ `col.startswith("_")` (causes attr-defined errors)

### Import Standards
- **Production code (src/**) and **tests/** must use absolute imports rooted at `src`:
  - ✅ `from src.utils.id_utils import normalize_sfid_series`
  - ❌ `from utils.id_utils import ...` (disallowed)
  - ❌ relative imports like `from .utils import ...` (disallowed)

- **App UI package (app/**)** may use absolute, app-scoped imports for intra-package organization:
  - ✅ `from app.components import group_list, group_details`
  - ✅ `from app.components.group_list import render_groups`

- Do not mix `src` and `app` namespaces within the same module unless the module’s role requires it
  (e.g., UI code in `app/` may import helpers from `src/`, but `src/` must not import from `app/`).
- **No module path conflicts**: Avoid dual module identities (e.g., `dtypes_map` vs `src.dtypes_map`)
- **Test imports**: All test files must use absolute imports matching production code

### QA Gates Enforcement
- **Zero tolerance**: All QA gates must pass with zero errors:
  - `black --check .` ✅
  - `ruff check .` ✅  
  - `mypy --config-file mypy.ini src tests app` ✅
  - `pytest -q` ✅
- **Test scope**: `pytest.ini` must be present with proper test discovery configuration
- **Snapshot cleanup**: Archive/review snapshot directories (`company_junction_phase*review/`) must be ignored by Git

### Type Annotation Standards
- **Function signatures**: All functions must have complete type annotations
- **Return types**: Explicit return types for all functions (use `-> None` for void functions)
- **Variable annotations**: Use type annotations for complex variables and data structures
- **Generic types**: Use proper generic types for collections (`List[str]`, `Dict[str, Any]`, etc.)

### Documentation Updates
- **Mandatory updates**: Every phase must update README.md, CHANGELOG.md, and cursor_rules.md
- **Consistency**: Ensure all three documents are consistent and up-to-date
- **Phase tracking**: Document all significant changes in CHANGELOG.md with proper semantic versioning
- **Rule maintenance**: Keep cursor_rules.md as the authoritative source for development standards

### Runtime Preservation
- **No functional changes**: All type safety improvements must maintain identical runtime behavior
- **Backward compatibility**: Preserve all existing APIs and function signatures
- **Test validation**: All existing tests must continue to pass without modification
- **Performance**: Type annotations should not impact runtime performance

---

## Phase 1.14 Progress, Heartbeats, and MiniDAG Orchestration

### Stage Banners and Progress Tracking
- **Stage boundaries**: Clear `[stage:start]` and `[stage:end]` messages for all major pipeline stages
- **Progress heartbeats**: Periodic logging with rate and ETA information for long-running operations
- **Optional tqdm**: Support for tqdm progress bars when `--progress` flag is used
- **Fallback behavior**: Graceful fallback to logging-only heartbeats if tqdm is not installed

### MiniDAG Orchestration & Smart Auto-Resume (Phase 1.14)
- **Stage tracking**: Use `src/utils/mini_dag.py` for atomic stage completion tracking
- **Smart auto-resume**: Automatically detect resume point based on last completed stage and file existence
- **Input hash validation**: Compute SHA256 of input files to prevent stale artifact usage
- **State file versioning**: Include `dag_version` field for backward compatibility (defaults to "1.0.0")
- **Error tolerance**: Handle corrupted state files gracefully with automatic reset to clean state
- **Resume decision logging**: Log explicit reason codes (SMART_DETECT, HASH_MISMATCH, NO_PREVIOUS_RUN, etc.)
- **CLI flags**: `--resume-from <stage>`, `--no-resume`, `--force`, `--state-path <path>`
- **Atomic writes**: Use `tempfile` + `os.replace` for state file updates

### Progress Logging (`src/utils/progress.py`)
- **Heavy loop instrumentation**: Wrap computationally intensive loops with `ProgressLogger`
- **Configurable intervals**: Support both step-based and time-based logging intervals
- **Rate and ETA calculation**: Provide processing rate and estimated time to completion
- **Memory efficiency**: Minimal memory overhead with comprehensive progress visibility

### Pipeline Stages
The following stages are tracked and support resumability:
- `normalization` - Company name normalization and cleaning
- `filtering` - Data filtering and problematic record removal
- `candidate_generation` - Candidate pair generation with blocking
- `grouping` - Duplicate group creation with edge-gating
- `survivorship` - Primary record selection and merge preview
- `disposition` - Disposition classification and assignment
- `alias_matching` - Alias matching and cross-reference generation
- `final_output` - Final review-ready output generation

### CLI Enhancements
- **`--progress` flag**: Enable optional tqdm progress bars (default: logging-only)
- **`--resume-from` flag**: Resume pipeline execution from specific stage
- **Backward compatibility**: Pipeline runs exactly as before without new flags
- **Error handling**: Graceful handling of missing intermediate files during resume

### Heavy Loop Instrumentation
- **Similarity module**: Progress tracking for block iteration and pair scoring
- **Grouping module**: Progress tracking for Union-Find pair processing
- **Survivorship module**: Progress tracking for per-group primary selection
- **Configurable intervals**: Step-based (e.g., every 10,000 items) and time-based (e.g., every 5 seconds) logging

### Streamlit Integration
- **Minimal spinner**: Add loading spinner for data loading operations
- **No heavy logic**: Keep all heavy computation in pipeline, not in Streamlit app
- **User feedback**: Provide clear feedback during long-running operations

### QA Gates Compliance
- **Zero MyPy errors**: All progress logging code must maintain strict type safety
- **Black/Ruff compliance**: All code must pass formatting and linting checks
- **Test coverage**: Maintain comprehensive test coverage for all new functionality
- **Performance preservation**: Progress logging must not significantly impact runtime performance

### Documentation Requirements
- **README updates**: Add progress and heartbeats section with usage examples
- **CHANGELOG updates**: Document all new features and improvements
- **Rule maintenance**: Keep cursor_rules.md as authoritative source for progress standards

### Smart Auto-Resume (Phase 1.14.2)
- **Auto-detection**: Intelligently determine resume point based on state file and intermediate files
- **Input hash validation**: Compute SHA256 hash of input and config files to detect changes
- **State metadata**: Track input paths, config files, command line, and timestamps in state file
- **Safety protection**: Prevent resuming with changed inputs unless `--force` specified

### Smart Auto-Resume CLI Semantics
- **Default behavior**: Smart auto-resume with input validation (no flags needed)
- **`--no-resume`**: Force full pipeline run, ignoring previous state
- **`--resume-from`**: Override auto-detection with specific stage
- **`--force`**: Override input hash mismatch protection
- **`--state-path`**: Custom path for pipeline state file

### Auto-Resume Decision Logging
- **Clear reasoning**: Log auto-resume decisions with explicit reasoning
- **Input hash status**: Include input hash validation results in decision logs
- **Stage information**: Show last completed stage and suggested resume point
- **Error conditions**: Clear error messages for hash mismatches and missing files

### State File Structure
- **Atomic writes**: Use temporary files and atomic replacement for state persistence
- **Metadata tracking**: Store input_hash, dag_version, cmdline, and timestamp
- **Stage information**: Track status, start_time, end_time for each stage
- **Backward compatibility**: Maintain compatibility with existing state files

### File Validation Logic
- **Stage-specific files**: Define required intermediate files for each pipeline stage
- **Existence checking**: Validate that required files exist before resuming
- **Graceful degradation**: Clear error messages when files are missing
- **Flexible paths**: Support custom interim directory paths

### Safety and Validation
- **Input invariance**: Ensure input and config files haven't changed since last run
- **Force flag requirement**: Require explicit `--force` flag to override hash mismatches
- **File integrity**: Validate intermediate file existence before resuming
- **State corruption protection**: Atomic writes prevent state file corruption

### Testing Requirements
- **Comprehensive coverage**: Test all auto-resume scenarios and edge cases
- **Input hash validation**: Test hash computation and invariance checking
- **File validation**: Test intermediate file existence checking
- **State persistence**: Test state file save/load operations
- **CLI semantics**: Test all flag combinations and behaviors

### Performance Profiling (Phase 1.15.3)
- **Memory tracking**: Use `src/utils/perf_utils.py` for memory usage monitoring
- **Stage timing**: Wrap heavy operations in `time_stage()` context managers
- **Regression detection**: Compare against baselines with configurable thresholds
- **Performance logging**: Log memory and timing data for all major stages
- **Context managers**: Use `track_memory_peak()` for peak memory detection
- **Baseline management**: Save/load performance baselines for regression testing
- **Graceful fallback**: Handle missing psutil dependency with zero-value fallback

---

## Phase 1.16 Performance & Caching Standards

### Parallel Execution Infrastructure
- **Joblib integration**: Use `src/utils/parallel_utils.py` for parallel execution with joblib
- **Backend support**: Default to loky (processes), fallback to threading if processes unavailable
- **Worker optimization**: Automatic worker count calculation based on CPU and memory constraints
- **Resource monitoring**: Use `src/utils/resource_monitor.py` for system resource tracking
- **Deterministic outputs**: Ensure identical results regardless of parallelization

### Parallel Execution Targets
- **Candidate generation**: Parallelize by blocking key with deterministic chunking
- **Similarity scoring**: Parallelize pair scoring in batches with configurable chunk sizes
- **Grouping**: Keep serial (Union-Find operations are inherently sequential)
- **Other stages**: Evaluate parallelization potential based on embarrassingly parallel operations

### Resource Guardrails
- **Memory cap**: Limit total memory usage to 75% of available RAM across all workers
- **Worker count formula**: `min(os.cpu_count(), max(1, os.cpu_count()-2))` with memory-based reduction
- **Small input guard**: Auto-switch to sequential for datasets < 10k records (configurable)
- **Disk space monitoring**: Warn if free space < 20% and respect `--keep-runs` pruning
- **Graceful fallback**: Automatic fallback to sequential execution on parallel failures

### Versioned Run Caching
- **Run ID format**: `{input_hash[:8]}_{config_hash[:8]}_{YYYYMMDDHHMMSS}`
- **Cache directories**: `data/interim/{run_id}/` and `data/processed/{run_id}/`
- **Run index**: `data/run_index.json` with metadata and status tracking
- **Latest pointer**: Symlink `data/processed/latest` and JSON backup `data/processed/latest.json`
- **Pruning policy**: Keep last N completed runs (default: 10) with `--keep-runs` override

### CLI Flags & Configuration
- **`--workers N`**: Number of parallel workers (None for auto-detection)
- **`--no-parallel`**: Force sequential execution
- **`--chunk-size N`**: Batch size for parallel processing (default: 1000)
- **`--parallel-backend {loky,threading}`**: Backend choice (default: loky)
- **`--run-id STR`**: Custom run ID (auto-generated if not specified)
- **`--keep-runs N`**: Number of completed runs to keep (default: 10)
- **Config precedence**: CLI flags take precedence over `config/settings.yaml` values

### macOS Compatibility
- **Spawn method**: Use spawn method for multiprocessing (required for macOS)
- **Process isolation**: Ensure all imports and dependencies available in worker processes
- **Fallback handling**: Graceful fallback to threading if process creation fails
- **Resource monitoring**: psutil integration with graceful fallback when not available

### Determinism Requirements
- **Bit-for-bit identical**: Outputs must be identical regardless of worker count or backend
- **Canonical sorting**: Sort all parallel outputs by (id_a, id_b, score) before writing
- **No random sources**: Fix any random seeds and document deterministic behavior
- **Test validation**: Add tests comparing `--workers 1` vs `--workers N` outputs

### Cache Management
- **Atomic operations**: Use temporary files and atomic replacement for cache updates
- **Run isolation**: Complete isolation between runs to prevent cross-contamination
- **Status tracking**: Track run status (running, complete, failed) in run index
- **Cleanup policies**: Automatic cleanup of failed runs and old completed runs
- **Latest pointer**: Maintain stable latest pointer for UI and downstream tools

### Performance Optimization
- **Memory efficiency**: Automatic worker count reduction based on available memory
- **Chunk size tuning**: Configurable chunk sizes for optimal memory usage
- **Progress tracking**: Maintain progress logging in parallel execution
- **Error isolation**: Worker failures don't affect other workers
- **Resource monitoring**: Real-time monitoring of CPU, memory, and disk usage

### Testing Requirements
- **Determinism tests**: Verify identical outputs across different parallel configurations
- **Resource monitoring tests**: Test memory estimation and worker count optimization
- **Cache management tests**: Test run ID generation, pruning, and latest pointer handling
- **Error handling tests**: Test parallel execution failures and fallbacks
- **macOS compatibility tests**: Test spawn method and process isolation

### Documentation Standards
- **README updates**: Document all new CLI flags and cache directory structure
- **CHANGELOG updates**: Comprehensive documentation of performance improvements
- **Usage examples**: Provide clear examples for parallel execution and run management
- **macOS caveats**: Document multiprocessing limitations and fallback behavior

---

## Phase 1.18 Streamlit Fragment & Backend Rules

### Fragment API (Streamlit ≥ 1.29)
- **Prefer `st.fragment`**. Do not mix `st.fragment` and `st.experimental_fragment` in the same codebase.
- Provide a unified decorator alias via `src/utils/fragment_utils.py`:
  - Export `fragment` that internally resolves to `st.fragment` (or `st.experimental_fragment` if running on older Streamlit).
  - Log once at app start: `Using fragment API: st.fragment | streamlit=<version>`.
- Wrap any potentially-slow UI sections in a fragment to avoid blocking the full page (e.g., group list, group details).

### UI Backend Routing (DuckDB-first for large runs)
- When `ui.use_duckdb_for_groups: true`, **route list fetches to DuckDB immediately**. **Do not** do any PyArrow work first.
- Persist backend choice per run via **namespaced session state**:  
  `st.session_state['cj.backend.groups'][run_id] = 'duckdb'`
- Include `backend` in all **list-level cache keys** to prevent PyArrow/DuckDB key collisions.
- Emit a pre-query log line:  
  `Using DuckDB backend for groups | run_id=<RID> reason=flag_true`.

### Per-Group Details (performance-safe)
- Details loaders **must query a single group only**: `WHERE group_id = ?`.
- **Strict projection**: only columns visible in the details table. Exclude heavy JSON/blob columns unless rendered.
- Wrap each group's details body in a **fragment** so only that expander shows a spinner.
- **Do not call `st.rerun()`** in the details path; drive with session flags (see "Session State Namespacing").
- Add timing logs for details loads: `details_query_exec`, `to_pandas`, `elapsed`, and a one-liner:  
  `Group details loaded | run_id=<RID> group_id=<GID> rows=<n> elapsed=<sec>`.

### Session State Namespacing (UI)
- Use `cj.*` namespaced keys exclusively (no legacy raw keys):
  - `cj.page.number`, `cj.page.size`
  - `cj.backend.groups[run_id]`
  - `cj.details.requested[(run_id, group_id)]`, `cj.details.loaded[(run_id, group_id)]`
  - `cj.details.data[(run_id, group_id)]`, `cj.explain.data[(run_id, group_id)]`, `cj.aliases.data[(run_id, group_id)]`
  - `cj.filters.signature`
  - `cj.cache.clear_requested_for_run_id`
- If legacy keys exist, add a one-time **migration shim** that reads old keys and writes to the new namespaced equivalents.

### Cache Hygiene (UI)
- Provide a **"Clear caches for current run"** button that clears **list-level** caches only; do not clear per-group details unless `run_id` changes.
- Cache keys **must include**:  
  `(run_id, parquet_fingerprint, sort_key, page, page_size, filters_signature, backend)` for list-level;  
  `(run_id, group_id, parquet_fingerprint, backend)` for per-group details.

### Legacy Backups & QA Gates
- Keep legacy snapshots under `deprecated/` and **exclude** them from QA gates (ruff, mypy, pytest). Configure local ignores in tool configs if needed.
- Never modify files under `deprecated/**` except to add new backups.

### Documentation Consistency
- For every Phase, update **README.md**, **CHANGELOG.md**, and **cursor_rules.md** together.
- **Changelog dates must match Git creation dates** of the corresponding `prompts/Cursor_Prompt_Phase*.md` files (use an audit script or `git log --diff-filter=A`).
- Add a short **Design Note** in PRs summarizing files touched, cache-key changes, and fragment usage.

---

## Phase 1.18.3 Fragment API & Backend Compliance

### Fragment API Unification (Mandatory)
- **Use `src/utils/fragment_utils.py`** for all fragment decorators. Do not import `st.fragment` or `st.experimental_fragment` directly.
- **Version detection**: Automatically choose `st.fragment` (≥ 1.29) or `st.experimental_fragment` (< 1.29).
- **Log once at app start**: `Using fragment API: st.fragment | streamlit=<version>` or `st.experimental_fragment` accordingly.
- **No mixing**: Do not use both APIs anywhere in the codebase.

### Session State Namespace Compliance
- **Use only `cj.backend.groups[run_id]`** (not `groups_backend`).
- **Migration shim**: Add one-time migration that maps legacy keys to namespaced ones, then deletes legacy keys.
- **Clean legacy keys**: Remove all legacy session state keys after migration.

### DuckDB-First Routing (Performance)
- **Route immediately**: When `ui.use_duckdb_for_groups: true`, route to DuckDB before any PyArrow work.
- **Persist choice**: Store backend choice in `st.session_state['cj.backend.groups'][run_id] = "duckdb"`.
- **Log backend selection**: `Using DuckDB backend for groups | run_id=<RID> reason=flag_true`.
- **Cache key inclusion**: Ensure every list-level cache key includes backend to avoid cross-backend collisions.

### Per-Group Details Optimization
- **Strict queries**: Use `WHERE group_id = ?` with projection limited to visible columns only.
- **No heavy fields**: Exclude JSON/blob fields unless they are actually rendered.
- **Fragment wrapping**: Wrap details body in individual `@fragment` decorators.
- **No st.rerun()**: Use session flags like `cj.details.requested[(run_id, group_id)] = True`.
- **Timing logs**: Include `details_query_exec`, `to_pandas`, `elapsed`, and summary:  
  `Group details loaded | run_id=<RID> group_id=<GID> rows=<n> elapsed=<sec>`.

### Cache Key Schema Compliance
- **List-level keys**: Must include `(run_id, parquet_fingerprint, sort_key, page, page_size, filters_signature, backend)`.
- **Details keys**: Must include `(run_id, group_id, parquet_fingerprint, backend)`.
- **Backend inclusion**: All cache keys must include backend parameter to prevent collisions.

### Legacy File Exclusions
- **Exclude `deprecated/**`** from all QA gates (ruff, mypy, pytest).
- **Configuration updates**: Add to `mypy.ini`, `pytest.ini`, and `pyproject.toml` (ruff).
- **No modifications**: Do not edit files under `deprecated/**` except to add new backups.

### Testing Requirements
- **Fragment utility tests**: `tests/test_fragment_utils.py` with availability and decorator smoke tests.
- **Backend routing tests**: Verify flag true → no PyArrow path invoked.
- **Details projection tests**: Assert only visible columns selected, no blob fields.
- **Cache key validation**: Ensure keys include backend parameter.
- **Import tests**: Update `tests/test_imports.py` to include new utils/components.

---

## Phase 1 Rules (Aliases) - Phase 1.21.1+

### Alias Matching Requirements
- **Equivalence guarantee**: Optimized path must produce identical core data to legacy path
- **Determinism**: Consistent outputs across multiple runs with same inputs
- **Validation scripts**: Use `scripts/check_alias_results.py` for equivalence and determinism verification
- **First-token bucketing**: Deterministic ordering for consistent blocking behavior
- **Forbidden changes**: No behavior modifications that break equivalence between optimized and legacy paths

### Alias Optimization Configuration
- **Default state**: `alias.optimize: true` enables parallel processing
- **Worker activation**: Requires `workers > 1`; single worker falls back to sequential
- **Progress logging**: Rate-limited progress updates every `alias.progress_interval_s` seconds
- **Memory guardrails**: `max_alias_pairs` limit and automatic BLAS thread clamping
- **Environment variables**: OMP_NUM_THREADS=1, OPENBLAS_NUM_THREADS=1, VECLIB_MAXIMUM_THREADS=1, NUMEXPR_NUM_THREADS=1

### Validation & Testing
- **Equivalence testing**: Run both paths and compare outputs using validation scripts
- **Determinism testing**: Multiple runs of optimized path must produce identical results
- **Performance benchmarking**: Use `scripts/bench_alias.py` for wall-clock measurements
- **Bucket analysis**: Use `scripts/check_alias_buckets.py` for first-token distribution analysis
- **Test coverage**: All new functionality must include comprehensive tests

### Safety & Guardrails
- **Memory monitoring**: Track memory usage during parallel processing
- **Progress tracking**: Rate-limited logging to prevent I/O overhead
- **Fallback behavior**: Graceful degradation when parallel resources unavailable
- **Error handling**: Comprehensive error handling with clear failure modes

---

## UI Backend Routing (Groups) - Phase 1.22.1+

### Duplicate Groups MVP Backend Selection
- **Primary Path**: `group_stats.parquet` is the first source for group list queries when available
  - **Artifact Path**: `data/processed/<run_id>/group_stats.parquet`
  - **Schema**: `group_id`, `group_size`, `max_score`, `primary_name`, `Disposition`
  - **Ultra-fast loading**: Direct DuckDB queries on pre-computed stats (≤2s cold load)
- **DuckDB-first routing**: When `ui_perf.groups.duckdb_prefer_over_pyarrow: true`
  - **Threshold detection**: Auto-route to DuckDB when `rows > rows_duckdb_threshold` or `groups > groups_duckdb_threshold`
  - **Smart fallback**: PyArrow for smaller datasets or when DuckDB unavailable
- **Backend persistence**: Store choice in `st.session_state['cj.backend.groups'][run_id]`
- **Cache hygiene**: Include `backend` in all cache keys to prevent cross-backend collisions

### Configuration Integration
- **Config section**: `ui_perf.groups.*` settings in `config/settings.yaml`
  - `use_stats_parquet: true` - Enable fast path when stats available
  - `duckdb_prefer_over_pyarrow: true` - Enable threshold-based routing
  - `rows_duckdb_threshold: 30000` - Row count threshold for DuckDB routing
  - `groups_duckdb_threshold: 10000` - Group count threshold for DuckDB routing
- **Rollback support**: `use_stats_parquet: false` disables fast path entirely

### Observability & Logging
- **Backend selection logs**: Required structured logs for groups performance tracking
  - `groups_perf: backend=<backend> reason=<reason> cold_load_s=<time> groups=<count> used_stats_parquet=<bool>`
- **UI indicators**: Show "⚡ Fast stats mode" when using pre-computed stats
- **Performance monitoring**: Track cold load times and navigation performance

### Artifacts & Caching Policy
- **Run-scoped artifacts**: Maintain `data/processed/<run_id>/group_stats.parquet` per run
- **No disk-persisted caches**: In-memory LRU only for page navigation
- **Cache key schema**: Include `(run_id, backend, filters_signature, sort_key, page, page_size)`
- **Automatic generation**: Pipeline generates stats during finalization (post-survivorship)

### Documentation Discipline
- **Consistency requirement**: For every CLI flag or config change, update README.md, CHANGELOG.md, and cursor_rules.md together
- **Module path accuracy**: Ensure references to modules/paths match reality (e.g., `src/alias_matching.py`, `scripts/check_alias_results.py`, `app/main.py`)
- **Streamlit/fragment consistency**: Maintain consistency with DuckDB-first approach for groups backend routing