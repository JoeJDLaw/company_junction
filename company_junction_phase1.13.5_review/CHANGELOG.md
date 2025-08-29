# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.5.0] - 2025-08-27

### Added
- **Performance logging infrastructure**: `log_perf` context manager with timing and memory tracking
- **Enhanced token filtering**: Improved problematic pattern detection with case-insensitive matching
- **Stop token logic**: Smart blocking strategy to avoid common suffixes (inc, llc, ltd) as blocking keys
- **Block visibility**: Top-10 token distribution logging and block statistics file generation
- **Performance summary**: `perf_summary.json` with key pipeline metrics and disposition statistics
- **Memory safety**: Better filtering reduces memory usage and prevents exhaustion

### Changed
- Performance logging integrated throughout all major pipeline stages
- Enhanced filtering patterns account for normalization changes (e.g., "n/a" → "n a")
- Blocking strategy improved with stop tokens and fallback logic
- Pipeline generates comprehensive performance metrics and audit information

### Technical
- New `log_perf` context manager in `src/utils.py` with `tracemalloc` integration
- Enhanced filtering logic in `src/cleaning.py` with better regex patterns
- Stop token implementation in `src/similarity.py` for improved blocking efficiency
- Block statistics written to `data/interim/block_top_tokens.csv`
- Performance summary generation with global cap detection

## [1.4.0] - 2025-08-27

### Added
- **Safer blacklist matching**: Word-boundary regex for single-word tokens, substring matching for phrases
- **Centralized manual I/O**: Single `src/manual_io.py` module for all manual file operations
- **Audit snapshots**: Run metadata with thresholds, counts, and git commit tracking
- **Pipeline command generator**: Copy-to-clipboard functionality for easy pipeline execution
- **Atomic file writes**: Prevents corruption during manual file updates

### Changed
- Blacklist matching now uses compiled regex patterns for better performance
- Manual file operations consolidated with robust error handling
- Manual overrides use centralized I/O functions with atomic writes
- Audit information written to `data/processed/review_meta.json` on each run

### Technical
- New `src/manual_io.py` module with atomic write operations
- Enhanced blacklist regex compilation with word boundaries
- Improved manual file loading with backward compatibility
- Audit snapshot generation with git commit tracking

## [1.3.0] - 2025-08-27

### Added
- **Blacklist transparency**: Three-pane view showing built-in, manual, and effective blacklist terms
- **Filter tooltips**: Clear explanations for "Show Suffix Mismatches Only" and "Has Aliases" filters
- **Account Name sorting**: New sorting options for groups by primary record's Account Name (ascending/descending)
- **Robust filtering**: Improved alias filter with fallback to `alias_candidates` column

### Changed
- Blacklist management consolidated into single expander with clear sections
- Filter functionality enhanced with better null-safe checks
- Sorting logic updated to include primary record's Account Name for group sorting

### Technical
- New `get_blacklist_terms()` helper function in `src/disposition.py`
- Enhanced filter logic with fallback column support
- Improved group statistics calculation for sorting

## [1.2.0] - 2025-08-27

### Added
- **Review UX improvements**: Disposition table replaces bar chart, group-level sorting controls
- **Manual disposition overrides**: Group-level dropdown for Keep/Delete/Update/Verify with JSON persistence
- **Manual blacklist editor**: Add/remove pattern-based rules for automatic Delete classification
- **Audit trail**: Timestamps and export functionality for manual changes
- **Better group layout**: Group info moved to top with badges, improved table readability

### Changed
- Streamlit UI layout reorganized for better workflow efficiency
- Account Name column now wraps fully for better readability
- Sorting options for groups (by size, score) and records (by name)
- Manual data stored in JSON format for better structure and validation

### Technical
- New `app/manual_data.py` module for manual override and blacklist management
- Enhanced `src/disposition.py` with optional manual file loading and override application
- Manual data directory `data/manual/` with git-ignored JSON files
- Unit tests for manual override and blacklist functionality
- Pipeline gracefully handles missing or malformed manual files

## [1.1.0] - 2025-08-27

### Added
- **Conservative alias extraction** (semicolon, numbered sequences; parentheses only when content contains a legal suffix or multiple capitalized words)
- **Alias matching** with high-confidence gating (suffix match + score ≥ high), cross-links only; writes `alias_matches.parquet` and minimal metadata to `review_ready.csv`
- **Minimal UI** support: alias badge/expander and "Has aliases" filter in Streamlit
- **Performance safeguards** for alias comparisons (config cap)

### Changed
- Parentheses handling is **preserved and flagged** by default (no blanket alias creation)
- Punctuation normalization remains conservative (no global comma/period stripping)

## [1.0.0] - 2025-08-27

### Added
- Legal-aware normalization (`src/normalize.py`)
- Similarity scoring (`src/similarity.py`)
- Grouping & survivorship (`src/grouping.py`, `src/survivorship.py`)
- Disposition logic (`src/disposition.py`)
- CLI orchestrator (`src/cleaning.py`)
- Streamlit review UI (`app/main.py`)
- Config updates (`config/settings.yaml`, `config/relationship_ranks.csv`)
- Unit tests across modules

### Changed
- README.md and cursor_rules.md updated to reflect Phase 1 rules

## [1.12.0] - 2024-12-19

### Changed
- **Utils package structure**: Refactored `src/utils.py` into logical modules under `src/utils/`
- **Import paths**: Updated all imports to use absolute paths rooted at `src`
- **Module organization**: 
  - `src/utils/logging_utils.py` - `setup_logging`
  - `src/utils/path_utils.py` - `get_project_root`, `ensure_directory_exists`, `get_data_paths`
  - `src/utils/validation_utils.py` - `validate_dataframe`
  - `src/utils/io_utils.py` - `get_file_info`, `list_data_files`, `load_settings`, `load_relationship_ranks`
  - `src/utils/perf_utils.py` - `log_perf`
  - `src/utils/hash_utils.py` - `config_hash`, `stable_group_id`, `_compute_config_hash`
  - `src/utils/dtypes.py` - Memory optimization utilities (existing)
- **Hash utilities**: Moved `config_hash` and `stable_group_id` from `src/grouping.py` to `src/utils/hash_utils.py`
- **Performance utilities**: Moved `_compute_config_hash` from `src/performance.py` to `src/utils/hash_utils.py`

### Removed
- **src/utils.py**: Deleted after successful refactor and import updates

### Technical Details
- **No backward compatibility**: All imports updated directly, no shims created
- **Import clarity**: Absolute imports eliminate ambiguity between `src/utils.py` and `src/utils/` package
- **Function preservation**: All functions maintain identical behavior and API
- **Test coverage**: All 81 tests pass after refactor

## [Phase1.13.5] - 2025-08-28

### Added
- **MyPy Error Reduction** - Targeted, no-churn typing improvements
  - Systematic addition of return type annotations (`-> None`) to all test functions
  - Enhanced type annotations for utility functions and app modules
  - Improved function signatures with proper parameter and return types

### Changed
- **Test functions**: All test methods now have explicit `-> None` return type annotations
- **Utils functions**: Added precise return types and parameter annotations
- **App functions**: Enhanced type safety with proper return type declarations
- **Error reduction**: Reduced MyPy errors from 186 → 70 (62% reduction)

### Fixed
- **Missing return types**: Added `-> None` to 179 test functions across 9 test files
- **Utils typing**: Fixed `load_settings`, `deep_merge`, and other utility function signatures
- **App typing**: Enhanced type safety in `manual_data.py` and `main.py` functions
- **Import test functions**: Added proper type annotations to import validation tests

## [Phase1.13.4] - 2025-08-28

### Added
- **MyPy Module-Path Conflict Resolution** - Zero-errors finalization
  - Eliminated all module path conflicts (e.g., `dtypes_map` vs `src.dtypes_map`)
  - Removed `mypy_path` configuration to prevent dual module identities
  - Enforced single canonical package root (`src`) via targeted MyPy invocation
  - All bare imports standardized to absolute `src.` paths

### Changed
- **MyPy configuration**: Removed `mypy_path` and enforced explicit package bases
- **Import standardization**: All remaining bare imports fixed across src/ and tests/
- **MyPy invocation**: Changed to `mypy --config-file mypy.ini src tests app` for targeted checking
- **Error reduction**: Reduced MyPy errors from 216 → 188 (28 fewer errors)

### Fixed
- **Module path conflicts**: Resolved all `dtypes_map` vs `src.dtypes_map` dual identity issues
- **Import consistency**: All first-party imports now use canonical `src.` package paths
- **Test imports**: All test files updated to use absolute imports

## [Phase1.13.3] - 2025-08-28

### Added
- **Type & Import Hygiene** - MyPy path fix and import test normalization
  - MyPy configuration updated to enforce single canonical package root (`src`)
  - `tests/test_imports.py` rewritten as pure-pytest with canonical import validation
  - All first-party imports now use absolute `src.` paths
  - Enhanced type checking with stricter configuration

### Changed
- **Import standardization**: All bare imports (e.g., `from normalize import`) replaced with absolute imports (`from src.normalize import`)
- **MyPy configuration**: Simplified to focus on `src` as single package root with explicit package bases
- **Test structure**: Import tests now validate production-like import paths via `src.` package

## [Phase1.13.2] - 2025-08-28

### Added
- **Column Naming & ID Standards** - Comprehensive rules for canonical column naming and Salesforce ID handling
  - Canonical column naming policy (inputs vs internal)
  - Salesforce ID canonicalization policy (18-char canonical IDs)
  - Utils layout & import rules (absolute imports required)
  - QA gates enforcement (Black/Ruff/MyPy/PyTest)
  - Logging, performance, and artifacts standards
  - Edit hygiene rules for maintaining consistency
- **Updated `cursor_rules.md`** with Phase 1.13 standards to prevent future churn and ensure consistency

## [Unreleased]

### Added
- Initial project scaffolding with Cookiecutter Data Science structure
- Basic data cleaning pipeline with duplicate detection
- Streamlit GUI for interactive data processing
- Salesforce CLI integration framework
- Utility functions for file management and validation
- Configuration system with `config/settings.yaml` and `config/logging.conf`
- Test fixtures with sample data in `tests/fixtures/`
- Enhanced documentation with prerequisites, usage examples, and contributing guidelines
- Tightened development guardrails in `cursor_rules.md`

### Changed

### Deprecated

### Removed

### Fixed

### Security
