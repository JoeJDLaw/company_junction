# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Phase1.17.2] - 2025-01-27

### Added
- **CLI Command Builder**: Interactive builder for pipeline commands with real-time validation
  - Input file selection from `data/raw/` directory with dropdown
  - Config file selection from `config/` directory with default to `settings.yaml`
  - Parallelism controls (workers, backend, chunk size, no-parallel)
  - Run control options (no-resume, keep-runs, custom run ID)
  - Real-time validation preventing invalid flag combinations
  - Command export (copy to clipboard or download as shell script)
- **Run Maintenance**: Safe deletion of pipeline runs with comprehensive safeguards
  - Destructive actions fuse requiring explicit enablement
  - Preview mode showing exactly what will be deleted
  - Two-step confirmation (checkbox + typed confirmation)
  - In-flight protection preventing deletion of running runs
  - Latest pointer management with automatic recomputation
  - Audit logging to `data/run_deletions.log`
- **Quick Actions**: One-click helpers for common maintenance tasks
  - "Delete all except latest" for cleaning old completed runs
  - "Delete all runs" for complete cleanup (use with caution)
- **Filters Polish**: UI cleanup removing duplicate "Minimum Edge to Primary" slider

### Changed
- **UI Safety**: Enhanced safety measures for destructive operations
  - Session-level destructive actions fuse requiring checkbox enablement
  - Preview before deletion showing file counts and byte sizes
  - Typed confirmation requiring exact run ID or "DELETE ALL"
  - Clear warnings about latest pointer effects
- **Latest Pointer Management**: Improved atomic updates and reliability
  - JSON pointer prioritized over symlink for better reliability
  - Automatic recomputation when latest run is deleted
  - Graceful handling of symlink creation failures
  - Atomic updates using temporary files and rename operations

### Technical Details
- **CLI Builder**: Pure functions in `src/utils/cli_builder.py` for command generation
  - `get_available_input_files()`: Discover CSV files in data/raw/
  - `get_available_config_files()`: Discover YAML files in config/
  - `validate_cli_args()`: Comprehensive validation with error messages
  - `build_cli_command()`: Generate complete CLI command string
  - `get_known_run_ids()`: Retrieve existing run IDs for custom run ID selection
- **Run Maintenance**: Safe deletion utilities in `src/utils/cache_utils.py`
  - `preview_delete_runs()`: Calculate deletion impact without performing deletion
  - `delete_runs()`: Perform safe deletion with latest pointer management
  - `recompute_latest_pointer()`: Atomic latest pointer recomputation
  - `log_deletion_audit()`: Comprehensive audit logging
- **Validation**: Real-time validation preventing invalid configurations
  - File existence checking for input and config files
  - Parallelism flag validation (no workers > 1 with --no-parallel)
  - Run ID validation preventing empty or invalid IDs
  - Clear error messages for each validation failure

### Testing
- **CLI Builder Tests**: 24 comprehensive tests for command generation and validation
  - Test file discovery for input and config files
  - Test validation logic for all CLI argument combinations
  - Test command building with various flag combinations
  - Test error handling and edge cases
- **Run Maintenance Tests**: 8 comprehensive tests for deletion functionality
  - Test preview functionality showing deletion impact
  - Test actual deletion with latest pointer management
  - Test latest pointer recomputation and atomic updates
  - Test audit logging and error handling

### Safety & Validation
- **Destructive Actions Fuse**: Session-level protection requiring explicit enablement
- **Preview Mode**: See exactly what will be deleted before confirming
- **Two-step Confirmation**: Checkbox + typed confirmation for all deletions
- **In-flight Protection**: Cannot delete runs with "running" status
- **Audit Logging**: Complete audit trail of all deletion operations
- **Atomic Operations**: Latest pointer updates use temporary files and atomic rename

## [Phase1.17.1] - 2025-01-27

### Added
- **Run Picker**: Select any pipeline run from sidebar with run-scoped artifact loading
  - Dropdown selection of all available runs sorted by timestamp (newest first)
  - Clear "Latest" indicator for the most recent successful run
  - Status icons (✅/⏳/❌) for run status visualization
  - Automatic run metadata display (input file, config, timestamp, status)
- **Stage Status (MiniDAG Lite)**: View pipeline execution status and timing for each stage
  - Compact table showing stage name, status, and duration
  - Status icons for visual indication (✅/⏳/❌/⏸️)
  - Stage timing information with formatted duration display
  - Graceful handling of missing stage information
- **Artifact Downloads**: Download review files, metadata, and intermediate artifacts
  - Review ready files (CSV and Parquet formats)
  - Review metadata (JSON format)
  - Run-scoped file naming with run_id in filename
- **Session State Caching**: Efficient data loading with automatic cache invalidation
  - Cache loaded data per run_id to avoid unnecessary reloading
  - Automatic cache clearing when switching between runs
  - Performance optimization for large datasets
- **Pure Helper Functions**: All UI logic moved to testable `src/utils/ui_helpers.py`
  - 15 comprehensive unit tests for all helper functions
  - No Streamlit dependencies in helper functions
  - Clean separation of concerns between UI and business logic

### Changed
- **UI Architecture**: Complete refactor to eliminate duplication and improve maintainability
  - Removed all global path fallbacks (run-scoped only)
  - Centralized run loading logic in pure helper functions
  - Enhanced error handling with clear user guidance
  - Improved session state management for better performance
- **Error Handling**: Explicit messages for missing runs, failed runs, or incomplete artifacts
  - Clear error messages when run artifacts are missing
  - Guidance for failed runs and incomplete pipeline execution
  - No silent fallbacks to legacy paths
- **Data Loading**: Run-scoped artifact loading with validation
  - Automatic validation of run artifacts before loading
  - Support for both CSV and Parquet formats with proper fallback
  - Enhanced error messages for missing or corrupted files

### Technical Details
- **Helper Functions**: Pure functions in `src/utils/ui_helpers.py` for all run management
  - `list_runs()`: Get sorted list of all runs with metadata
  - `get_run_metadata()`: Retrieve detailed run information
  - `validate_run_artifacts()`: Check run completeness and file existence
  - `load_stage_state()`: Parse MiniDAG state for stage status display
  - `get_artifact_paths()`: Generate run-scoped artifact paths
- **Session State**: Efficient caching with automatic invalidation
  - Cache key: `selected_run_id` for current run selection
  - Cache key: `cached_data` for loaded DataFrame per run
  - Cache key: `cached_run_id` for tracking cache validity
- **Error Handling**: Comprehensive validation and user guidance
  - Run existence validation via run index
  - Artifact completeness checking
  - Clear error messages with actionable guidance

### Testing
- **Unit Tests**: 15 comprehensive tests for all UI helper functions
  - Test run listing and metadata retrieval
  - Test artifact validation and path generation
  - Test stage state loading and parsing
  - Test run display name formatting and status icons
  - Mock-based testing without Streamlit dependencies

### Safety & Validation
- **Run-Scoped Only**: Complete elimination of global path fallbacks
- **Pure Functions**: All helper functions are pure and easily testable
- **Type Safety**: Comprehensive type annotations with MyPy validation
- **Error Isolation**: Clear error boundaries with graceful degradation

## [Phase1.16] - 2025-01-27

### Added
- **Parallel execution support**: Joblib-based parallel processing for candidate generation and similarity scoring
  - `ParallelExecutor` class with automatic worker count optimization
  - Support for loky (processes) and threading backends with automatic fallback
  - Resource monitoring with memory and CPU usage tracking
  - Deterministic output ordering regardless of parallelization
- **Versioned run caching**: Complete run isolation with per-run_id directories
  - Run ID format: `{input_hash[:8]}_{config_hash[:8]}_{YYYYMMDDHHMMSS}`
  - Cache directories: `data/interim/{run_id}/` and `data/processed/{run_id}/`
  - Run index management with `data/run_index.json`
  - Latest pointer via symlink and JSON backup
  - Automatic pruning with `--keep-runs N` (default: 10)
- **Resource monitoring**: Comprehensive system resource tracking
  - CPU and memory usage monitoring with psutil integration
  - Automatic worker count optimization based on available resources
  - Memory cap enforcement (75% of total RAM by default)
  - Disk space monitoring with warning thresholds
- **New CLI flags**: Complete control over parallel execution and caching
  - `--workers N`: Number of parallel workers (auto-detection if not specified)
  - `--no-parallel`: Force sequential execution
  - `--chunk-size N`: Batch size for parallel processing (default: 1000)
  - `--parallel-backend {loky,threading}`: Backend choice (default: loky)
  - `--run-id STR`: Custom run ID (auto-generated if not specified)
  - `--keep-runs N`: Number of completed runs to keep (default: 10)

### Changed
- **Pipeline architecture**: Run_id-based execution is now mandatory (no backward compatibility)
  - All outputs stored under run-specific directories
  - Legacy paths no longer supported
  - MiniDAG enhanced with run_id support
- **Similarity scoring**: Parallel candidate generation and similarity scoring
  - Deterministic chunking and ordering
  - Automatic fallback to sequential for small inputs (< 10k records)
  - Resource-aware worker allocation
- **Performance optimization**: Significant speedup for large datasets
  - Parallel candidate pair generation by blocking key
  - Parallel similarity scoring with configurable chunk sizes
  - Memory-efficient processing with automatic resource management
- **Streamlit integration**: Updated to handle run_id-based outputs
  - Automatic detection of latest run via symlink/JSON pointer
  - Fallback to legacy paths for backward compatibility
  - Enhanced error handling and user feedback

### Technical Details
- **Parallel execution**: Joblib integration with loky backend (processes) and threading fallback
- **Resource management**: psutil-based monitoring with graceful fallback when not available
- **Determinism**: Canonical sorting by (id_a, id_b, score) ensures identical outputs
- **Cache management**: Atomic operations for run index and latest pointer updates
- **Error handling**: Graceful fallback from parallel to sequential execution
- **macOS compatibility**: Spawn method support for multiprocessing

### Testing
- **Comprehensive test coverage**: 30+ new tests for parallel execution and caching
- **Determinism validation**: Tests comparing `--workers 1` vs `--workers N` outputs
- **Resource monitoring**: Tests for memory estimation and worker count optimization
- **Cache management**: Tests for run ID generation, pruning, and latest pointer handling
- **Error handling**: Tests for parallel execution failures and fallbacks

### Safety & Validation
- **Bit-for-bit determinism**: Identical outputs regardless of parallelization
- **Resource protection**: Automatic worker count reduction based on available memory
- **Cache isolation**: Complete run isolation prevents cross-contamination
- **Graceful degradation**: Automatic fallback to sequential execution on errors
- **macOS compatibility**: Proper spawn method handling for multiprocessing

### Performance Impact
- **Significant speedup**: 2-4x improvement for large datasets with parallel execution
- **Memory efficiency**: Automatic resource optimization prevents memory exhaustion
- **Scalability**: Linear scaling with available CPU cores (up to memory limits)
- **Small input optimization**: Automatic sequential execution for datasets < 10k records

## [Phase1.15.3] - 2025-08-29

### Added
- **Performance profiling utilities**: Memory tracking, stage timing, regression detection in `src/utils/perf_utils.py`
- **Memory tracking**: Peak memory detection and usage monitoring with `track_memory_peak()` context manager
- **Stage timing**: Automatic timing with memory delta tracking via `time_stage()` context manager
- **Performance regression detection**: Baseline comparison with configurable thresholds
- **Pipeline integration**: Automatic performance tracking in all major stages (candidate generation, grouping, survivorship, disposition)
- **Performance summary logging**: Final memory usage and stage-by-stage performance metrics

### Changed
- **Pipeline performance monitoring**: Enhanced logging with memory and timing data for heavy operations
- **Streamlit UX**: Improved error handling and loading indicators with better user feedback
- **Documentation**: Added comprehensive performance profiling guidelines and usage examples

### Technical Details
- **Memory profiling**: RSS and VMS tracking with psutil fallback for environments without psutil
- **Context managers**: `time_stage()` and `track_memory_peak()` for automatic performance monitoring
- **Regression detection**: Configurable thresholds (default 10%) for performance comparison
- **Baseline management**: Save/load performance baselines for regression testing
- **Exception handling**: Graceful fallback when psutil is not available

### Testing
- **Comprehensive test coverage**: 15 new tests for all performance utilities
- **Edge case testing**: Invalid JSON, missing files, exception handling
- **Memory tracking tests**: Peak detection and usage monitoring validation
- **Regression detection tests**: Baseline comparison and threshold validation

### Safety & Validation
- **Additive changes only**: No business logic modifications, pure observability improvements
- **Artifact invariance**: All changes preserve bit-for-bit output consistency
- **Backward compatibility**: Optional psutil dependency with graceful fallback
- **Zero performance impact**: Profiling overhead is minimal and configurable

## [Phase1.15.2] - 2025-08-29

### Added
- **MiniDAG versioning**: Added `dag_version` field for future compatibility and backward compatibility
- **Enhanced error tolerance**: Robust handling of corrupted/partial `pipeline_state.json` files
- **Explicit reason codes**: Enhanced resume decision logging with detailed reason codes
- **Comprehensive test coverage**: Tests for corrupted state files, flag interactions, and invariance

### Changed
- **Resume decision logging**: Added explicit reason codes (SMART_DETECT, HASH_MISMATCH, NO_PREVIOUS_RUN, etc.)
- **State file robustness**: Graceful handling of JSON corruption with automatic reset to clean state
- **CLI flag documentation**: Clarified flag interactions and precedence rules

### Technical Details
- **Version defaulting**: Missing `dag_version` defaults to "1.0.0" with logging
- **Error tolerance**: Individual stage loading failures don't crash the entire DAG
- **Reason codes**: 8 distinct reason codes for resume decisions (SMART_DETECT, HASH_MISMATCH, NO_PREVIOUS_RUN, MANUAL_OVERRIDE, FORCE_OVERRIDE, etc.)
- **Test coverage**: Added tests for edge cases including corrupted state files and flag interactions

### Safety & Validation
- **Backward compatibility**: Existing state files work with version defaulting
- **Artifact invariance**: All changes preserve bit-for-bit output consistency
- **Graceful degradation**: Corrupted state files reset to clean state rather than crashing

## [Phase1.15.1] - 2025-08-29

### Added
- **--state-path CLI argument**: Custom path for pipeline state file (default: `data/interim/pipeline_state.json`)
- **Stage map clarity**: `filtering` stage now writes `accounts_filtered.parquet` instead of reusing `accounts_normalized.parquet`
- **Alias parquet hygiene**: Sanitized parquet output with scalar-only columns and proper dtypes

### Changed
- **Stage file mapping**: Updated MiniDAG to include `accounts_filtered.parquet` in all stages after filtering
- **Alias matches storage**: Only scalar columns (`account_id`, `alias_text`, `matched_account_id`, `match_group_id`, `match_score`, `source`) with enforced dtypes
- **Documentation**: Updated README, CHANGELOG, and cursor_rules to reflect new stage map and CLI options

### Technical Details
- **Parquet sanitization**: Force string dtypes for ID/text columns, float32 for scores
- **Stage separation**: Clear distinction between normalization and filtering outputs
- **CLI parity**: All documented flags now implemented in code

### Testing
- **Invariance tests**: Verify byte-for-byte identity for full run vs resume
- **Hash mismatch guard**: Test input hash validation prevents stale artifact usage
- **Missing artifact fallback**: Test missing `accounts_filtered.parquet` prevents unsafe resume

### Safety & Validation
- **No packaging changes**: Maintains venv-only workflow
- **Artifact invariance**: Preserves bit-for-bit output consistency
- **Backward compatibility**: Existing pipeline behavior unchanged

## [Phase1.14.2] - 2025-08-29

### Added
- **Smart Auto-Resume for MiniDAG**: Intelligent pipeline resumption without explicit flags
  - Auto-detection of last completed stage based on state file and intermediate files
  - Input hash validation to prevent stale artifact usage
  - Automatic resume decision logging with clear reasoning
  - State metadata tracking (input_hash, dag_version, cmdline, timestamp)

### Changed
- **CLI Semantics**: Updated command-line interface for smart auto-resume
  - Default behavior: smart auto-resume with input validation
  - `--no-resume`: Force full pipeline run (ignore state)
  - `--resume-from`: Override auto-detection with specific stage
  - `--force`: Override input hash mismatch protection
  - `--state-path`: Custom path for pipeline state file

### Technical Details
- **MiniDAG Enhancements**: Added smart auto-resume methods
  - `get_last_completed_stage()`: Finds highest completed stage in order
  - `validate_intermediate_files()`: Checks file existence for each stage
  - `get_smart_resume_stage()`: Combines state + file validation logic
  - `_compute_input_hash()`: SHA256 hash of input + config files
  - `_validate_input_invariance()`: Compares current vs stored hash
  - `_update_state_metadata()`: Updates metadata with run information

### Safety & Validation
- **Input Hash Protection**: Prevents resuming with changed inputs unless `--force` specified
- **File Validation**: Ensures intermediate files exist before resuming
- **State Persistence**: Atomic JSON writes with metadata tracking
- **Error Handling**: Clear error messages for hash mismatches and missing files

### Pipeline Stages
- normalization → filtering → candidate_generation → grouping → survivorship → disposition → alias_matching → final_output

### Testing
- **Comprehensive Test Suite**: 8 test cases covering all smart auto-resume functionality
- **Input Hash Validation**: Tests for hash computation and invariance checking
- **File Validation**: Tests for intermediate file existence checking
- **Metadata Storage**: Tests for state persistence and retrieval
- **Auto-Resume Logic**: Tests for intelligent stage detection

## [Phase1.14.1] - 2025-08-29

### Added
- **Progress logging and heartbeats**: Comprehensive progress tracking with rate and ETA information
- **MiniDAG orchestration**: Lightweight DAG orchestrator for pipeline stage tracking and resumability
- **Optional tqdm support**: Progress bars when `--progress` flag is used, graceful fallback to logging
- **Pipeline resumability**: Resume from any completed stage using `--resume-from` flag
- **Stage banners**: Clear `[stage:start]` and `[stage:end]` messages for all major pipeline stages
- **Heavy loop instrumentation**: Progress tracking for computationally intensive operations

### Changed
- **CLI enhancements**: Added `--progress` and `--resume-from` flags for better pipeline control
- **Stage tracking**: All pipeline stages now tracked with completion status and timing
- **Progress visibility**: Enhanced logging with processing rates and estimated completion times
- **Streamlit integration**: Added loading spinner for data loading operations

### Technical Details
- **New utilities**: `src/utils/mini_dag.py` for stage orchestration, `src/utils/progress.py` for progress logging
- **Atomic state management**: Pipeline state saved atomically to `data/interim/pipeline_state.json`
- **Heavy loop instrumentation**: Progress tracking in similarity (block iteration, pair scoring), grouping (Union-Find), survivorship (per-group processing)
- **Backward compatibility**: Pipeline runs exactly as before without new flags
- **QA gates compliance**: All progress logging maintains zero MyPy errors and passes all QA gates

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

## [Phase1.13.7] - 2025-08-29

### Added
- **Zero MyPy Errors Milestone**: Complete type safety across entire codebase (33 source files)
- **Comprehensive type annotations**: All functions, methods, and variables properly typed
- **Enhanced pandas operations**: Fixed DataFrame indexing, boolean masking, and arithmetic operations
- **Import hygiene standardization**: All imports use absolute paths rooted at `src`
- **Strict QA gates enforcement**: Black, Ruff, MyPy, and PyTest all green with zero errors

### Changed
- **MyPy configuration**: Enforced strict type checking with zero tolerance for errors
- **Test function signatures**: All test methods now have explicit `-> None` return type annotations
- **Pandas DataFrame operations**: Replaced tuple indexing with boolean masking for type safety
- **JSON parsing**: Enhanced type safety for `json.load()` and `ast.literal_eval()` operations
- **Salesforce ID handling**: Improved type safety in ID conversion and validation functions

### Fixed
- **DataFrame indexing errors**: Replaced `.loc[idx, "column"]` with boolean masking patterns
- **Union-attr errors**: Added null checks for optional pandas operations
- **Call-overload errors**: Fixed dict constructor calls with proper type annotations
- **No-any-return errors**: Added explicit type casting and validation for return values
- **Unreachable code warnings**: Simplified logic to eliminate unreachable code paths
- **Import path conflicts**: Resolved all module path ambiguity issues

### Technical Details
- **Error reduction**: Achieved 100% MyPy compliance (46 errors → 0 errors)
- **Test coverage**: All 128 tests passing with comprehensive type annotations
- **Runtime preservation**: All improvements maintain identical runtime behavior
- **Code quality**: Enhanced maintainability through strict type safety
- **Future-proofing**: Robust type annotations prevent regressions and improve IDE support

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
- **Hash utilities**: Moved `config_hash` and `stable_group_id` from `src/grouping.py` to `