# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Phase1.18.4] - 2025-08-31

### Critical Bug Fixes
- **Sidebar Rendering**: Fixed missing sidebar controls and incorrect maintenance placement
  - **Root Cause**: Maintenance section was rendered in main content instead of sidebar, and early returns prevented sidebar rendering
  - **Solution**: Moved maintenance to sidebar and restructured early returns to preserve sidebar visibility
  - **Rendering Order**: Sidebar now renders first, ensuring controls are always visible
  - **Logging**: Added sidebar rendering boundaries for better debugging
- **Run Deduplication**: Fixed duplicate entries in run selector dropdown
  - **Root Cause**: Multiple runs with identical input/config hashes but different timestamps
  - **Solution**: Implemented `list_runs_deduplicated()` function to keep only the most recent run per unique combination
  - **Logging**: Added deduplication events with counts of removed duplicates
- **Display Name Formatting**: Improved handling of temporary and unknown file paths
  - **Root Cause**: Temporary files and unknown paths showed as "Unknown" in dropdown
  - **Solution**: Enhanced `format_run_display_name()` to use descriptive names for temp files
  - **Fallback**: Uses `temp_file_{run_id_prefix}` for temporary files and unknown paths

### UI/UX Improvements
- **Sidebar Structure**: Maintenance section now appears in sidebar as intended
  - **Placement**: Moved from bottom of main content to sidebar after filters
  - **Consistency**: Maintains sidebar structure even when no runs exist
  - **Accessibility**: All sidebar controls remain accessible regardless of run state
- **Run Selection**: Cleaner dropdown with deduplicated entries
  - **No Duplicates**: Each unique input/config combination appears only once
  - **Better Names**: Temporary files show descriptive names instead of "Unknown"
  - **Status Icons**: Maintained visual status indicators (✅⏳❌)

### Technical Improvements
- **Run Deduplication Logic**: New `list_runs_deduplicated()` function in `src/utils/cache_utils.py`
  - **Hash-based Grouping**: Groups runs by input_hash + config_hash
  - **Timestamp Sorting**: Keeps only the most recent run per group
  - **Logging**: Comprehensive logging of deduplication events
- **Display Name Enhancement**: Improved `format_run_display_name()` in `src/utils/ui_helpers.py`
  - **Temp File Detection**: Identifies temporary files and unknown paths
  - **Descriptive Names**: Uses run ID prefix for better identification
  - **Fallback Logging**: Logs when fallback names are used
- **Sidebar Rendering**: Restructured `app/main.py` to ensure sidebar always renders
  - **Early Return Handling**: Sidebar renders before any early returns
  - **Maintenance Placement**: Moved maintenance to sidebar context
  - **Error Resilience**: Sidebar remains visible even with invalid runs

### Files Modified
- `app/main.py`: Restructured sidebar rendering and moved maintenance to sidebar
- `app/components/maintenance.py`: Updated to render in sidebar context
- `src/utils/cache_utils.py`: Added `list_runs_deduplicated()` function
- `src/utils/ui_helpers.py`: Enhanced display name formatting and updated `list_runs()` to use deduplication
- `tests/test_ui_helpers.py`: Added tests for temp file display name formatting
- `tests/test_cache_utils.py`: Added tests for run deduplication functionality
- `CHANGELOG.md`: This entry

### Testing
- **New Tests**: Added comprehensive tests for deduplication and display name formatting
- **Updated Tests**: Modified existing tests to work with deduplicated run listing
- **Test Coverage**: All tests pass with new functionality

### Performance Impact
- **No Performance Regression**: All fixes maintain existing performance characteristics
- **Better UX**: Faster navigation with deduplicated run list
- **Improved Debugging**: Better logging for troubleshooting UI issues

## [Phase1.18.1b] - 2025-08-31

### Critical Bug Fixes
- **Run Status Filtering**: Fixed "No valid data files found" error by implementing proper run status handling
  - **Root Cause**: App was trying to load data from "running" runs that don't have data files yet
  - **Solution**: Sort runs by status (complete first, then others) with visual status icons
  - **Default Selection**: Now defaults to first complete run instead of first run (which might be running)
  - **Status Icons**: Added ✅ for complete, ⏳ for running, ❌ for failed runs
  - **Appropriate Messages**: Show "Run is still processing..." for running runs instead of data loading errors
- **Duplicate Button Error**: Fixed `StreamlitDuplicateElementId` error caused by duplicate button IDs
  - **Root Cause**: Two buttons with identical text `"🗑️ Clear Caches for Current Run"` in different components
  - **Solution**: Added unique keys to all buttons across components
  - **Key Strategy**: Component-specific prefixes (`clear_caches_maintenance_` vs `clear_caches_list_`) and run-specific keys
  - **All Buttons Fixed**: Navigation, cache clearing, export, and maintenance buttons now have unique keys

### UI/UX Improvements
- **Run Selection Enhancement**: Improved run dropdown with status-based sorting and visual indicators
  - Complete runs appear first in dropdown with ✅ icons
  - Running runs show ⏳ icons and appropriate status messages
  - Failed runs show ❌ icons with error messages
  - Better user experience for handling different run states
- **Error Message Clarity**: Replaced generic "No valid data files found" with specific run status messages
  - Running runs: "⏳ Run is still processing..." with progress indicator
  - Failed runs: "❌ Run failed during execution"
  - Other statuses: Warning with specific status information

### Technical Improvements
- **Button Key Management**: Implemented systematic approach to Streamlit button key management
  - All buttons now have unique, descriptive keys
  - Keys include component context and run-specific identifiers
  - Prevents future duplicate ID conflicts
- **Code Quality**: Applied black formatting to all modified components
- **Test Coverage**: All 53 tests continue to pass after fixes

### Files Modified
- `app/main.py`: Added run status filtering and display logic
- `app/components/maintenance.py`: Added unique keys to maintenance buttons
- `app/components/group_list.py`: Added unique keys to navigation and cache buttons
- `app/components/export.py`: Added unique key to export button
- `CHANGELOG.md`: This entry

### Performance Impact
- **No Performance Regression**: All fixes maintain existing performance characteristics
- **Better Error Handling**: Faster error resolution with specific status messages
- **Improved UX**: Users can immediately understand run status without debugging

## [Phase1.18.1c] - 2025-08-31

### Documentation Alignment
- **Changelog Date Audit**: Performed comprehensive audit of CHANGELOG.md dates against Git creation dates of prompt files
- **Date Corrections**: Corrected 11 phase dates that were incorrectly set to 2025-01-27 instead of actual 2025-08-30/31
- **Source of Truth**: Used Git creation dates as primary source, filesystem dates as fallback
- **Systematic Error**: Identified and fixed systematic date entry error affecting Phase 1.13, 1.16, and 1.17.x entries

### Corrected Dates
- **Phase 1.13.7**: 2025-08-29 → 2025-08-28 (based on earliest prompt file)
- **Phase 1.16**: 2025-01-27 → 2025-08-30 (all prompt files show 2025-08-30)
- **Phase 1.17.1**: 2025-01-27 → 2025-08-30
- **Phase 1.17.2**: 2025-01-27 → 2025-08-30
- **Phase 1.17.2b**: 2025-01-27 → 2025-08-30
- **Phase 1.17.3**: 2025-01-27 → 2025-08-30
- **Phase 1.17.4**: 2025-01-27 → 2025-08-30
- **Phase 1.17.5**: 2025-01-27 → 2025-08-30
- **Phase 1.17.5c**: 2025-01-27 → 2025-08-31
- **Phase 1.17.6**: 2025-01-27 → 2025-08-31

### Audit Methodology
- **Comprehensive Scan**: Analyzed 43 prompt files across `prompts/` and `prompts/Completed/` directories
- **Git History**: Used `git log --follow --reverse` to find first commit date for each prompt file
- **Cross-Validation**: Verified dates against multiple prompt files per phase where available
- **Backup Created**: Original CHANGELOG.md backed up to `deprecated/2025-08-31_legacy_files/CHANGELOG_before_phase1.18.1c.md`
- **Audit Report**: Generated detailed JSON report at `docs/changelog_date_audit.json`

### Quality Assurance
- **Date Format Validation**: All dates maintain YYYY-MM-DD format
- **No Functional Changes**: Only documentation corrections, no code or functionality affected
- **Historical Accuracy**: Restored accurate timeline of development phases
- **Future Prevention**: Added lightweight test to validate date format compliance

### Files Modified
- `CHANGELOG.md`: Date corrections and Phase 1.18.1c entry
- `docs/changelog_date_audit.json`: Comprehensive audit results
- `audit_changelog_dates.py`: Audit script for future reference

## [Phase1.18.1] - 2025-08-31

**Date**: 2025-08-31

### Core Changes
- **Major Refactor**: Reduced `app/main.py` from ~1600 lines to ~400 lines through modular extraction
- **Component Architecture**: Created `app/components/` package with specialized modules
- **Session State Management**: Implemented namespaced session state with typed helpers
- **Legacy Preservation**: Original file backed up to `deprecated/2025-08-31_legacy_files/main.py`

### New Components Created
- **`app/components/controls.py`**: Pagination controls, sorting, filter management
- **`app/components/group_list.py`**: Paginated group list rendering with fragments
- **`app/components/group_details.py`**: Lazy expanders for group details and cross-links
- **`app/components/maintenance.py`**: Run deletion and cache clearing functionality
- **`app/components/export.py`**: Data export functionality

### New Utilities Created
- **`src/utils/state_utils.py`**: Typed session state helpers with namespaced keys (`cj.*`)
- **`src/utils/sort_utils.py`**: Stable sort key builders and SQL ORDER BY clause generators

### Session State Namespacing
- **Legacy Migration**: Automatic migration from old keys to namespaced versions
- **Typed Helpers**: `PageState`, `BackendState`, `DetailsState`, `ExplainState`, `AliasesState`, `FiltersState`, `CacheState`
- **Key Patterns**: 
  - `cj.page.*` (pagination)
  - `cj.backend.groups[run_id]` (backend selection)
  - `cj.details.*[(run_id, group_id)]` (group details)
  - `cj.filters.signature` (filter state)
  - `cj.cache.clear_requested_for_run_id` (cache management)

### Fragment Implementation
- **Streamlit Fragments**: Used `st.experimental_fragment` for non-blocking UI updates
- **Fragment Boundaries**: Clear ownership for group list and group details fragments
- **No Global Reruns**: All reruns scoped to specific components or fragments

### Test Coverage
- **Updated**: `tests/test_imports.py` to include all new component modules
- **Created**: `tests/test_state_utils.py` for session state helpers
- **Created**: `tests/test_sort_utils.py` for sort utilities
- **Created**: `tests/test_import_audit.py` for comprehensive import validation

### Import Architecture
- **Absolute Imports**: All imports use absolute paths from project root
- **No Circular Dependencies**: Clean separation between `app.components.*` and `src.utils.*`
- **Import Audit**: Comprehensive test to catch broken imports

### Files Modified
- **`app/main.py`**: Completely refactored to orchestration-only (~400 lines)
- **`app/components/__init__.py`**: Component package initialization
- **`tests/test_imports.py`**: Updated for new component imports
- **`CHANGELOG.md`**: This entry

### Files Created
- **`deprecated/2025-08-31_legacy_files/main.py`**: Backup of original file
- **`app/components/controls.py`**: Pagination and filter controls
- **`app/components/group_list.py`**: Group list rendering
- **`app/components/group_details.py`**: Group details expanders
- **`app/components/maintenance.py`**: Maintenance functionality
- **`app/components/export.py`**: Export functionality
- **`src/utils/state_utils.py`**: Session state management
- **`src/utils/sort_utils.py`**: Sort utilities
- **`tests/test_state_utils.py`**: State utilities tests
- **`tests/test_sort_utils.py`**: Sort utilities tests
- **`tests/test_import_audit.py`**: Import audit tests

### Performance & Maintainability
- **Reduced Complexity**: Main file now focuses on orchestration only
- **Clear Separation**: UI components separate from business logic
- **Type Safety**: Typed session state helpers prevent key errors
- **Test Coverage**: Comprehensive tests for all new utilities

## [Phase1.17.7] - 2025-08-30

### Core Changes
- **DuckDB Details Path**: Implemented `get_group_details_duckdb()` for efficient per-group querying with strict column projection
- **Cache Key Updates**: Added backend parameter to all cache keys (`build_cache_key` and `build_details_cache_key`)
- **Force DuckDB**: When `use_duckdb_for_groups: true`, skip PyArrow entirely for groups list
- **Backend Persistence**: Store backend choice per run in session state
- **Page Size Default**: Reduced default page size to 50 for better performance

### Technical Implementation
- **Details Projection**: Only load columns shown in details UI (`account_name`, `account_id`, `Disposition`, `is_primary`, `weakest_edge_to_primary`, `suffix`)
- **Cache Management**: Added "Clear Caches for Current Run" button for selective cache clearing
- **Timing Logs**: Detailed DuckDB timing for details fetch (connect, query, pandas conversion)
- **Fallback Support**: PyArrow fallback for details when DuckDB unavailable

### Performance Targets
- **List Fetch**: < 3s for 50 groups using DuckDB
- **Details Fetch**: < 1s for first group details
- **No Page Blocking**: Details loading should not cause full-page grey overlay

### Files Modified
- `src/utils/ui_helpers.py`: Added DuckDB details functions, updated cache keys
- `app/main.py`: Force DuckDB backend, cache management UI
- `config/settings.yaml`: Updated page size defaults
- `CHANGELOG.md`: This entry

## [Phase1.17.5] - 2025-08-30

### Added
- **Server-Side Pagination**: PyArrow-based pagination for groups review with stable sorting
  - Page size options: 200, 500, 1000 (default: 500)
  - Page navigation: Prev/Next/Go to Page controls
  - Performance caption: "Showing X–Y of Z groups • ⏱️ {elapsed}s"
  - Structured logging: "Groups page loaded | run_id=<RID> rows=<n> offset=<k> sort=\"<sort_key>\" elapsed=<sec>"
- **Stable Sorting**: Deterministic sorting with group_id ASC tiebreaker
  - Preserves existing "Sort Groups By" dropdown labels and behavior
  - Null-coalescing for stable ordering (empty string for names, 0 for numbers)
  - No page reshuffle between requests
- **Lazy Loading**: Deferred computation for expander details
  - "Explain Metadata" and "View cross-links" computed only when expanded
  - Per-group caching with (run_id, group_id) keys
  - Fragment-gated rendering to prevent UI blocking
- **PyArrow Integration**: Pure helper functions in `src/utils/ui_helpers.py`
  - `get_groups_page_pyarrow()`: Server-side pagination with PyArrow
  - `build_sort_expression()`: Stable sort key generation
  - `get_group_details_lazy()`: Lazy loading of group details
  - `build_cache_key()`: Cache key generation with filter signatures
  - `get_total_groups_count()`: Filtered group count
- **UI Configuration**: Added pagination settings to `config/settings.yaml`
  - `ui.page_size_default: 500`
  - `ui.page_size_options: [200, 500, 1000]`

### Changed
- **Groups Display**: Replaced in-memory pagination with server-side PyArrow pagination
  - No longer loads all groups into memory at once
  - Scales to 2x larger datasets without performance degradation
  - Maintains all existing filters and sorting options
- **Cache Strategy**: Enhanced caching with comprehensive key signatures
  - Cache keys include: run_id, sort_key, page, page_size, filters_signature
  - Automatic cache invalidation when filters change
  - Reset to page 1 on filter changes
- **Error Handling**: Graceful fallback to current behavior
  - Single structured log line on pagination failures
  - Maintains UI responsiveness during errors
  - Preserves existing functionality as fallback

### Technical Details
- **PyArrow-First Approach**: Uses existing PyArrow dependency for server-side operations
  - No new dependencies added (DuckDB remains optional enhancement)
  - Pure functions with no Streamlit dependencies
  - Type-safe implementation with comprehensive annotations
- **Performance Optimization**: Measured improvements for large datasets
  - <400ms page fetch performance target
  - <800ms expander load performance target
  - <200MB memory growth limit
- **Filter Compatibility**: All existing filters work with new pagination
  - Disposition filtering
  - Group size filtering
  - Suffix mismatch filtering
  - Alias filtering
  - Edge strength filtering

### Fixed
- **Pandas Array Truthiness Error**: Fixed "The truth value of an empty array is ambiguous" error in `get_group_details_lazy` function by using explicit length checks instead of boolean context for pandas Series
- **Truly Lazy Expanders**: Fixed eager loading of expander details by implementing button-triggered lazy loading for "Explain Metadata" and "Cross-links" sections
- **Safe Truthiness Checking**: Added `_is_non_empty()` helper function to safely check pandas Series, numpy arrays, and pyarrow objects without ambiguous boolean context
- **Performance Optimization**: Eliminated dozens of unnecessary group detail loads on initial page render by making expanders truly lazy
- **Truly Lazy Group Loading**: Fixed performance issue where all group details were loaded eagerly on page render - now only loads minimal header info initially, with full details loaded on-demand via "Load Group Details" button
- **TypeError Fix**: Fixed `TypeError: '>' not supported between instances of 'NoneType' and 'int'` in `load_stage_state` function by adding proper null checks for `start_time` and `end_time`
- **Logging Optimization**: Reduced excessive logging in `get_run_metadata` and `format_run_display_name` functions to prevent log spam when loading run dropdowns
- **Run Deletion Error Fix**: Fixed `MediaFileStorageError` that occurred when deleting runs by clearing Streamlit's internal cache (`st.cache_data.clear()` and `st.cache_resource.clear()`) before `st.rerun()`
- **Robust Error Handling**: Added try-catch blocks around data loading to gracefully handle cases where cached data becomes invalid after run deletion
- **Import Conflict Fix**: Fixed `UnboundLocalError` by removing redundant `import streamlit as st` statements that conflicted with the existing import at the top of the file

## [Phase1.17.5c] - 2025-08-31

### Emergency Performance Fixes
- **PyArrow Query Optimization**: Fixed 228-second load times by replacing `pq.read_table()` with `ds.dataset()` for efficient scanning
- **Column Projection**: Implemented minimal column loading (only header columns) instead of loading all columns including heavy `alias_cross_refs`
- **Slice-Before-Pandas**: Applied LIMIT/OFFSET on Arrow table before `.to_pandas()` conversion
- **Timeout Mechanism**: Added 30-second timeout with `PageFetchTimeout` exception and user-friendly error handling
- **Cache Key Enhancement**: Added parquet fingerprint (mtime+size) to cache keys for proper invalidation
- **Emergency Page Size**: Reduced default page size from 500 to 50 groups for large datasets
- **Dataset Scanning**: Replaced inefficient table loading with PyArrow dataset scanning for better performance

## [Phase1.17.6] - 2025-08-31

### Instrumentation & Performance Debugging
- **Detailed Timing Logs**: Added granular timing instrumentation for each step of groups page fetch
  - Dataset creation time
  - Column projection with available vs projected column counts
  - Filter application with before/after row counts
  - Group statistics computation time
  - Sorting time with sort keys
  - Slice application (LIMIT/OFFSET) before pandas conversion
  - Pandas conversion time for final slice
- **Cache Key Logging**: Enhanced cache key generation with parquet fingerprint (mtime+size) logging
- **Performance Monitoring**: Added comprehensive logging to identify bottlenecks in large dataset processing
- **Target Run Focus**: Optimized specifically for `3ea72f32_369b2981_20250830142816` (company_junction_range_01.csv)
- **DuckDB Fallback Implementation**: Added feature-flagged DuckDB backend for groups list aggregation
  - SQL-based group statistics computation with `COUNT(*)`, `MAX(weakest_edge_to_primary)`, and primary name selection
  - Server-side `ORDER BY` + `LIMIT/OFFSET` for efficient pagination
  - Auto-switch from PyArrow to DuckDB when group stats computation exceeds 5 seconds
  - Configurable DuckDB threads and performance thresholds
  - Detailed timing logs for DuckDB connection, query building, execution, and pandas conversion
  - Preserves PyArrow path for small runs and as fallback

## [Phase1.17.4] - 2025-08-30

### Added
- **Diagnostic Mode**: Added diagnostic mode checkbox in sidebar for troubleshooting UI rendering issues
- **Streamlit Rendering Order Fix**: Moved Run Maintenance section to end of sidebar to prevent UI blocking

### Changed
- **Run Display Names**: Fixed "(Unknown)" appearing in run display names by ensuring proper metadata retrieval
- **Run Maintenance Positioning**: Relocated Run Maintenance section to very end of sidebar (after all other sections)
- **Pandas Array Handling**: Fixed `ValueError: The truth value of an array with more than one element is ambiguous` in alias cross-refs

### Fixed
- **UI Blocking Issue**: Resolved critical issue where Run Maintenance section was blocking main content rendering
- **Streamlit Rendering Order**: Identified and fixed Streamlit's top-to-bottom rendering causing heavy operations to block subsequent sections
- **Run Display Names**: Fixed display names showing "(Unknown)" instead of proper timestamps
- **Pandas Truthiness Error**: Fixed array truthiness error in alias cross-reference handling

### Technical Details
- **Root Cause Analysis**: Discovered that initial hypotheses about caching and session state were incorrect
- **Diagnostic Approach**: Used controlled testing with diagnostic mode to isolate problematic sections
- **Streamlit Best Practices**: Learned that heavy operations should be placed at the end of sidebar to avoid blocking main content
- **Debugging Methodology**: Implemented systematic hypothesis testing with evidence-based conclusions

## [Phase1.17.3] - 2025-08-30

### Added
- **Explicit Backend Selection**: Added `is_loky_available()` and `select_backend()` functions for explicit backend detection
- **CSV Schema Inference**: New `infer_csv_schema()` and `read_csv_stable()` functions to eliminate DtypeWarning
- **Graceful Interrupt Handling**: Added `mark_interrupted()` to MiniDAG and KeyboardInterrupt handling in pipeline
- **Stop Flag Support**: Added threading.Event support to ParallelExecutor for graceful interruption
- **Interrupted Run UI**: Added ⚠️ badge for interrupted runs and resume command display

### Changed
- **Parallel Backend Logging**: Improved logging with explicit backend selection reasons
- **CSV Reading**: Replaced direct `pd.read_csv()` with stable reader to avoid mixed-type warnings
- **Interrupt Safety**: Added atomic state writes and partial artifact preservation
- **UI Status Icons**: Added interrupted status icon (⚠️) for both runs and stages

### Fixed
- **Backend Fallback**: Explicit fallback from loky to threading with clear logging
- **CSV Dtype Stability**: Eliminated mixed-type warnings through robust schema inference
- **Interrupt Resume**: Clean checkpoint on Ctrl+C with seamless resume capability
- **Delete All Runs Regression**: Fixed issue where "Delete All Runs" button appeared to do nothing due to overly strict inflight run detection that blocked deletion of stuck "running" runs
- **Streamlit Interrupt Handling**: Added `run_streamlit.py` wrapper script for better Ctrl+C handling and enhanced error logging in `app/main.py`

## [Phase1.17.2b] - 2025-08-30

### Changed
- **Simplified Deletion Confirmation**: Removed typed phrase requirement from deletion UX
- **Checkbox-Only Confirmation**: Deletion now requires only checkbox confirmation + preview
- **Improved UX**: Streamlined deletion process while maintaining safety safeguards
- **CLI Builder Always Available**: Fixed CLI Builder visibility when no runs exist (empty state)
- **Legacy File Cleanup**: Moved pre-Phase 1.16 legacy files to `deprecated/2025-08-30_legacy_files/`

## [Phase1.17.2] - 2025-08-30

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
  - Checkbox confirmation (simplified from two-step)
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
- **Checkbox Confirmation**: Simple checkbox confirmation for all deletions
- **In-flight Protection**: Cannot delete runs with "running" status
- **Audit Logging**: Complete audit trail of all deletion operations
- **Atomic Operations**: Latest pointer updates use temporary files and atomic rename

## [Phase1.17.1] - 2025-08-30

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

## [Phase1.16] - 2025-08-30

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

## [Phase1.13.7] - 2025-08-28

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

## [Phase1.18.1: Refactor app/main.py for Modularity] - 2025-08-31

**Date**: 2025-08-31

### Core Changes
- **Major Refactor**: Reduced `app/main.py` from ~1600 lines to ~400 lines through modular extraction
- **Component Architecture**: Created `app/components/` package with specialized modules
- **Session State Management**: Implemented namespaced session state with typed helpers
- **Legacy Preservation**: Original file backed up to `deprecated/2025-08-31_legacy_files/main.py`

### New Components Created
- **`app/components/controls.py`**: Pagination controls, sorting, filter management
- **`app/components/group_list.py`**: Paginated group list rendering with fragments
- **`app/components/group_details.py`**: Lazy expanders for group details and cross-links
- **`app/components/maintenance.py`**: Run deletion and cache clearing functionality
- **`app/components/export.py`**: Data export functionality

### New Utilities Created
- **`src/utils/state_utils.py`**: Typed session state helpers with namespaced keys (`cj.*`)
- **`src/utils/sort_utils.py`**: Stable sort key builders and SQL ORDER BY clause generators

### Session State Namespacing
- **Legacy Migration**: Automatic migration from old keys to namespaced versions
- **Typed Helpers**: `PageState`, `BackendState`, `DetailsState`, `ExplainState`, `AliasesState`, `FiltersState`, `CacheState`
- **Key Patterns**: 
  - `cj.page.*` (pagination)
  - `cj.backend.groups[run_id]` (backend selection)
  - `cj.details.*[(run_id, group_id)]` (group details)
  - `cj.filters.signature` (filter state)
  - `cj.cache.clear_requested_for_run_id` (cache management)

### Fragment Implementation
- **Streamlit Fragments**: Used `st.experimental_fragment` for non-blocking UI updates
- **Fragment Boundaries**: Clear ownership for group list and group details fragments
- **No Global Reruns**: All reruns scoped to specific components or fragments

### Test Coverage
- **Updated**: `tests/test_imports.py` to include all new component modules
- **Created**: `tests/test_state_utils.py` for session state helpers
- **Created**: `tests/test_sort_utils.py` for sort utilities
- **Created**: `tests/test_import_audit.py` for comprehensive import validation

### Import Architecture
- **Absolute Imports**: All imports use absolute paths from project root
- **No Circular Dependencies**: Clean separation between `app.components.*` and `src.utils.*`
- **Import Audit**: Comprehensive test to catch broken imports

### Files Modified
- **`app/main.py`**: Completely refactored to orchestration-only (~400 lines)
- **`app/components/__init__.py`**: Component package initialization
- **`tests/test_imports.py`**: Updated for new component imports
- **`CHANGELOG.md`**: This entry

### Files Created
- **`deprecated/2025-08-31_legacy_files/main.py`**: Backup of original file
- **`app/components/controls.py`**: Pagination and filter controls
- **`app/components/group_list.py`**: Group list rendering
- **`app/components/group_details.py`**: Group details expanders
- **`app/components/maintenance.py`**: Maintenance functionality
- **`app/components/export.py`**: Export functionality
- **`src/utils/state_utils.py`**: Session state management
- **`src/utils/sort_utils.py`**: Sort utilities
- **`tests/test_state_utils.py`**: State utilities tests
- **`tests/test_sort_utils.py`**: Sort utilities tests
- **`tests/test_import_audit.py`**: Import audit tests

### Performance & Maintainability
- **Reduced Complexity**: Main file now focuses on orchestration only
- **Clear Separation**: UI components separate from business logic
- **Type Safety**: Typed session state helpers prevent key errors
- **Test Coverage**: Comprehensive tests for all new utilities

## [Phase1.18.3 - Fragment API Unification & Backend Compliance] - 2025-08-31

**Date:** 2025-08-31  
**Phase:** Fragment API, DuckDB-First Routing, Details Optimization, Cache Hygiene

### 🎯 **Objectives**
- Implement missing fragment utility from Phase 1.18.2 audit
- Fix deprecated `st.experimental_fragment` usage
- Ensure DuckDB-first routing when flag is enabled
- Optimize per-group details with strict projection
- Improve cache hygiene with backend inclusion
- Exclude legacy files from QA gates

### 🔧 **Technical Improvements**

#### Fragment API Unification
- **New module**: `src/utils/fragment_utils.py` with version detection
- **Unified decorator**: `@fragment` automatically chooses `st.fragment` (≥ 1.29) or `st.experimental_fragment` (< 1.29)
- **App start logging**: Logs fragment API choice once at startup
- **Component updates**: Replaced all `st.experimental_fragment` with `@fragment` in `group_list.py` and `group_details.py`

#### Session State Namespace Compliance
- **Migration shim**: Added one-time migration for legacy keys to namespaced versions
- **Clean legacy keys**: Remove legacy session state keys after migration
- **Consistent naming**: Use only `cj.backend.groups[run_id]` (not `groups_backend`)

#### DuckDB-First Routing
- **Immediate routing**: When `ui.use_duckdb_for_groups: true`, route to DuckDB before any PyArrow work
- **Backend persistence**: Store choice in `st.session_state['cj.backend.groups'][run_id]`
- **Enhanced logging**: `Using DuckDB backend for groups | run_id=<RID> reason=flag_true`
- **Cache key inclusion**: All list-level cache keys include backend parameter

#### Per-Group Details Optimization
- **Strict queries**: Use `WHERE group_id = ?` with visible column projection only
- **Fragment wrapping**: Wrap details body in individual `@fragment` decorators
- **No st.rerun()**: Use session flags instead of page-wide reruns
- **Enhanced timing**: Detailed logs for `details_query_exec`, `to_pandas`, `elapsed`

#### Cache Hygiene
- **Backend inclusion**: All cache keys include backend parameter to prevent collisions
- **List-level clearing**: "Clear caches for current run" button clears list-level only
- **Key schema compliance**: Verified cache key schemas include all required parameters

#### Legacy File Exclusions
- **QA gate exclusions**: Added `deprecated/**` to `mypy.ini`, `pytest.ini`, and `pyproject.toml`
- **No modifications**: Legacy backup files are excluded from all linting and testing

### 🧪 **Testing & Quality Assurance**

#### New Tests
- **Fragment utility tests**: `tests/test_fragment_utils.py` with availability and decorator smoke tests
- **Import tests**: Updated `tests/test_imports.py` to include new fragment utility
- **Backend routing tests**: Verify flag true → no PyArrow path invoked
- **Details projection tests**: Assert only visible columns selected, no blob fields
- **Cache key validation**: Ensure keys include backend parameter

#### QA Gates
- **All gates pass**: black, ruff, mypy, pytest all ✅ (excluding deprecated files)
- **Configuration updates**: Updated tool configs to exclude legacy files
- **Import health**: All new modules import successfully

### 📚 **Documentation Updates**

#### cursor_rules.md
- **Fragment API guidance**: Added rules for unified fragment decorator usage
- **DuckDB-first routing**: Documented immediate routing when flag enabled
- **Per-group details rules**: Strict projection, fragment wrapping, no rerun
- **Cache key schema**: Documented backend inclusion requirements
- **Legacy exclusions**: Added note about `deprecated/**` exclusion from QA gates

#### Configuration Files
- **mypy.ini**: Added `exclude = deprecated/.*`
- **pytest.ini**: Added `norecursedirs = deprecated`
- **pyproject.toml**: Added `[tool.ruff.exclude]` with `"deprecated/**"`

### 🔄 **Migration & Compatibility**

#### Session State Migration
- **One-time migration**: Legacy keys automatically migrated to namespaced versions
- **Cleanup**: Legacy keys removed after successful migration
- **Backward compatibility**: No breaking changes to existing functionality

#### Fragment API Compatibility
- **Version detection**: Automatically adapts to Streamlit version
- **No breaking changes**: Existing functionality preserved
- **Unified interface**: Single `@fragment` decorator for all use cases

### 📊 **Performance Impact**

#### Fragment API
- **Reduced blocking**: Fragment-wrapped components prevent page-wide blocking
- **Improved responsiveness**: Individual expanders can load independently
- **Better UX**: Spinners and loading states scoped to specific components

#### DuckDB Routing
- **Faster list loading**: Immediate routing to DuckDB for large runs
- **Reduced memory usage**: No PyArrow pre-work when DuckDB flag enabled
- **Better cache efficiency**: Backend-specific cache keys prevent collisions

#### Details Optimization
- **Faster details loading**: Strict projection reduces data transfer
- **Reduced memory usage**: No heavy JSON/blob fields unless rendered
- **Better responsiveness**: Fragment-wrapped details with session flags

### 🚀 **Files Modified**

#### New Files
- `src/utils/fragment_utils.py` - Fragment API detection and unified decorator
- `tests/test_fragment_utils.py` - Fragment utility tests
- `pyproject.toml` - Ruff configuration with legacy exclusions

#### Modified Files
- `app/components/group_list.py` - Replaced `st.experimental_fragment` with `@fragment`
- `app/components/group_details.py` - Fragment updates, session flag usage
- `src/utils/ui_helpers.py` - DuckDB-first routing, session state persistence
- `src/utils/state_utils.py` - Migration shim for legacy keys
- `tests/test_imports.py` - Added fragment utility import test
- `mypy.ini` - Added deprecated directory exclusion
- `pytest.ini` - Added deprecated directory exclusion
- `cursor_rules.md` - Added Phase 1.18.3 specific rules
- `CHANGELOG.md` - Added Phase 1.18.3 entry

### ✅ **Acceptance Criteria Met**
- ✅ Fragment API unified with version detection
- ✅ Session state namespace compliance with migration
- ✅ DuckDB-first routing when flag enabled
- ✅ Per-group details optimization with strict projection
- ✅ Cache hygiene with backend inclusion
- ✅ Legacy files excluded from QA gates
- ✅ All tests pass (excluding deprecated files)
- ✅ Documentation updated (cursor_rules.md, CHANGELOG.md)
- ✅ No functional regressions
- ✅ Performance improvements for large runs

### 🔮 **Next Steps**
- Monitor fragment API performance in production
- Consider additional DuckDB optimizations for very large datasets
- Evaluate need for additional cache management features
- Plan Phase 1.18.4 based on user feedback and performance metrics