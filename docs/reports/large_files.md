## Large File Audit & Refactor Plan

### 1. **File: `src/utils/ui_helpers.py` — ~1750 lines**

**Responsibilities:** Core UI data loading, caching, and pagination for Streamlit app. Handles DuckDB/PyArrow routing, group details caching, pagination logic, and data filtering.

**Pain Points:**
- **Massive file size** (1750 lines) violates single responsibility principle
- **Mixed concerns**: caching, data loading, pagination, filtering, and UI logic all in one place
- **Hardcoded paths**: Direct references to "data/interim", "data/processed" 
- **Complex functions**: `get_groups_page_duckdb()` is 100+ lines with multiple responsibilities
- **Duplicate logic**: Similar pagination patterns repeated for different data types
- **Tight coupling**: UI helpers directly import from multiple utility modules

**Proposed Extractions:**
- **What**: `DetailsCache` class and cache management functions
- **To**: `src/utils/cache_utils.py` (extend existing module)
- **Why**: Centralize all caching logic, improve testability, reduce coupling

- **What**: Pagination logic (`get_groups_page_*`, `get_total_groups_count`)
- **To**: `src/utils/pagination_utils.py` (new module)
- **Why**: Separate pagination concerns, enable reuse across different data types

- **What**: Data filtering functions (`apply_filters_pyarrow`, `build_sort_expression`)
- **To**: `src/utils/filter_utils.py` (new module)
- **Why**: Centralize filtering logic, improve testability

- **What**: Backend routing logic (DuckDB vs PyArrow selection)
- **To**: `src/utils/backend_utils.py` (new module)
- **Why**: Single place for backend selection logic, easier to maintain

**Centralization Candidates:**
- Path handling: Use `src/utils/path_utils.py` consistently
- Cache key generation: Centralize in `src/utils/cache_utils.py`
- Backend selection: Single helper for DuckDB/PyArrow routing
- Session state keys: Namespace all keys under `cj.*` pattern

**Safety/Compat Notes:** No behavior changes; extract functions maintain identical signatures. Test pagination, caching, and filtering behavior remains identical.

**Config Moves:**
- `cache_capacity` → `ui.cache_capacity` (already in config)
- `page_timeout` → `ui.page_timeout` (currently hardcoded 30s)
- `default_page_size` → `ui.default_page_size` (currently hardcoded 500)

### 2. **File: `src/cleaning.py` — ~1213 lines**

**Responsibilities:** Main pipeline orchestration, CLI interface, and end-to-end data processing workflow. Coordinates all stages from input loading to final output.

**Pain Points:**
- **Pipeline orchestration mixed with CLI parsing**: Main function handles both argument parsing and pipeline execution
- **Large functions**: `run_pipeline()` is 200+ lines with multiple responsibilities
- **Hardcoded paths**: Direct references to "config/settings.yaml", "data/interim", "data/processed"
- **Mixed concerns**: CLI logic, pipeline orchestration, and stage execution all in one file
- **Complex error handling**: Multiple try-catch blocks with different exit codes

**Proposed Extractions:**
- **What**: CLI argument parsing and validation
- **To**: `src/cli.py` (new module)
- **Why**: Separate CLI concerns from pipeline logic, improve testability

- **What**: Pipeline orchestration logic (`run_pipeline`, `_run_stage`)
- **To**: `src/pipeline.py` (new module)
- **Why**: Focus on pipeline execution, separate from CLI and individual stages

- **What**: Stage execution functions (`_run_*_stage`)
- **To**: `src/stages.py` (new module)
- **Why**: Group related stage logic, improve maintainability

**Centralization Candidates:**
- Path handling: Use `src/utils/path_utils.py` consistently
- Configuration loading: Centralize in `src/utils/io_utils.py`
- Exit codes: Define constants for different exit scenarios
- Progress handling: Use `src/utils/progress.py` consistently

**Safety/Compat Notes:** CLI interface remains identical; pipeline behavior unchanged. Test all pipeline stages and CLI options.

**Config Moves:**
- `--chunk-size` default → `parallel.chunk_size` (currently hardcoded 1000)
- `--keep-runs` default → `pipeline.keep_runs` (currently hardcoded 10)
- Exit codes → `pipeline.exit_codes` (currently hardcoded 87, 130)

### 3. **File: `src/alias_matching.py` — ~781 lines**

**Responsibilities:** Alias candidate extraction, matching, and cross-reference creation. Handles fuzzy string matching and alias relationship building.

**Pain Points:**
- **Large functions**: `_process_one_record_optimized()` is 100+ lines with complex logic
- **Mixed concerns**: Record processing, bucket building, and alias matching all in one file
- **Complex data structures**: Multiple index mappings and bucket arrays
- **Performance optimizations**: Array operations mixed with business logic

**Proposed Extractions:**
- **What**: Bucket building and index mapping logic
- **To**: `src/utils/bucket_utils.py` (new module)
- **Why**: Separate data structure optimization from business logic

- **What**: Alias matching algorithms and scoring
- **To**: `src/utils/alias_utils.py` (new module)
- **Why**: Centralize alias matching logic, improve testability

- **What**: Record processing pipeline
- **To**: `src/utils/record_processor.py` (new module)
- **Why**: Separate processing logic from data structure management

**Centralization Candidates:**
- Fuzzy matching thresholds: Move to `config/settings.yaml`
- Progress logging: Use `src/utils/progress.py` consistently
- Parallel execution: Use `src/utils/parallel_utils.py` consistently

**Safety/Compat Notes:** Alias matching results identical; performance characteristics maintained. Test all alias scenarios and edge cases.

**Config Moves:**
- `high_threshold` → `alias_matching.high_threshold`
- `medium_threshold` → `alias_matching.medium_threshold`
- `bucket_size_limit` → `alias_matching.bucket_size_limit`

### 4. **File: `src/similarity.py` — ~728 lines**

**Responsibilities:** Candidate pair generation, similarity scoring, and blocking strategies. Handles rapidfuzz integration and parallel processing.

**Pain Points:**
- **Large functions**: `_compute_similarity_scores_parallel()` is 150+ lines
- **Mixed concerns**: Pair generation, scoring, and parallel execution all in one file
- **Complex checkpointing**: Checkpoint logic mixed with scoring logic
- **Hardcoded values**: Checkpoint sizes, progress intervals hardcoded

**Proposed Extractions:**
- **What**: Checkpointing and resume logic
- **To**: `src/utils/checkpoint_utils.py` (new module)
- **Why**: Separate checkpoint concerns, enable reuse across other modules

- **What**: Pair generation and blocking strategies
- **To**: `src/utils/pair_utils.py` (new module)
- **Why**: Separate blocking logic from scoring logic

- **What**: Similarity scoring algorithms
- **To**: `src/utils/scoring_utils.py` (new module)
- **Why**: Centralize scoring logic, improve testability

**Centralization Candidates:**
- Progress logging: Use `src/utils/progress.py` consistently
- Parallel execution: Use `src/utils/parallel_utils.py` consistently
- Checkpoint paths: Use `src/utils/path_utils.py` consistently

**Safety/Compat Notes:** Similarity scores identical; checkpointing behavior maintained. Test all scoring scenarios and resume functionality.

**Config Moves:**
- `checkpoint_size` → `similarity.checkpoint_size` (currently hardcoded 50000)
- `progress_step_every` → `similarity.progress_step_every` (currently hardcoded 5000)

### 5. **File: `src/utils/cache_utils.py` — ~642 lines**

**Responsibilities:** Run management, cache directories, run indexing, and latest pointer handling. Manages the entire run lifecycle.

**Pain Points:**
- **Mixed concerns**: Run management, cache directories, and process monitoring all in one file
- **Large functions**: `list_runs_deduplicated()` is complex with multiple responsibilities
- **Process monitoring**: psutil dependency mixed with cache utilities
- **Hardcoded paths**: Direct references to "data/run_index.json"

**Proposed Extractions:**
- **What**: Process monitoring and run status checking
- **To**: `src/utils/process_utils.py` (new module)
- **Why**: Separate process concerns from cache management

- **What**: Run deduplication and sorting logic
- **To**: `src/utils/run_utils.py` (new module)
- **Why**: Separate run management from cache utilities

**Centralization Candidates:**
- Path handling: Use `src/utils/path_utils.py` consistently
- Run index path: Move to configuration
- Process monitoring: Centralize in dedicated module

**Safety/Compat Notes:** Run management behavior identical; cache operations unchanged. Test all run scenarios and cleanup operations.

**Config Moves:**
- `run_index_path` → `cache.run_index_path` (currently hardcoded "data/run_index.json")
- `default_keep_runs` → `cache.default_keep_runs` (currently hardcoded 10)

## Ranked Refactor Plan

### **Phase 1: Extract Core Utilities (Small PRs, 1-2 files each)**

1. **PR 1: Extract Pagination Logic**
   - Create `src/utils/pagination_utils.py`
   - Move pagination functions from `ui_helpers.py`
   - Update imports in `ui_helpers.py`
   - **Files**: 2 files, limited blast radius

2. **PR 2: Extract Filtering Logic**
   - Create `src/utils/filter_utils.py`
   - Move filtering functions from `ui_helpers.py`
   - Update imports in `ui_helpers.py`
   - **Files**: 2 files, limited blast radius

3. **PR 3: Extract Backend Routing**
   - Create `src/utils/backend_utils.py`
   - Move backend selection logic from `ui_helpers.py`
   - Update imports in `ui_helpers.py`
   - **Files**: 2 files, limited blast radius

### **Phase 2: Extract Business Logic (Medium PRs, 2-3 files each)**

4. **PR 4: Extract CLI and Pipeline Separation**
   - Create `src/cli.py` and `src/pipeline.py`
   - Move CLI logic from `cleaning.py`
   - Move pipeline orchestration from `cleaning.py`
   - **Files**: 3 files, moderate blast radius

5. **PR 5: Extract Alias Matching Components**
   - Create `src/utils/bucket_utils.py` and `src/utils/alias_utils.py`
   - Move bucket logic from `alias_matching.py`
   - Move alias matching logic from `alias_matching.py`
   - **Files**: 3 files, moderate blast radius

### **Phase 3: Extract Advanced Utilities (Larger PRs, 3-4 files each)**

6. **PR 6: Extract Similarity Components**
   - Create `src/utils/checkpoint_utils.py`, `src/utils/pair_utils.py`, `src/utils/scoring_utils.py`
   - Move checkpointing from `similarity.py`
   - Move pair generation from `similarity.py`
   - Move scoring logic from `similarity.py`
   - **Files**: 4 files, larger blast radius

7. **PR 7: Extract Cache Components**
   - Create `src/utils/process_utils.py` and `src/utils/run_utils.py`
   - Move process monitoring from `cache_utils.py`
   - Move run management from `cache_utils.py`
   - **Files**: 3 files, moderate blast radius

### **Phase 4: Configuration and Hardcoded Value Removal**

8. **PR 8: Remove Hardcoded Values**
   - Move all magic numbers to `config/settings.yaml`
   - Update all modules to use configuration
   - **Files**: Multiple files, configuration changes only

9. **PR 9: Path Centralization**
   - Ensure all modules use `src/utils/path_utils.py`
   - Remove any remaining hardcoded paths
   - **Files**: Multiple files, path changes only

## Edge Cases & Considerations

- **Determinism**: All extractions maintain identical function signatures and behavior
- **Memory caps**: Cache capacity and checkpoint sizes remain configurable
- **Parallelism behavior**: All parallel execution logic preserved in dedicated modules
- **UI rendering order**: Pagination and filtering logic extracted without changing UI behavior
- **Test impacts**: Each extraction includes corresponding test file updates
- **Import cycles**: All extractions avoid creating circular dependencies

This refactor plan follows your rules by:
- Maintaining all QA gates (black, ruff, mypy, pytest)
- Preserving determinism and caching behavior
- Centralizing paths and configuration
- Keeping heavy logic in `src/**` (not `app/**`)
- Using absolute imports from `src`
- Maintaining test behavior identical

Each PR is sized to be manageable and safe, with clear rollback paths if issues arise.