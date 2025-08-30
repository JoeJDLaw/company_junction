# Company Junction Deduplication Pipeline

## Project Status
**Phase 1 complete** ✅ — The system identifies likely duplicate Accounts, proposes a primary record using Relationship rank + Created Date, and assigns Disposition (`Keep`, `Update`, `Delete`, `Verify`). No Salesforce writes are performed in this phase.

**Phase 1.5 (refinements)**: Conservative alias extraction (semicolon & numbered sequences; parentheses only when content looks like a company), high-confidence alias matching (suffix match + score ≥ high), minimal Streamlit alias UI.

**Phase 1.7 (UX & Manual Controls)**: Review UX improvements (disposition table, sorting, better layout), manual disposition overrides, manual blacklist editor, JSON persistence with audit trail.

**Phase 1.8 (Blacklist Visibility & Filter Improvements)**: Three-pane blacklist visibility (built-in/manual/effective), filter tooltips and functionality fixes, Account Name sorting options.

**Phase 1.9 (Blacklist Improvements & Centralized I/O)**: Word-boundary blacklist matching, centralized manual I/O operations, audit snapshots, pipeline command generator.

**Phase 1.10 (Performance & Memory Hardening)**: Performance logging infrastructure, enhanced token filtering, stop token logic, block visibility, memory safety improvements.

**Phase 1.12 (Utils Package Refactor)**: Eliminated import ambiguity by splitting `src/utils.py` into logical modules, standardized all imports to absolute `src.` paths.

**Phase 1.13 (Type Safety & Code Quality)**: Comprehensive MyPy type annotations, pandas typing fixes, import hygiene, strict QA gates enforcement.

**Phase 1.13.7 (Zero MyPy Errors)**: ✅ **Complete type safety** - 0 MyPy errors across entire codebase, all QA gates green (Black/Ruff/MyPy/PyTest), enhanced pandas operations, comprehensive test coverage.

**Phase 1.14.1 (Progress Logging & MiniDAG)**: ✅ **Progress tracking and resumability** - Stage banners, progress heartbeats, optional tqdm support, MiniDAG orchestration with atomic state management, pipeline resumability from any stage.

**Docs:** see `docs/DLaw_Company_Junction_Dedup_Plan.md` for the detailed plan and acceptance criteria.

**Next:** Phase 2 (future) will add Split detection & parsing, optional LLM "real-company" classifier, and Salesforce sync steps.

---

## Phase 1 Quick Start

### 1) Install
```bash
pip install -r requirements.txt
```

### 2) Run the pipeline
```bash
python src/cleaning.py \
  --input data/raw/company_junction_range_01.csv \
  --outdir data/processed \
  --config config/settings.yaml
```

#### Progress & Heartbeats
The pipeline provides detailed progress tracking and heartbeats:

**Log-only mode (default):**
```bash
python src/cleaning.py --input data/raw/your_data.csv --outdir data/processed --config config/settings.yaml
```

**With tqdm progress bars (if installed):**
```bash
python src/cleaning.py --input data/raw/your_data.csv --outdir data/processed --config config/settings.yaml --progress
```

**Resume from specific stage:**
```bash
python src/cleaning.py --input data/raw/your_data.csv --outdir data/processed --config config/settings.yaml --resume-from final_output
```

**Monitor progress:**
```bash
tail -f pipeline.log | ts
```

### 3) Review results in Streamlit (read-only)
```bash
streamlit run app/main.py
```

### 4) Where to look
- **Interim artifacts:** `data/interim/`
  - `accounts_normalized.parquet`
  - `candidate_pairs.parquet` (if created)
  - `groups.parquet`
  - `dispositions.parquet`
  - `alias_matches.parquet` (Phase 1.5)
  - `pipeline_state.json` (stage tracking)
- **Manual data:** `data/manual/` (Phase 1.7)
  - `manual_dispositions.json` (overrides)
  - `manual_blacklist.json` (pattern rules)
- **Final review file:** `data/processed/review_ready.csv`
- **Config:** `config/settings.yaml`, `config/relationship_ranks.csv`

#### Pipeline Resumability with MiniDAG
The pipeline tracks stage completion in `data/interim/pipeline_state.json` and supports resuming from any stage:

**Available stages:**
- `normalization` - Company name normalization
- `filtering` - Data filtering and cleaning
- `candidate_generation` - Candidate pair generation
- `grouping` - Duplicate group creation
- `survivorship` - Primary record selection
- `disposition` - Disposition classification
- `alias_matching` - Alias matching and cross-references
- `final_output` - Final review-ready output generation

**Resume example:**
```bash
# Resume from final_output stage (skip all previous stages)
python src/cleaning.py --input data/raw/your_data.csv --outdir data/processed --config config/settings.yaml --resume-from final_output
```

#### Smart Auto-Resume (Phase 1.14.2)
The pipeline now includes intelligent auto-resume functionality that automatically detects where to resume from based on previous runs and input validation:

**Default behavior (smart auto-resume):**
```bash
# Automatically detects resume point and validates input hash
python src/cleaning.py --input data/raw/your_data.csv --outdir data/processed --config config/settings.yaml
```

**Force full pipeline run:**
```bash
# Ignore previous state and run full pipeline
python src/cleaning.py --input data/raw/your_data.csv --outdir data/processed --config config/settings.yaml --no-resume
```

**Override auto-detection:**
```bash
# Force resume from specific stage
python src/cleaning.py --input data/raw/your_data.csv --outdir data/processed --config config/settings.yaml --resume-from grouping
```

**Override input hash validation:**
```bash
# Force resume even if input/config files have changed
python src/cleaning.py --input data/raw/your_data.csv --outdir data/processed --config config/settings.yaml --resume-from final_output --force
```

**Custom state file location:**
```bash
# Use custom path for pipeline state
python src/cleaning.py --input data/raw/your_data.csv --outdir data/processed --config config/settings.yaml --state-path /custom/path/pipeline_state.json
```

**Auto-resume decision logging:**
The pipeline logs its auto-resume decisions with explicit reason codes:
```
Auto-resume decision: resume_from='final_output' | last_completed='final_output' | input_hash=PASS | reason=SMART_DETECT
Auto-resume decision: input_hash=FAIL - forcing full run due to input/config changes | reason=HASH_MISMATCH
Auto-resume decision: no previous run found - starting fresh | reason=NO_PREVIOUS_RUN
Manual resume decision: resume_from='grouping' | input_hash=PASS | reason=MANUAL_OVERRIDE
```

**Flag interactions:**
- `--resume-from` + `--force`: Override input hash validation
- `--no-resume` + `--resume-from`: `--no-resume` takes precedence
- `--state-path`: Custom location for pipeline state file (default: `data/interim/pipeline_state.json`)

**Safety features:**
- **Input hash validation**: Prevents resuming with changed input files unless `--force` specified
- **File validation**: Ensures intermediate files exist before resuming
- **State metadata**: Tracks input paths, config files, command line, and timestamps
- **Atomic writes**: State file updates are atomic to prevent corruption
- **Stage map clarity**: `normalization` writes `accounts_normalized.parquet`, `filtering` writes `accounts_filtered.parquet`

---

## Overview
This project provides a mini data pipeline with a Streamlit GUI for cleaning Salesforce export data. It identifies duplicates based on name, merges field values into a master record, and supports syncing back to Salesforce via CLI.

## Workflow
1. Export reports from Salesforce into `data/raw/`
2. Run the cleaning pipeline (in `src/`)
3. Review duplicates and approve merges in the Streamlit app (`app/main.py`)
4. Save final cleaned dataset to `data/processed/`
5. Use Salesforce CLI to update master records and delete duplicates

## Folder Structure
- **app/**: Streamlit GUI interface  
- **src/**: Pipeline code (cleaning, utils, Salesforce integration)  
- **data/raw/**: Original Salesforce exports  
- **data/interim/**: Temporary processing outputs  
- **data/processed/**: Final cleaned datasets  
- **tests/**: Unit tests  
- **docs/**: Additional documentation  
- **README.md**: This file (verbose project overview)  
- **cursor_rules.md**: Lean source of truth for Cursor  

## Getting Started
1. Activate your virtual environment: `source .venv/bin/activate`
2. Install dependencies: `pip install -r requirements.txt`  
3. Install the package in development mode: `pip install -e .`
4. Run the Streamlit app: `streamlit run app/main.py`  
5. Upload your Salesforce CSV and review results interactively.  

## Prerequisites
- Python 3.10+
- Salesforce CLI installed and authenticated

## Usage Examples
```bash
# Run cleaning pipeline directly
python src/cleaning.py --input data/raw/my_export.csv --outdir data/processed --config config/settings.yaml

# Run Streamlit app
streamlit run app/main.py
```

## Contributing
- Follow PEP 8.
- Run tests with `pytest` before committing.
- Update `CHANGELOG.md` for significant changes.

## Developer Guide

### Utils Package Structure (Phase 1.12+)
The `src/utils/` package contains logically organized utility modules:

- **`src/utils/logging_utils.py`** - Logging setup and configuration
  - `setup_logging()` - Configure logging for the pipeline
- **`src/utils/path_utils.py`** - File path management
  - `get_project_root()` - Get project root directory
  - `ensure_directory_exists()` - Create directory if needed
  - `get_data_paths()` - Get standard data directory paths
- **`src/utils/validation_utils.py`** - Data validation
  - `validate_dataframe()` - Validate DataFrame contains required columns
- **`src/utils/io_utils.py`** - File I/O operations
  - `get_file_info()` - Get file information
  - `list_data_files()` - List data files in directory
  - `load_settings()` - Load YAML settings with defaults
  - `load_relationship_ranks()` - Load relationship ranks from CSV
- **`src/utils/perf_utils.py`** - Performance monitoring
  - `log_perf()` - Context manager for timing and memory tracking
- **`src/utils/hash_utils.py`** - Hash utilities
  - `config_hash()` - Compute deterministic config hash
  - `stable_group_id()` - Generate stable group IDs
  - `_compute_config_hash()` - Internal config hash computation
- **`src/utils/dtypes.py`** - Memory optimization
  - `optimize_dataframe_memory()` - Apply lean dtypes and drop intermediate columns
  - `apply_dtypes()` - Apply dtype mapping to DataFrame
  - `assert_no_unexpected_object_columns()` - Validate object columns
  - `drop_intermediate_columns()` - Remove temporary processing columns
- **`src/utils/id_utils.py`** - Salesforce ID utilities (Phase 1.13.1+)
  - `sfid15_to_18()` - Convert 15-char Salesforce IDs to 18-char canonical form
  - `normalize_sfid_series()` - Normalize pandas Series of Salesforce IDs

### Adding New Utilities
When adding new utility functions:
1. **Choose the appropriate module** based on function purpose
2. **Use absolute imports** rooted at `src` (e.g., `from src.utils.path_utils import ensure_directory_exists`)
3. **Update `src/utils/__init__.py`** to export new functions
4. **Add tests** in the appropriate test file
5. **Update documentation** if needed

### Import Guidelines
- **Prefer absolute imports**: `from src.utils.io_utils import load_settings`
- **Avoid relative imports**: Don't use `from .utils import ...` or `from ..utils import ...`
- **Import specific functions**: Import only what you need, not entire modules

## Current Phase Summary
- Legal-aware normalization preserves suffix differences (INC vs LLC, etc.)
- Similarity scoring via RapidFuzz with configurable thresholds (`high=92`, `medium=84`)
- Grouping with connected components (edges require suffix match + score ≥ medium)
- Survivorship by Relationship rank → Created Date → Account ID
- Disposition per record: `Keep`, `Update`, `Delete`, `Verify`
- No Salesforce writes in Phase 1

### Phase 1.5 Highlights
- Conservative **alias extraction** (semicolon, numbered sequences; filtered parentheses)
- **Alias matching** is cross-link only (no regrouping), requires high-confidence match

### Phase 1.7 Highlights
- **Review UX**: Disposition table replaces chart, group-level sorting, better layout
- **Manual overrides**: Group-level disposition overrides with JSON persistence
- **Manual blacklist**: Pattern-based rules for automatic Delete classification
- **Audit trail**: Timestamps and export functionality for manual changes

### Phase 1.8 Highlights
- **Blacklist transparency**: Three-pane view showing built-in, manual, and effective blacklist terms
- **Filter improvements**: Tooltips and robust functionality for suffix mismatch and alias filters
- **Enhanced sorting**: Account Name sorting options (ascending/descending)
- **Better UX**: Clear visibility into what blacklist terms are being used by the pipeline

### Phase 1.9 Highlights
- **Safer blacklist matching**: Word-boundary regex for tokens, substring matching for phrases
- **Centralized I/O**: Single module for all manual file operations with atomic writes
- **Audit snapshots**: Run metadata with thresholds, counts, and git commit tracking
- **Pipeline launcher**: Copy-to-clipboard command generator for easy pipeline execution
- **Minimal Streamlit** updates to surface aliases without overwhelming the UI

### Phase 1.10 Highlights
- **Performance logging**: Comprehensive timing and memory tracking throughout pipeline stages
- **Enhanced filtering**: Better problematic pattern detection with case-insensitive matching
- **Stop token logic**: Smart blocking strategy to avoid common suffixes as blocking keys
- **Block visibility**: Top-10 token distribution logging and block statistics generation
- **Memory safety**: Improved filtering reduces memory usage and prevents exhaustion
- **Performance summary**: Key metrics and disposition statistics in `perf_summary.json`

### Phase 1.12 Highlights
- **Utils package refactor**: Eliminated import ambiguity by splitting `src/utils.py` into logical modules
- **Clean import structure**: All imports now use absolute paths rooted at `src`
- **Modular organization**: Utilities organized by function (logging, paths, I/O, validation, performance, hashing, dtypes)
- **No backward compatibility**: Direct import updates with no shims or compatibility layers
- **Enhanced maintainability**: Smaller, focused modules with clear responsibilities

### Phase 1.13 Highlights
- **Salesforce ID canonicalization**: All IDs converted to 18-char canonical form for consistency
- **Type safety improvements**: Comprehensive MyPy type annotations across all modules
- **Import hygiene**: Standardized all imports to absolute paths, eliminated module path conflicts
- **Code quality enforcement**: Strict QA gates (Black/Ruff/MyPy/PyTest) with zero tolerance for errors

### Phase 1.13.7 Highlights
- **Zero MyPy errors**: Complete type safety across entire codebase (33 source files)
- **Pandas typing fixes**: Resolved DataFrame indexing issues, boolean masking, arithmetic operations
- **Enhanced test coverage**: All 128 tests passing with comprehensive type annotations
- **Strict QA gates**: Black, Ruff, MyPy, and PyTest all green with zero errors
- **Import standardization**: All imports use absolute paths rooted at `src`
- **Runtime preservation**: All improvements maintain identical runtime behavior

## Performance Profiling

The pipeline includes comprehensive performance profiling utilities to monitor memory usage, timing, and detect performance regressions.

### Memory Tracking
Track memory usage across pipeline stages with automatic peak detection:
```python
from src.utils.perf_utils import track_memory_peak, log_memory_usage

# Track peak memory during a stage
with track_memory_peak("my_stage", logger):
    # Your code here
    pass

# Log current memory usage
log_memory_usage("checkpoint", logger)
```

### Stage Timing
Automatically time pipeline stages with memory delta tracking:
```python
from src.utils.perf_utils import time_stage

# Time a stage with automatic logging
with time_stage("my_stage", logger):
    # Your code here
    pass
```

### Performance Regression Detection
Compare current performance against baseline to detect regressions:
```python
from src.utils.perf_utils import (
    save_performance_baseline,
    detect_performance_regression
)

# Save baseline performance
baseline_data = {"time": 100.0, "memory": 50.0}
save_performance_baseline(baseline_data, "baseline.json")

# Detect regressions (10% threshold)
current_metrics = {"time": 120.0, "memory": 60.0}
is_regression = detect_performance_regression(
    "baseline.json", current_metrics, threshold=0.1
)
```

### Pipeline Integration
Performance tracking is automatically integrated into all major pipeline stages:
- **Candidate Generation**: Memory and timing for pair generation
- **Grouping**: Peak memory tracking for group creation
- **Survivorship**: Performance monitoring for primary selection
- **Disposition**: Timing for classification processing

### Performance Summary
The pipeline automatically logs performance summaries including:
- Final memory usage (RSS and VMS)
- Stage-by-stage timing and memory deltas
- Peak memory usage during heavy operations

---

## License
MIT License - see LICENSE file for details.