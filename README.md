# Company Junction Deduplication Pipeline

## Project Status
**Phase 1 complete** ✅ — The system identifies likely duplicate Accounts, proposes a primary record using Relationship rank + Created Date, and assigns Disposition (`Keep`, `Update`, `Delete`, `Verify`). No Salesforce writes are performed in this phase.

**Phase 1.5 (refinements)**: Conservative alias extraction (semicolon & numbered sequences; parentheses only when content looks like a company), high-confidence alias matching (suffix match + score ≥ high), minimal Streamlit alias UI.

**Phase 1.7 (UX & Manual Controls)**: Review UX improvements (disposition table, sorting, better layout), manual disposition overrides, manual blacklist editor, JSON persistence with audit trail.

**Phase 1.8 (Blacklist Visibility & Filter Improvements)**: Three-pane blacklist visibility (built-in/manual/effective), filter tooltips and functionality fixes, Account Name sorting options.

**Phase 1.9 (Blacklist Improvements & Centralized I/O)**: Word-boundary blacklist matching, centralized manual I/O operations, audit snapshots, pipeline command generator.

**Phase 1.10 (Performance & Memory Hardening)**: Performance logging infrastructure, enhanced token filtering, stop token logic, block visibility, memory safety improvements.

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
- **Manual data:** `data/manual/` (Phase 1.7)
  - `manual_dispositions.json` (overrides)
  - `manual_blacklist.json` (pattern rules)
- **Final review file:** `data/processed/review_ready.csv`
- **Config:** `config/settings.yaml`, `config/relationship_ranks.csv`

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
1. Install dependencies: `pip install -r requirements.txt`  
2. Run the Streamlit app: `streamlit run app/main.py`  
3. Upload your Salesforce CSV and review results interactively.  

## Prerequisites
- Python 3.10+
- Salesforce CLI installed and authenticated

## Usage Examples
```bash
# Run cleaning pipeline directly
python src/cleaning.py --input data/raw/my_export.csv --output data/interim/cleaned.csv

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

## License
MIT License - see LICENSE file for details.