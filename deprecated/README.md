# Deprecated Files

This folder contains files that have been moved or retired during the Phase 1.12 Utils Package Refactor.

## Moved Files

### src/utils.py → src/utils/ (Phase 1.12)
- **Date**: December 2024
- **Reason**: Refactored into logical modules to eliminate import ambiguity and improve code organization
- **New Location**: Functions moved to:
  - `src/utils/logging_utils.py` - `setup_logging`
  - `src/utils/path_utils.py` - `get_project_root`, `ensure_directory_exists`, `get_data_paths`
  - `src/utils/validation_utils.py` - `validate_dataframe`
  - `src/utils/io_utils.py` - `get_file_info`, `list_data_files`, `load_settings`, `load_relationship_ranks`
  - `src/utils/perf_utils.py` - `log_perf`
  - `src/utils/hash_utils.py` - `config_hash`, `stable_group_id`, `_compute_config_hash`
- **Deletion Date**: Can be deleted after Phase 1.12 is complete and all imports are verified

## Notes
- All functions maintain the same behavior and API
- Import paths have been updated to use absolute imports rooted at `src`
- No backward compatibility shims were created
