# Legacy Files - Pre-Run-Scoped System

**Date Moved:** 2025-08-30  
**Reason:** Cleanup of pre-Phase 1.16 legacy files after implementing run-scoped caching

## Files Moved

### From `data/interim/` (Intermediate Pipeline Artifacts)
- `accounts_filtered.parquet` - Filtered account records
- `accounts_normalized.parquet` - Normalized account records  
- `alias_matches.parquet` - Alias matching results
- `block_top_tokens.csv` - Blocking token statistics
- `candidate_pairs.parquet` - Generated candidate pairs
- `dispositions.parquet` - Disposition classification results
- `groups.parquet` - Duplicate group assignments
- `pipeline_state.json` - Legacy pipeline state file
- `survivorship.parquet` - Survivorship selection results

### From `data/processed/` (Final Output Artifacts)
- `review_meta.json` - Review metadata
- `review_ready.csv` - Final review-ready CSV output
- `review_ready.parquet` - Final review-ready Parquet output

## Context

These files were created by the pipeline before **Phase 1.16** introduced run-scoped caching. In the new system:

- All pipeline artifacts are stored in run-scoped directories: `data/interim/{run_id}/` and `data/processed/{run_id}/`
- Each run has its own isolated set of artifacts
- The `run_index.json` tracks all runs and their metadata
- The `latest` pointer (symlink + JSON) points to the most recent successful run

## Current System

The current system uses:
- **Run-scoped directories** for all artifacts
- **Versioned caching** with unique run IDs
- **Atomic operations** for state management
- **Audit logging** for all operations

These legacy files are preserved for reference but are no longer part of the active pipeline system.
