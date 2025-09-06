# Pipeline Cleanup Guide

This guide covers the pipeline cleanup utility for managing run artifacts and maintaining a clean workspace.

## Overview

The pipeline creates run artifacts in `data/processed/{run_id}` and `data/interim/{run_id}` directories. Over time, these can accumulate and consume significant disk space. The cleanup utility provides a safe way to manage these artifacts.

## Run Types

Pipeline runs are categorized by type for selective cleanup:

- **`dev`** (default) - Development runs for testing and experimentation
- **`test`** - Test runs for validation and quality assurance  
- **`prod`** - Production runs for live data processing

### Setting Run Type

Use the `--run-type` flag when running the pipeline:

```bash
# Development run (default)
python src/cleaning.py --input data.csv --outdir data/processed

# Test run
python src/cleaning.py --input data.csv --outdir data/processed --run-type test

# Production run
python src/cleaning.py --input data.csv --outdir data/processed --run-type prod
```

The run type is stored in both `review_meta.json` and `run_index.json` for cleanup operations.

## CLI Usage

### Basic Commands

```bash
# List all runs grouped by type
python pipeline_cleanup.py --list

# Preview deletions (recommended first step)
python pipeline_cleanup.py --delete-tests --dry-run
python pipeline_cleanup.py --delete-prod --dry-run
python pipeline_cleanup.py --delete-all --dry-run

# Actually delete (requires confirmation)
python pipeline_cleanup.py --delete-tests
python pipeline_cleanup.py --delete-prod
python pipeline_cleanup.py --delete-all
```

### Command Reference

| Command | Description |
|---------|-------------|
| `--list` | Show all runs grouped by type with counts |
| `--delete-tests` | Delete test runs |
| `--delete-prod` | Delete production runs |
| `--delete-all` | Delete all runs |
| `--dry-run` | Preview what would be deleted without actually deleting |

### Exit Codes

- **0** = No candidates found for deletion
- **2** = Candidates found (dry-run mode)
- **>0** = Errors occurred during execution

## Safety Features

### Fuse Protection

The cleanup utility respects the `PHASE1_DESTRUCTIVE_FUSE` environment variable:

```bash
# Disable destructive operations
export PHASE1_DESTRUCTIVE_FUSE=false
python pipeline_cleanup.py --delete-tests  # Will show error and refuse

# Enable destructive operations (default)
export PHASE1_DESTRUCTIVE_FUSE=true
python pipeline_cleanup.py --delete-tests  # Will proceed with confirmation
```

### Running Run Protection

The utility blocks deletion of currently running pipeline processes using `psutil` to detect active processes. Running runs are automatically skipped with a warning.

### Latest Run Protection

The latest successful run (pointed to by `data/processed/latest.json`) is never deleted to maintain a stable reference point.

### Confirmation Prompts

All destructive operations require explicit user confirmation:

```
⚠️  Are you sure you want to delete 3 test runs? (y/N): 
```

## Streamlit UI Integration

Run deletion is available in the Streamlit UI under "⚙️ Advanced: Maintenance" when enabled in configuration.

### Configuration

Enable run deletion in `config/settings.yaml`:

```yaml
ui:
  enable_run_deletion: true
  admin_mode: true
```

### UI Features

- **Preview** - Shows run count and total size before deletion
- **Delete** - Performs deletion with confirmation
- **Cancel** - Cancels the deletion operation
- **Status Display** - Shows success/error messages and warnings

## File Locations

### Run Artifacts

- **Processed**: `data/processed/{run_id}/`
- **Interim**: `data/interim/{run_id}/`
- **Index**: `data/processed/index/run_index.json`
- **Latest**: `data/processed/latest.json`

### Audit Logs

Deletion operations are logged to:
- `data/processed/audit/run_deletions.log`

## Legacy Run Handling

Runs created before the run type system are treated as `dev` runs with a warning logged:

```
WARNING: Legacy run cj20250905190659 missing run_type, treating as 'dev'
```

## Best Practices

1. **Always use `--dry-run` first** to preview what will be deleted
2. **Use specific run types** when running the pipeline for easier cleanup
3. **Keep production runs** longer than test runs
4. **Monitor disk usage** and clean up regularly
5. **Use the Streamlit UI** for interactive cleanup when available

## Troubleshooting

### Permission Errors

If you encounter permission errors, ensure the cleanup utility has write access to the data directories:

```bash
# Check permissions
ls -la data/processed/
ls -la data/interim/

# Fix permissions if needed
chmod -R 755 data/processed/
chmod -R 755 data/interim/
```

### Fuse Disabled

If the fuse is disabled, you'll see an error message:

```
Delete runs disabled: Phase 1 destructive fuse not enabled
```

Enable it with:

```bash
export PHASE1_DESTRUCTIVE_FUSE=true
```

### Running Process Detection

If the utility incorrectly detects a running process, you can check for active processes:

```bash
# Check for active cleaning.py processes
ps aux | grep cleaning.py
```

## Migration from Old Cleanup Script

The old `tools/cleanup_test_artifacts.py` has been deprecated. Use the new `pipeline_cleanup.py` instead:

```bash
# Old way (deprecated)
python tools/cleanup_test_artifacts.py --types test,dev --older-than 7

# New way (current)
python pipeline_cleanup.py --delete-tests --dry-run
python pipeline_cleanup.py --delete-tests
```

See `deprecated/README.md` for more migration information.
