# Deprecated Files

This directory contains deprecated files that have been superseded by newer implementations.

## Cleanup Script

- **Old**: `tools/cleanup_test_artifacts.py` (deprecated)
- **New**: `pipeline_cleanup.py` (current)

The old cleanup script has been replaced with a simpler, safer cleanup utility. Use `pipeline_cleanup.py` for all cleanup operations.

### Quick Migration

```bash
# Old way (deprecated)
python tools/cleanup_test_artifacts.py --types test,dev --older-than 7

# New way (current)
python pipeline_cleanup.py --delete-tests --dry-run
python pipeline_cleanup.py --delete-tests
```

See `pipeline_cleanup.py --help` for full usage information.
