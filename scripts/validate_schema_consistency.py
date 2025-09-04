#!/usr/bin/env python3
"""
Schema Consistency Validator

This script validates that all DataFrame column references match the canonical schema constants.
It prevents silent casing drift and ensures consistent column naming across the pipeline.

Usage:
    python scripts/validate_schema_consistency.py
"""

import os
import re
import sys
from pathlib import Path
from typing import Dict, List, Set, Tuple

# Canonical schema constants from schema_utils.py
CANONICAL_COLUMNS = {
    "GROUP_ID": "group_id",
    "ACCOUNT_ID": "account_id", 
    "ACCOUNT_NAME": "account_name",
    "DISPOSITION": "disposition",  # Lowercase
    "DISPOSITION_REASON": "disposition_reason",
    "GROUP_SIZE": "group_size",
    "MAX_SCORE": "max_score",
    "PRIMARY_NAME": "primary_name",
    "IS_PRIMARY": "is_primary",
    "WEAKEST_EDGE_TO_PRIMARY": "weakest_edge_to_primary",
    "SUFFIX_CLASS": "suffix_class",
    "CREATED_DATE": "created_date",
    "NAME_CORE": "name_core",
    "ALIAS_CROSS_REFS": "alias_cross_refs",
    "HAS_MULTIPLE_NAMES": "has_multiple_names",
}

# Directories to scan
SCAN_DIRS = [
    "src/",
    "app/",
    "tests/",
]

# Files to exclude
EXCLUDE_FILES = {
    "scripts/validate_schema_consistency.py",  # This file itself
    "src/utils/schema_utils.py",  # Schema constants file
    "src/dtypes_map.py",  # Will be validated separately
}

# Patterns to find column references
COLUMN_PATTERNS = [
    r'df\[["\']([^"\']+)["\']\]',  # df["column_name"]
    r'df\.([a-zA-Z_][a-zA-Z0-9_]*)',  # df.column_name
    r'\[["\']([^"\']+)["\']\]',  # ["column_name"]
    r'\.([a-zA-Z_][a-zA-Z0-9_]*)',  # .column_name in DataFrame operations
]

def find_column_references(file_path: Path) -> Set[str]:
    """Find all column references in a Python file."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
    except Exception as e:
        print(f"Warning: Could not read {file_path}: {e}")
        return set()
    
    columns = set()
    
    for pattern in COLUMN_PATTERNS:
        matches = re.findall(pattern, content)
        columns.update(matches)
    
    return columns

def validate_dtypes_map() -> Tuple[bool, List[str]]:
    """Validate that dtypes_map.py uses canonical column names."""
    dtypes_file = Path("src/dtypes_map.py")
    if not dtypes_file.exists():
        return True, []
    
    try:
        with open(dtypes_file, 'r', encoding='utf-8') as f:
            content = f.read()
    except Exception as e:
        return False, [f"Could not read dtypes_map.py: {e}"]
    
    errors = []
    
    # Check DTYPES dictionary
    dtypes_match = re.search(r'DTYPES\s*=\s*\{([^}]+)\}', content, re.DOTALL)
    if dtypes_match:
        dtypes_content = dtypes_match.group(1)
        
        # Find all column names in DTYPES
        column_matches = re.findall(r'["\']([^"\']+)["\']\s*:', dtypes_content)
        
        for col in column_matches:
            if col in CANONICAL_COLUMNS.values():
                # This is a canonical column name
                continue
            elif col.lower() in [canonical.lower() for canonical in CANONICAL_COLUMNS.values()]:
                # Found a case mismatch
                canonical = next(
                    canonical for canonical in CANONICAL_COLUMNS.values() 
                    if canonical.lower() == col.lower()
                )
                errors.append(f"dtypes_map.py: '{col}' should be '{canonical}'")
    
    return len(errors) == 0, errors

def scan_directory_for_column_references() -> Tuple[bool, List[str]]:
    """Scan directories for column references and validate against canonical schema."""
    errors = []
    warnings = []
    
    for scan_dir in SCAN_DIRS:
        scan_path = Path(scan_dir)
        if not scan_path.exists():
            continue
            
        for file_path in scan_path.rglob("*.py"):
            if str(file_path) in EXCLUDE_FILES:
                continue
                
            columns = find_column_references(file_path)
            
            for col in columns:
                # Skip non-column-like strings
                if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', col):
                    continue
                    
                # Check if this looks like a column name
                if col.lower() in [canonical.lower() for canonical in CANONICAL_COLUMNS.values()]:
                    canonical = next(
                        canonical for canonical in CANONICAL_COLUMNS.values() 
                        if canonical.lower() == col.lower()
                    )
                    
                    if col != canonical:
                        errors.append(f"{file_path}: '{col}' should be '{canonical}'")
                    elif col in CANONICAL_COLUMNS.values():
                        # This is a canonical column name - good!
                        pass
                    else:
                        # Potential column name that's not in canonical schema
                        warnings.append(f"{file_path}: '{col}' not in canonical schema")
    
    return len(errors) == 0, errors, warnings

def main():
    """Main validation function."""
    print("🔍 Validating Schema Consistency...")
    print("=" * 50)
    
    # Validate dtypes_map.py
    print("\n1. Validating dtypes_map.py...")
    dtypes_valid, dtypes_errors = validate_dtypes_map()
    
    if dtypes_errors:
        print("❌ DTYPES map validation failed:")
        for error in dtypes_errors:
            print(f"   {error}")
    else:
        print("✅ DTYPES map validation passed")
    
    # Scan for column references
    print("\n2. Scanning for column references...")
    scan_valid, scan_errors, scan_warnings = scan_directory_for_column_references()
    
    if scan_errors:
        print("❌ Column reference validation failed:")
        for error in scan_errors:
            print(f"   {error}")
    else:
        print("✅ Column reference validation passed")
    
    if scan_warnings:
        print("\n⚠️  Potential column names not in canonical schema:")
        for warning in scan_warnings[:10]:  # Limit warnings
            print(f"   {warning}")
        if len(scan_warnings) > 10:
            print(f"   ... and {len(scan_warnings) - 10} more")
    
    # Summary
    print("\n" + "=" * 50)
    total_errors = len(dtypes_errors) + len(scan_errors)
    
    if total_errors == 0:
        print("🎉 All schema validations passed!")
        return 0
    else:
        print(f"❌ Schema validation failed with {total_errors} errors")
        print("\nTo fix these issues:")
        print("1. Update column references to use canonical casing")
        print("2. Ensure dtypes_map.py uses canonical column names")
        print("3. Use schema constants from src/utils/schema_utils.py")
        return 1

if __name__ == "__main__":
    sys.exit(main())
