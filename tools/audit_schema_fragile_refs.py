#!/usr/bin/env python3
"""Schema-fragile references audit script.

This script searches for hardcoded references to columns that may not exist
in all parquet files, helping identify potential schema mismatches.

Usage:
    python tools/audit_schema_fragile_refs.py
"""

import re
import sys
from pathlib import Path

# Schema-fragile column names to search for
FRAGILE_COLUMNS = {
    "is_primary",
    "weakest_edge_to_primary",
    "primary_name",
    "WEAKEST_EDGE_TO_PRIMARY",
    "IS_PRIMARY",
    "PRIMARY_NAME",
}

# Directories to search
SEARCH_DIRS = ["src", "app"]

# Files to exclude from search
EXCLUDE_PATTERNS = {
    "schema_utils.py",  # Constants definitions
    "__pycache__",  # Python cache
    ".pyc",  # Compiled Python
    ".pyo",  # Optimized Python
    ".git",  # Git directory
}

# Patterns to exclude (comments, markdown, etc.)
EXCLUDE_LINE_PATTERNS = [
    r"^\s*#",  # Comments
    r"^\s*\"\"\"",  # Docstring start
    r"^\s*'''",  # Docstring start
    r"^\s*\*",  # Comment continuation
    r"^\s*$",  # Empty lines
]

# Allowed patterns (legitimate uses)
ALLOWED_PATTERNS = [
    r"from.*schema_utils.*import.*",  # Import statements
    r"#.*",  # Comments
    r"\"\"\".*\"\"\"",  # Docstrings
    r"'''.*'''",  # Docstrings
    r"get_order_by\(.*context=",  # Context-aware usage
    r"build_sort_expression\(.*context=",  # Context-aware usage
    r"_build_where_clause\(.*available_columns",  # Conditional usage
    r"apply_filters_pyarrow\(.*available_columns",  # Conditional usage
    r"available_columns.*in.*",  # Availability checks
    r"if.*in.*available_columns",  # Availability checks
    r"WEAKEST_EDGE_TO_PRIMARY.*in.*available_columns",  # Explicit checks
    r"IS_PRIMARY.*in.*available_columns",  # Explicit checks
    r"PRIMARY_NAME.*in.*available_columns",  # Explicit checks
    r"if.*WEAKEST_EDGE_TO_PRIMARY.*in.*available_columns",  # Conditional checks
    r"if.*IS_PRIMARY.*in.*available_columns",  # Conditional checks
    r"if.*PRIMARY_NAME.*in.*available_columns",  # Conditional checks
    r"available_columns.*is.*None.*or.*WEAKEST_EDGE_TO_PRIMARY",  # Conditional checks
    r"available_columns.*is.*None.*or.*IS_PRIMARY",  # Conditional checks
    r"available_columns.*is.*None.*or.*PRIMARY_NAME",  # Conditional checks
    r"LEGACY_COLUMNS.*=.*\[",  # Legacy column definitions
    r"\"is_primary\":",  # Dictionary key definitions
    r"\"weakest_edge_to_primary\":",  # Dictionary key definitions
    r"\"primary_name\":",  # Dictionary key definitions
    r"DTYPES.*=.*{",  # Dtype definitions
    r"get_dtypes_for_schema",  # Schema function definitions
    r"group_data\[.*IS_PRIMARY.*\]",  # DataFrame column access (legitimate)
    r"group_data\[.*WEAKEST_EDGE_TO_PRIMARY.*\]",  # DataFrame column access (legitimate)
    r"group_data\[.*PRIMARY_NAME.*\]",  # DataFrame column access (legitimate)
    r"\.get\(.*is_primary.*\)",  # Safe .get() access
    r"\.get\(.*weakest_edge_to_primary.*\)",  # Safe .get() access
    r"\.get\(.*primary_name.*\)",  # Safe .get() access
    r"primary_record\.get\(",  # Safe primary record access
    r"primary_name.*or.*\"\"",  # Safe fallback patterns
    r"primary_name.*if.*primary_name",  # Safe conditional patterns
]

# Files that are allowed to use these columns (pipeline code that creates them)
ALLOWED_FILES = {
    "grouping.py",  # Creates these columns
    "disposition.py",  # Uses these columns safely
    "survivorship.py",  # Creates these columns
    "cleaning.py",  # Creates these columns
    "dtypes_map.py",  # Defines dtypes
    "group_stats.py",  # Computes stats from these columns
    "duckdb_group_stats.py",  # Computes stats from these columns
    "parity_validator.py",  # Validates these columns exist
}


def should_exclude_file(file_path: Path) -> bool:
    """Check if file should be excluded from search."""
    file_str = str(file_path)

    # Check exclude patterns
    if any(pattern in file_str for pattern in EXCLUDE_PATTERNS):
        return True

    # Check if file is in allowed list (pipeline code that creates these columns)
    file_name = file_path.name
    if file_name in ALLOWED_FILES:
        return True

    return False


def should_exclude_line(line: str) -> bool:
    """Check if line should be excluded from search."""
    # Check exclude patterns
    for pattern in EXCLUDE_LINE_PATTERNS:
        if re.match(pattern, line):
            return True

    # Check allowed patterns
    for pattern in ALLOWED_PATTERNS:
        if re.search(pattern, line):
            return True

    # Check for conditional context (lines that are inside conditional blocks)
    # This is a simple heuristic - if the line is indented and contains a column name,
    # check if the previous context suggests it's conditional
    if line.strip().startswith(
        ("if ", "elif ", "else:", "try:", "except:", "finally:"),
    ):
        return True

    # Check for lines that are clearly inside conditional blocks
    if re.match(r"^\s{4,}.*(if|elif|else|try|except|finally)", line):
        return True

    return False


def find_fragile_references(file_path: Path) -> list[tuple[int, str]]:
    """Find schema-fragile references in a file."""
    references = []

    try:
        with open(file_path, encoding="utf-8") as f:
            lines = f.readlines()

        for line_num, line in enumerate(lines, 1):
            if should_exclude_line(line):
                continue

            # Check for fragile column references
            for column in FRAGILE_COLUMNS:
                if column in line:
                    references.append((line_num, line.strip()))
                    break  # Only report each line once

    except Exception as e:
        print(f"Error reading {file_path}: {e}", file=sys.stderr)

    return references


def audit_directory(directory: Path) -> list[tuple[Path, int, str]]:
    """Audit a directory for schema-fragile references."""
    all_references: list[tuple[Path, int, str]] = []

    if not directory.exists():
        print(f"Directory {directory} does not exist", file=sys.stderr)
        return all_references

    for file_path in directory.rglob("*.py"):
        if should_exclude_file(file_path):
            continue

        references = find_fragile_references(file_path)
        for line_num, line_content in references:
            all_references.append((file_path, line_num, line_content))

    return all_references


def main() -> int:
    """Main audit function."""
    print("🔍 Auditing for schema-fragile references...")
    print(f"Searching for: {', '.join(sorted(FRAGILE_COLUMNS))}")
    print(f"Searching in: {', '.join(SEARCH_DIRS)}")
    print()

    # Get project root
    project_root = Path(__file__).parent.parent

    all_references = []

    # Audit each directory
    for dir_name in SEARCH_DIRS:
        dir_path = project_root / dir_name
        print(f"📁 Auditing {dir_name}/...")

        references = audit_directory(dir_path)
        all_references.extend(references)

        if references:
            print(f"   Found {len(references)} potential issues")
        else:
            print("   ✅ No issues found")

    print()

    # Report results
    if all_references:
        print("⚠️  POTENTIAL SCHEMA-FRAGILE REFERENCES FOUND:")
        print("=" * 60)

        for file_path, line_num, line_content in all_references:
            rel_path = file_path.relative_to(project_root)
            print(f"{rel_path}:{line_num}")
            print(f"  {line_content}")
            print()

        print(f"Total: {len(all_references)} potential issues")
        print()
        print("💡 RECOMMENDATIONS:")
        print(
            "- Use context-aware functions: get_order_by(sort_key, context='group_details')",
        )
        print(
            "- Use conditional filtering: _build_where_clause(filters, available_columns)",
        )
        print("- Check column availability: if COLUMN in available_columns")
        print("- Use schema constants from src.utils.schema_utils")

        return 1  # Exit with error code
    print("✅ No schema-fragile references found!")
    print("All column references appear to be properly handled.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
