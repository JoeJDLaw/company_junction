#!/usr/bin/env python3
"""PyArrow Usage Policy Enforcer for Phase 1.35.4.

This script enforces the PyArrow usage policy by checking that:
1. No PyArrow imports exist outside of allowed modules
2. Only I/O utilities and tests can use PyArrow
3. Stats/aggregation code must use DuckDB instead

Usage:
    python scripts/enforce_pyarrow_policy.py
"""

import os
import re
import sys
from pathlib import Path
from typing import Any, Dict, List

# Allowed files that can import PyArrow (but with restrictions)
ALLOWED_FILES = {
    "src/utils/io_utils.py",
    "src/utils/parquet_size_reporter.py",
    "tests/",
    "scripts/",
}

# ui_helpers.py should NOT be allowlisted - it's stats-facing

# Allowed import lines (only in allowed files)
ALLOWED_IMPORT_LINES = [
    r"^\s*import\s+pyarrow\s+as\s+pa\s*$",
    r"^\s*import\s+pyarrow\.parquet\s+as\s+pq\s*$",
]

# Forbidden anywhere (no PyArrow compute/dataset)
FORBIDDEN_ANYWHERE = [
    r"\bpyarrow\.compute\b",
    r"\bpyarrow\.dataset\b",
]

# Hard ban paths - no PyArrow import at all
HARD_BAN_PATHS = [
    "src/performance.py",
    "src/cleaning.py",
    "src/grouping.py",
    "src/similarity.py",
    "src/survivorship.py",
    "src/disposition.py",
    "src/alias_matching.py",
    "src/utils/ui_helpers.py",  # No PyArrow here either
]


def find_pyarrow_imports(file_path: str) -> List[Dict[str, Any]]:
    """Find PyArrow imports in a file according to new policy rules.

    Args:
        file_path: Path to the file to check

    Returns:
        List of import violations with line numbers and context

    """
    violations: List[Dict[str, Any]] = []

    try:
        with open(file_path, encoding="utf-8") as f:
            lines = f.readlines()

        # Check if file is in hard ban paths or allowed files
        # Convert to relative path for comparison
        try:
            relative_path = os.path.relpath(file_path, os.getcwd())
        except ValueError:
            relative_path = file_path

        is_hard_banned = any(relative_path.startswith(path) for path in HARD_BAN_PATHS)
        is_allowed = any(relative_path.startswith(path) for path in ALLOWED_FILES)

        for line_num, line in enumerate(lines, 1):
            # Check for forbidden patterns anywhere
            for pattern in FORBIDDEN_ANYWHERE:
                if re.search(pattern, line, re.IGNORECASE):
                    violations.append(
                        {
                            "file": file_path,
                            "line": line_num,
                            "pattern": pattern,
                            "content": line.strip(),
                            "severity": "ERROR",
                            "reason": "PyArrow compute/dataset usage forbidden everywhere",
                        },
                    )

            # Check for any PyArrow import in hard ban paths
            if is_hard_banned:
                if re.search(
                    r"\bimport\s+pyarrow\b|\bfrom\s+pyarrow\b",
                    line,
                    re.IGNORECASE,
                ):
                    violations.append(
                        {
                            "file": file_path,
                            "line": line_num,
                            "pattern": "PyArrow import in hard ban path",
                            "content": line.strip(),
                            "severity": "ERROR",
                            "reason": "No PyArrow imports allowed in stats/aggregation code",
                        },
                    )

            # Check for restricted imports in allowed files
            elif is_allowed:
                # Check for any PyArrow import not matching allowed patterns
                if re.search(
                    r"\bimport\s+pyarrow\b|\bfrom\s+pyarrow\b",
                    line,
                    re.IGNORECASE,
                ):
                    # Skip comment lines
                    if line.strip().startswith("#"):
                        continue

                    is_allowed_import = any(
                        re.search(pattern, line) for pattern in ALLOWED_IMPORT_LINES
                    )
                    if not is_allowed_import:
                        violations.append(
                            {
                                "file": file_path,
                                "line": line_num,
                                "pattern": "Restricted PyArrow import in allowed file",
                                "content": line.strip(),
                                "severity": "ERROR",
                                "reason": "Only specific PyArrow imports allowed in I/O utilities",
                            },
                        )

            # Check for any PyArrow import in other files
            elif re.search(
                r"\bimport\s+pyarrow\b|\bfrom\s+pyarrow\b",
                line,
                re.IGNORECASE,
            ):
                violations.append(
                    {
                        "file": file_path,
                        "line": line_num,
                        "pattern": "PyArrow import in non-allowed file",
                        "content": line.strip(),
                        "severity": "ERROR",
                        "reason": "PyArrow imports only allowed in designated I/O utilities",
                    },
                )

    except Exception as e:
        violations.append(
            {
                "file": file_path,
                "line": 0,
                "pattern": "FILE_READ_ERROR",
                "content": f"Could not read file: {e}",
                "severity": "WARNING",
            },
        )

    return violations


def scan_directory(directory: str) -> List[Dict[str, Any]]:
    """Scan a directory for PyArrow usage violations.

    Args:
        directory: Directory to scan

    Returns:
        List of all violations found

    """
    all_violations: List[Dict[str, Any]] = []

    for root, dirs, files in os.walk(directory):
        # Skip common directories that shouldn't contain Python code
        dirs[:] = [
            d
            for d in dirs
            if not d.startswith(".")
            and d not in ["__pycache__", "node_modules", "venv", ".venv"]
        ]

        for file in files:
            if file.endswith(".py"):
                file_path = os.path.join(root, file)

                # Scan ALL files - don't skip any
                violations = find_pyarrow_imports(file_path)
                all_violations.extend(violations)

    return all_violations


def generate_report(violations: List[Dict[str, Any]]) -> str:
    """Generate a human-readable report of violations.

    Args:
        violations: List of violations found

    Returns:
        Formatted report string

    """
    if not violations:
        return "✅ No PyArrow usage policy violations found!"

    # Group violations by file
    violations_by_file: dict[str, list[dict[str, str]]] = {}
    for violation in violations:
        file_path = violation["file"]
        if file_path not in violations_by_file:
            violations_by_file[file_path] = []
        violations_by_file[file_path].append(violation)

    report = "❌ PyArrow Usage Policy Violations Found!\n\n"
    report += (
        "The following files contain PyArrow imports outside of allowed modules:\n\n"
    )

    for file_path, file_violations in violations_by_file.items():
        report += f"📁 {file_path}:\n"
        for violation in file_violations:
            if violation["pattern"] != "FILE_READ_ERROR":
                report += f"  Line {violation['line']}: {violation['content']}\n"
                report += f"        Pattern: {violation['pattern']}\n"
                if "reason" in violation:
                    report += f"        Reason: {violation['reason']}\n"
            else:
                report += f"  {violation['content']}\n"
        report += "\n"

    report += "\n🔧 Remediation Steps:\n"
    report += "1. Remove PyArrow imports from stats/aggregation code\n"
    report += "2. Use DuckDB for group statistics computation\n"
    report += "3. Use DuckDB for Parquet I/O operations\n"
    report += "4. Keep PyArrow only in I/O utilities and tests\n"
    report += "\n📋 Allowed PyArrow Usage:\n"
    for allowed in ALLOWED_FILES:
        report += f"  - {allowed}\n"

    return report


def main() -> None:
    """Main entry point."""
    print("🔍 Enforcing PyArrow Usage Policy for Phase 1.35.4...")
    print("=" * 60)

    # Get the project root directory
    script_dir = Path(__file__).parent
    project_root = script_dir.parent

    print(f"📂 Scanning project root: {project_root}")

    # Scan for violations
    violations = scan_directory(str(project_root))

    # Generate and display report
    report = generate_report(violations)
    print(report)

    # Exit with error code if violations found
    if violations:
        print("❌ Policy enforcement failed. Please fix violations before proceeding.")
        sys.exit(1)
    else:
        print("✅ Policy enforcement passed!")
        sys.exit(0)


if __name__ == "__main__":
    main()
