#!/usr/bin/env python3
"""
PyArrow Usage Policy Enforcer for Phase 1.35.4.

This script enforces the PyArrow usage policy by checking that:
1. No PyArrow imports exist outside of allowed modules
2. Only I/O utilities and tests can use PyArrow
3. Stats/aggregation code must use DuckDB instead

Usage:
    python scripts/enforce_pyarrow_policy.py
"""

import os
import sys
import re
from pathlib import Path
from typing import List, Dict, Any


# Allowed modules that can import PyArrow
ALLOWED_PYARROW_MODULES = {
    "src/utils/io_utils.py",  # I/O utilities
    "tests/",                  # All test files
    "scripts/",                # Scripts (including this one)
}

# Forbidden patterns in stats/aggregation code
FORBIDDEN_PATTERNS = [
    r"import pyarrow",
    r"from pyarrow",
    r"pa\.",
    r"pyarrow\.",
    r"pyarrow\.compute",
    r"pyarrow\.parquet",
    r"pyarrow\.Table",
    r"pyarrow\.Array",
    r"pyarrow\.Schema",
]

# Stats/aggregation related directories
STATS_DIRECTORIES = {
    "src/performance.py",
    "src/utils/ui_helpers.py",
    "src/cleaning.py",
    "src/grouping.py",
    "src/similarity.py",
    "src/survivorship.py",
    "src/disposition.py",
    "src/alias_matching.py",
}


def find_pyarrow_imports(file_path: str) -> List[Dict[str, Any]]:
    """
    Find PyArrow imports in a file.
    
    Args:
        file_path: Path to the file to check
        
    Returns:
        List of import violations with line numbers and context
    """
    violations = []
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            
        for line_num, line in enumerate(lines, 1):
            for pattern in FORBIDDEN_PATTERNS:
                if re.search(pattern, line, re.IGNORECASE):
                    violations.append({
                        'file': file_path,
                        'line': line_num,
                        'pattern': pattern,
                        'content': line.strip(),
                        'severity': 'ERROR'
                    })
                    
    except Exception as e:
        violations.append({
            'file': file_path,
            'line': 0,
            'pattern': 'FILE_READ_ERROR',
            'content': f"Could not read file: {e}",
            'severity': 'WARNING'
        })
    
    return violations


def is_allowed_module(file_path: str) -> bool:
    """
    Check if a module is allowed to use PyArrow.
    
    Args:
        file_path: Path to the file
        
    Returns:
        True if PyArrow usage is allowed
    """
    file_path_str = str(file_path)
    
    for allowed in ALLOWED_PYARROW_MODULES:
        if allowed in file_path_str:
            return True
    
    return False


def scan_directory(directory: str) -> List[Dict[str, Any]]:
    """
    Scan a directory for PyArrow usage violations.
    
    Args:
        directory: Directory to scan
        
    Returns:
        List of all violations found
    """
    all_violations = []
    
    for root, dirs, files in os.walk(directory):
        # Skip common directories that shouldn't contain Python code
        dirs[:] = [d for d in dirs if not d.startswith('.') and d not in ['__pycache__', 'node_modules', 'venv', '.venv']]
        
        for file in files:
            if file.endswith('.py'):
                file_path = os.path.join(root, file)
                
                # Check if this module is allowed to use PyArrow
                if is_allowed_module(file_path):
                    continue
                
                # Check for PyArrow imports
                violations = find_pyarrow_imports(file_path)
                all_violations.extend(violations)
    
    return all_violations


def generate_report(violations: List[Dict[str, Any]]) -> str:
    """
    Generate a human-readable report of violations.
    
    Args:
        violations: List of violations found
        
    Returns:
        Formatted report string
    """
    if not violations:
        return "✅ No PyArrow usage policy violations found!"
    
    # Group violations by file
    violations_by_file = {}
    for violation in violations:
        file_path = violation['file']
        if file_path not in violations_by_file:
            violations_by_file[file_path] = []
        violations_by_file[file_path].append(violation)
    
    report = "❌ PyArrow Usage Policy Violations Found!\n\n"
    report += "The following files contain PyArrow imports outside of allowed modules:\n\n"
    
    for file_path, file_violations in violations_by_file.items():
        report += f"📁 {file_path}:\n"
        for violation in file_violations:
            if violation['pattern'] != 'FILE_READ_ERROR':
                report += f"  Line {violation['line']}: {violation['content']}\n"
                report += f"        Pattern: {violation['pattern']}\n"
            else:
                report += f"  {violation['content']}\n"
        report += "\n"
    
    report += "\n🔧 Remediation Steps:\n"
    report += "1. Remove PyArrow imports from stats/aggregation code\n"
    report += "2. Use DuckDB for group statistics computation\n"
    report += "3. Use DuckDB for Parquet I/O operations\n"
    report += "4. Keep PyArrow only in I/O utilities and tests\n"
    report += "\n📋 Allowed PyArrow Usage:\n"
    for allowed in ALLOWED_PYARROW_MODULES:
        report += f"  - {allowed}\n"
    
    return report


def main():
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
