"""
CI safety test to prevent hardcoded values from creeping back into the codebase.

This test scans Python files for hardcoded paths, artifact names, and other values
that should be configurable or use helper functions.
"""

import ast
import re
from pathlib import Path
from typing import List


# Only scan these code roots for hardcoded values
CODE_ROOTS = ["src/", "app/", "tools/", "scripts/"]
# Exclude these patterns
EXCLUDE_PATTERNS = [
    "tests/",  # Test files are excluded
    "deprecated/",
    "*.ipynb",
    "*.md",
    "*.yaml",
    "*.yml",
    "schema_utils.py",  # Column constants are allowed here
]

# Banned patterns for hardcoded values
BANNED_PATTERNS = {
    # Hardcoded config paths (only in src/ and app/)
    "config_paths": {
        "pattern": r'["\']config/settings\.yaml["\']',
        "description": "Hardcoded config path - use get_config_path()",
        "files": ["src/", "app/"],
    },
    # Hardcoded data directory paths (only in src/ and app/)
    "data_paths": {
        "pattern": r'["\']data/(processed|interim|raw)["\']',
        "description": "Hardcoded data path - use path_utils functions",
        "files": ["src/", "app/"],
    },
    # Hardcoded artifact filenames (only in src/ and app/)
    "artifact_names": {
        "pattern": r'["\'](group_stats|group_details|review_ready|pipeline_state)\.(parquet|csv|json)["\']',
        "description": "Hardcoded artifact name - use get_artifact_path()",
        "files": ["src/", "app/"],
    },
    # Hardcoded magic numbers as assigned constants (only in src/ and app/)
    "magic_numbers": {
        "pattern": r'^\s*[A-Z_][A-Z0-9_]*\s*=\s*["\']?(\d{2,})["\']?\s*$',
        "description": "Magic number constant - move to config/settings.yaml",
        "files": ["src/", "app/"],
    },
    # Hardcoded backend assumptions (only in src/ and app/)
    "backend_assumptions": {
        "pattern": r'["\'](loky|threading)["\']',
        "description": "Hardcoded backend - load from config",
        "files": ["src/", "app/"],
    },
}


def is_excluded_file(file_path: Path) -> bool:
    """Check if file should be excluded from scanning."""
    file_str = str(file_path)

    # Check exclude patterns
    for pattern in EXCLUDE_PATTERNS:
        if pattern in file_str:
            return True

    # Only scan files in code roots
    in_code_root = any(file_str.startswith(root) for root in CODE_ROOTS)
    if not in_code_root:
        return True

    return False


def extract_string_literals(file_path: Path) -> List[tuple]:
    """Extract string literals from Python file, excluding comments and docstrings."""
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read()

        # Parse AST to identify string literals
        tree = ast.parse(content)
        string_literals = []

        for node in ast.walk(tree):
            if isinstance(node, ast.Str):
                # Get line number and value
                line_no = getattr(node, "lineno", 0)
                value = node.s
                string_literals.append((line_no, value))

        return string_literals
    except (SyntaxError, UnicodeDecodeError):
        # Skip files with syntax errors
        return []


def check_file_for_hardcoded_values(file_path: Path) -> List[dict]:
    """Check a single file for hardcoded values."""
    violations = []

    if is_excluded_file(file_path):
        return violations

    # Extract string literals
    string_literals = extract_string_literals(file_path)

    for line_no, value in string_literals:
        for pattern_name, pattern_info in BANNED_PATTERNS.items():
            # Check if this file type should be scanned for this pattern
            if not any(file_path.match(f"{root}*") for root in pattern_info["files"]):
                continue

            # Check pattern
            if re.search(pattern_info["pattern"], value):
                # Get context (few lines around the match)
                try:
                    with open(file_path, "r", encoding="utf-8") as f:
                        lines = f.readlines()
                        start = max(0, line_no - 2)
                        end = min(len(lines), line_no + 1)
                        context = "".join(lines[start:end])
                except Exception:
                    context = "Context unavailable"

                violations.append(
                    {
                        "file": str(file_path),
                        "line": line_no,
                        "pattern": pattern_name,
                        "match": value,
                        "description": pattern_info["description"],
                        "context": context,
                    }
                )

    return violations


def find_python_files() -> List[Path]:
    """Find all Python files in the project."""
    project_root = Path(__file__).parent.parent
    python_files = []

    for root in CODE_ROOTS:
        root_path = project_root / root.rstrip("/")
        if root_path.exists():
            python_files.extend(root_path.rglob("*.py"))

    return python_files


def test_no_hardcoded_literals():
    """Test that no hardcoded values exist in the codebase."""
    python_files = find_python_files()

    all_violations = []
    for file_path in python_files:
        violations = check_file_for_hardcoded_values(file_path)
        all_violations.extend(violations)

    if all_violations:
        # Format violations for better readability
        violation_messages = []
        for v in all_violations:
            msg = f"\nFile: {v['file']}:{v['line']}"
            msg += f"\nPattern: {v['pattern']}"
            msg += f"\nMatch: {v['match']}"
            msg += f"\nDescription: {v['description']}"
            msg += f"\nContext: {v['context']}"
            violation_messages.append(msg)

        raise AssertionError(
            f"Found {len(all_violations)} hardcoded values that violate Phase 1.25.1 guardrails:"
            + "".join(violation_messages)
        )


def test_path_utils_functions_exist():
    """Test that path utility functions exist and work."""
    from src.utils.path_utils import (
        get_config_path,
        get_processed_dir,
        get_interim_dir,
        get_artifact_path,
    )

    # Test basic functionality
    config_path = get_config_path()
    assert config_path.exists()
    assert config_path.name == "settings.yaml"

    processed_dir = get_processed_dir("test_run")
    assert "processed" in str(processed_dir)
    assert "test_run" in str(processed_dir)

    interim_dir = get_interim_dir("test_run")
    assert "interim" in str(interim_dir)
    assert "test_run" in str(interim_dir)

    artifact_path = get_artifact_path("test_run", "test.parquet")
    assert "test_run" in str(artifact_path)
    assert "test.parquet" in str(artifact_path)


def test_schema_utils_constants_exist():
    """Test that schema utility constants exist."""
    from src.utils.schema_utils import (
        GROUP_ID,
        ACCOUNT_ID,
        ACCOUNT_NAME,
        DISPOSITION,
        get_canonical_columns,
    )

    # Test that constants are strings
    assert isinstance(GROUP_ID, str)
    assert isinstance(ACCOUNT_ID, str)
    assert isinstance(ACCOUNT_NAME, str)
    assert isinstance(DISPOSITION, str)

    # Test that get_canonical_columns returns a dict
    columns = get_canonical_columns()
    assert isinstance(columns, dict)
    assert len(columns) > 0


def test_config_structure():
    """Test that config/settings.yaml contains expected structure."""
    from src.utils.path_utils import get_config_path
    import yaml

    config_path = get_config_path()
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    # Check for required sections
    assert "ui" in config, "Missing 'ui' section in config"
    assert "parallelism" in config, "Missing 'parallelism' section in config"

    # Check for specific keys that replace hardcoded values
    ui_section = config.get("ui", {})
    assert "timeout_seconds" in ui_section, "Missing 'ui.timeout_seconds' in config"
    assert "cache_capacity" in ui_section, "Missing 'ui.cache_capacity' in config"

    parallelism_section = config.get("parallelism", {})
    assert "backend" in parallelism_section, "Missing 'parallelism.backend' in config"
    assert (
        "chunk_size" in parallelism_section
    ), "Missing 'parallelism.chunk_size' in config"
