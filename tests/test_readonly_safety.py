"""Regression tests for Phase 1 read-only safety."""

import ast
import os
from pathlib import Path
from typing import Set, Union

import pytest


def find_destructive_functions() -> Set[str]:
    """Scan code for potentially destructive function calls that are NOT gated."""
    destructive_patterns = {
        "delete_run",
        "remove_run",
        "cleanup_run",
        "prune",
        "gc_runs",
        "os.remove",
        "Path.unlink",
        "shutil.rmtree",
        "drop table",
        "delete from",
    }

    found_functions = set()

    # Scan all Python files in src/ and app/ directories
    for root, dirs, files in os.walk("."):
        # Skip test files, deprecated, and other excluded directories
        if any(
            skip in root
            for skip in ["tests/", "deprecated/", ".venv/", "__pycache__/", ".git/"]
        ):
            continue

        for file in files:
            if file.endswith(".py"):
                file_path = Path(root) / file
                try:
                    with open(file_path) as f:
                        content = f.read()

                    # Parse AST to find function calls
                    tree = ast.parse(content)
                    for node in ast.walk(tree):
                        if isinstance(node, ast.Call):
                            if isinstance(node.func, ast.Name):
                                func_name = node.func.id
                                if any(
                                    pattern in func_name
                                    for pattern in destructive_patterns
                                ):
                                    # Check if this call is gated by a fuse
                                    if not is_gated_by_fuse(node, content):
                                        found_functions.add(
                                            f"{file_path}:{node.lineno}:{func_name}",
                                        )
                            elif isinstance(node.func, ast.Attribute):
                                if isinstance(node.func.value, ast.Name):
                                    module_name = node.func.value.id
                                    attr_name = node.func.attr
                                    full_name = f"{module_name}.{attr_name}"
                                    if any(
                                        pattern in full_name
                                        for pattern in destructive_patterns
                                    ):
                                        # Check if this call is gated by a fuse
                                        if not is_gated_by_fuse(node, content):
                                            found_functions.add(
                                                f"{file_path}:{node.lineno}:{full_name}",
                                            )

                except (SyntaxError, UnicodeDecodeError):
                    # Skip files with syntax errors or encoding issues
                    continue

    return found_functions


def is_gated_by_fuse(node: Union[ast.Call, ast.Delete], content: str) -> bool:
    """Check if a function call or delete operation is gated by a Phase 1 fuse."""
    # Get the line number of the operation
    lineno = node.lineno

    # Look at the context around the operation
    lines = content.split("\n")
    start_line = max(0, lineno - 20)  # Look at 20 lines before
    end_line = min(len(lines), lineno + 5)  # Look at 5 lines after

    context_lines = lines[start_line:end_line]
    context = "\n".join(context_lines)

    # Check for fuse patterns
    fuse_patterns = [
        "PHASE_1_DESTRUCTIVE_FUSE",
        "_get_destructive_fuse",
        "if not _get_destructive_fuse():",
        "if _get_destructive_fuse():",
        "destructive.*fuse",
        "phase.*fuse",
    ]

    # Also check if we're inside a function that has a fuse check
    # Look for function definitions that contain fuse checks
    if any(
        pattern in context
        for pattern in ["PHASE_1_DESTRUCTIVE_FUSE", "_get_destructive_fuse"]
    ):
        return True

    # Look for the function definition that contains this operation
    for i in range(start_line, -1, -1):
        line = lines[i].strip()
        if line.startswith("def "):
            # Found a function definition, check if it has a fuse
            func_start = i
            func_end = len(lines)
            # Find the end of the function
            for j in range(func_start + 1, len(lines)):
                if lines[j].strip().startswith("def ") and lines[j].strip() != "":
                    func_end = j
                    break

            func_content = "\n".join(lines[func_start:func_end])
            if (
                "PHASE_1_DESTRUCTIVE_FUSE" in func_content
                or "_get_destructive_fuse" in func_content
            ):
                return True
            break

    return any(pattern.lower() in context.lower() for pattern in fuse_patterns)


def find_direct_run_index_writes() -> Set[str]:
    """Find direct writes to run_index.json that remove runs and are NOT gated."""
    found_writes = set()

    for root, dirs, files in os.walk("."):
        if any(
            skip in root
            for skip in ["tests/", "deprecated/", ".venv/", "__pycache__/", ".git/"]
        ):
            continue

        for file in files:
            if file.endswith(".py"):
                file_path = Path(root) / file
                try:
                    with open(file_path) as f:
                        content = f.read()

                    # Parse AST to find del statements
                    tree = ast.parse(content)
                    for node in ast.walk(tree):
                        if isinstance(node, ast.Delete):
                            for target in node.targets:
                                if isinstance(target, ast.Subscript):
                                    if (
                                        isinstance(target.value, ast.Name)
                                        and target.value.id == "run_index"
                                    ):
                                        # Check if this deletion is gated by a fuse
                                        if not is_gated_by_fuse(node, content):
                                            found_writes.add(
                                                f"{file_path}:{node.lineno}:del run_index[...]",
                                            )

                except (SyntaxError, UnicodeDecodeError):
                    continue

    return found_writes


def check_maintenance_ui_copy() -> bool:
    """Check that maintenance UI shows the correct read-only copy."""
    maintenance_file = Path("app/components/maintenance.py")

    if not maintenance_file.exists():
        return False

    try:
        with open(maintenance_file) as f:
            content = f.read()

        expected_copy = (
            "Run deletion functionality will be implemented in a future phase."
        )
        return expected_copy in content

    except (OSError, UnicodeDecodeError):
        return False


def check_sidebar_placement() -> bool:
    """Check that maintenance is rendered in sidebar."""
    maintenance_file = Path("app/components/maintenance.py")

    if not maintenance_file.exists():
        return False

    try:
        with open(maintenance_file) as f:
            content = f.read()

        # Should use st.sidebar.subheader
        return "st.sidebar.subheader" in content

    except (OSError, UnicodeDecodeError):
        return False


class TestReadOnlySafety:
    """Test that Phase 1 maintains read-only posture."""

    def test_no_destructive_functions_in_code(self) -> None:
        """Test that no destructive functions are called in the codebase."""
        destructive_functions = find_destructive_functions()

        # Filter out legitimate uses (like in tests or cleanup tools)
        legitimate_uses = set()
        for func in destructive_functions:
            file_path = func.split(":")[0]
            if any(legit in file_path for legit in ["test_", "cleanup_", "tools/"]):
                legitimate_uses.add(func)

        # Remove legitimate uses from the set
        problematic_functions = destructive_functions - legitimate_uses

        assert len(problematic_functions) == 0, (
            f"Found potentially destructive functions in production code:\n"
            f"{chr(10).join(sorted(problematic_functions))}\n"
            f"All destructive operations must be gated behind Phase-1 fuses."
        )

    def test_no_direct_run_index_deletions(self) -> None:
        """Test that no code directly removes runs from run_index.json."""
        direct_writes = find_direct_run_index_writes()

        # Filter out legitimate uses (like in cleanup tools)
        legitimate_uses = set()
        for write in direct_writes:
            file_path = write.split(":")[0]
            if any(legit in file_path for legit in ["cleanup_", "tools/"]):
                legitimate_uses.add(write)

        # Remove legitimate uses from the set
        problematic_writes = direct_writes - legitimate_uses

        assert len(problematic_writes) == 0, (
            f"Found direct run index deletions in production code:\n"
            f"{chr(10).join(sorted(problematic_writes))}\n"
            f"All run index modifications must be gated behind Phase-1 fuses."
        )

    def test_maintenance_ui_shows_readonly_copy(self) -> None:
        """Test that maintenance UI shows the correct read-only message."""
        assert (
            check_maintenance_ui_copy()
        ), "Maintenance UI must show: 'Run deletion functionality will be implemented in a future phase.'"

    def test_maintenance_rendered_in_sidebar(self) -> None:
        """Test that maintenance is rendered in sidebar context."""
        assert (
            check_sidebar_placement()
        ), "Maintenance component must be rendered in sidebar using st.sidebar.subheader"

    def test_phase_1_fuse_not_enabled(self) -> None:
        """Test that Phase 1 destructive fuse is disabled by default."""
        # Check config files for any enabled destructive fuses
        config_files = ["config/settings.yaml", "config/settings.json"]

        for config_file in config_files:
            if Path(config_file).exists():
                try:
                    with open(config_file) as f:
                        content = f.read()

                    # Look for enabled destructive fuses
                    if any(
                        pattern in content.lower()
                        for pattern in [
                            "destructive.*true",
                            "delete.*true",
                            "cleanup.*true",
                            "phase.*fuse.*true",
                        ]
                    ):
                        pytest.fail(f"Found enabled destructive fuse in {config_file}")

                except (OSError, UnicodeDecodeError):
                    continue

    def test_no_destructive_ui_buttons(self) -> None:
        """Test that no destructive buttons are exposed in the UI."""
        ui_files = [
            "app/main.py",
            "app/components/maintenance.py",
            "app/components/controls.py",
        ]

        destructive_patterns = ["delete", "remove", "cleanup", "prune", "gc"]

        for ui_file in ui_files:
            if Path(ui_file).exists():
                try:
                    with open(ui_file) as f:
                        content = f.read()

                    lines = content.split("\n")
                    for i, line in enumerate(lines, 1):
                        line_lower = line.lower()
                        if any(
                            pattern in line_lower for pattern in destructive_patterns
                        ):
                            # Check if it's a button or action
                            if any(
                                ui_element in line_lower
                                for ui_element in [
                                    "st.button",
                                    "st.selectbox",
                                    "st.checkbox",
                                    "on_click",
                                ]
                            ):
                                pytest.fail(
                                    f"Found potentially destructive UI element in {ui_file}:{i}:\n"
                                    f"{line.strip()}\n"
                                    f"All destructive actions must be gated behind Phase-1 fuses.",
                                )

                except (OSError, UnicodeDecodeError):
                    continue
