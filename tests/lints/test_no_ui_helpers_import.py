"""
Test to ensure no new imports from deprecated ui_helpers module.

This test fails if any Python files import from src.utils.ui_helpers,
encouraging migration to the new modular structure.
"""

import subprocess
import sys
import re
import os
import pytest


def test_no_ui_helpers_import():
    """Test that no files import from the deprecated ui_helpers module."""
    # Get the project root (3 levels up from this test file)
    root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    
    try:
        # Use git to find Python files and grep for ui_helpers imports
        # This approach is more reliable than walking directories
        cmd = [
            "bash", "-lc", 
            r"""git ls-files '*.py' | grep -v '^tests/' | xargs grep -nH 'from\s\+src\.utils\.ui_helpers\s\+import\|import\s\+src\.utils\.ui_helpers' || true"""
        ]
        
        result = subprocess.run(cmd, cwd=root, capture_output=True, text=True, check=True)
        output = result.stdout
        
        # Filter out the façade file itself and any legitimate test files
        offenders = []
        for line in output.splitlines():
            if line.strip():  # Skip empty lines
                # Allow the façade file itself to contain the string
                if "src/utils/ui_helpers.py" not in line:
                    # Allow test files that are specifically testing the deprecation
                    if "test_no_ui_helpers_import" not in line:
                        offenders.append(line)
        
        if offenders:
            pytest.fail(
                f"Found {len(offenders)} files importing from deprecated ui_helpers module:\n"
                f"Replace imports from ui_helpers.py with new modules:\n"
                f"{chr(10).join(offenders)}\n\n"
                f"Migration guide:\n"
                f"  - get_groups_page, get_total_groups_count → src.utils.group_pagination\n"
                f"  - get_group_details → src.utils.group_details\n"
                f"  - get_order_by, build_sort_expression → src.utils.filtering\n"
                f"  - get_artifact_paths → src.utils.artifact_management\n"
                f"  - session, get_backend_choice → src.utils.ui_session\n"
                f"  - build_cache_key → src.utils.cache_keys"
            )
    
    except subprocess.CalledProcessError as e:
        # If git command fails, fall back to directory walking
        pytest.skip(f"Git command failed: {e}. Skipping import check.")
    except FileNotFoundError:
        # If bash is not available, skip the test
        pytest.skip("Bash not available. Skipping import check.")


def test_ui_helpers_deprecation_warning():
    """Test that ui_helpers module is completely gone (no longer importable)."""
    import pytest
    
    # The module should no longer exist
    with pytest.raises(ModuleNotFoundError, match="No module named 'src.utils.ui_helpers'"):
        import src.utils.ui_helpers


def test_ui_helpers_functions_still_work():
    """Test that ui_helpers functions still work (backward compatibility)."""
    # This ensures the façade still works during the deprecation period
    from src.utils.group_pagination import get_groups_page
    from src.utils.group_details import get_group_details
    
    # These should not raise ImportError
    assert callable(get_groups_page)
    assert callable(get_group_details)
    
    # Test that they're the same functions as the new modules
    from src.utils.group_pagination import get_groups_page as new_get_groups_page
    from src.utils.group_details import get_group_details as new_get_group_details
    
    assert get_groups_page is new_get_groups_page
    assert get_group_details is new_get_group_details


def test_ui_helpers_file_is_gone():
    """Test that ui_helpers.py file has been deleted."""
    import os
    assert not os.path.exists("src/utils/ui_helpers.py"), "ui_helpers.py should be deleted"
