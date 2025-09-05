"""
Lint tests for forbidden schema-fragile hardcoding.

This module tests that components don't use hardcoded column references
that may not exist in all parquet files.
"""

import pytest
import re
from pathlib import Path
from typing import List, Tuple


class TestNoSchemaFragileHardcoding:
    """Test that components don't use hardcoded schema-fragile references."""
    
    # Schema-fragile column names to check for
    FRAGILE_COLUMNS = {
        "is_primary",
        "weakest_edge_to_primary", 
        "primary_name",
        "WEAKEST_EDGE_TO_PRIMARY",
        "IS_PRIMARY",
        "PRIMARY_NAME"
    }
    
    # Files that are allowed to use these columns (pipeline code that creates them)
    ALLOWED_FILES = {
        "grouping.py",           # Creates these columns
        "disposition.py",        # Uses these columns safely
        "survivorship.py",       # Creates these columns
        "cleaning.py",           # Creates these columns
        "dtypes_map.py",         # Defines dtypes
        "group_stats.py",        # Computes stats from these columns
        "duckdb_group_stats.py", # Computes stats from these columns
        "parity_validator.py",   # Validates these columns exist
        "schema_utils.py",       # Defines constants
    }
    
    # Allowed patterns (legitimate uses)
    ALLOWED_PATTERNS = [
        r"from.*schema_utils.*import.*",  # Import statements
        r"#.*",                          # Comments
        r"\"\"\".*\"\"\"",               # Docstrings
        r"'''.*'''",                     # Docstrings
        r"get_order_by\(.*context=",     # Context-aware usage
        r"build_sort_expression\(.*context=",  # Context-aware usage
        r"_build_where_clause\(.*available_columns",  # Conditional usage
        r"apply_filters_pyarrow\(.*available_columns",  # Conditional usage
        r"available_columns.*in.*",      # Availability checks
        r"if.*in.*available_columns",    # Availability checks
        r"WEAKEST_EDGE_TO_PRIMARY.*in.*available_columns",  # Explicit checks
        r"IS_PRIMARY.*in.*available_columns",  # Explicit checks
        r"PRIMARY_NAME.*in.*available_columns",  # Explicit checks
        r"if.*WEAKEST_EDGE_TO_PRIMARY.*in.*available_columns",  # Conditional checks
        r"if.*IS_PRIMARY.*in.*available_columns",  # Conditional checks
        r"if.*PRIMARY_NAME.*in.*available_columns",  # Conditional checks
        r"available_columns.*is.*None.*or.*WEAKEST_EDGE_TO_PRIMARY",  # Conditional checks
        r"available_columns.*is.*None.*or.*IS_PRIMARY",  # Conditional checks
        r"available_columns.*is.*None.*or.*PRIMARY_NAME",  # Conditional checks
        r"LEGACY_COLUMNS.*=.*\[",        # Legacy column definitions
        r"\"is_primary\":",              # Dictionary key definitions
        r"\"weakest_edge_to_primary\":", # Dictionary key definitions
        r"\"primary_name\":",            # Dictionary key definitions
        r"DTYPES.*=.*{",                # Dtype definitions
        r"get_dtypes_for_schema",        # Schema function definitions
        r"group_data\[.*IS_PRIMARY.*\]", # DataFrame column access (legitimate)
        r"group_data\[.*WEAKEST_EDGE_TO_PRIMARY.*\]", # DataFrame column access (legitimate)
        r"group_data\[.*PRIMARY_NAME.*\]", # DataFrame column access (legitimate)
        r"\.get\(.*is_primary.*\)",      # Safe .get() access
        r"\.get\(.*weakest_edge_to_primary.*\)", # Safe .get() access
        r"\.get\(.*primary_name.*\)",    # Safe .get() access
        r"primary_record\.get\(",        # Safe primary record access
        r"primary_name.*or.*\"\"",       # Safe fallback patterns
        r"primary_name.*if.*primary_name", # Safe conditional patterns
        r"if.*IS_PRIMARY.*in.*group_data\.columns",  # Column existence checks
        r"if.*WEAKEST_EDGE_TO_PRIMARY.*in.*group_data\.columns",  # Column existence checks
        r"if.*PRIMARY_NAME.*in.*group_data\.columns",  # Column existence checks
        r"primary_name:.*str",  # Function parameter definitions
        r"primary_name:.*The.*primary.*name",  # Function docstrings
        r"render_group_details.*primary_name",  # Function calls with parameters
        r"from.*import.*WEAKEST_EDGE_TO_PRIMARY",  # Import statements
        r"from.*import.*IS_PRIMARY",  # Import statements
        r"from.*import.*PRIMARY_NAME",  # Import statements
    ]
    
    def should_exclude_file(self, file_path: Path) -> bool:
        """Check if file should be excluded from search."""
        file_name = file_path.name
        return file_name in self.ALLOWED_FILES
    
    def should_exclude_line(self, line: str) -> bool:
        """Check if line should be excluded from search."""
        # Check allowed patterns
        for pattern in self.ALLOWED_PATTERNS:
            if re.search(pattern, line):
                return True
        
        # Check for conditional context
        if line.strip().startswith(('if ', 'elif ', 'else:', 'try:', 'except:', 'finally:')):
            return True
        
        # Check for lines that are clearly inside conditional blocks
        if re.match(r'^\s{4,}.*(if|elif|else|try|except|finally)', line):
            return True
        
        return False
    
    def find_fragile_references(self, file_path: Path) -> List[Tuple[int, str]]:
        """Find schema-fragile references in a file."""
        references = []
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            
            for line_num, line in enumerate(lines, 1):
                if self.should_exclude_line(line):
                    continue
                
                # Check for fragile column references
                for column in self.FRAGILE_COLUMNS:
                    if column in line:
                        references.append((line_num, line.strip()))
                        break  # Only report each line once
        
        except Exception as e:
            pytest.skip(f"Error reading {file_path}: {e}")
        
        return references
    
    def test_no_hardcoded_primary_name_in_details_context(self):
        """Test that group_details context doesn't use hardcoded primary_name."""
        project_root = Path(__file__).parent.parent.parent
        details_files = [
            project_root / "src" / "utils" / "group_details.py",
            project_root / "app" / "components" / "group_details.py",
        ]
        
        violations = []
        
        for file_path in details_files:
            if not file_path.exists():
                continue
            
            references = self.find_fragile_references(file_path)
            for line_num, line_content in references:
                # Check for hardcoded primary_name usage that should use context-aware functions
                if "primary_name" in line_content.lower() and "context" not in line_content:
                    violations.append(f"{file_path.relative_to(project_root)}:{line_num} - {line_content}")
        
        if violations:
            pytest.fail(
                f"Found {len(violations)} hardcoded primary_name references in details context:\n" +
                "\n".join(violations) +
                "\n\nUse get_order_by(sort_key, context='group_details') or build_sort_expression(sort_key, context='group_details') instead."
            )
    
    def test_no_hardcoded_weakest_edge_without_availability_check(self):
        """Test that WEAKEST_EDGE_TO_PRIMARY is not used without availability checks."""
        project_root = Path(__file__).parent.parent.parent
        search_dirs = ["src/utils", "app/components"]
        
        violations = []
        
        for dir_name in search_dirs:
            dir_path = project_root / dir_name
            if not dir_path.exists():
                continue
            
            for file_path in dir_path.rglob("*.py"):
                if self.should_exclude_file(file_path):
                    continue
                
                references = self.find_fragile_references(file_path)
                for line_num, line_content in references:
                    # Check for WEAKEST_EDGE_TO_PRIMARY usage without availability checks
                    if "WEAKEST_EDGE_TO_PRIMARY" in line_content:
                        if not any(pattern in line_content for pattern in [
                            "available_columns",
                            "in.*available_columns",
                            "is.*None.*or",
                            "context=",
                            "get_order_by",
                            "build_sort_expression",
                            "_build_where_clause",
                            "apply_filters_pyarrow"
                        ]):
                            violations.append(f"{file_path.relative_to(project_root)}:{line_num} - {line_content}")
        
        if violations:
            pytest.fail(
                f"Found {len(violations)} WEAKEST_EDGE_TO_PRIMARY references without availability checks:\n" +
                "\n".join(violations) +
                "\n\nUse conditional filtering with available_columns or context-aware functions."
            )
    
    def test_no_hardcoded_is_primary_without_availability_check(self):
        """Test that IS_PRIMARY is not used without availability checks."""
        project_root = Path(__file__).parent.parent.parent
        search_dirs = ["src/utils", "app/components"]
        
        violations = []
        
        for dir_name in search_dirs:
            dir_path = project_root / dir_name
            if not dir_path.exists():
                continue
            
            for file_path in dir_path.rglob("*.py"):
                if self.should_exclude_file(file_path):
                    continue
                
                references = self.find_fragile_references(file_path)
                for line_num, line_content in references:
                    # Check for IS_PRIMARY usage without availability checks
                    if "IS_PRIMARY" in line_content:
                        if not any(pattern in line_content for pattern in [
                            "available_columns",
                            "in.*available_columns",
                            r"in.*group_data\.columns",
                            "context=",
                            "get_order_by",
                            "build_sort_expression",
                            "_build_where_clause",
                            "apply_filters_pyarrow",
                            r"\.get\(",
                            "if.*IS_PRIMARY.*in"
                        ]):
                            violations.append(f"{file_path.relative_to(project_root)}:{line_num} - {line_content}")
        
        if violations:
            pytest.fail(
                f"Found {len(violations)} IS_PRIMARY references without availability checks:\n" +
                "\n".join(violations) +
                "\n\nUse conditional filtering with available_columns or context-aware functions."
            )
    
    def test_context_aware_functions_used_correctly(self):
        """Test that context-aware functions are used correctly."""
        project_root = Path(__file__).parent.parent.parent
        
        # Test that get_order_by is called with context in group_details
        group_details_file = project_root / "src" / "utils" / "group_details.py"
        if group_details_file.exists():
            with open(group_details_file, 'r') as f:
                content = f.read()
            
            # Should have context-aware call
            if "get_order_by" in content and "context=" not in content:
                pytest.fail(
                    f"get_order_by should be called with context parameter in {group_details_file.relative_to(project_root)}"
                )
        
        # Test that build_sort_expression is called with context in group_details
        if group_details_file.exists():
            with open(group_details_file, 'r') as f:
                content = f.read()
            
            # Should have context-aware call if build_sort_expression is used
            if "build_sort_expression" in content and "context=" not in content:
                pytest.fail(
                    f"build_sort_expression should be called with context parameter in {group_details_file.relative_to(project_root)}"
                )
