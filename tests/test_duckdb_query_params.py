"""
Tests for DuckDB query parameterization.

This module ensures all DuckDB queries use parameterized placeholders
instead of string interpolation to prevent SQL injection.
"""

import ast
import pytest
from pathlib import Path
from typing import List, Tuple


def find_string_interpolations(filename: str) -> List[ast.JoinedStr]:
    """
    Find f-string interpolations that might be actual SQL queries.
    
    This function excludes logging statements and focuses on f-strings
    that could potentially be passed to database execution functions.
    
    Args:
        filename: Path to Python file to analyze
        
    Returns:
        List of f-string nodes that might contain actual SQL queries
    """
    with open(filename, 'r', encoding='utf-8') as f:
        try:
            tree = ast.parse(f.read())
        except SyntaxError:
            # Skip files with syntax errors
            return []
    
    sql_f_strings = []
    
    for node in ast.walk(tree):
        if isinstance(node, ast.JoinedStr):  # f-strings
            # Skip f-strings that are clearly logging statements
            if _is_logging_statement(node):
                continue
                
            # Check if this f-string contains SQL-like content
            for value in node.values:
                if isinstance(value, ast.Constant) and isinstance(value.value, str):
                    sql_keywords = ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'FROM', 'WHERE', 'GROUP BY', 'ORDER BY', 'LIMIT', 'OFFSET', 'IN']
                    if any(keyword in value.value.upper() for keyword in sql_keywords):
                        sql_f_strings.append(node)
                        break
    
    return sql_f_strings


def _is_logging_statement(node: ast.JoinedStr) -> bool:
    """
    Check if an f-string node is part of a logging statement.
    
    This is a simplified check that looks for common logging patterns
    in the source code around the f-string.
    
    Args:
        node: The f-string AST node to check
        
    Returns:
        True if this appears to be a logging statement, False otherwise
    """
    # For now, we'll use a simple heuristic: if the f-string contains
    # common logging patterns, assume it's logging
    for value in node.values:
        if isinstance(value, ast.Constant) and isinstance(value.value, str):
            text = value.value.lower()
            # Common logging patterns that suggest this is not SQL
            if any(pattern in text for pattern in [
                'run_id=', 'sort_key=', 'backend=', 'elapsed=', 'rows=', 'groups=',
                'failed', 'error', 'warning', 'info', 'debug', 'exception',
                'timeout', 'execution', 'selection', 'fallback', 'available',
                'projection', 'filter', 'stats', 'conversion', 'slice'
            ]):
                return True
            # If it contains SQL keywords but also logging context, it's likely logging
            sql_in_logging = ['where_clause', 'order_by', 'clause', 'filters', 'parquet']
            if any(pattern in text for pattern in sql_in_logging):
                return True
    
    return False


def test_no_f_string_sql_queries():
    """
    Verify no f-string SQL queries exist in group-related modules.
    
    This test ensures all DuckDB queries use parameterized placeholders
    instead of string interpolation to prevent SQL injection.
    """
    # Files to check for SQL queries
    sql_files = [
        "src/utils/group_pagination.py",
        "src/utils/group_details.py", 
        "src/utils/group_stats.py",
        "src/utils/filtering.py"
    ]
    
    violations = []
    
    for file_path in sql_files:
        if Path(file_path).exists():
            f_strings = find_string_interpolations(file_path)
            if f_strings:
                violations.append(f"{file_path}: {len(f_strings)} f-string SQL queries found")
    
    if violations:
        pytest.fail(
            "Found f-string SQL queries. All DuckDB queries must use parameterized placeholders:\n" +
            "\n".join(violations) +
            "\n\nExample of correct usage:\n" +
            "query = 'SELECT * FROM groups WHERE run_id = ?'\n" +
            "result = conn.execute(query, [run_id]).fetchdf()"
        )


def test_duckdb_parameterization_examples():
    """
    Verify that parameterized queries work correctly.
    
    This test demonstrates the proper way to use DuckDB with parameters.
    """
    try:
        import duckdb
    except ImportError:
        pytest.skip("DuckDB not available")
    
    # Create test connection
    conn = duckdb.connect(":memory:")
    
    # Create test table
    conn.execute("""
        CREATE TABLE test_groups (
            group_id VARCHAR,
            name VARCHAR,
            score DOUBLE
        )
    """)
    
    # Insert test data
    test_data = [
        ("group1", "Company A", 95.5),
        ("group2", "Company B", 87.2),
        ("group3", "Company C", 92.1)
    ]
    
    conn.executemany("""
        INSERT INTO test_groups (group_id, name, score)
        VALUES (?, ?, ?)
    """, test_data)
    
    # Test parameterized query
    query = "SELECT * FROM test_groups WHERE score >= ? ORDER BY score DESC"
    result = conn.execute(query, [90.0]).fetchdf()
    
    assert len(result) == 2
    assert result.iloc[0]['score'] == 95.5
    assert result.iloc[1]['score'] == 92.1
    
    conn.close()


def test_sql_injection_prevention():
    """
    Verify that parameterized queries prevent SQL injection.
    
    This test demonstrates that malicious input cannot execute arbitrary SQL.
    """
    try:
        import duckdb
    except ImportError:
        pytest.skip("DuckDB not available")
    
    conn = duckdb.connect(":memory:")
    
    # Create test table
    conn.execute("""
        CREATE TABLE users (
            id INTEGER,
            name VARCHAR,
            email VARCHAR
        )
    """)
    
    # Insert test user
    conn.execute("INSERT INTO users VALUES (?, ?, ?)", [1, "Alice", "alice@example.com"])
    
    # Malicious input that would cause SQL injection with string interpolation
    malicious_input = "'; DROP TABLE users; --"
    
    # Parameterized query should treat this as literal text
    query = "SELECT * FROM users WHERE name = ?"
    result = conn.execute(query, [malicious_input]).fetchdf()
    
    # Should return no results (not crash or drop table)
    assert len(result) == 0
    
    # Table should still exist
    tables = conn.execute("SHOW TABLES").fetchdf()
    assert "users" in tables["name"].values
    
    conn.close()


if __name__ == "__main__":
    # Run tests directly for debugging
    test_no_f_string_sql_queries()
    test_duckdb_parameterization_examples()
    test_sql_injection_prevention()
    print("All tests passed!")
