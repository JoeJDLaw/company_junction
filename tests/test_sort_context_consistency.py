"""Consistency tests for ORDER BY vs sort expression.

This module ensures that get_order_by and build_sort_expression
use the same logical column mappings for each context.
"""

import pytest

from src.utils.filtering import build_sort_expression, get_order_by


class TestSortContextConsistency:
    """Test that ORDER BY and sort expression mappings are consistent."""

    # Test cases: (sort_key, context, expected_column_family)
    TEST_CASES = [
        # Group Size sorting
        ("Group Size (Desc)", "default", "group_size"),
        ("Group Size (Desc)", "group_details", "group_size"),
        ("Group Size (Asc)", "default", "group_size"),
        ("Group Size (Asc)", "group_details", "group_size"),
        # Max Score sorting
        ("Max Score (Desc)", "default", "max_score"),
        ("Max Score (Desc)", "group_details", "max_score"),
        ("Max Score (Asc)", "default", "max_score"),
        ("Max Score (Asc)", "group_details", "max_score"),
        # Account Name sorting (context-dependent)
        ("Account Name (Asc)", "default", "primary_name"),
        ("Account Name (Asc)", "group_details", "account_name"),
        ("Account Name (Desc)", "default", "primary_name"),
        ("Account Name (Desc)", "group_details", "account_name"),
    ]

    @pytest.mark.parametrize("sort_key,context,expected_column_family", TEST_CASES)
    def test_order_by_and_sort_expression_consistency(
        self, sort_key, context, expected_column_family,
    ):
        """Test that get_order_by and build_sort_expression use consistent column mappings."""
        # Get ORDER BY clause
        order_by_clause = get_order_by(sort_key, context)

        # Get sort expression
        sort_expression = build_sort_expression(sort_key, context)

        # Extract the primary column from ORDER BY clause
        order_by_column = self._extract_column_from_order_by(order_by_clause)

        # Extract the primary column from sort expression
        sort_expression_column = sort_expression[0][0] if sort_expression else None

        # Both should map to the same logical column family
        assert order_by_column == sort_expression_column, (
            f"Inconsistent column mapping for {sort_key} in {context} context:\n"
            f"  get_order_by: {order_by_clause} -> {order_by_column}\n"
            f"  build_sort_expression: {sort_expression} -> {sort_expression_column}"
        )

        # Verify the column family is correct
        assert (
            expected_column_family in order_by_column.lower()
        ), f"Expected column family '{expected_column_family}' not found in '{order_by_column}'"

    def _extract_column_from_order_by(self, order_by_clause: str) -> str:
        """Extract the primary column from an ORDER BY clause."""
        # Remove ORDER BY keywords and direction
        clause = order_by_clause.strip()
        if clause.upper().startswith("ORDER BY "):
            clause = clause[9:]

        # Split by comma and take the first part
        first_part = clause.split(",")[0].strip()

        # Remove direction keywords
        for direction in ["ASC", "DESC"]:
            if first_part.upper().endswith(f" {direction}"):
                first_part = first_part[: -len(f" {direction}")].strip()

        return first_part

    def test_context_aware_account_name_mapping(self):
        """Test that Account Name sorting is context-aware."""
        # Default context should use primary_name
        default_order_by = get_order_by("Account Name (Asc)", "default")
        default_sort = build_sort_expression("Account Name (Asc)", "default")

        assert "primary_name" in default_order_by.lower()
        assert "primary_name" in default_sort[0][0].lower()

        # Group details context should use account_name
        details_order_by = get_order_by("Account Name (Asc)", "group_details")
        details_sort = build_sort_expression("Account Name (Asc)", "group_details")

        assert "account_name" in details_order_by.lower()
        assert "account_name" in details_sort[0][0].lower()

    def test_all_sort_keys_have_consistent_mappings(self):
        """Test that all sort keys have consistent mappings across contexts."""
        sort_keys = [
            "Group Size (Desc)",
            "Group Size (Asc)",
            "Max Score (Desc)",
            "Max Score (Asc)",
            "Account Name (Asc)",
            "Account Name (Desc)",
        ]

        contexts = ["default", "group_details"]

        for sort_key in sort_keys:
            for context in contexts:
                try:
                    order_by = get_order_by(sort_key, context)
                    sort_expr = build_sort_expression(sort_key, context)

                    # Both should succeed without errors
                    assert order_by is not None
                    assert sort_expr is not None
                    assert len(sort_expr) > 0

                except Exception as e:
                    pytest.fail(f"Failed for {sort_key} in {context} context: {e}")

    def test_sort_direction_consistency(self):
        """Test that sort directions are consistent between ORDER BY and sort expression."""
        test_cases = [
            ("Group Size (Desc)", "desc", "descending"),
            ("Group Size (Asc)", "asc", "ascending"),
            ("Max Score (Desc)", "desc", "descending"),
            ("Max Score (Asc)", "asc", "ascending"),
            ("Account Name (Desc)", "desc", "descending"),
            ("Account Name (Asc)", "asc", "ascending"),
        ]

        for (
            sort_key,
            expected_order_by_direction,
            expected_sort_expr_direction,
        ) in test_cases:
            # Test default context
            order_by = get_order_by(sort_key, "default")
            sort_expr = build_sort_expression(sort_key, "default")

            order_by_direction = self._extract_direction_from_order_by(order_by)
            sort_expr_direction = sort_expr[0][1]

            assert (
                order_by_direction == expected_order_by_direction
            ), f"ORDER BY direction mismatch for {sort_key}: expected {expected_order_by_direction}, got {order_by_direction}"

            assert (
                sort_expr_direction == expected_sort_expr_direction
            ), f"Sort expression direction mismatch for {sort_key}: expected {expected_sort_expr_direction}, got {sort_expr_direction}"

    def _extract_direction_from_order_by(self, order_by_clause: str) -> str:
        """Extract the sort direction from an ORDER BY clause."""
        clause = order_by_clause.strip().upper()
        if " DESC" in clause:
            return "desc"
        if " ASC" in clause:
            return "asc"
        return "asc"  # Default to ascending

    def test_tiebreaker_consistency(self):
        """Test that tiebreaker columns are consistent."""
        sort_keys = ["Group Size (Desc)", "Max Score (Asc)", "Account Name (Desc)"]

        for sort_key in sort_keys:
            for context in ["default", "group_details"]:
                sort_expr = build_sort_expression(sort_key, context)

                # Should have at least 2 sort keys (primary + tiebreaker)
                assert (
                    len(sort_expr) >= 2
                ), f"Missing tiebreaker for {sort_key} in {context}"

                # Tiebreaker should be group_id
                tiebreaker_column, tiebreaker_direction = sort_expr[1]
                assert "group_id" in tiebreaker_column.lower()
                assert (
                    tiebreaker_direction == "ascending"
                )  # Tiebreaker should always be ascending
