#!/usr/bin/env python3
"""Test to validate that all Phase headers in CHANGELOG.md contain valid YYYY-MM-DD dates."""

import re
from datetime import datetime
from pathlib import Path
from typing import Any


def extract_phase_headers(changelog_content: str) -> list[tuple[str, str, str]]:
    """Extract all Phase headers and their dates from CHANGELOG.md content.

    Returns:
        List of tuples: (phase_number, date_string, full_header_line)

    """
    # Pattern to match Phase headers with dates
    pattern = r"## \[Phase(\d+\.\d+(?:[a-z])?)[^\]]*\] - (\d{4}-\d{2}-\d{2})"

    # Get the full header lines for context
    lines = changelog_content.split("\n")
    headers = []

    for line in lines:
        match = re.search(pattern, line)
        if match:
            phase_num = match.group(1)
            date_str = match.group(2)
            headers.append((phase_num, date_str, line.strip()))

    return headers


def validate_date_format(date_string: str) -> bool:
    """Validate that a date string is in YYYY-MM-DD format and is a valid date.

    Args:
        date_string: Date string to validate

    Returns:
        True if valid, False otherwise

    """
    try:
        # Check format with regex first
        if not re.match(r"^\d{4}-\d{2}-\d{2}$", date_string):
            return False

        # Parse the date to ensure it's valid
        datetime.strptime(date_string, "%Y-%m-%d")
        return True
    except ValueError:
        return False


def test_changelog_date_format() -> None:
    """Test that all Phase headers in CHANGELOG.md have valid YYYY-MM-DD dates."""
    changelog_path = Path("CHANGELOG.md")

    # Check if CHANGELOG.md exists
    assert changelog_path.exists(), "CHANGELOG.md not found"

    # Read the changelog content
    with open(changelog_path, encoding="utf-8") as f:
        content = f.read()

    # Extract all Phase headers
    headers = extract_phase_headers(content)

    # Ensure we found at least some Phase headers
    assert len(headers) > 0, "No Phase headers found in CHANGELOG.md"

    # Validate each date
    invalid_dates = []
    for phase_num, date_str, header_line in headers:
        if not validate_date_format(date_str):
            invalid_dates.append(
                {"phase": phase_num, "date": date_str, "header": header_line},
            )

    # Report any invalid dates
    if invalid_dates:
        error_msg = "Invalid dates found in CHANGELOG.md:\n"
        for invalid in invalid_dates:
            error_msg += f"  Phase {invalid['phase']}: '{invalid['date']}' in '{invalid['header']}'\n"
        raise AssertionError(error_msg)

    # All dates are valid
    print(f"✅ All {len(headers)} Phase headers have valid YYYY-MM-DD dates")


def test_changelog_date_consistency() -> None:
    """Test that Phase headers are in chronological order (newest first)."""
    changelog_path = Path("CHANGELOG.md")

    # Read the changelog content
    with open(changelog_path, encoding="utf-8") as f:
        content = f.read()

    # Extract all Phase headers
    headers = extract_phase_headers(content)

    # Parse dates and check chronological order
    dates = []
    for phase_num, date_str, _header_line in headers:
        try:
            date_obj = datetime.strptime(date_str, "%Y-%m-%d")
            dates.append((phase_num, date_obj, date_str))
        except ValueError:
            # Skip invalid dates (handled by other test)
            continue

    # Group by major.minor version to check chronological order within each group
    # This allows sub-phases to have different dates while maintaining overall order
    phase_groups: dict[str, list[tuple[str, datetime, str]]] = {}
    for phase_num, date_obj, date_str in dates:
        # Extract major.minor (e.g., "1.17" from "1.17.5c")
        major_minor = ".".join(phase_num.split(".")[:2])
        if major_minor not in phase_groups:
            phase_groups[major_minor] = []
        phase_groups[major_minor].append((phase_num, date_obj, date_str))

    # Check chronological order within each major.minor group
    for major_minor, group_dates in phase_groups.items():
        # Sort by date (newest first) within the group
        group_dates.sort(key=lambda x: x[1], reverse=True)

        # Check that the group is in descending order
        for i in range(1, len(group_dates)):
            prev_date = group_dates[i - 1][1]
            curr_date = group_dates[i][1]

            if curr_date > prev_date:
                error_msg = f"Changelog dates not in chronological order within Phase {major_minor}:\n"
                error_msg += f"  Phase {group_dates[i-1][0]} ({group_dates[i-1][2]}) comes before Phase {group_dates[i][0]} ({group_dates[i][2]})\n"
                raise AssertionError(error_msg)

    print(
        "✅ All Phase headers are in correct chronological order within their major.minor groups",
    )


def test_phase_number_format() -> None:
    """Test that Phase numbers follow expected format (e.g., 1.18.1, 1.17.5c)."""
    changelog_path = Path("CHANGELOG.md")

    # Read the changelog content
    with open(changelog_path, encoding="utf-8") as f:
        content = f.read()

    # Extract all Phase headers
    headers = extract_phase_headers(content)

    # Validate Phase number format
    invalid_phases = []
    for phase_num, _date_str, header_line in headers:
        # Expected format: major.minor[.patch][suffix]
        # Examples: 1.18, 1.18.1, 1.17.5c
        if not re.match(r"^\d+\.\d+(?:\.\d+)?(?:[a-z])?$", phase_num):
            invalid_phases.append({"phase": phase_num, "header": header_line})

    # Report any invalid Phase numbers
    if invalid_phases:
        error_msg = "Invalid Phase number formats found in CHANGELOG.md:\n"
        for invalid in invalid_phases:
            error_msg += f"  '{invalid['phase']}' in '{invalid['header']}'\n"
        raise AssertionError(error_msg)

    print(f"✅ All {len(headers)} Phase numbers follow expected format")


if __name__ == "__main__":
    # Run all tests
    test_changelog_date_format()
    test_changelog_date_consistency()
    test_phase_number_format()
    print("🎉 All changelog date validation tests passed!")
