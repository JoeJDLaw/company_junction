#!/usr/bin/env python3
"""Simple pipeline cleanup utility.

This tool provides a clean, safe interface for managing pipeline runs.
Delete commands support --dry-run (recommended); without it, you'll be prompted for confirmation.

Usage:
    # List all runs grouped by type
    python pipeline_cleanup.py --list

    # Preview what would be deleted (recommended first step)
    python pipeline_cleanup.py --delete-tests --dry-run
    python pipeline_cleanup.py --delete-prod --dry-run
    python pipeline_cleanup.py --delete-all --dry-run

    # Actually delete (requires confirmation)
    python pipeline_cleanup.py --delete-tests
    python pipeline_cleanup.py --delete-prod
    python pipeline_cleanup.py --delete-all

Exit codes:
    0 = No candidates found for deletion
    2 = Candidates found (dry-run mode)
    >0 = Errors occurred during execution
"""

import argparse
import sys

from src.utils.cleanup_api import (
    DeleteResult,
    PreviewInfo,
    RunInfo,
    delete_runs,
    list_runs,
    preview_delete,
)
from src.utils.logging_utils import get_logger

logger = get_logger(__name__)


def format_bytes(bytes_value: int) -> str:
    """Format bytes as human-readable string."""
    if bytes_value == 0:
        return "0 B"

    value: float = float(bytes_value)
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if value < 1024.0:
            return f"{value:.1f} {unit}"
        value = value / 1024.0

    return f"{value:.1f} PB"


def print_runs_by_type() -> None:
    """Print runs grouped by type with counts."""
    runs = list_runs()

    if not runs:
        print("No runs found.")
        return

    # Group by run_type (runs are already sorted by timestamp desc from list_runs)
    by_type: dict[str, list[RunInfo]] = {}
    for run in runs:
        run_type = run.run_type
        if run_type not in by_type:
            by_type[run_type] = []
        by_type[run_type].append(run)

    print("Pipeline runs by type:")
    print("=" * 50)

    total_runs = 0
    for run_type in sorted(by_type.keys()):
        type_runs = by_type[run_type]
        total_runs += len(type_runs)

        print(f"\n{run_type.upper()} ({len(type_runs)} runs):")
        for run in type_runs:
            status_icon = {
                "complete": "✅",
                "failed": "❌",
                "running": "🔄",
            }.get(run.status, "❓")

            print(f"  {status_icon} {run.run_id} ({run.status})")

    print(f"\nTotal: {total_runs} runs")


def print_preview(preview: PreviewInfo, run_type: str) -> None:
    """Print deletion preview information."""
    if not preview.runs_to_delete:
        print(f"No {run_type} runs found for deletion.")
        return

    print(f"Would delete {len(preview.runs_to_delete)} {run_type} runs:")
    print("=" * 50)

    for run in preview.runs_to_delete:
        status_icon = {
            "complete": "✅",
            "failed": "❌",
            "running": "🔄",
        }.get(run.status, "❓")

        print(f"  {status_icon} {run.run_id} ({run.status})")

    if preview.runs_inflight:
        print(f"\n⚠️  Skipping {len(preview.runs_inflight)} running runs:")
        for run_id in preview.runs_inflight:
            print(f"  🔄 {run_id}")

    if preview.runs_not_found:
        print(f"\n❌ {len(preview.runs_not_found)} runs not found:")
        for run_id in preview.runs_not_found:
            print(f"  ❓ {run_id}")

    if preview.latest_affected:
        print("\n⚠️  WARNING: This would delete the latest run!")

    total_mb = preview.total_bytes / (1024 * 1024)
    print(f"\nTotal size: {format_bytes(preview.total_bytes)} ({total_mb:.1f} MB)")


def confirm_deletion(run_type: str, count: int) -> bool:
    """Ask user for confirmation before deletion."""
    print(
        f"\n⚠️  Are you sure you want to delete {count} {run_type} runs? (y/N): ", end=""
    )
    try:
        response = input().strip().lower()
        return response in ["y", "yes"]
    except (EOFError, KeyboardInterrupt):
        print("\nCancelled.")
        return False


def perform_deletion(run_ids: list[str], run_type: str) -> DeleteResult:
    """Perform the actual deletion with confirmation."""
    # Get preview first
    preview = preview_delete(run_ids)

    if not preview.runs_to_delete:
        print(f"No {run_type} runs found for deletion.")
        return DeleteResult(
            deleted=[],
            not_found=[],
            inflight_blocked=[],
            errors=[],
            total_bytes_freed=0,
            latest_reassigned=False,
            new_latest=None,
        )

    # Show preview
    print_preview(preview, run_type)

    # Ask for confirmation
    if not confirm_deletion(run_type, len(preview.runs_to_delete)):
        print("Deletion cancelled.")
        return DeleteResult(
            deleted=[],
            not_found=[],
            inflight_blocked=[],
            errors=[],
            total_bytes_freed=0,
            latest_reassigned=False,
            new_latest=None,
        )

    # Perform deletion
    print(f"\nDeleting {len(preview.runs_to_delete)} {run_type} runs...")
    result = delete_runs(run_ids)

    # Show results
    if result.deleted:
        print(f"✅ Deleted {len(result.deleted)} runs:")
        for run_id in result.deleted:
            print(f"  🗑️  {run_id}")

    if result.errors:
        print(f"❌ {len(result.errors)} errors occurred:")
        for error in result.errors:
            print(f"  ⚠️  {error}")

    if result.inflight_blocked:
        print(f"⚠️  Skipped {len(result.inflight_blocked)} running runs:")
        for run_id in result.inflight_blocked:
            print(f"  🔄 {run_id}")

    if result.latest_reassigned and result.new_latest:
        print(f"🔄 Latest pointer reassigned to: {result.new_latest}")

    total_mb = result.total_bytes_freed / (1024 * 1024)
    print(f"\nFreed: {format_bytes(result.total_bytes_freed)} ({total_mb:.1f} MB)")

    return result


def get_runs_by_type(run_type: str) -> list[str]:
    """Get run IDs for a specific type."""
    runs = list_runs()
    return [run.run_id for run in runs if run.run_type == run_type]


def main() -> None:
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Simple pipeline cleanup utility",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    # Action arguments (mutually exclusive)
    action_group = parser.add_mutually_exclusive_group(required=True)
    action_group.add_argument(
        "--list",
        action="store_true",
        help="List all runs grouped by type with counts",
    )
    action_group.add_argument(
        "--delete-all",
        action="store_true",
        help="Delete all runs (with confirmation)",
    )
    action_group.add_argument(
        "--delete-tests",
        action="store_true",
        help="Delete test runs (with confirmation)",
    )
    action_group.add_argument(
        "--delete-prod",
        action="store_true",
        help="Delete production runs (with confirmation)",
    )

    # Dry-run flag
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be deleted without actually deleting (default for delete commands)",
    )

    args = parser.parse_args()

    try:
        if args.list:
            print_runs_by_type()
            sys.exit(0)

        # Determine run type and get run IDs
        if args.delete_all:
            run_type = "all"
            runs = list_runs()
            run_ids = [run.run_id for run in runs]
        elif args.delete_tests:
            run_type = "test"
            run_ids = get_runs_by_type("test")
        elif args.delete_prod:
            run_type = "prod"
            run_ids = get_runs_by_type("prod")
        else:
            # This shouldn't happen due to mutually exclusive group
            parser.error("No action specified")

        if not run_ids:
            print(f"No {run_type} runs found.")
            sys.exit(0)

        if args.dry_run:
            # Dry-run mode
            preview = preview_delete(run_ids)
            print_preview(preview, run_type)

            if preview.runs_to_delete:
                sys.exit(2)  # Candidates found
            else:
                sys.exit(0)  # No candidates
        else:
            # Actual deletion
            result = perform_deletion(run_ids, run_type)

            if result.errors:
                sys.exit(1)  # Errors occurred
            elif result.deleted:
                sys.exit(0)  # Success
            else:
                sys.exit(0)  # No deletions performed

    except KeyboardInterrupt:
        print("\nOperation cancelled by user.")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
