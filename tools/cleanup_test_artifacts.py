#!/usr/bin/env python3
"""Simplified cleanup utility for test/demo artifacts.

This tool safely removes test and demo artifacts using deterministic discovery.
It operates in dry-run mode by default and requires explicit flags for actual deletion.

Usage:
    # Dry-run: list candidates by type
    python tools/cleanup_test_artifacts.py --types test,dev --older-than 7

    # Prod sweep (keep prod + pinned)
    python tools/cleanup_test_artifacts.py --prod-sweep --dry-run

    # Actually delete (requires confirmation)
    python tools/cleanup_test_artifacts.py --prod-sweep --really-delete --yes
"""

import argparse
import json
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))
from src.utils.path_utils import get_interim_dir, get_processed_dir

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


class CleanupPlan:
    """Represents a cleanup plan with candidates and reasons."""

    def __init__(self) -> None:
        self.candidates: list[tuple[str, dict[str, Any], str]] = (
            []
        )  # (run_id, run_data, reason)
        self.latest_run_id: Optional[str] = None
        self.pinned_runs: set[str] = set()
        self.prod_runs: set[str] = set()

    def add_candidate(self, run_id: str, run_data: dict[str, Any], reason: str) -> None:
        """Add a run to the cleanup candidates."""
        self.candidates.append((run_id, run_data, reason))

    def is_protected(self, run_id: str) -> bool:
        """Check if a run is protected from deletion."""
        return (
            run_id == self.latest_run_id
            or run_id in self.pinned_runs
            or run_id in self.prod_runs
        )

    def get_protected_candidates(self) -> list[str]:
        """Get list of protected candidates that would be excluded."""
        return [run_id for run_id, _, _ in self.candidates if self.is_protected(run_id)]

    def get_deletable_candidates(self) -> list[tuple[str, dict[str, Any], str]]:
        """Get list of candidates that can actually be deleted."""
        return [
            (run_id, run_data, reason)
            for run_id, run_data, reason in self.candidates
            if not self.is_protected(run_id)
        ]

    def sort_candidates(self) -> None:
        """Sort candidates by run_id for deterministic output."""
        self.candidates.sort(key=lambda x: x[0])


def load_run_index() -> dict[str, Any]:
    """Load the run index file."""
    index_path = Path(str(get_processed_dir("index") / "run_index.json"))
    if not index_path.exists():
        return {}

    try:
        with open(index_path) as f:
            data = json.load(f)
            if isinstance(data, dict):
                return data
            logger.warning(f"Run index is not a dict, got {type(data)}")
            return {}
    except (OSError, json.JSONDecodeError) as e:
        logger.error(f"Could not load run index: {e}")
        return {}


def save_run_index(run_index: dict[str, Any]) -> None:
    """Save the run index file atomically."""
    index_path = Path(str(get_processed_dir("index") / "run_index.json"))
    temp_path = index_path.with_suffix(".tmp")

    try:
        with open(temp_path, "w") as f:
            json.dump(run_index, f, indent=2)
        temp_path.replace(index_path)
    except OSError as e:
        logger.error(f"Could not save run index: {e}")
        if temp_path.exists():
            temp_path.unlink()


def get_latest_run_id() -> Optional[str]:
    """Get the current latest run id (prefer latest.json, fallback to symlink)."""
    # Prefer latest.json (Phase 1.27.3 empty-state support)
    try:
        from src.utils.path_utils import read_latest_run_id

        rid = read_latest_run_id()
        if rid:
            return rid
    except Exception:
        pass
    # Fallback to symlink
    latest_path = get_processed_dir("latest")
    if not latest_path.exists() or not latest_path.is_symlink():
        return None
    try:
        target = latest_path.resolve()
        return target.name
    except OSError:
        return None


def _list_run_dirs(root: Path) -> set[str]:
    """List top-level run directories under a root, excluding known non-run dirs."""
    if not root.exists():
        return set()

    # Import constants from pipeline_constants
    try:
        from src.utils.pipeline_constants import CLEANUP_EXCLUDE_DIRS

        exclude = CLEANUP_EXCLUDE_DIRS
    except ImportError:
        # Fallback if constants not available
        exclude = {"default", "index", "legacy", "test_save_run", ".DS_Store"}

    return {p.name for p in root.iterdir() if p.is_dir() and p.name not in exclude}


def scan_filesystem_runs() -> set[str]:
    """Union of run directory names found in data/interim and data/processed."""
    interim_root = Path("data/interim")
    processed_root = Path("data/processed")
    return _list_run_dirs(interim_root) | _list_run_dirs(processed_root)


def detect_run_type(run_data: dict[str, Any]) -> str:
    """Detect run type from existing metadata (heuristic for MVP)."""
    # Check input paths for test indicators
    input_paths = run_data.get("input_paths", [])
    if not input_paths:
        return "dev"

    # Convert to string and check for test indicators
    input_paths_str = str(input_paths).lower()
    if "sample_test" in input_paths_str or "test_" in input_paths_str:
        return "test"

    # Default to dev for existing runs
    return "dev"


def get_run_age_days(run_data: dict[str, Any]) -> int:
    """Get the age of a run in days."""
    timestamp_str = run_data.get("timestamp", "")
    if not timestamp_str:
        return 999  # Very old if no timestamp

    try:
        # Parse ISO format timestamp
        run_time = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
        age = datetime.now(run_time.tzinfo) - run_time
        return age.days
    except (ValueError, TypeError):
        return 999  # Very old if parsing fails


def discover_candidates(
    run_index: dict[str, Any],
    types: Optional[list[str]] = None,
    older_than: Optional[int] = None,
    prod_sweep: bool = False,
    include_prod: bool = False,
    pinned_run_ids: set[str] | None = None,
    reconcile: bool = False,
) -> CleanupPlan:
    """Discover cleanup candidates using deterministic logic.

    Args:
        run_index: The run index data
        types: List of run types to include (test, dev, prod, benchmark)
        older_than: Only include runs older than N days
        prod_sweep: Select all runs except prod and pinned
        include_prod: Allow prod runs to be candidates (requires explicit confirmation)
        pinned_run_ids: Additional run IDs to protect

    Returns:
        CleanupPlan with candidates and protection info

    """
    plan = CleanupPlan()

    # Get latest and pinned runs
    plan.latest_run_id = get_latest_run_id()
    plan.pinned_runs = set(pinned_run_ids or [])

    # Load config values
    try:
        from src.utils.io_utils import load_settings

        config = load_settings("config/settings.yaml")
        config_pinned = config.get("cleanup", {}).get("pinned_runs", [])
        plan.pinned_runs.update(config_pinned)
    except ImportError:
        # Fallback to default if config loading fails
        pass

    # Process each run
    for run_id, run_data in run_index.items():
        run_type = detect_run_type(run_data)

        # Track prod runs for protection
        if run_type == "prod":
            plan.prod_runs.add(run_id)

        reason = None

        # Type filter
        if types and run_type not in types:
            continue

        # Age filter
        if older_than:
            age_days = get_run_age_days(run_data)
            if age_days < older_than:
                continue
            reason = "age_filter"

        # Prod sweep logic
        if prod_sweep:
            if run_type == "prod" and not include_prod:
                # Prod runs excluded unless explicitly included
                continue
            if run_type == "prod" and include_prod:
                reason = "prod_sweep_include_prod"
            else:
                reason = reason or "prod_sweep"
        elif not reason and types:
            # If no prod sweep and we have type filter, use type filter reason
            reason = "type_filter"

        # Type filter logic (if no other reason found)
        if not reason and types:
            reason = "type_filter"

        # Add to candidates if we have a reason
        if reason:
            plan.add_candidate(run_id, run_data, reason)

    # Sort for deterministic output
    plan.sort_candidates()

    # Optional reconciliation pass: include orphans / stale index entries
    if reconcile:
        fs_runs = scan_filesystem_runs()
        index_runs = set(run_index.keys())
        # Orphans = on disk but not in index
        orphan_runs = sorted(fs_runs - index_runs)
        # Stale index = in index but no dirs on disk
        stale_index = sorted(index_runs - fs_runs)

        for rid in orphan_runs:
            # dummy run_data for display; delete_run_directories handles dirs
            plan.add_candidate(
                rid,
                {"input_paths": [], "timestamp": ""},
                "orphan_directory",
            )
        for rid in stale_index:
            plan.add_candidate(rid, run_index.get(rid, {}), "stale_index")
        plan.sort_candidates()

    return plan


def delete_run_directories(run_id: str) -> bool:
    """Delete run directories safely."""
    success = True

    # Delete interim directory
    interim_dir = get_interim_dir(run_id)
    if interim_dir.exists():
        try:
            import shutil

            shutil.rmtree(interim_dir)
            logger.info(f"Deleted interim directory: {interim_dir}")
        except OSError as e:
            logger.warning(f"Could not delete {interim_dir}: {e}")
            success = False

    # Delete processed directory
    processed_dir = get_processed_dir(run_id)
    if processed_dir.exists():
        try:
            import shutil

            shutil.rmtree(processed_dir)
            logger.info(f"Deleted processed directory: {processed_dir}")
        except OSError as e:
            logger.warning(f"Could not delete {processed_dir}: {e}")
            success = False

    return success


def update_latest_symlink() -> None:
    """Update the latest symlink if it points to a deleted run."""
    latest_path = get_processed_dir("latest")
    if not latest_path.exists() or not latest_path.is_symlink():
        return

    try:
        target = latest_path.resolve()
        if not target.exists():
            # Latest symlink points to non-existent directory
            latest_path.unlink()
            logger.info("Removed stale latest symlink")
    except OSError:
        # Symlink is broken
        latest_path.unlink()
        logger.info("Removed broken latest symlink")


def execute_cleanup(
    plan: CleanupPlan,
    run_index: dict[str, Any],
) -> tuple[list[str], int]:
    """Execute the cleanup plan.

    Returns:
        Tuple of (deleted_run_ids, pruned_index_count)

    """
    deleted_runs = []
    pruned_index_count = 0

    deletable_candidates = plan.get_deletable_candidates()

    logger.info(f"Executing cleanup for {len(deletable_candidates)} runs...")

    for run_id, _run_data, reason in deletable_candidates:
        if reason == "stale_index":
            # Only remove from index
            del run_index[run_id]
            pruned_index_count += 1
            logger.info(f"Removed stale index entry: {run_id}")
        elif reason == "orphan_directory":
            # Only delete directories (not in index)
            if delete_run_directories(run_id):
                deleted_runs.append(run_id)
                logger.info(f"Deleted orphan directory: {run_id}")
            else:
                logger.warning(f"Failed to delete orphan directory: {run_id}")
        # Delete directories and remove from index
        elif delete_run_directories(run_id):
            del run_index[run_id]
            deleted_runs.append(run_id)
            logger.info(f"Deleted: {run_id}")
        else:
            logger.warning(f"Failed to delete: {run_id}")

    # Update latest symlink if needed
    update_latest_symlink()

    return deleted_runs, pruned_index_count


def main() -> int:
    """Main cleanup function."""
    parser = argparse.ArgumentParser(
        description="Clean up test and demo artifacts safely",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Dry-run: list test/dev runs older than 7 days
  python tools/cleanup_test_artifacts.py --types test,dev --older-than 7
  # Prod sweep (keep prod + pinned)
  python tools/cleanup_test_artifacts.py --prod-sweep --dry-run
  # Actually delete (requires confirmation)
  python tools/cleanup_test_artifacts.py --prod-sweep --really-delete --yes
        """,
    )

    parser.add_argument(
        "--types",
        help="Comma-separated run types to include (test,dev,prod,benchmark)",
    )
    parser.add_argument(
        "--older-than",
        type=int,
        help="Only consider runs older than N days",
    )
    parser.add_argument(
        "--prod-sweep",
        action="store_true",
        help="Select all runs except prod and pinned",
    )
    parser.add_argument(
        "--include-prod",
        action="store_true",
        help="Allow prod runs to be candidates (requires explicit confirmation)",
    )
    parser.add_argument(
        "--pin-run-ids",
        help="Comma-separated run IDs to protect (merged with config.pinned_runs)",
    )
    parser.add_argument(
        "--really-delete",
        action="store_true",
        help="Actually delete files (default is dry-run)",
    )
    parser.add_argument("--yes", action="store_true", help="Skip confirmation prompts")
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print machine-readable JSON output",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Explicit dry-run (alias: omit --really-delete)",
    )
    parser.add_argument(
        "--reconcile",
        action="store_true",
        help="Also discover orphan run directories and stale index entries",
    )
    parser.add_argument(
        "--allow-empty",
        action="store_true",
        help="Allow cleanup to leave zero runs (empty state)",
    )
    parser.add_argument(
        "--delete-latest-symlink",
        action="store_true",
        help="Remove latest symlink if its target is deleted",
    )
    parser.add_argument(
        "--keep-at-least",
        type=int,
        help="Override config minimum runs to keep (default from config.cleanup.keep_at_least)",
    )

    args = parser.parse_args()

    # Parse comma-separated values
    types = [t.strip() for t in args.types.split(",")] if args.types else None
    pinned_run_ids = (
        set([rid.strip() for rid in args.pin_run_ids.split(",")])
        if args.pin_run_ids
        else set()
    )

    # Validate arguments
    if not any([types, args.older_than, args.prod_sweep]):
        logger.error(
            "Must specify at least one filter criterion (--types, --older-than, or --prod-sweep)",
        )
        return 1

    # Load run index
    run_index = load_run_index()
    # Do not early-return: --reconcile may still find orphans

    # Discover candidates
    plan = discover_candidates(
        run_index=run_index,
        types=types,
        older_than=args.older_than,
        prod_sweep=args.prod_sweep,
        include_prod=args.include_prod,
        pinned_run_ids=pinned_run_ids,
        reconcile=args.reconcile,
    )

    if not plan.candidates:
        logger.info("No candidates found matching criteria")
        return 0

    # Check for protected candidates
    protected_candidates = plan.get_protected_candidates()
    if protected_candidates:
        logger.info(f"Protected candidates (will be excluded): {protected_candidates}")

    # Get deletable candidates
    deletable_candidates = plan.get_deletable_candidates()
    if not deletable_candidates:
        logger.info("No deletable candidates found (all are protected)")
        return 0

    # Enforce keep-at-least unless allow-empty or explicit override
    keep_min = args.keep_at_least if args.keep_at_least is not None else 1
    if not args.allow_empty and keep_min > 0:
        remaining = len(run_index) - len(plan.candidates)
        if remaining < keep_min:
            logger.warning(
                f"Aborting: would leave {remaining} runs (< keep-at-least={keep_min}). "
                "Use --allow-empty or --keep-at-least 0 to override.",
            )
            return 2

    # Check for prod candidates requiring special handling
    prod_candidates = [
        rid for rid, _, _ in deletable_candidates if rid in plan.prod_runs
    ]
    if prod_candidates and not args.include_prod:
        logger.warning(f"Prod runs found but --include-prod not set: {prod_candidates}")
        logger.info("Excluding prod runs from candidates")
        # Remove prod candidates
        plan.candidates = [
            (rid, data, reason)
            for rid, data, reason in plan.candidates
            if rid not in plan.prod_runs
        ]
        deletable_candidates = plan.get_deletable_candidates()
        if not deletable_candidates:
            logger.info("No deletable candidates remaining after excluding prod runs")
            return 0

    # Print candidates
    if args.json:
        out = {
            "candidates": [
                {
                    "run_id": rid,
                    "reason": reason,
                    "age_days": get_run_age_days(rdata),
                    "run_type": detect_run_type(rdata),
                    "protected": plan.is_protected(rid),
                    "input_paths": rdata.get("input_paths", []),
                }
                for (rid, rdata, reason) in plan.candidates
            ],
            "deletable": [
                {
                    "run_id": rid,
                    "reason": reason,
                    "age_days": get_run_age_days(rdata),
                    "run_type": detect_run_type(rdata),
                }
                for (rid, rdata, reason) in deletable_candidates
            ],
            "protected": list(plan.pinned_runs),
            "latest": plan.latest_run_id,
        }
        print(json.dumps(out, indent=2))
    else:
        logger.info(f"Found {len(plan.candidates)} candidate(s) for cleanup:")
        for run_id, run_data, reason in plan.candidates:
            input_paths = run_data.get("input_paths", [])
            age_days = get_run_age_days(run_data)
            run_type = detect_run_type(run_data)
            prot = " [protected]" if plan.is_protected(run_id) else ""
            print(
                f"  {run_id} ({reason}) - {run_type} - {input_paths[0] if input_paths else 'no input'} - {age_days} days old{prot}",
            )

        if deletable_candidates:
            logger.info(f"\n{len(deletable_candidates)} run(s) will be deleted:")
            for run_id, _, reason in deletable_candidates:
                print(f"  {run_id} ({reason})")

    # Check for interactive confirmation
    if args.really_delete and not args.yes and sys.stdin.isatty():
        # Special confirmation for prod runs
        if prod_candidates and args.include_prod:
            logger.warning("⚠️  PRODUCTION RUNS WILL BE DELETED!")
            logger.warning(f"Prod runs to delete: {prod_candidates}")
            response = input(
                "Are you absolutely sure you want to delete PRODUCTION runs? Type 'DELETE PROD' to confirm: ",
            )
            if response != "DELETE PROD":
                logger.info("Aborted - production runs require explicit confirmation")
                return 1

        # General confirmation
        print(f"\nAbout to delete {len(deletable_candidates)} run(s)")
        response = input("Continue? (y/N): ")
        if response.lower() not in ["y", "yes"]:
            logger.info("Aborted")
            return 1

    # Execute cleanup
    if args.really_delete and not args.dry_run:
        deleted_runs, pruned_index_count = execute_cleanup(plan, run_index)

        # Save updated index
        save_run_index(run_index)

        # Handle empty state if all runs were deleted
        if len(run_index) == 0:
            if args.allow_empty:
                logger.info("All runs deleted - entering empty state")
                from src.utils.path_utils import write_latest_pointer

                write_latest_pointer(None)  # Set latest to None
                logger.info(
                    "Empty state established - latest.json updated, symlink removed",
                )
            else:
                logger.warning("All runs would be deleted but --allow-empty not set")
                logger.warning("Consider using --allow-empty to support empty state")

        # Print summary
        summary = {
            "dry_run": False,
            "scanned": len(run_index),
            "candidates": [run_id for run_id, _, _ in plan.candidates],
            "deleted": deleted_runs,
            "pruned_index": pruned_index_count,
            "protected_latest": bool(plan.latest_run_id),
            "pinned_runs": list(plan.pinned_runs),
            "empty_state": len(run_index) == 0,
        }

        logger.info(f"Cleanup completed: {json.dumps(summary, indent=2)}")

        return 0
    # Dry run - return exit code 2 to indicate candidates found
    return 2


if __name__ == "__main__":
    sys.exit(main())
