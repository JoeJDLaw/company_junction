"""
Salesforce integration module for the Company Junction pipeline.

This module handles:
- Salesforce CLI operations
- Data synchronization back to Salesforce
- Record updates and deletions
- Batch operations
"""

import subprocess
import logging
from typing import List, Dict, Optional, Any
import pandas as pd

logger = logging.getLogger(__name__)


class SalesforceCLI:
    """Wrapper for Salesforce CLI operations."""

    def __init__(self, org_alias: Optional[str] = None):
        """
        Initialize Salesforce CLI wrapper.

        Args:
            org_alias: Salesforce org alias to use (optional)
        """
        self.org_alias = org_alias
        self._check_cli_installed()

    def _check_cli_installed(self) -> None:
        """Check if Salesforce CLI is installed and accessible."""
        try:
            result = subprocess.run(
                ["sf", "--version"], capture_output=True, text=True, check=True
            )
            logger.info(f"Salesforce CLI version: {result.stdout.strip()}")
        except (subprocess.CalledProcessError, FileNotFoundError):
            raise RuntimeError("Salesforce CLI not found. Please install it first.")

    def _run_command(self, command: List[str]) -> Dict[str, Any]:
        """
        Run a Salesforce CLI command.

        Args:
            command: List of command arguments

        Returns:
            Dictionary containing command result
        """
        try:
            result = subprocess.run(command, capture_output=True, text=True, check=True)
            return {"success": True, "stdout": result.stdout, "stderr": result.stderr}
        except subprocess.CalledProcessError as e:
            logger.error(f"Salesforce CLI command failed: {e}")
            return {
                "success": False,
                "stdout": e.stdout,
                "stderr": e.stderr,
                "return_code": e.returncode,
            }

    def list_orgs(self) -> Dict[str, Any]:
        """List available Salesforce orgs."""
        return self._run_command(["sf", "org", "list"])

    def get_org_info(self) -> Dict[str, Any]:
        """Get information about the current/default org."""
        command = ["sf", "org", "display"]
        if self.org_alias:
            command.extend(["--target-org", self.org_alias])
        return self._run_command(command)

    def update_record(
        self, object_type: str, record_id: str, fields: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Update a single Salesforce record.

        Args:
            object_type: Salesforce object type (e.g., 'Account', 'Contact')
            record_id: Record ID to update
            fields: Dictionary of field names and values to update

        Returns:
            Command result dictionary
        """
        # Create a temporary JSON file with the update data
        _ = {
            "records": [
                {"attributes": {"type": object_type}, "Id": record_id, **fields}
            ]
        }

        # TODO: Implement actual record update logic
        # This would typically use sf data update or similar command
        logger.info(
            f"Would update {object_type} record {record_id} with fields: {fields}"
        )

        return {"success": True, "message": "Update operation simulated"}

    def delete_record(self, object_type: str, record_id: str) -> Dict[str, Any]:
        """
        Delete a Salesforce record.

        Args:
            object_type: Salesforce object type
            record_id: Record ID to delete

        Returns:
            Command result dictionary
        """
        # TODO: Implement actual record deletion logic
        logger.info(f"Would delete {object_type} record {record_id}")

        return {"success": True, "message": "Delete operation simulated"}

    def batch_update(
        self, object_type: str, records: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Perform batch update of multiple records.

        Args:
            object_type: Salesforce object type
            records: List of record dictionaries with 'Id' and field updates

        Returns:
            Command result dictionary
        """
        # TODO: Implement batch update logic
        logger.info(f"Would batch update {len(records)} {object_type} records")

        return {
            "success": True,
            "message": f"Batch update of {len(records)} records simulated",
        }


def sync_cleaned_data_to_salesforce(
    cleaned_df: pd.DataFrame, object_type: str, org_alias: Optional[str] = None
) -> Dict[str, Any]:
    """
    Sync cleaned and merged data back to Salesforce.

    Args:
        cleaned_df: DataFrame containing cleaned data with merge information
        object_type: Salesforce object type to sync
        org_alias: Salesforce org alias to use

    Returns:
        Dictionary containing sync results
    """
    sf_cli = SalesforceCLI(org_alias)

    # Filter for records that need updates (master records)
    master_records = cleaned_df[~cleaned_df["_is_duplicate"]].copy()

    # Filter for records to delete (duplicates)
    duplicate_records = cleaned_df[cleaned_df["_is_duplicate"]].copy()

    updates_count = 0
    deletes_count = 0
    errors: List[str] = []

    # Process master record updates
    for _, record in master_records.iterrows():
        # Remove internal columns
        update_fields: Dict[str, Any] = {
            str(col): val
            for col, val in record.items()
            if not str(col).startswith("_") and pd.notna(val)
        }

        if "Id" in update_fields:
            record_id = str(update_fields.pop("Id"))
            result = sf_cli.update_record(object_type, record_id, update_fields)
            if result["success"]:
                updates_count += 1
            else:
                errors.append(f"Failed to update record {record_id}")

    # Process duplicate record deletions
    for _, record in duplicate_records.iterrows():
        if "Id" in record and pd.notna(record["Id"]):
            result = sf_cli.delete_record(object_type, str(record["Id"]))
            if result["success"]:
                deletes_count += 1
            else:
                errors.append(f"Failed to delete record {record['Id']}")

    results = {"updates": updates_count, "deletes": deletes_count, "errors": errors}

    logger.info(
        f"Sync completed: {results['updates']} updates, {results['deletes']} deletes"
    )
    return results
