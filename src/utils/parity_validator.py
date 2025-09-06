"""Parity Validator for Phase 1.35.4.

This module validates that DuckDB and pandas produce identical results
for group statistics computation.
"""

import json
import logging
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# Constants for group stats
GROUP_ID = "group_id"
GROUP_SIZE = "group_size"
MAX_SCORE = "max_score"
PRIMARY_NAME = "primary_name"
DISPOSITION = "disposition"

# Parity validation constants
MAX_ALLOWED_MISMATCHES = 2


class ParityValidator:
    """Validates parity between DuckDB and pandas group stats."""

    def __init__(self, tolerance: float = 1e-9):
        """Initialize parity validator.

        Args:
            tolerance: Tolerance for floating point comparisons

        """
        self.tolerance = tolerance

    def validate_group_stats_parity(
        self,
        duckdb_df: pd.DataFrame,
        pandas_df: pd.DataFrame,
        run_id: str,
    ) -> tuple[bool, dict[str, Any]]:
        """Validate that DuckDB and pandas group stats are identical.

        Args:
            duckdb_df: Group stats from DuckDB
            pandas_df: Group stats from pandas
            run_id: Pipeline run ID for reporting

        Returns:
            Tuple of (is_parity_valid, parity_report_dict)

        """
        logger.info(
            f"parity_validator | starting_validation | duckdb_groups={len(duckdb_df)} | pandas_groups={len(pandas_df)}",
        )

        # Ensure both DataFrames have the same columns
        required_columns = [GROUP_ID, GROUP_SIZE, MAX_SCORE, PRIMARY_NAME, DISPOSITION]
        missing_columns = []

        for col in required_columns:
            if col not in duckdb_df.columns:
                missing_columns.append(f"duckdb_missing_{col}")
            if col not in pandas_df.columns:
                missing_columns.append(f"pandas_missing_{col}")

        if missing_columns:
            error_msg = f"Missing columns: {missing_columns}"
            logger.error(f"parity_validator | validation_failed | error={error_msg}")
            return False, {"error": error_msg}

        # Sort both DataFrames by group_id for comparison
        duckdb_sorted = duckdb_df.sort_values(GROUP_ID).reset_index(drop=True)
        pandas_sorted = pandas_df.sort_values(GROUP_ID).reset_index(drop=True)

        # Check row count parity
        if len(duckdb_sorted) != len(pandas_sorted):
            error_msg = f"Row count mismatch: DuckDB={len(duckdb_sorted)}, Pandas={len(pandas_sorted)}"
            logger.error(f"parity_validator | validation_failed | error={error_msg}")
            return False, {"error": error_msg}

        # Initialize parity report
        parity_report: dict[str, Any] = {
            "rows_compared": len(duckdb_sorted),
            "mismatches": 0,
            "metrics": {},
            "schema_parity": True,
            "dtype_mismatches": [],
        }

        # Validate each metric column
        for col in required_columns:
            col_report = self._validate_column_parity(
                duckdb_sorted[col],
                pandas_sorted[col],
                col,
            )
            parity_report["metrics"][col] = col_report

            if not col_report["equal"]:
                parity_report["mismatches"] += 1

        # Validate schema parity (dtypes)
        schema_parity = self._validate_schema_parity(duckdb_sorted, pandas_sorted)
        parity_report["schema_parity"] = schema_parity
        parity_report["dtype_mismatches"] = schema_parity["mismatches"]

        if schema_parity["mismatches"] > 0:
            parity_report["mismatches"] += schema_parity["mismatches"]

        # Determine overall parity
        is_parity_valid = parity_report["mismatches"] == 0

        # Log results
        if is_parity_valid:
            logger.info(
                f"parity_validator | validation_passed | rows={parity_report['rows_compared']} | mismatches=0",
            )
        else:
            logger.error(
                f"parity_validator | validation_failed | rows={parity_report['rows_compared']} | mismatches={parity_report['mismatches']}",
            )

        # Save parity report
        self._save_parity_report(parity_report, run_id)

        return is_parity_valid, parity_report

    def _validate_column_parity(
        self,
        duckdb_series: pd.Series,
        pandas_series: pd.Series,
        column_name: str,
    ) -> dict[str, Any]:
        """Validate parity for a single column.

        Args:
            duckdb_series: Series from DuckDB
            pandas_series: Series from pandas
            column_name: Name of the column being validated

        Returns:
            Column parity report

        """
        # Check if series are identical
        if duckdb_series.equals(pandas_series):
            return {
                "equal": True,
                "max_abs_diff": 0.0,
                "mismatch_count": 0,
                "mismatch_details": [],
            }

        # For numeric columns, check tolerance
        if pd.api.types.is_numeric_dtype(
            duckdb_series,
        ) and pd.api.types.is_numeric_dtype(pandas_series):
            # Convert to float64 for comparison
            duckdb_float = duckdb_series.astype("float64")
            pandas_float = pandas_series.astype("float64")

            # Calculate absolute differences
            abs_diff = np.abs(duckdb_float - pandas_float)
            max_abs_diff = abs_diff.max()

            # Check if differences are within tolerance
            within_tolerance = max_abs_diff <= self.tolerance

            if within_tolerance:
                return {
                    "equal": True,
                    "max_abs_diff": max_abs_diff,
                    "mismatch_count": 0,
                    "mismatch_details": [],
                }
            # Find mismatches
            mismatch_mask = abs_diff > self.tolerance
            mismatch_count = int(
                mismatch_mask.sum(),
            )  # Convert numpy int64 to Python int
            mismatch_indices = np.where(mismatch_mask)[0].tolist()

            return {
                "equal": False,
                "max_abs_diff": max_abs_diff,
                "mismatch_count": mismatch_count,
                "mismatch_details": mismatch_indices[:10],  # Limit to first 10
            }

        # For string columns, check exact match
        if pd.api.types.is_string_dtype(
            duckdb_series,
        ) or pd.api.types.is_object_dtype(duckdb_series):
            # Convert to string for comparison
            duckdb_str = duckdb_series.astype(str)
            pandas_str = pandas_series.astype(str)

            # Check exact match
            if duckdb_str.equals(pandas_str):
                return {
                    "equal": True,
                    "max_abs_diff": 0.0,
                    "mismatch_count": 0,
                    "mismatch_details": [],
                }
            # Find mismatches
            mismatch_mask = (duckdb_str != pandas_str).to_numpy()
            mismatch_count = int(
                mismatch_mask.sum(),
            )  # Convert numpy int64 to Python int
            mismatch_indices = np.where(mismatch_mask)[0].tolist()

            return {
                "equal": False,
                "max_abs_diff": float("inf"),  # Not applicable for strings
                "mismatch_count": mismatch_count,
                "mismatch_details": mismatch_indices[:10],  # Limit to first 10
            }

        # For other dtypes, check exact match
        if duckdb_series.equals(pandas_series):
            return {
                "equal": True,
                "max_abs_diff": 0.0,
                "mismatch_count": 0,
                "mismatch_details": [],
            }
        return {
            "equal": False,
            "max_abs_diff": float("inf"),
            "mismatch_count": -1,  # Unknown count
            "mismatch_details": [],
        }

    def _validate_schema_parity(
        self,
        duckdb_df: pd.DataFrame,
        pandas_df: pd.DataFrame,
    ) -> dict[str, Any]:
        """Validate that both DataFrames have matching dtypes.

        Args:
            duckdb_df: DataFrame from DuckDB
            pandas_df: DataFrame from pandas

        Returns:
            Schema parity report

        """
        schema_report: dict[str, Any] = {
            "mismatches": 0,
            "dtype_comparisons": {},
            "mismatch_details": [],
        }

        # Compare dtypes for each column
        for col in duckdb_df.columns:
            if col not in pandas_df.columns:
                continue

            duckdb_dtype = str(duckdb_df[col].dtype)
            pandas_dtype = str(pandas_df[col].dtype)

            # Normalize dtypes for comparison
            duckdb_normalized = self._normalize_dtype(duckdb_dtype)
            pandas_normalized = self._normalize_dtype(pandas_dtype)

            is_match = duckdb_normalized == pandas_normalized

            schema_report["dtype_comparisons"][col] = {
                "duckdb_dtype": duckdb_dtype,
                "pandas_dtype": pandas_dtype,
                "duckdb_normalized": duckdb_normalized,
                "pandas_normalized": pandas_normalized,
                "match": is_match,
            }

            if not is_match:
                schema_report["mismatches"] += 1
                schema_report["mismatch_details"].append(
                    {
                        "column": col,
                        "duckdb_dtype": duckdb_dtype,
                        "pandas_dtype": pandas_dtype,
                    },
                )

        return schema_report

    def _normalize_dtype(self, dtype_str: str) -> str:
        """Normalize dtype string for comparison.

        Args:
            dtype_str: Pandas dtype string

        Returns:
            Normalized dtype string

        """
        dtype_lower = dtype_str.lower()

        # Map similar dtypes
        if "int" in dtype_lower:
            return "int"
        if "float" in dtype_lower:
            return "float"
        if "string" in dtype_lower or "object" in dtype_lower:
            return "string"
        if "bool" in dtype_lower:
            return "bool"
        return dtype_lower

    def _save_parity_report(self, parity_report: dict[str, Any], run_id: str) -> None:
        """Save parity report to file.

        Args:
            parity_report: Parity validation report
            run_id: Pipeline run ID

        """
        try:
            # Create output directory
            output_dir = Path(f"data/processed/{run_id}")
            output_dir.mkdir(parents=True, exist_ok=True)

            # Generate output path with no-overwrite policy
            output_path = output_dir / "parity_report_group_stats.json"
            if output_path.exists():
                # Add timestamp suffix
                from datetime import datetime

                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                output_path = output_dir / f"parity_report_group_stats_{timestamp}.json"
                logger.info(
                    f"parity_validator | existing_file_present | fallback_path={output_path} | reason=no_overwrite_policy",
                )

            # Save report
            with open(output_path, "w") as f:
                json.dump(parity_report, f, indent=2, default=str)

            logger.info(
                f"parity_validator | report_saved | path={output_path} | mismatches={parity_report['mismatches']}",
            )

        except Exception as e:
            logger.error(f"parity_validator | report_save_failed | error={e}")


def create_parity_validator(tolerance: float = 1e-9) -> ParityValidator:
    """Factory function to create parity validator."""
    return ParityValidator(tolerance)


def assert_parity_or_exit(report: dict) -> None:
    """Assert parity validation passed or exit with error.

    Args:
        report: Parity validation report

    Raises:
        SystemExit: If mismatches exceed MAX_ALLOWED_MISMATCHES

    """
    mismatches = int(report.get("mismatches", 0))
    if mismatches > MAX_ALLOWED_MISMATCHES:
        raise SystemExit(
            f"Parity failed: mismatches={mismatches} > {MAX_ALLOWED_MISMATCHES}",
        )
