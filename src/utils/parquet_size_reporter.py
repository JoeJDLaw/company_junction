"""Parquet Size Reporter for Phase 1.35.4.

This module provides:
- File size analysis for Parquet files
- Compression and encoding metrics
- Column pruning analysis
- Size reporting for review parquet files
"""

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

logger = logging.getLogger(__name__)


class ParquetSizeReporter:
    """Analyzes and reports on Parquet file sizes and characteristics."""

    def __init__(self, target_size_mb: float = 180.0):
        """Initialize parquet size reporter.

        Args:
            target_size_mb: Target size for review parquet files

        """
        self.target_size_mb = target_size_mb

    def analyze_parquet_file(self, file_path: str) -> Dict[str, Any]:
        """Analyze a single Parquet file.

        Args:
            file_path: Path to the Parquet file

        Returns:
            Analysis report dictionary

        """
        path_obj = Path(file_path)

        if not path_obj.exists():
            return {
                "error": f"File not found: {file_path}",
                "path": str(file_path),
                "size_mb": 0.0,
            }

        try:
            # Get file size
            file_size_bytes = path_obj.stat().st_size
            file_size_mb = file_size_bytes / (1024 * 1024)

            # Read Parquet metadata using pyarrow directly
            import pyarrow.parquet as pq

            metadata = pq.read_metadata(path_obj)

            # Extract compression info
            compression_info = self._extract_compression_info(metadata)

            # Extract column info
            column_info = self._extract_column_info(metadata)

            # Calculate compression ratio
            compression_ratio = self._calculate_compression_ratio(metadata)

            # Generate report
            report = {
                "path": str(file_path),
                "size_mb": round(file_size_mb, 2),
                "size_bytes": file_size_bytes,
                "compression": compression_info.get("compression", "unknown"),
                "dictionary_encoding": compression_info.get(
                    "dictionary_encoding",
                    False,
                ),
                "columns_pruned": 0,  # Will be updated if comparison data available
                "compression_ratio": compression_ratio,
                "row_groups": metadata.num_row_groups,
                "total_rows": metadata.num_rows,
                "columns": len(metadata.schema),
                "column_details": column_info,
                "meets_target_size": file_size_mb <= self.target_size_mb,
                "target_size_mb": self.target_size_mb,
            }

            logger.info(
                f"parquet_size_reporter | analysis_complete | path={file_path} | "
                f"size_mb={file_size_mb:.2f} | compression={report['compression']} | "
                f"meets_target={report['meets_target_size']}",
            )

            return report

        except Exception as e:
            logger.error(
                f"parquet_size_reporter | analysis_failed | path={file_path} | error={e}",
            )
            return {"error": str(e), "path": str(file_path), "size_mb": 0.0}

    def _extract_compression_info(self, metadata: Any) -> Dict[str, Any]:
        """Extract compression information from Parquet metadata."""
        try:
            # Check if any column groups use dictionary encoding
            dictionary_encoding = False
            compression_type = "unknown"

            for rg_idx in range(metadata.num_row_groups):
                row_group = metadata.row_group(rg_idx)
                for col_idx in range(row_group.num_columns):
                    column = row_group.column(col_idx)
                    if column.is_stats_set:
                        stats = column.statistics
                        if hasattr(stats, "has_min_max") and stats.has_min_max:
                            # Check if dictionary encoding is used
                            if hasattr(column, "encoding_stats"):
                                for encoding in column.encoding_stats:
                                    if encoding.encoding == 2:  # Dictionary encoding
                                        dictionary_encoding = True

                    # Get compression type
                    if hasattr(column, "compression"):
                        if compression_type == "unknown":
                            compression_type = self._compression_code_to_name(
                                column.compression,
                            )

            return {
                "compression": compression_type,
                "dictionary_encoding": dictionary_encoding,
            }

        except Exception as e:
            logger.warning(
                f"parquet_size_reporter | compression_info_extraction_failed | error={e}",
            )
            return {"compression": "unknown", "dictionary_encoding": False}

    def _compression_code_to_name(self, compression_code: int) -> str:
        """Convert compression code to human-readable name."""
        compression_map = {
            0: "uncompressed",
            1: "snappy",
            2: "gzip",
            3: "lzo",
            4: "brotli",
            5: "lz4",
            6: "zstd",
            7: "lz4_raw",
        }
        return compression_map.get(compression_code, f"unknown_{compression_code}")

    def _extract_column_info(self, metadata: Any) -> List[Dict[str, Any]]:
        """Extract detailed column information from Parquet metadata."""
        columns = []

        try:
            for col_idx in range(len(metadata.schema.names)):
                col = metadata.schema.column(col_idx)

                # Get column statistics if available
                col_stats = {
                    "name": col.name,
                    "type": str(col.physical_type),
                    "logical_type": (
                        str(col.logical_type) if col.logical_type else "none"
                    ),
                    "repetition_level": col.max_repetition_level,
                }

                # Try to get size information
                try:
                    total_size = 0
                    for rg_idx in range(metadata.num_row_groups):
                        row_group = metadata.row_group(rg_idx)
                        if col_idx < row_group.num_columns:
                            column = row_group.column(col_idx)
                            total_size += column.total_compressed_size

                    col_stats["total_compressed_size"] = total_size
                    col_stats["avg_size_per_row"] = (
                        total_size / metadata.num_rows if metadata.num_rows > 0 else 0
                    )
                except Exception:
                    col_stats["total_compressed_size"] = 0
                    col_stats["avg_size_per_row"] = 0

                columns.append(col_stats)

        except Exception as e:
            logger.warning(
                f"parquet_size_reporter | column_info_extraction_failed | error={e}",
            )

        return columns

    def _calculate_compression_ratio(self, metadata: Any) -> Optional[float]:
        """Calculate compression ratio if possible."""
        try:
            total_compressed = 0
            total_uncompressed = 0

            for rg_idx in range(metadata.num_row_groups):
                row_group = metadata.row_group(rg_idx)
                for col_idx in range(row_group.num_columns):
                    column = row_group.column(col_idx)
                    total_compressed += column.total_compressed_size
                    total_uncompressed += column.total_uncompressed_size

            if total_uncompressed > 0:
                return round(total_compressed / total_uncompressed, 3)
            return None

        except Exception as e:
            logger.warning(
                f"parquet_size_reporter | compression_ratio_calculation_failed | error={e}",
            )
            return None

    def compare_parquet_files(
        self,
        original_path: str,
        optimized_path: str,
        run_id: str,
    ) -> Dict[str, Any]:
        """Compare two Parquet files and report differences.

        Args:
            original_path: Path to original Parquet file
            optimized_path: Path to optimized Parquet file
            run_id: Pipeline run ID for reporting

        Returns:
            Comparison report dictionary

        """
        logger.info(
            f"parquet_size_reporter | starting_comparison | original={original_path} | optimized={optimized_path}",
        )

        # Analyze both files
        original_report = self.analyze_parquet_file(original_path)
        optimized_report = self.analyze_parquet_file(optimized_path)

        # Calculate differences
        if "error" not in original_report and "error" not in optimized_report:
            size_reduction_mb = original_report["size_mb"] - optimized_report["size_mb"]
            size_reduction_percent = (
                (size_reduction_mb / original_report["size_mb"]) * 100
                if original_report["size_mb"] > 0
                else 0
            )

            comparison_report: Dict[str, Any] = {
                "original": original_report,
                "optimized": optimized_report,
                "comparison": {
                    "size_reduction_mb": round(size_reduction_mb, 2),
                    "size_reduction_percent": round(size_reduction_percent, 1),
                    "meets_target_size": optimized_report["size_mb"]
                    <= self.target_size_mb,
                    "target_size_mb": self.target_size_mb,
                },
                "timestamp": pd.Timestamp.now().isoformat(),
            }

            # Log comparison results
            logger.info(
                f"parquet_size_reporter | comparison_complete | "
                f"size_reduction={size_reduction_mb:.2f}MB ({size_reduction_percent:.1f}%) | "
                f"meets_target={comparison_report['comparison']['meets_target_size']}",
            )

        else:
            comparison_report = {
                "error": "Failed to analyze one or both files",
                "original": original_report,
                "optimized": optimized_report,
                "timestamp": pd.Timestamp.now().isoformat(),
            }

        # Save comparison report
        self._save_size_report(comparison_report, run_id)

        return comparison_report

    def _save_size_report(self, size_report: Dict[str, Any], run_id: str) -> None:
        """Save size report to file.

        Args:
            size_report: Size analysis report
            run_id: Pipeline run ID

        """
        try:
            # Create output directory
            output_dir = Path(f"data/processed/{run_id}")
            output_dir.mkdir(parents=True, exist_ok=True)

            # Generate output path with no-overwrite policy
            output_path = output_dir / "parquet_size_report.json"
            if output_path.exists():
                # Add timestamp suffix
                from datetime import datetime

                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                output_path = output_dir / f"parquet_size_report_{timestamp}.json"
                logger.info(
                    f"parquet_size_reporter | existing_file_present | fallback_path={output_path} | reason=no_overwrite_policy",
                )

            # Save report
            with open(output_path, "w") as f:
                json.dump(size_report, f, indent=2, default=str)

            logger.info(f"parquet_size_reporter | report_saved | path={output_path}")

        except Exception as e:
            logger.error(f"parquet_size_reporter | report_save_failed | error={e}")


def create_parquet_size_reporter(target_size_mb: float = 180.0) -> ParquetSizeReporter:
    """Factory function to create parquet size reporter."""
    return ParquetSizeReporter(target_size_mb)


def load_or_build_report(run_id: str, target_size_mb: float = 180.0) -> Dict[str, Any]:
    """Load existing size report or build a new one for a given run.

    Args:
        run_id: Pipeline run ID
        target_size_mb: Target size in MB for validation

    Returns:
        Size report dictionary

    """
    import json
    from pathlib import Path

    # Try to load existing report
    report_path = Path(f"data/processed/{run_id}/parquet_size_report.json")

    if report_path.exists():
        try:
            with open(report_path) as f:
                report: Dict[str, Any] = json.load(f)
            logger.info(f"parquet_size_reporter | report_loaded | path={report_path}")
            return report
        except Exception as e:
            logger.warning(f"parquet_size_reporter | report_load_failed | error={e}")

    # Build new report if loading failed
    logger.info(f"parquet_size_reporter | building_new_report | run_id={run_id}")

    # Look for parquet files in the run directory
    run_dir = Path(f"data/processed/{run_id}")
    if not run_dir.exists():
        return {"error": f"Run directory not found: {run_dir}"}

    # Find parquet files
    parquet_files = list(run_dir.glob("*.parquet"))
    if not parquet_files:
        return {"error": f"No parquet files found in {run_dir}"}

    # Create reporter and analyze files
    reporter = create_parquet_size_reporter(target_size_mb)

    # Analyze each file
    file_reports = []
    for parquet_file in parquet_files:
        try:
            file_report = reporter.analyze_parquet_file(str(parquet_file))
            file_reports.append(file_report)
        except Exception as e:
            logger.warning(
                f"parquet_size_reporter | file_analysis_failed | file={parquet_file} | error={e}",
            )

    # Create summary report
    summary_report = {
        "run_id": run_id,
        "timestamp": pd.Timestamp.now().isoformat(),
        "files": file_reports,
        "total_files": len(file_reports),
        "total_size_mb": sum(f.get("size_mb", 0) for f in file_reports),
        "target_size_mb": target_size_mb,
    }

    # Save the report
    reporter._save_size_report(summary_report, run_id)

    return summary_report
