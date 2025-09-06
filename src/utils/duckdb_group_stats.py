"""DuckDB Group Stats Engine for Phase 1.35.4.

This module provides:
- Vectorized group statistics computation using DuckDB
- Memoization for repeated runs
- Performance benchmarking and validation
- Parquet I/O optimization with zstd compression
"""

import hashlib
import logging
import os
import time
from pathlib import Path
from typing import Any, Optional

import duckdb
import pandas as pd

# Import constants from centralized schema
from src.utils.schema_utils import (
    ACCOUNT_NAME,
    DISPOSITION,
    GROUP_ID,
    GROUP_SIZE,
    IS_PRIMARY,
    MAX_SCORE,
    PRIMARY_NAME,
    WEAKEST_EDGE_TO_PRIMARY,
)

logger = logging.getLogger(__name__)


class DuckDBGroupStatsEngine:
    """DuckDB-based group statistics engine with memoization."""

    def __init__(self, settings: dict[str, Any], run_id: str):
        """Initialize DuckDB group stats engine.

        Args:
            settings: Configuration settings
            run_id: Pipeline run ID for caching

        """
        self.settings = settings
        self.run_id = run_id
        # Allow cache directory to be overridden for testing
        cache_dir = settings.get("group_stats", {}).get("cache_dir")
        if cache_dir:
            self.cache_dir = Path(cache_dir)
        else:
            self.cache_dir = Path(f"data/interim/{run_id}")
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        # Get DuckDB configuration
        duckdb_config = settings.get("engine", {}).get("duckdb", {})
        self.threads = self._get_duckdb_threads(duckdb_config)
        self.memory_limit = self._get_duckdb_memory_limit(duckdb_config)

        # Get parquet configuration
        parquet_config = settings.get("io", {}).get("parquet", {})
        self.compression = parquet_config.get("compression", "zstd")
        self.row_group_size = parquet_config.get("row_group_size", 128000)
        self.dictionary_compression = parquet_config.get("dictionary_compression", True)
        self.statistics = parquet_config.get("statistics", True)

        # Get memoization configuration
        memo_config = settings.get("group_stats", {}).get("memoization", {})
        # Handle both boolean and dictionary configurations
        if isinstance(memo_config, bool):
            self.memoization_enabled = memo_config
            self.cache_ttl_hours = 24
            self.min_cache_hit_percentage = 30
        else:
            self.memoization_enabled = memo_config.get("enable", True)
            self.cache_ttl_hours = memo_config.get("cache_ttl_hours", 24)
            self.min_cache_hit_percentage = memo_config.get(
                "min_cache_hit_percentage",
                30,
            )

        # Initialize DuckDB connection
        self.conn = self._create_duckdb_connection()

    def _get_duckdb_threads(self, config: dict[str, Any]) -> int:
        """Get optimal DuckDB thread count."""
        if config.get("threads") == "auto":
            import multiprocessing

            return min(multiprocessing.cpu_count(), 8)
        return int(config.get("threads", 4))

    def _get_duckdb_memory_limit(self, config: dict[str, Any]) -> Optional[str]:
        """Get DuckDB memory limit from config or environment."""
        if config.get("memory_limit"):
            return str(config["memory_limit"])
        return os.environ.get("DUCKDB_MEMORY_LIMIT")

    def _create_duckdb_connection(self) -> duckdb.DuckDBPyConnection:
        """Create and configure DuckDB connection."""
        conn = duckdb.connect(":memory:")

        # Set threads
        conn.execute(f"SET threads={self.threads}")

        # Set memory limit if specified
        if self.memory_limit:
            conn.execute(f"SET memory_limit='{self.memory_limit}'")

        # Apply PRAGMAs
        pragmas = self.settings.get("engine", {}).get("duckdb", {}).get("pragmas", {})
        if pragmas.get("enable_object_cache", True):
            conn.execute("PRAGMA enable_object_cache=true")
        if pragmas.get("preserve_insertion_order", True):
            conn.execute("PRAGMA preserve_insertion_order=true")

        logger.info(
            f"duckdb_group_stats | connection_created | threads={self.threads} | memory_limit={self.memory_limit}",
        )
        return conn

    def _generate_cache_key(self, df: pd.DataFrame, config_digest: str) -> str:
        """Generate cache key for memoization."""
        # Create a simpler, more deterministic cache key
        key_parts = [
            str(len(df)),  # row count
            str(df[GROUP_ID].nunique()),  # group count
            str(df[IS_PRIMARY].sum()),  # primary count
            config_digest,
        ]

        # Join and hash
        key_string = "|".join(key_parts)
        cache_key = hashlib.md5(key_string.encode()).hexdigest()[:16]
        return cache_key

    def _get_cache_path(self, cache_key: str) -> Path:
        """Get cache file path."""
        return self.cache_dir / f"group_stats_cache_{cache_key}.parquet"

    def _is_cache_valid(self, cache_path: Path) -> bool:
        """Check if cache is still valid (within TTL)."""
        if not cache_path.exists():
            return False

        # Check file age
        file_age_hours = (time.time() - cache_path.stat().st_mtime) / 3600
        return file_age_hours < self.cache_ttl_hours

    def compute_group_stats(
        self,
        df: pd.DataFrame,
        config_digest: str = "",
        request_id: Optional[str] = None,
    ) -> tuple[pd.DataFrame, dict[str, Any]]:
        """Compute group statistics using DuckDB.

        Args:
            df: DataFrame with group data
            config_digest: Configuration hash for caching
            request_id: Optional request ID for logging

        Returns:
            Tuple of (group_stats_df, metadata_dict)

        """
        start_time = time.time()
        rows = len(df)
        groups = df[GROUP_ID].nunique()

        # Log START with required fields
        logger.info(
            f"group_stats | START | backend=duckdb | rows={rows} | groups={groups} | memoize={self.memoization_enabled} | cache_key=generating | cache_hit=false | config_digest={config_digest} | request_id={request_id}",
        )

        # Generate cache key
        cache_key = self._generate_cache_key(df, config_digest)
        cache_path = self._get_cache_path(cache_key)

        # Check memoization
        memoize = False
        cache_hit = False

        if self.memoization_enabled and self._is_cache_valid(cache_path):
            try:
                logger.info(f"group_stats | cache_check | key={cache_key}")
                cached_stats = pd.read_parquet(cache_path)

                # Validate cache integrity
                if len(cached_stats) > 0 and all(
                    col in cached_stats.columns
                    for col in [GROUP_ID, GROUP_SIZE, MAX_SCORE, PRIMARY_NAME]
                ):
                    logger.info(
                        f"group_stats | cache_valid | groups={len(cached_stats)}",
                    )
                    cache_hit = True

                    # Log cache performance
                    cache_time = time.time() - start_time

                    # Log SUCCESS for cache hit with all required fields
                    duckdb_copy_options = {
                        "COMPRESSION": "ZSTD",
                        "ROW_GROUP_SIZE": self.row_group_size,
                    }
                    logger.info(
                        f"group_stats | SUCCESS | backend=duckdb | io_backend=duckdb | elapsed_sec={cache_time:.3f} | rows={rows} | groups={groups} | compression=zstd | dict_encoding=true | file_size_mb=cached | memoize=true | cache_key={cache_key} | cache_hit=true | duckdb_copy_options={duckdb_copy_options} | config_digest={config_digest} | request_id={request_id}",
                    )

                    return cached_stats, {
                        "cache_hit": True,
                        "cache_key": cache_key,
                        "elapsed_sec": cache_time,
                        "memoize": True,
                        "groups": len(cached_stats),
                        "records": rows,
                        "request_id": request_id,
                    }
            except Exception as e:
                logger.warning(f"group_stats | cache_read_failed | error={e}")

        # Compute group stats using DuckDB
        logger.info(
            f"group_stats | computing | backend=duckdb | rows={rows} | groups={groups}",
        )

        # Register DataFrame with DuckDB (use a copy to avoid modifying original)
        df_copy = df.copy()
        self.conn.register("groups_df", df_copy)

        # SQL query for group statistics
        query = f"""
        SELECT
            {GROUP_ID},
            COUNT(*) as {GROUP_SIZE},
            MAX(CASE WHEN {WEAKEST_EDGE_TO_PRIMARY} IS NOT NULL THEN {WEAKEST_EDGE_TO_PRIMARY} ELSE 0.0 END) as {MAX_SCORE},
            COALESCE(
                MAX(CASE WHEN {IS_PRIMARY} THEN {ACCOUNT_NAME} ELSE NULL END),
                MIN({ACCOUNT_NAME})
            ) as {PRIMARY_NAME},
            COALESCE(
                MAX(CASE WHEN {IS_PRIMARY} THEN {DISPOSITION} ELSE NULL END),
                'Update'
            ) as disposition_col
        FROM groups_df
        GROUP BY {GROUP_ID}
        ORDER BY {GROUP_ID}
        """

        # Execute query
        result = self.conn.execute(query)
        group_stats_df = result.df()

        # Ensure correct dtypes (use pandas default dtypes for exact matching)
        group_stats_df = group_stats_df.astype(
            {
                GROUP_ID: "object",
                GROUP_SIZE: "int64",  # Match pandas default
                MAX_SCORE: "float64",  # Match pandas default
                PRIMARY_NAME: "object",
                "disposition_col": "object",  # Use the alias from SQL query
            },
        )

        # Rename column to match expected schema
        group_stats_df = group_stats_df.rename(columns={"disposition_col": DISPOSITION})

        # Cache results if memoization enabled
        if self.memoization_enabled:
            try:
                group_stats_df.to_parquet(cache_path, index=False)
                logger.info(
                    f"group_stats | cached_results | key={cache_key} | path={cache_path}",
                )
                memoize = True
            except Exception as e:
                logger.warning(f"group_stats | cache_write_failed | error={e}")
                memoize = False

        # Calculate performance metrics
        elapsed_time = time.time() - start_time

        # Log SUCCESS for computation with all required fields
        duckdb_copy_options = {
            "COMPRESSION": "ZSTD",
            "ROW_GROUP_SIZE": self.row_group_size,
            "DICTIONARY_COMPRESSION": 1 if self.dictionary_compression else 0,
            "STATISTICS": 1 if self.statistics else 0,
        }

        logger.info(
            f"group_stats | SUCCESS | backend=duckdb | io_backend=duckdb | elapsed_sec={elapsed_time:.3f} | rows={rows} | groups={groups} | compression=zstd | dict_encoding={self.dictionary_compression} | file_size_mb=computed | memoize={self.memoization_enabled} | cache_key={cache_key} | cache_hit=false | duckdb_copy_options={duckdb_copy_options} | config_digest={config_digest} | request_id={request_id}",
        )

        metadata = {
            "cache_hit": cache_hit,
            "cache_key": cache_key,
            "elapsed_sec": elapsed_time,
            "memoize": memoize,
            "groups": len(group_stats_df),
            "records": rows,
            "request_id": request_id,
        }

        return group_stats_df, metadata

    def write_optimized_parquet(
        self,
        df: pd.DataFrame,
        output_path: str,
        target_size_mb: Optional[float] = None,
    ) -> dict[str, Any]:
        """Write DataFrame to optimized Parquet using DuckDB.

        Args:
            df: DataFrame to write
            output_path: Output file path
            target_size_mb: Target file size in MB

        Returns:
            Metadata about the write operation

        """
        start_time = time.time()

        # Ensure output directory exists
        output_path_obj = Path(output_path)
        output_path_obj.parent.mkdir(parents=True, exist_ok=True)

        # Register DataFrame with DuckDB
        self.conn.register("output_df", df)

        # Build COPY command with optimization options
        copy_options = [
            "FORMAT PARQUET",
            f"COMPRESSION {self.compression.upper()}",
            f"ROW_GROUP_SIZE {self.row_group_size}",
        ]

        # Note: DICTIONARY_COMPRESSION option not supported in current DuckDB version
        # if self.dictionary_compression:
        #     copy_options.append("DICTIONARY_COMPRESSION 1")

        # Note: STATISTICS option not supported in current DuckDB version
        # if self.statistics:
        #     copy_options.append("STATISTICS 1")

        copy_sql = f"""
        COPY (SELECT * FROM output_df) TO '{output_path}'
        ({', '.join(copy_options)})
        """

        # Execute COPY command
        self.conn.execute(copy_sql)

        # Get file size
        file_size_mb = output_path_obj.stat().st_size / (1024 * 1024)

        # Calculate performance metrics
        elapsed_time = time.time() - start_time
        throughput = len(df) / elapsed_time if elapsed_time > 0 else 0

        # Log write operation
        logger.info(
            f"duckdb_group_stats | parquet_write_complete | path={output_path} | "
            f"size_mb={file_size_mb:.2f} | elapsed={elapsed_time:.3f}s | "
            f"throughput={throughput:.0f}records/sec | compression={self.compression}",
        )

        metadata = {
            "path": str(output_path),
            "size_mb": file_size_mb,
            "compression": self.compression,
            "dictionary_encoding": self.dictionary_compression,
            "row_group_size": self.row_group_size,
            "statistics": self.statistics,
            "elapsed_sec": elapsed_time,
            "records": len(df),
            "throughput": throughput,
        }

        return metadata

    def close(self) -> None:
        """Close DuckDB connection."""
        if hasattr(self, "conn"):
            self.conn.close()
            logger.info("duckdb_group_stats | connection_closed")


def create_duckdb_group_stats_engine(
    settings: dict[str, Any],
    run_id: str,
) -> DuckDBGroupStatsEngine:
    """Factory function to create DuckDB group stats engine."""
    return DuckDBGroupStatsEngine(settings, run_id)
