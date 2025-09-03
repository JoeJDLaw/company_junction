"""
Pipeline constants for the company junction deduplication pipeline.

This module provides standardized file naming and stage definitions to ensure
consistency between pipeline stages and mini-DAG resume logic.

Phase 1.27.2: Mini-DAG resume system standardization
"""

from typing import Dict, List, Literal

# Pipeline stage names in execution order
PIPELINE_STAGES = [
    "normalization",
    "filtering", 
    "candidate_generation",
    "grouping",
    "survivorship",
    "disposition",
    "alias_matching",
    "final_output",
]

# Expected intermediate files for each stage
# Each stage requires all files from previous stages plus its own output
STAGE_INTERMEDIATE_FILES: Dict[str, List[str]] = {
    "normalization": [
        "accounts_filtered.parquet",  # Pipeline produces filtered, not normalized
    ],
    "filtering": [
        "accounts_filtered.parquet",
    ],
    "candidate_generation": [
        "accounts_filtered.parquet",
        "candidate_pairs.parquet",
    ],
    "grouping": [
        "accounts_filtered.parquet",
        "candidate_pairs.parquet", 
        "groups.parquet",
    ],
    "survivorship": [
        "accounts_filtered.parquet",
        "candidate_pairs.parquet",
        "groups.parquet",
        "survivorship.parquet",
    ],
    "disposition": [
        "accounts_filtered.parquet",
        "candidate_pairs.parquet",
        "groups.parquet",
        "survivorship.parquet",
        "dispositions.parquet",
    ],
    "alias_matching": [
        "accounts_filtered.parquet",
        "candidate_pairs.parquet",
        "groups.parquet",
        "survivorship.parquet",
        "dispositions.parquet",
        "alias_matches.parquet",
    ],
    "final_output": [
        "accounts_filtered.parquet",
        "candidate_pairs.parquet",
        "groups.parquet",
        "survivorship.parquet",
        "dispositions.parquet",
        "alias_matches.parquet",
    ],
}

# Final output files in processed directory
PROCESSED_OUTPUT_FILES = [
    "review_ready.parquet",
    "review_ready.csv", 
    "group_stats.parquet",
    "group_details.parquet",
    "review_meta.json",
]

# Resume validation constants
RESUME_VALIDATION_TIMEOUT = 5.0  # seconds
RESUME_STATE_REPAIR_ENABLED = True  # Feature flag for state repair

# Stage status types
StageStatus = Literal["pending", "running", "completed", "failed", "interrupted"]

# Resume decision codes for enhanced logging
ResumeDecision = Literal[
    "NO_PREVIOUS_RUN",
    "NEXT_STAGE_READY", 
    "NEXT_STAGE_MISSING",
    "FINAL_STAGE",
    "INVALID_STAGE_ORDER",
    "MISSING_FILES",
    "STATE_INCONSISTENT"
]

# Cleanup and reconciliation constants
CLEANUP_EXCLUDE_DIRS = {"default", "index", "legacy", "test_save_run", ".DS_Store"}

# Cleanup reason codes
CleanupReason = Literal[
    "type_filter",
    "age_filter", 
    "prod_sweep",
    "orphan_directory",
    "stale_index"
]
