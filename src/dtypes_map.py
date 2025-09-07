"""Centralized dtype mapping for memory-efficient data processing.

This module provides consistent dtype definitions across the pipeline
to minimize memory usage and ensure data type consistency.
"""

# Central dtype mapping for all pipeline dataframes
DTYPES = {
    # Core account fields
    "account_id": "string",  # Keep as string for alphanumeric IDs
    "account_name": "string",
    "created_date": "string",  # Keep as string for Excel serial compatibility
    # Normalization fields
    "name_core": "string",
    "name_core_tokens": "string",  # JSON string of token list
    "suffix_class": "category",
    "has_parentheses": "boolean",
    "has_semicolon": "boolean",
    "has_multiple_names": "boolean",
    # Similarity and grouping fields
    "group_id": "string",
    "score": "float32",
    "weakest_edge_to_primary": "float32",
    "is_primary": "boolean",
    "group_size": "int16",
    "group_rank": "int16",
    # Blocking fields
    "block_key": "string",
    "block_size": "int32",
    # Edge-gating fields
    "group_join_reason": "category",
    "shared_tokens_count": "int16",
    # Disposition fields
    "disposition": "category",
    "disposition_reason": "string",
    "applied_penalties": "string",  # JSON string of penalty dict
    "survivorship_reason": "string",
    # Alias fields
    "alias_candidates": "string",  # JSON string of alias list
    "alias_matches_count": "int16",
    # Relationship fields
    "relationship_rank": "int16",
    # Performance tracking
    "processing_time_ms": "int32",
}

# Columns that are allowed to remain as object dtype
ALLOWED_OBJECT_COLUMNS = {
    "name_core",
    "disposition_reason",
    "block_key",
    "applied_penalties",
    "survivorship_reason",
    "alias_candidates",
    "name_core_tokens",
    "account_id",
    "group_id",
    "group_join_reason",
    "id_a",
    "id_b",
    # Original Salesforce columns that may be present
    "account_name",
    "relationship",
    "disposition",
    "name_raw",
    "name_base",
    "alias_sources",
    "parent_account_id",
    "Disposition",
    "Relationship", 
    "Cleaned Account Name",
    # Additional Salesforce columns
    "Potential Case Employer Junction ID",
    "Relationship (If Other)",
    "Potential Case Employer Junction ID_6",
    "Cleaned Potential Case Name",
    "Potential Case Name: Potential Case Name",
    "Record Type",
    "Employer Name",
    "Account Owner: Full Name",
    "Account Record Type",
    "Created By: Full Name",
    "Format Search Name",
    "Known to have Arbitration Agreements?",
    "Last Modified By: Full Name",
    "Main Address",
    "Main Country",
    "Main Country (text only)",
    "Stage",
    "Company Association",
    "LR ID",
    "COI",
    "Last Modified Date",
    "Last Modified Date_5",
    "Created Date",  # Allow Created Date column from test data
    "created_date",  # Allow lowercase created_date column from test data
    # Additional columns from survivorship and alias matching
    "merge_preview_json",
    "alias_cross_refs",
}

# Columns to drop after intermediate processing steps
INTERMEDIATE_COLUMNS_TO_DROP = {
    "raw_name_tokens",
    "normalized_tokens",
    "block_members",
    "edge_scores",
    "temp_group_assignments",
}


def get_dtypes_for_schema(schema_name: str) -> dict[str, str]:
    """Get dtype mapping for specific pipeline schema.

    Args:
        schema_name: Name of the schema (e.g., 'accounts', 'pairs', 'groups')

    Returns:
        Dict mapping column names to dtypes

    """
    if schema_name == "accounts":
        return {
            k: v
            for k, v in DTYPES.items()
            if k
            in {
                "account_id",
                "account_name",
                "created_date",
                "name_core",
                "name_core_tokens",
                "suffix_class",
                "has_parentheses",
                "has_semicolon",
                "has_multiple_names",
                "relationship_rank",
            }
        }
    if schema_name == "pairs":
        return {
            k: v
            for k, v in DTYPES.items()
            if k
            in {
                "account_id",
                "name_core",
                "suffix_class",
                "score",
                "block_key",
                "block_size",
            }
        }
    if schema_name == "groups":
        return {
            k: v
            for k, v in DTYPES.items()
            if k
            in {
                "account_id",
                "group_id",
                "name_core",
                "suffix_class",
                "weakest_edge_to_primary",
                "is_primary",
                "group_size",
                "group_rank",
                "group_join_reason",
                "shared_tokens_count",
                "survivorship_reason",
            }
        }
    if schema_name == "review_ready":
        return {
            k: v
            for k, v in DTYPES.items()
            if k
            in {
                "account_id",
                "account_name",
                "created_date",
                "group_id",
                "name_core",
                "suffix_class",
                "is_primary",
                "group_size",
                "disposition",
                "disposition_reason",
                "applied_penalties",
                "alias_candidates",
                "alias_matches_count",
                "relationship_rank",
                "group_join_reason",
                "weakest_edge_to_primary",
                "shared_tokens_count",
                "survivorship_reason",
            }
        }
    return DTYPES.copy()
