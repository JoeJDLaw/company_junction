"""Test helper utilities for data ingestion and normalization.

This module provides utilities to normalize test data to match the expected
internal schema used by the pipeline.
"""

from typing import Dict

import pandas as pd

from src.utils.schema_utils import ACCOUNT_ID, ACCOUNT_NAME, DISPOSITION, GROUP_ID


def canonicalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize CSV-style column names to canonical internal names.

    Maps common CSV column variations to the internal schema expected by
    the pipeline components.

    Args:
        df: DataFrame with CSV-style column names

    Returns:
        DataFrame with canonical column names

    """
    # Column mapping from CSV-style to canonical internal names
    column_mapping = {
        # Account/Company fields
        "Account ID": ACCOUNT_ID,
        "Account Name": ACCOUNT_NAME,
        "Id": ACCOUNT_ID,
        "Name": ACCOUNT_NAME,
        # Relationship fields
        "Relationship": "relationship",
        "Relationship (If Other)": "relationship",
        # Date fields
        "Created Date": "created_date",
        "Last Modified Date": "last_modified_date",
        "Created By: Full Name": "created_by",
        "Last Modified By: Full Name": "last_modified_by",
        # Address/Contact fields
        "Main Address": "main_address",
        "Main Country": "main_country",
        "Main Country (text only)": "main_country",
        "Email": "email",
        "Phone": "phone",
        # Business fields
        "Industry": "industry",
        "Rating": "rating",
        "Stage": "stage",
        "COI": "coi",
        # Junction/Association fields
        "Company Association": "company_association",
        "Potential Case Employer Junction ID": "potential_case_employer_junction_id",
        "Potential Case Employer Junction ID_6": "potential_case_employer_junction_id_6",
        # Other fields
        "Record Type": "record_type",
        "Account Record Type": "account_record_type",
        "Account Owner: Full Name": "account_owner",
        "Cleaned Potential Case Name": "cleaned_potential_case_name",
        "Potential Case Name: Potential Case Name": "potential_case_name",
        "Known to have Arbitration Agreements?": "arbitration_agreements",
        "LR ID": "lr_id",
        "Format Search Name": "format_search_name",
        "Employer Name": "employer_name",
        # Disposition fields
        "disposition": DISPOSITION,
        "disposition_reason": "disposition_reason",
        "survivorship_reason": "survivorship_reason",
        "applied_penalties": "applied_penalties",
        "group_join_reason": "group_join_reason",
        "merge_preview_json": "merge_preview_json",
        "alias_cross_refs": "alias_cross_refs",
    }

    # Create a copy to avoid modifying the original
    df_canonical = df.copy()

    # Rename columns that exist in the mapping
    rename_dict = {}
    for csv_name, canonical_name in column_mapping.items():
        if csv_name in df_canonical.columns:
            rename_dict[csv_name] = canonical_name

    if rename_dict:
        df_canonical = df_canonical.rename(columns=rename_dict)

    return df_canonical


def ensure_required_columns(df: pd.DataFrame, required_columns: list) -> pd.DataFrame:
    """Ensure DataFrame has required columns, adding defaults if missing.

    Args:
        df: DataFrame to check
        required_columns: List of required column names

    Returns:
        DataFrame with all required columns present

    """
    df_ensured = df.copy()

    for col in required_columns:
        if col not in df_ensured.columns:
            # Add default values based on column type
            if col == ACCOUNT_ID:
                df_ensured[col] = [f"test_id_{i}" for i in range(len(df_ensured))]
            elif col == GROUP_ID:
                df_ensured[col] = [f"group_{i}" for i in range(len(df_ensured))]
            elif col == "name_core":
                df_ensured[col] = df_ensured.get(
                    ACCOUNT_NAME, ["test_name"] * len(df_ensured),
                )
            elif col == "suffix_class":
                df_ensured[col] = ["corp"] * len(df_ensured)
            elif col == "alias_candidates" or col == "alias_sources":
                df_ensured[col] = [[] for _ in range(len(df_ensured))]
            else:
                df_ensured[col] = None

    return df_ensured


def create_test_fixture_data(
    base_data: Dict[str, list], required_columns: list | None = None,
) -> pd.DataFrame:
    """Create a test fixture DataFrame with canonical column names.

    Args:
        base_data: Dictionary of column data
        required_columns: List of required columns to ensure

    Returns:
        DataFrame with canonical column names and required columns

    """
    df = pd.DataFrame(base_data)
    df = canonicalize_columns(df)

    if required_columns:
        df = ensure_required_columns(df, required_columns)

    return df
