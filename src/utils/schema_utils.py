"""
Schema utilities for the company junction pipeline.

This module provides canonical column names and schema mapping functionality.
Phase 1.25.1: Simple constants for column names
Phase 1.26.1: Dynamic schema resolution from spreadsheet headers
"""

import json
import logging
import re
from typing import Dict, Any, Optional, Mapping, List
import pandas as pd
from rapidfuzz import fuzz

logger = logging.getLogger(__name__)

# Phase 1.25.1: Canonical column constants
# These will be replaced by dynamic resolution in Phase 1.26.1

# Core identification columns
GROUP_ID = "group_id"
ACCOUNT_ID = "account_id"
ACCOUNT_NAME = "account_name"
PARENT_ACCOUNT_ID = "parent_account_id"

# Metadata columns
CREATED_DATE = "created_date"
SUFFIX_CLASS = "suffix_class"
RELATIONSHIP = "relationship"

# Grouping and similarity columns
WEAKEST_EDGE_TO_PRIMARY = "weakest_edge_to_primary"
IS_PRIMARY = "is_primary"
SCORE = "score"

# Disposition and classification columns
DISPOSITION = "Disposition"
DISPOSITION_REASON = "disposition_reason"

# Alias and cross-reference columns
ALIAS_CROSS_REFS = "alias_cross_refs"
ALIAS_CANDIDATES = "alias_candidates"

# Normalization columns
NAME_CORE = "name_core"
NAME_CORE_TOKENS = "name_core_tokens"
HAS_PARENTHESES = "has_parentheses"
HAS_SEMICOLON = "has_semicolon"
HAS_MULTIPLE_NAMES = "has_multiple_names"

# Candidate pair columns
ID_A = "id_a"
ID_B = "id_b"

# Performance and statistics columns
GROUP_SIZE = "group_size"
MAX_SCORE = "max_score"
PRIMARY_NAME = "primary_name"


def get_canonical_columns() -> Dict[str, str]:
    """
    Get mapping of canonical column names to their constants.

    Returns:
        Dictionary mapping canonical names to constants
    """
    return {
        "GROUP_ID": GROUP_ID,
        "ACCOUNT_ID": ACCOUNT_ID,
        "ACCOUNT_NAME": ACCOUNT_NAME,
        "PARENT_ACCOUNT_ID": PARENT_ACCOUNT_ID,
        "CREATED_DATE": CREATED_DATE,
        "SUFFIX_CLASS": SUFFIX_CLASS,
        "RELATIONSHIP": RELATIONSHIP,
        "WEAKEST_EDGE_TO_PRIMARY": WEAKEST_EDGE_TO_PRIMARY,
        "IS_PRIMARY": IS_PRIMARY,
        "SCORE": SCORE,
        "DISPOSITION": DISPOSITION,
        "DISPOSITION_REASON": DISPOSITION_REASON,
        "ALIAS_CROSS_REFS": ALIAS_CROSS_REFS,
        "ALIAS_CANDIDATES": ALIAS_CANDIDATES,
        "NAME_CORE": NAME_CORE,
        "NAME_CORE_TOKENS": NAME_CORE_TOKENS,
        "HAS_PARENTHESES": HAS_PARENTHESES,
        "HAS_SEMICOLON": HAS_SEMICOLON,
        "HAS_MULTIPLE_NAMES": HAS_MULTIPLE_NAMES,
        "ID_A": ID_A,
        "ID_B": ID_B,
        "GROUP_SIZE": GROUP_SIZE,
        "MAX_SCORE": MAX_SCORE,
        "PRIMARY_NAME": PRIMARY_NAME,
    }


def ensure_required_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure required columns exist in the DataFrame.

    Phase 1.25.1: Simple validation
    Phase 1.26.1: Will include dynamic schema resolution

    Args:
        df: DataFrame to validate

    Returns:
        DataFrame with required columns (no changes in Phase 1.25.1)

    Raises:
        ValueError: If required columns are missing
    """
    required_columns = [ACCOUNT_NAME]  # Only account_name is required for now

    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")

    return df


# Phase 1.26.1: Dynamic schema resolution implementation


def resolve_schema(
    df: pd.DataFrame,
    settings: Optional[Dict[str, Any]] = None,
    cli_overrides: Optional[Dict[str, str]] = None,
    input_filename: Optional[str] = None,
) -> Mapping[str, str]:
    """
    Resolve schema mapping from DataFrame headers to canonical names.

    Phase 1.26.1: Full implementation with CLI → template → synonym → heuristic fallback

    Args:
        df: DataFrame with headers to resolve
        settings: Configuration settings
        cli_overrides: Manual column overrides from CLI (--col flags)
        input_filename: Input filename for template matching

    Returns:
        Mapping from canonical names to actual column names

    Raises:
        ValueError: If required columns cannot be resolved
    """
    settings = settings or {}
    cli_overrides = cli_overrides or {}

    logger.info(f"Resolving schema for DataFrame with {len(df.columns)} columns")
    logger.debug(f"Available columns: {list(df.columns)}")

    # Start with empty mapping
    mapping = {}

    # 1. CLI overrides take precedence
    if cli_overrides:
        logger.info(f"Applying CLI overrides: {cli_overrides}")
        cli_mapping = _apply_cli_overrides(df, cli_overrides)
        mapping.update(cli_mapping)

    # 2. Template matching by filename (only for columns not already mapped)
    if input_filename:
        template_mapping = _match_filename_template(df, input_filename, settings)
        if template_mapping:
            # Only add columns not already mapped by CLI overrides
            for canonical_name, actual_name in template_mapping.items():
                if canonical_name not in mapping:
                    mapping[canonical_name] = actual_name
            if template_mapping and _validate_required_columns(mapping):
                logger.info(f"Schema resolved via filename template: {input_filename}")
                return mapping

    # 3. Synonym matching (only for columns not already mapped)
    synonym_mapping = _match_synonyms(df, settings)
    for canonical_name, actual_name in synonym_mapping.items():
        if canonical_name not in mapping:
            mapping[canonical_name] = actual_name

    if _validate_required_columns(mapping):
        logger.info("Schema resolved via synonym matching")
        return mapping

    # 4. Heuristic matching (only for columns not already mapped)
    heuristic_mapping = _apply_heuristics(df, settings)
    for canonical_name, actual_name in heuristic_mapping.items():
        if canonical_name not in mapping:
            mapping[canonical_name] = actual_name

    if _validate_required_columns(mapping):
        logger.info("Schema resolved via heuristics")
        return mapping

    # 5. Fail with helpful error
    available_cols = list(df.columns)
    logger.error(f"Could not resolve required columns. Available: {available_cols}")
    raise ValueError(
        f"Schema resolution failed. Required 'account_name' column not found. "
        f"Available columns: {available_cols}. "
        f"Use --col account_name=<column_name> to specify manually."
    )


def _apply_cli_overrides(
    df: pd.DataFrame, cli_overrides: Dict[str, str]
) -> Dict[str, str]:
    """Apply CLI column overrides."""
    mapping = {}

    for canonical_name, actual_name in cli_overrides.items():
        if actual_name in df.columns:
            mapping[canonical_name] = actual_name
            logger.debug(f"CLI override: {canonical_name} -> {actual_name}")
        else:
            logger.warning(
                f"CLI override column '{actual_name}' not found in DataFrame"
            )

    return mapping


def _match_filename_template(
    df: pd.DataFrame, input_filename: str, settings: Dict[str, Any]
) -> Optional[Dict[str, str]]:
    """Match filename against configured templates."""
    schema_config = settings.get("schema", {})
    templates = schema_config.get("templates", [])

    for template in templates:
        pattern = template.get("match", "")
        if not pattern:
            continue

        try:
            if re.match(pattern, input_filename):
                logger.debug(
                    f"Filename '{input_filename}' matches template pattern '{pattern}'"
                )
                aliases = template.get("aliases", {})
                return _build_mapping_from_aliases(df, aliases)
        except re.error as e:
            logger.warning(f"Invalid regex pattern '{pattern}': {e}")

    return None


def _match_synonyms(df: pd.DataFrame, settings: Dict[str, Any]) -> Dict[str, str]:
    """Match columns using configured synonyms."""
    schema_config = settings.get("schema", {})
    synonyms = schema_config.get("synonyms", {})

    return _build_mapping_from_aliases(df, synonyms)


def _build_mapping_from_aliases(
    df: pd.DataFrame, aliases: Dict[str, List[str]]
) -> Dict[str, str]:
    """Build column mapping from aliases configuration."""
    mapping = {}
    available_columns = set(df.columns)

    for canonical_name, possible_names in aliases.items():
        for possible_name in possible_names:
            # Exact match first
            if possible_name in available_columns:
                mapping[canonical_name] = possible_name
                logger.debug(
                    f"Exact synonym match: {canonical_name} -> {possible_name}"
                )
                break

            # Case-insensitive match
            for col in available_columns:
                if col.lower() == possible_name.lower():
                    mapping[canonical_name] = col
                    logger.debug(
                        f"Case-insensitive synonym match: {canonical_name} -> {col}"
                    )
                    break

            if canonical_name in mapping:
                break

    return mapping


def _apply_heuristics(df: pd.DataFrame, settings: Dict[str, Any]) -> Dict[str, str]:
    """Apply heuristics for column matching."""
    mapping = {}
    available_columns = list(df.columns)

    # Heuristic 1: String similarity for account_name
    if ACCOUNT_NAME not in mapping:
        best_match = _find_best_similarity_match(
            available_columns, ["name", "company", "customer"], threshold=80
        )
        if best_match:
            mapping[ACCOUNT_NAME] = best_match
            logger.debug(f"Heuristic match: {ACCOUNT_NAME} -> {best_match}")

    # Heuristic 2: Type-based matching for account_id
    if ACCOUNT_ID not in mapping:
        id_candidates = _find_id_columns(df, available_columns)
        if id_candidates:
            mapping[ACCOUNT_ID] = id_candidates[0]
            logger.debug(f"Type-based match: {ACCOUNT_ID} -> {id_candidates[0]}")

    # Heuristic 3: Date pattern matching
    if CREATED_DATE not in mapping:
        date_candidates = _find_date_columns(df, available_columns)
        if date_candidates:
            mapping[CREATED_DATE] = date_candidates[0]
            logger.debug(f"Date pattern match: {CREATED_DATE} -> {date_candidates[0]}")

    return mapping


def _find_best_similarity_match(
    available_columns: List[str], target_terms: List[str], threshold: int = 80
) -> Optional[str]:
    """Find best column match using string similarity."""
    best_score = 0
    best_match = None

    for col in available_columns:
        for term in target_terms:
            # Try different similarity metrics
            ratio = fuzz.ratio(col.lower(), term.lower())
            token_ratio = fuzz.token_sort_ratio(col.lower(), term.lower())
            token_set_ratio = fuzz.token_set_ratio(col.lower(), term.lower())

            # Use the best score
            score = max(ratio, token_ratio, token_set_ratio)

            if score > best_score and score >= threshold:
                best_score = score
                best_match = col

    return best_match


def _find_id_columns(df: pd.DataFrame, available_columns: List[str]) -> List[str]:
    """Find columns that look like IDs based on data patterns."""
    id_candidates = []

    for col in available_columns:
        if col.lower() in ["id", "id_", "_id"]:
            id_candidates.append(col)
            continue

        # Check if column contains mostly alphanumeric strings of consistent length
        sample_data = df[col].dropna().astype(str).head(100)
        if len(sample_data) > 0:
            # Check if most values look like IDs (alphanumeric, consistent length)
            id_like = sum(
                1 for val in sample_data if val.isalnum() and len(val) >= 10
            ) / len(sample_data)
            if id_like > 0.8:
                id_candidates.append(col)

    return sorted(id_candidates)


def _find_date_columns(df: pd.DataFrame, available_columns: List[str]) -> List[str]:
    """Find columns that look like dates based on data patterns."""
    date_candidates = []

    for col in available_columns:
        if any(
            term in col.lower() for term in ["date", "created", "modified", "updated"]
        ):
            date_candidates.append(col)
            continue

        # Check if column contains date-like data
        sample_data = df[col].dropna().astype(str).head(100)
        if len(sample_data) > 0:
            # Simple date pattern detection
            date_pattern = re.compile(r"\d{1,4}[-/]\d{1,2}[-/]\d{1,4}")
            date_like = sum(1 for val in sample_data if date_pattern.search(val)) / len(
                sample_data
            )
            if date_like > 0.3:
                date_candidates.append(col)

    return sorted(date_candidates)


def _validate_required_columns(mapping: Dict[str, str]) -> bool:
    """Validate that required columns are present in mapping."""
    required_columns = [ACCOUNT_NAME]
    return all(col in mapping for col in required_columns)


def save_schema_mapping(mapping: Mapping[str, str], run_id: str) -> None:
    """
    Save schema mapping to file for observability and reproducibility.

    Args:
        mapping: Schema mapping from canonical names to actual column names
        run_id: Run ID for file organization
    """
    try:
        from src.utils.path_utils import get_processed_dir

        processed_dir = get_processed_dir(run_id)
        schema_file = processed_dir / "schema_mapping.json"

        # Create directory if it doesn't exist
        schema_file.parent.mkdir(parents=True, exist_ok=True)

        # Save mapping with metadata
        schema_data = {
            "run_id": run_id,
            "timestamp": pd.Timestamp.now().isoformat(),
            "mapping": dict(mapping),
            "canonical_columns": list(mapping.keys()),
            "actual_columns": list(mapping.values()),
        }

        with open(schema_file, "w") as f:
            json.dump(schema_data, f, indent=2)

        logger.info(f"Schema mapping saved to {schema_file}")

    except Exception as e:
        logger.error(f"Failed to save schema mapping: {e}")


def load_schema_mapping(run_id: str) -> Optional[Dict[str, str]]:
    """
    Load schema mapping from file.

    Args:
        run_id: Run ID to load mapping for

    Returns:
        Schema mapping dictionary or None if not found
    """
    try:
        from src.utils.path_utils import get_processed_dir

        processed_dir = get_processed_dir(run_id)
        schema_file = processed_dir / "schema_mapping.json"

        if not schema_file.exists():
            logger.debug(f"Schema mapping file not found: {schema_file}")
            return None

        with open(schema_file, "r") as f:
            schema_data = json.load(f)

        mapping = schema_data.get("mapping", {})
        logger.info(f"Loaded schema mapping from {schema_file}")
        return mapping

    except Exception as e:
        logger.error(f"Failed to load schema mapping: {e}")
        return None
