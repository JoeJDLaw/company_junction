"""Disposition logic for Company Junction deduplication.

This module handles:
- Record classification (Keep, Update, Delete, Verify)
- Blacklist detection
- Suffix mismatch handling
- LLM gate integration (stub for Phase 1)
"""

import logging
import re
from typing import Any, Optional

import pandas as pd

from src.utils.schema_utils import DISPOSITION

logger = logging.getLogger(__name__)

# Module-level regex caches for performance optimization
_TOKEN_REGEX_CACHE = {}
_PHRASE_REGEX_CACHE = {}
_SUSPICIOUS_REGEX_CACHE = {}


def _get_token_regex(tokens: list[str]) -> re.Pattern:
    """Get cached compiled regex for blacklist tokens."""
    key = tuple(sorted(tokens))
    if key not in _TOKEN_REGEX_CACHE:
        pattern = r"\b(?:" + "|".join(map(re.escape, tokens)) + r")\b"
        _TOKEN_REGEX_CACHE[key] = re.compile(pattern, re.IGNORECASE)
    return _TOKEN_REGEX_CACHE[key]


def _get_phrase_regex(phrases: tuple[str, ...]) -> Optional[re.Pattern]:
    """Get cached compiled regex for blacklist phrases."""
    if not phrases:
        return None
    
    cache_key = tuple(sorted(phrases))
    if cache_key not in _PHRASE_REGEX_CACHE:
        pattern = "|".join(map(re.escape, phrases))
        _PHRASE_REGEX_CACHE[cache_key] = re.compile(pattern, re.IGNORECASE)
    return _PHRASE_REGEX_CACHE[cache_key]


def _get_suspicious_regex(pattern: str) -> re.Pattern:
    """Get cached compiled regex for suspicious singleton patterns."""
    if pattern not in _SUSPICIOUS_REGEX_CACHE:
        _SUSPICIOUS_REGEX_CACHE[pattern] = re.compile(pattern, re.IGNORECASE)
    return _SUSPICIOUS_REGEX_CACHE[pattern]


# Phase 1.35.3: Moved hardcoded blacklist to configuration
# These are now loaded from settings.yaml disposition.blacklist section
# Legacy constants kept for backward compatibility

# Blacklist of suspicious company names
# Single-word tokens (use word-boundary regex)
BLACKLIST_TOKENS = [
    "temp",
    "temporary",
    "unknown",
    "na",
    "n/a",
    "tbd",
    "test",
    "sample",
    "paystub",
    "employees",
    "delete",
    "unsure",
]

# Multi-word phrases (use substring matching)
BLACKLIST_PHRASES = [
    "pnc is not sure",
    "pnc is unsure",
    "no paystub",
    "no paystubs",
    "1099",
    "1099 pnc",
    "none",
    "do not use",
    "not sure",
    "unknown company",
    "no company",
    "no employer",
]

# Legacy: keep for backward compatibility
BLACKLIST = BLACKLIST_TOKENS + BLACKLIST_PHRASES


def classify_disposition(
    row: pd.Series,
    group_meta: dict[str, Any],
    settings: dict[str, Any],
) -> str:
    """Classify disposition for a single record.

    Args:
        row: Record to classify
        group_meta: Group metadata (size, has_suffix_mismatch, etc.)
        settings: Configuration settings

    Returns:
        Disposition string: 'Keep', 'Update', 'Delete', or 'Verify'

    """
    # Check for blacklisted names
    # Handle both standardized and original column names
    account_name_col = "account_name" if "account_name" in row.index else "Account Name"
    if _is_blacklisted(row.get(account_name_col, "")):
        return "Delete"

    # Check for multiple names (requires splitting)
    if row.get("has_multiple_names", False):
        return "Verify"

    # Check for alias matches
    alias_cross_refs = row.get("alias_cross_refs", [])
    if alias_cross_refs:
        return "Verify"

    # Check for suffix mismatch in group
    if group_meta.get("has_suffix_mismatch", False):
        return "Verify"

    # Check group size and primary status
    group_size = group_meta.get("group_size", 1)

    if group_size == 1:
        # Singleton - check if suspicious
        if _is_suspicious_singleton(row, settings):
            return "Verify"
        return "Keep"

    # Multi-record group
    is_primary = row.get("is_primary", False)

    if is_primary:
        return "Keep"
    return "Update"


def _load_manual_blacklist() -> list[str]:
    """Load manual blacklist terms from JSON file.

    Returns:
        List of blacklist terms, empty list if file doesn't exist

    """
    try:
        from src.manual_io import load_manual_blacklist

        return list(load_manual_blacklist())
    except Exception as e:
        logger.warning(f"Could not load manual blacklist: {e}")
        return []


def _load_manual_dispositions() -> dict[str, str]:
    """Load manual disposition overrides from JSON file.

    Returns:
        Dictionary mapping record_id to override disposition

    """
    try:
        from src.manual_io import load_manual_overrides

        overrides = load_manual_overrides()

        # Extract just the override values
        override_map = {}
        for record_id, override_data in overrides.items():
            override = override_data.get("override")
            if override:
                override_map[record_id] = override

        return override_map
    except Exception as e:
        logger.warning(f"Could not load manual dispositions: {e}")
        return {}


def get_blacklist_terms(settings: Optional[dict[str, Any]] = None) -> list[str]:
    """Get blacklist terms from configuration or fallback to built-in.

    Args:
        settings: Configuration settings (optional)

    Returns:
        List of blacklist terms

    """
    if settings and "disposition" in settings:
        # Phase 1.35.3: Load from configuration first
        config_blacklist = settings.get("disposition", {}).get("blacklist", {})
        if config_blacklist:
            tokens: list[str] = config_blacklist.get("tokens", [])
            phrases: list[str] = config_blacklist.get("phrases", [])
            if tokens or phrases:
                logger.info(
                    f"disposition | loaded_blacklist | tokens={len(tokens)} | phrases={len(phrases)} | source=config",
                )
                return tokens + phrases

    # Fallback to built-in blacklist
    logger.info(
        f"disposition | loaded_blacklist | tokens={len(BLACKLIST_TOKENS)} | phrases={len(BLACKLIST_PHRASES)} | source=builtin",
    )
    return BLACKLIST.copy()


def _is_blacklisted_improved(
    name: str,
    manual_terms: Optional[set[str]] = None,
) -> bool:
    """Improved blacklist checking with word-boundary matching for tokens.

    Args:
        name: Company name to check
        manual_terms: Pre-loaded manual blacklist terms (optional)

    Returns:
        True if blacklisted

    """
    if not name or pd.isna(name):
        return False

    name_lower = str(name).lower()

    # Check single-word tokens with word boundaries - use cached regex
    token_regex = _get_token_regex(BLACKLIST_TOKENS)
    if token_regex.search(name):
        return True

    # Check multi-word phrases with substring matching
    for phrase in BLACKLIST_PHRASES:
        if phrase.lower() in name_lower:
            return True

    # Check manual blacklist terms (if provided)
    if manual_terms:
        for term in manual_terms:
            if term.lower() in name_lower:
                return True

    # Check for very short or very long names
    if len(name.strip()) < 3:
        return True

    if len(name.strip()) > 100:
        return True

    # Check if mostly punctuation or stopwords
    if _is_mostly_punctuation_or_stopwords(name):
        return True

    return False


def _is_blacklisted(name: str) -> bool:
    """Check if a company name is blacklisted.

    Args:
        name: Company name to check

    Returns:
        True if blacklisted

    """
    # Load manual blacklist terms once
    manual_blacklist = _load_manual_blacklist()
    manual_terms = set(manual_blacklist) if manual_blacklist else None

    return _is_blacklisted_improved(name, manual_terms if manual_terms else set())


def _is_mostly_punctuation_or_stopwords(name: str) -> bool:
    """Check if name is mostly punctuation or stopwords.

    Args:
        name: Company name to check

    Returns:
        True if mostly punctuation or stopwords

    """
    if not name:
        return False

    # Common stopwords in company names
    stopwords = {
        "the",
        "a",
        "an",
        "and",
        "or",
        "but",
        "in",
        "on",
        "at",
        "to",
        "for",
        "of",
        "with",
        "by",
        "from",
        "up",
        "about",
        "into",
        "through",
        "during",
        "before",
        "after",
        "above",
        "below",
        "between",
        "among",
        "within",
        "without",
        "against",
        "toward",
        "towards",
        "upon",
        "over",
        "under",
        "behind",
        "beneath",
        "beside",
        "beyond",
    }

    # Count alphanumeric characters
    alnum_count = len(re.findall(r"[a-zA-Z0-9]", name))
    total_chars = len(name.strip())

    if total_chars == 0:
        return True

    # If less than 30% alphanumeric, likely mostly punctuation
    if alnum_count / total_chars < 0.3:
        return True

    # Check if mostly stopwords
    words = re.findall(r"\b[a-zA-Z]+\b", name.lower())
    if words:
        stopword_count = sum(1 for word in words if word in stopwords)
        if stopword_count / len(words) > 0.8:  # More than 80% stopwords
            return True

    return False


def _is_suspicious_singleton(row: pd.Series, settings: dict[str, Any]) -> bool:
    """Check if a singleton record is suspicious.

    Args:
        row: Record to check
        settings: Configuration settings

    Returns:
        True if suspicious

    """
    # Handle both standardized and original column names
    account_name_col = "account_name" if "account_name" in row.index else "Account Name"
    name = row.get(account_name_col, "")

    # Check for suspicious patterns
    suspicious_patterns = [
        r"\b(unknown|unsure|not sure|no idea)\b",
        r"\b(test|sample|example|dummy)\b",
        r"\b(temp|temporary|temp agency)\b",
        r"\b(none|n/a|na|tbd|to be determined)\b",
        r"\b(delete|remove|do not use)\b",
    ]

    name_lower = str(name).lower()
    for pattern in suspicious_patterns:
        if re.search(pattern, name_lower):
            return True

    # Check LLM gate if enabled
    if settings.get("llm", {}).get("enabled", False):
        # Stub for Phase 1 - would call LLM classifier
        logger.debug("LLM gate would be called here in Phase 2")

    return False


def compute_group_metadata(df_groups: pd.DataFrame) -> dict[int, dict[str, Any]]:
    """Compute metadata for each group.

    Args:
        df_groups: DataFrame with group assignments

    Returns:
        Dictionary mapping group_id to metadata

    """
    group_metadata = {}

    for group_id in df_groups["group_id"].unique():
        if group_id == -1:  # Skip unassigned records
            continue

        group_mask = df_groups["group_id"] == group_id
        group_data = df_groups[group_mask]

        # Check for suffix mismatches
        suffix_classes = group_data["suffix_class"].unique()
        has_suffix_mismatch = len(suffix_classes) > 1

        # Check for blacklist hits
        # Handle both standardized and original column names
        account_name_col = (
            "account_name" if "account_name" in group_data.columns else "Account Name"
        )
        blacklist_hits = group_data[account_name_col].apply(_is_blacklisted).sum()

        group_metadata[group_id] = {
            "group_size": len(group_data),
            "has_suffix_mismatch": has_suffix_mismatch,
            "blacklist_hits": blacklist_hits,
            "suffix_classes": list(suffix_classes),
            "has_primary": group_data["is_primary"].any(),
        }

    return group_metadata


def apply_dispositions(
    df_groups: pd.DataFrame,
    settings: dict[str, Any],
) -> pd.DataFrame:
    """Apply disposition classification to all records.

    Args:
        df_groups: DataFrame with group assignments
        settings: Configuration settings

    Returns:
        DataFrame with disposition column added

    """
    logger.info("Applying disposition classification")

    # Phase 1.35.3: Check if vectorized disposition is enabled
    use_vectorized = (
        settings.get("disposition", {}).get("performance", {}).get("vectorized", True)
    )

    if use_vectorized:
        logger.info(
            "disposition | backend=vectorized | records=%d | method=np.select",
            len(df_groups),
        )
        return _apply_dispositions_vectorized(df_groups, settings)
    logger.info(
        "disposition | backend=legacy | records=%d | method=iterrows",
        len(df_groups),
    )
    return _apply_dispositions_legacy(df_groups, settings)


def _apply_dispositions_vectorized(
    df_groups: pd.DataFrame,
    settings: dict[str, Any],
) -> pd.DataFrame:
    """Apply disposition classification using vectorized operations (np.select).

    Args:
        df_groups: DataFrame with group assignments
        settings: Configuration settings

    Returns:
        DataFrame with disposition column added

    """
    import time

    import numpy as np

    start_time = time.time()

    # Load manual overrides
    manual_overrides = _load_manual_dispositions()
    override_count = len(manual_overrides)
    if override_count > 0:
        logger.info(f"disposition | manual_overrides | count={override_count}")

    # Compute group metadata
    group_metadata = compute_group_metadata(df_groups)

    # Create result DataFrame
    result_df = df_groups.copy()

    # Phase 1.35.3: Vectorized blacklist detection
    blacklist_terms = get_blacklist_terms(settings)
    account_name_col = (
        "account_name" if "account_name" in result_df.columns else "Account Name"
    )

    # Create blacklist masks - OPTIMIZED VERSION
    name_series = result_df[account_name_col].fillna("").astype(str)
    name_lower = name_series.str.lower()  # Precompute lowercase once
    false_series = pd.Series(False, index=result_df.index)  # Prebuild false series
    
    # PROFILING: Start timing blacklist mask building
    blacklist_start = time.time()

    # Split tokens and phrases correctly
    tokens = [t for t in blacklist_terms if " " not in t]
    phrases = tuple(p for p in blacklist_terms if " " in p)

    # Single-word token detection (word boundaries) - use cached regex
    token_regex = _get_token_regex(tokens)
    token_mask = name_series.str.contains(token_regex, na=False)

    # Multi-word phrase detection (substring) - vectorized
    if phrases:
        phrase_regex = _get_phrase_regex(phrases)
        phrase_mask = name_lower.str.contains(phrase_regex, na=False)
    else:
        phrase_mask = false_series

    # Combined blacklist mask
    blacklist_mask = token_mask | phrase_mask
    
    # PROFILING: End timing blacklist mask building
    blacklist_time = time.time() - blacklist_start
    logger.info(f"disposition | profiling | blacklist_masks | duration={blacklist_time:.3f}s")

    # Phase 1.35.3: Vectorized disposition classification using np.select
    # Define conditions and choices for np.select
    conditions = []
    choices = []

    # Manual override condition (highest priority)
    if manual_overrides:
        override_mask = result_df.index.astype(str).isin(manual_overrides.keys())
        conditions.append(override_mask)
        choices.append("manual_override")

    # Blacklist condition
    conditions.append(blacklist_mask.to_numpy())
    choices.append("Delete")

    # Multiple names condition
    multiple_names_mask = result_df.get(
        "has_multiple_names",
        pd.Series([False] * len(result_df)),
    )
    conditions.append(multiple_names_mask.to_numpy())
    choices.append("Verify")

    # PROFILING: Start timing alias cross-refs mask
    alias_start = time.time()
    
    # Alias matches condition - OPTIMIZED: use .map instead of .apply
    alias_col = result_df.get("alias_cross_refs", pd.Series([[]] * len(result_df)))
    alias_mask = alias_col.map(lambda x: bool(x) if isinstance(x, list) else False)
    conditions.append(alias_mask.to_numpy())
    choices.append("Verify")
    
    # PROFILING: End timing alias cross-refs mask
    alias_time = time.time() - alias_start
    logger.info(f"disposition | profiling | alias_cross_refs_mask | duration={alias_time:.3f}s")

    # PROFILING: Start timing suffix mismatch mask
    suffix_start = time.time()
    
    # Vectorized group size and suffix mismatch - OPTIMIZED
    group_size_series = result_df.groupby("group_id")["group_id"].transform("size")
    suffix_mismatch_series = (
        result_df.groupby("group_id")["suffix_class"].transform(lambda s: s.nunique() > 1)
    )
    
    conditions.append(suffix_mismatch_series.to_numpy())
    choices.append("Verify")
    
    # PROFILING: End timing suffix mismatch mask
    suffix_time = time.time() - suffix_start
    logger.info(f"disposition | profiling | suffix_mismatch_mask | duration={suffix_time:.3f}s")
    is_primary_series = result_df.get("is_primary", pd.Series([False] * len(result_df)))

    # Singleton suspicious condition
    singleton_mask = (group_size_series == 1) & (~blacklist_mask)
    suspicious_singleton_mask = false_series
    if "disposition" in settings and "performance" in settings["disposition"]:
        suspicious_regex = settings["disposition"]["performance"].get(
            "suspicious_singleton_regex",
        )
        if suspicious_regex:
            comp = _get_suspicious_regex(suspicious_regex)
            suspicious_singleton_mask = name_lower.str.contains(comp, na=False)
    singleton_suspicious = singleton_mask & suspicious_singleton_mask
    conditions.append(singleton_suspicious.to_numpy())
    choices.append("Verify")

    # Singleton clean condition
    singleton_clean = singleton_mask & (~suspicious_singleton_mask)
    conditions.append(singleton_clean.to_numpy())
    choices.append("Keep")

    # Multi-record group conditions
    multi_record_mask = group_size_series > 1

    # Primary record condition
    primary_mask = multi_record_mask & is_primary_series
    conditions.append(primary_mask.to_numpy())
    choices.append("Keep")

    # Duplicate record condition (default for multi-record non-primary)
    duplicate_mask = multi_record_mask & (~is_primary_series)
    conditions.append(duplicate_mask.to_numpy())
    choices.append("Update")

    # Apply np.select for vectorized classification - ensure object dtype
    dispositions = np.select(conditions, choices, default="Keep").astype(object)

    # Apply manual overrides - OPTIMIZED: vectorized reindex
    if manual_overrides:
        idx_str = result_df.index.astype(str)
        overrides_s = pd.Series(manual_overrides)  # index=record_id str
        aligned = overrides_s.reindex(idx_str)
        override_mask = aligned.notna().to_numpy()
        dispositions = np.where(override_mask, aligned.to_numpy(object), dispositions)

    # Add disposition column
    result_df[DISPOSITION] = dispositions

    # PROFILING: Start timing reason generation
    reasons_start = time.time()
    
    # Phase 1.35.3: Vectorized reason generation - reuse computed masks
    reasons = _generate_disposition_reasons_vectorized(
        result_df,
        blacklist_mask=blacklist_mask,
        manual_overrides=manual_overrides,
        settings=settings,
        group_size_series=group_size_series,
        suffix_mismatch_series=suffix_mismatch_series,
        alias_mask=alias_mask,
        suspicious_singleton_mask=suspicious_singleton_mask,
    )
    
    # PROFILING: End timing reason generation
    reasons_time = time.time() - reasons_start
    logger.info(f"disposition | profiling | reason_generation | duration={reasons_time:.3f}s")
    result_df["disposition_reason"] = reasons

    # Log performance metrics
    duration = time.time() - start_time
    logger.info(
        f"disposition | vectorized_complete | duration={duration:.2f}s | records={len(result_df)} | throughput={len(result_df)/duration:.0f}records/sec",
    )

    # Log disposition summary
    if DISPOSITION in result_df.columns:
        disposition_counts = result_df[DISPOSITION].value_counts()
        logger.info(f"disposition | summary | counts={disposition_counts.to_dict()}")
    else:
        logger.warning(
            f"disposition | summary | column '{DISPOSITION}' not found in result_df.columns: {list(result_df.columns)}",
        )

    # Stage summary for performance monitoring
    logger.info(
        f"disposition | stage_summary | blacklist={blacklist_time:.3f}s | alias={alias_time:.3f}s | "
        f"suffix={suffix_time:.3f}s | reasons={reasons_time:.3f}s | total={duration:.3f}s | "
        f"throughput={len(result_df)/duration:.0f}rec/s"
    )

    return result_df


def _apply_dispositions_legacy(
    df_groups: pd.DataFrame,
    settings: dict[str, Any],
) -> pd.DataFrame:
    """Legacy disposition classification using iterrows (fallback).

    Args:
        df_groups: DataFrame with group assignments
        settings: Configuration settings

    Returns:
        DataFrame with disposition column added

    """
    logger.info("disposition | using_legacy_method | iterrows_approach")

    # Load manual overrides
    manual_overrides = _load_manual_dispositions()
    override_count = len(manual_overrides)
    if override_count > 0:
        logger.info(f"Loaded {override_count} manual disposition overrides")

    # Compute group metadata
    group_metadata = compute_group_metadata(df_groups)

    # Apply dispositions
    result_df = df_groups.copy()
    result_df[DISPOSITION] = ""
    result_df["disposition_reason"] = ""

    for idx, row in result_df.iterrows():
        # Check for manual override first
        record_id = str(idx)
        if record_id in manual_overrides:
            override = manual_overrides[record_id]
            mask = result_df.index == idx
            result_df.loc[mask, DISPOSITION] = override
            result_df.loc[mask, "disposition_reason"] = f"manual_override:{override}"
            continue

        group_id = row["group_id"]

        if group_id == -1:
            # Unassigned record - treat as singleton
            group_meta = {
                "group_size": 1,
                "has_suffix_mismatch": False,
                "blacklist_hits": 0,
            }
        else:
            group_meta = group_metadata.get(group_id, {})

        disposition = classify_disposition(row, group_meta, settings)
        reason = get_disposition_reason(row, group_meta, settings)

        mask = result_df.index == idx
        result_df.loc[mask, DISPOSITION] = disposition
        result_df.loc[mask, "disposition_reason"] = reason

    # Log disposition summary
    disposition_counts = result_df[DISPOSITION].value_counts()
    logger.info(
        f"disposition | legacy_complete | summary={disposition_counts.to_dict()}",
    )

    return result_df


def save_dispositions(df_dispositions: pd.DataFrame, output_path: str) -> None:
    """Save dispositions DataFrame to parquet file.

    Args:
        df_dispositions: DataFrame with dispositions
        output_path: Output file path

    """
    df_dispositions.to_parquet(output_path, index=False)
    logger.info(f"Saved dispositions to {output_path}")


def load_dispositions(input_path: str) -> pd.DataFrame:
    """Load dispositions DataFrame from parquet file.

    Args:
        input_path: Input file path

    Returns:
        DataFrame with dispositions

    """
    try:
        df_dispositions = pd.read_parquet(input_path)
        logger.info(f"Loaded dispositions from {input_path}")
        return df_dispositions
    except Exception as e:
        logger.error(f"Error loading dispositions: {e}")
        return pd.DataFrame()


def get_disposition_reason(
    row: pd.Series,
    group_meta: dict[str, Any],
    settings: dict[str, Any],
) -> str:
    """Get the reason for a disposition classification.

    Args:
        row: Record to classify
        group_meta: Group metadata
        settings: Configuration settings

    Returns:
        Reason string

    """
    # Check for blacklisted names
    # Handle both standardized and original column names
    account_name_col = "account_name" if "account_name" in row.index else "Account Name"
    if _is_blacklisted(row.get(account_name_col, "")):
        return "blacklisted_name"

    # Check for multiple names
    if row.get("has_multiple_names", False):
        return "multi_name_string_requires_split"

    # Check for alias matches
    alias_cross_refs = row.get("alias_cross_refs", [])
    if alias_cross_refs:
        sources = list(set([str(ref.get("source", "")) for ref in alias_cross_refs]))
        return f"alias_matches_{len(alias_cross_refs)}_groups_via_{sources}"

    # Check for suffix mismatch
    if group_meta.get("has_suffix_mismatch", False):
        return "suffix_mismatch"

    # Check group size
    group_size = group_meta.get("group_size", 1)

    if group_size == 1:
        if _is_suspicious_singleton(row, settings):
            return "suspicious_singleton"
        return "clean_singleton"

    # Multi-record group
    is_primary = row.get("is_primary", False)

    if is_primary:
        return "primary_record"
    return "duplicate_record"


def _generate_disposition_reasons_vectorized(
    df: pd.DataFrame,
    blacklist_mask: pd.Series,
    manual_overrides: dict[str, str],
    settings: dict[str, Any],
    group_size_series: pd.Series,
    suffix_mismatch_series: pd.Series,
    alias_mask: pd.Series,
    suspicious_singleton_mask: pd.Series,
) -> pd.Series:
    """Generate disposition reasons using vectorized operations.

    Args:
        df: DataFrame with dispositions
        blacklist_mask: Boolean mask for blacklisted records
        manual_overrides: Manual override mapping
        settings: Configuration settings
        group_size_series: Pre-computed group sizes
        suffix_mismatch_series: Pre-computed suffix mismatch flags
        alias_mask: Pre-computed alias match flags
        suspicious_singleton_mask: Pre-computed suspicious singleton flags

    Returns:
        Series with disposition reasons

    """
    import numpy as np

    # Initialize reasons array
    reasons = np.full(len(df), "unknown", dtype=object)

    # Manual override reasons - OPTIMIZED: vectorized reindex
    if manual_overrides:
        idx_str = df.index.astype(str)
        overrides_s = pd.Series(manual_overrides)
        aligned = overrides_s.reindex(idx_str)
        has_override = aligned.notna()
        reasons[has_override.to_numpy()] = ("manual_override:" + aligned[has_override]).to_numpy(object)

    # Blacklist reasons
    reasons[blacklist_mask] = "blacklisted_name"

    # Multiple names reasons
    multiple_names_mask = df.get("has_multiple_names", pd.Series([False] * len(df)))
    reasons[multiple_names_mask] = "multi_name_string_requires_split"

    # Alias match reasons - use pre-computed mask
    reasons[alias_mask] = "alias_matches_detected"

    # Suffix mismatch reasons - use pre-computed series
    reasons[suffix_mismatch_series] = "suffix_mismatch"

    # Group size and primary status reasons - use pre-computed series
    is_primary_series = df.get("is_primary", pd.Series([False] * len(df)))

    # Singleton reasons - use pre-computed series
    singleton_mask = (group_size_series == 1) & (~blacklist_mask)

    # Use pre-computed suspicious singleton mask
    singleton_suspicious = singleton_mask & suspicious_singleton_mask
    reasons[singleton_suspicious] = "suspicious_singleton"

    singleton_clean = singleton_mask & (~suspicious_singleton_mask)
    reasons[singleton_clean] = "clean_singleton"

    # Multi-record group reasons
    multi_record_mask = group_size_series > 1

    # Primary record reasons
    primary_mask = multi_record_mask & is_primary_series
    reasons[primary_mask] = "primary_record"

    # Duplicate record reasons
    duplicate_mask = multi_record_mask & (~is_primary_series)
    reasons[duplicate_mask] = "duplicate_record"

    return pd.Series(reasons, index=df.index)
