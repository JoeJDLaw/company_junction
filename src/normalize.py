"""Legal-aware name normalization for Company Junction deduplication.

This module handles:
- Legal suffix detection and classification
- Name normalization with symbol mapping
- Numeric style unification
- Core name extraction for similarity matching
"""

import logging
import re
from dataclasses import dataclass
from typing import Any, Optional

import pandas as pd
import yaml

logger = logging.getLogger(__name__)

# Global normalization settings (loaded from config)
_normalization_settings = None


@dataclass
class NameNorm:
    """Normalized name representation with legal suffix classification."""

    name_raw: str
    name_base: str
    name_core: str
    suffix_class: str  # INC/LLC/LTD/CORP/LLP/LP/PLLC/PC/CO/GMBH/NONE
    has_parentheses: bool
    has_semicolon: bool
    has_multiple_names: bool
    alias_candidates: list[str]
    alias_sources: list[str]


# Legal suffix mapping (case-insensitive)
LEGAL_SUFFIXES = {
    "inc": "INC",
    "inc.": "INC",
    "incorporated": "INC",
    "llc": "LLC",
    "l.l.c.": "LLC",
    "limited liability company": "LLC",
    "ltd": "LTD",
    "ltd.": "LTD",
    "limited": "LTD",
    "corp": "CORP",
    "corp.": "CORP",
    "corporation": "CORP",
    "llp": "LLP",
    "limited liability partnership": "LLP",
    "lp": "LP",
    "limited partnership": "LP",
    "pllc": "PLLC",
    "professional limited liability company": "PLLC",
    "pc": "PC",
    "professional corporation": "PC",
    "co": "CO",
    "co.": "CO",
    "company": "CO",
    "gmbh": "GMBH",
    "gesellschaft mit beschränkter haftung": "GMBH",
}

# Parentheses content that should never be treated as company names
PARENTHESES_BLACKLIST = {
    "paystub",
    "pay stubs",
    "paystubs",
    "not sure",
    "unsure",
    "unknown",
    "staffing agency",
    "temp",
    "temporary",
    "delaware",
    "california",
    "new york",
    "florida",
    "texas",
    "ohio",
    "illinois",
    "pennsylvania",
    "michigan",
    "georgia",
    "north carolina",
    "virginia",
    "washington",
    "massachusetts",
    "indiana",
    "tennessee",
    "missouri",
    "maryland",
    "colorado",
    "wisconsin",
    "minnesota",
    "alabama",
    "south carolina",
    "louisiana",
    "kentucky",
    "oregon",
    "oklahoma",
    "connecticut",
    "utah",
    "iowa",
    "nevada",
    "arkansas",
    "mississippi",
    "kansas",
    "nebraska",
    "idaho",
    "new mexico",
    "west virginia",
    "hawaii",
    "new hampshire",
    "maine",
    "montana",
    "rhode island",
    "south dakota",
    "north dakota",
    "alaska",
    "vermont",
    "wyoming",
    "district of columbia",
    "dc",
}


def normalize_name(name: Optional[str]) -> NameNorm:
    """Normalize a company name with legal suffix detection.

    Args:
        name: Raw company name string

    Returns:
        NameNorm object with normalized components

    """
    if not name or pd.isna(name):
        return NameNorm(
            name_raw=str(name),
            name_base="",
            name_core="",
            suffix_class="NONE",
            has_parentheses=False,
            has_semicolon=False,
            has_multiple_names=False,
            alias_candidates=[],
            alias_sources=[],
        )

    name_raw = str(name).strip()

    # Detect patterns before normalization
    has_parentheses = "(" in name_raw and ")" in name_raw
    has_semicolon = ";" in name_raw
    has_multiple_names = _detect_multiple_names(name_raw)

    # Extract alias candidates
    alias_candidates, alias_sources = _extract_alias_candidates(name_raw)

    # Remove numbered markers from the name for scoring
    name_for_scoring = re.sub(r"^\(\d+\)\s*", "", name_raw)

    # Step 1: Create name_base (lowercase, symbol mapping, whitespace normalization)
    name_base = _create_name_base(name_for_scoring)

    # Step 2: Extract suffix and core
    suffix_class, name_core = extract_suffix(name_base)

    return NameNorm(
        name_raw=name_raw,
        name_base=name_base,
        name_core=name_core,
        suffix_class=suffix_class,
        has_parentheses=has_parentheses,
        has_semicolon=has_semicolon,
        has_multiple_names=has_multiple_names,
        alias_candidates=alias_candidates,
        alias_sources=alias_sources,
    )


def _create_name_base(name_raw: str) -> str:
    """Create normalized base name with symbol mapping and whitespace cleanup.

    Args:
        name_raw: Original name string

    Returns:
        Normalized base name

    """
    # Convert to lowercase
    base = name_raw.lower()

    # Symbol mapping
    symbol_map = {
        "&": " and ",
        "/": " ",
        "-": " ",
        "@": " at ",
        "+": " plus ",
        ",": " ",
        ".": " ",
        ";": " ",
        ":": " ",
        "_": " ",  # Normalize underscores to spaces
    }

    for symbol, replacement in symbol_map.items():
        base = base.replace(symbol, replacement)

    # Collapse multiple spaces
    base = re.sub(r"\s+", " ", base)

    # Keep only alphanumeric and spaces
    base = re.sub(r"[^a-z0-9\s]", "", base)

    # Unify numeric styles (20-20, 20/20, 20 20 -> 20 20)
    base = _unify_numeric_style(base)

    return base.strip()


def _unify_numeric_style(text: str) -> str:
    """Unify numeric representations in text.

    Args:
        text: Text with potential numeric variations

    Returns:
        Text with unified numeric style

    """
    # Pattern to match numbers with separators
    pattern = r"(\d+)[\-\/](\d+)"

    def replace_numeric(match: re.Match[str]) -> str:
        num1, num2 = match.groups()
        return f"{num1} {num2}"

    return re.sub(pattern, replace_numeric, text)


def _detect_multiple_names(name: str) -> bool:
    """Detect if a name string contains multiple company names.

    Args:
        name: Raw name string

    Returns:
        True if multiple names detected

    """
    # Clear indicators of multiple names
    if ";" in name or ":" in name:
        return True

    # Numbered patterns like "(1)", "(2)", "1.", "2."
    if re.search(r"\(\d+\)|^\d+\.", name):
        return True

    # Multiple "and" or "&" separators
    and_count = name.lower().count(" and ") + name.count("&")
    if and_count > 1:
        return True

    return False


def _extract_alias_candidates(name: str) -> tuple[list[str], list[str]]:
    """Extract alias candidates from a name string.

    Args:
        name: Raw name string

    Returns:
        Tuple of (alias_candidates, alias_sources)

    """
    aliases = []
    sources = []

    # Extract semicolon-separated aliases (including numbered sequences)
    if ";" in name:
        segments = [s.strip() for s in name.split(";")]
        for segment in segments:
            if segment:
                # Remove numbered markers from the segment
                clean_segment = re.sub(r"^\(\d+\)\s*", "", segment)
                if clean_segment:
                    aliases.append(clean_segment)
                    sources.append("semicolon")

    # Extract filtered parentheses aliases (not already captured by semicolon)
    parentheses_pattern = r"\(([^)]+)\)"
    matches = re.finditer(parentheses_pattern, name)
    for match in matches:
        content = match.group(1).strip()
        # Skip if this content is already captured by semicolon splitting
        if ";" not in name or content not in [s.strip() for s in name.split(";")]:
            if _is_valid_parentheses_alias(content):
                aliases.append(content)
                sources.append("parentheses")

    return aliases, sources


def _is_valid_parentheses_alias(content: str) -> bool:
    """Check if parentheses content should be treated as a company alias.

    Args:
        content: Content inside parentheses

    Returns:
        True if content should be treated as company alias

    """
    content_lower = content.lower()

    # Check blacklist (exact matches and contains blacklist terms)
    for blacklist_term in PARENTHESES_BLACKLIST:
        if blacklist_term in content_lower:
            return False

    # Check if it's just a number or single word
    if re.match(r"^\d+$", content.strip()):
        return False

    # Check for legal suffix
    words = content.lower().split()
    for word in words:
        if word in LEGAL_SUFFIXES:
            return True

    # Check for multiple capitalized words (likely company name)
    capitalized_words = re.findall(r"\b[A-Z][a-z]+", content)
    if len(capitalized_words) >= 2:
        return True

    return False


def _normalize_alias(alias: str) -> str:
    """Normalize an alias using the same rules as name_core.

    Args:
        alias: Raw alias string

    Returns:
        Normalized alias

    """
    # Apply same normalization as name_base
    normalized = _create_name_base(alias)

    # Extract suffix and core (same as normalize_name)
    suffix_class, name_core = extract_suffix(normalized)

    return name_core


def extract_suffix_from_tokens(tokens: list[str]) -> tuple[str, str]:
    """Extract legal suffix from tokenized name.

    Args:
        tokens: List of name tokens

    Returns:
        Tuple of (suffix_class, core_name)

    """
    if not tokens:
        return "NONE", ""

    # Check for trailing suffix tokens
    for i in range(len(tokens), 0, -1):
        suffix_candidate = " ".join(tokens[-i:])
        if suffix_candidate in LEGAL_SUFFIXES:
            suffix_class = LEGAL_SUFFIXES[suffix_candidate]
            core_tokens = tokens[:-i]
            return suffix_class, " ".join(core_tokens)

    return "NONE", " ".join(tokens)


def extract_suffix(name_base: str) -> tuple[str, str]:
    """Extract legal suffix from normalized base name.

    Args:
        name_base: Normalized base name

    Returns:
        Tuple of (suffix_class, core_name)

    """
    tokens = name_base.split()
    return extract_suffix_from_tokens(tokens)


def excel_serial_to_datetime(val: Any) -> Optional[pd.Timestamp]:
    """Convert Excel serial number to datetime.

    Args:
        val: Excel serial number or datetime-like value

    Returns:
        pandas Timestamp or None if conversion fails

    """
    if pd.isna(val):
        return None

    try:
        # If it's already a datetime, return as is
        if pd.api.types.is_datetime64_any_dtype(val):
            return pd.Timestamp(val)

        # If it's a string that looks like a date, parse it
        if isinstance(val, str):
            try:
                # Try different date formats
                for fmt in ["%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y", "%Y/%m/%d"]:
                    try:
                        result = pd.to_datetime(val, format=fmt)
                        if pd.notna(result):
                            return result
                    except (ValueError, TypeError):
                        continue
                # If no specific format works, try pandas default parsing with coerce
                try:
                    result = pd.to_datetime(val, errors="coerce")
                    if pd.notna(result):
                        return result
                except (ValueError, TypeError):
                    pass
            except Exception as e:
                # Log the specific error for debugging (only at debug level)
                logger.debug(f"Date parsing failed for '{val}': {e}")

        # If it's a number, try Excel serial conversion
        if isinstance(val, (int, float)):
            # Excel serial dates start from 1900-01-01
            # But Excel has a bug where it thinks 1900 is a leap year
            # So we need to adjust for dates after 1900-02-28
            excel_epoch = pd.Timestamp("1900-01-01")

            # Adjust for Excel's leap year bug
            if val > 59:  # After 1900-02-28
                val = val - 1

            return excel_epoch + pd.Timedelta(days=val - 1)

        return None

    except Exception as e:
        # Only log at debug level to avoid spam - invalid dates are common in real data
        logger.debug(f"Failed to convert {val} to datetime: {e}")
        return None


def normalize_dataframe(
    df: pd.DataFrame,
    name_column: str = "Account Name",
) -> pd.DataFrame:
    """Normalize name column in a DataFrame.

    Args:
        df: Input DataFrame
        name_column: Column name containing company names

    Returns:
        DataFrame with normalized name columns added

    """
    if name_column not in df.columns:
        logger.warning(f"Name column '{name_column}' not found in DataFrame")
        return df

    # Apply normalization
    normalized_list = []
    for val in df[name_column]:
        normalized_list.append(normalize_name(str(val) if pd.notna(val) else None))
    normalized = pd.Series(normalized_list, index=df.index)

    # Add normalized columns
    df = df.copy()
    df["name_raw"] = [n.name_raw for n in normalized]
    df["name_base"] = [n.name_base for n in normalized]
    df["name_core"] = [n.name_core for n in normalized]
    df["suffix_class"] = [n.suffix_class for n in normalized]
    df["has_parentheses"] = [n.has_parentheses for n in normalized]
    df["has_semicolon"] = [n.has_semicolon for n in normalized]
    df["has_multiple_names"] = [n.has_multiple_names for n in normalized]
    df["alias_candidates"] = [n.alias_candidates for n in normalized]
    df["alias_sources"] = [n.alias_sources for n in normalized]

    return df


def load_normalization_settings(
    config_path: str = "config/settings.yaml",
) -> dict[str, Any]:
    """Load normalization settings from config file."""
    global _normalization_settings
    if _normalization_settings is None:
        try:
            with open(config_path) as f:
                config = yaml.safe_load(f)
            _normalization_settings = config.get("similarity", {}).get(
                "normalization",
                {},
            )
        except Exception as e:
            logger.warning(f"Could not load normalization settings: {e}")
            _normalization_settings = {}
    return _normalization_settings


def enhance_name_core(
    name_core: str,
    settings: Optional[dict[str, Any]] = None,
) -> tuple[str, set[str]]:
    """Apply enhanced normalization to name_core for better retail brand matching.

    Args:
        name_core: Original name_core string
        settings: Normalization settings (optional, will load from config if not provided)

    Returns:
        Tuple of (enhanced_name_core, weak_tokens_set)

    """
    if not name_core:
        return name_core, set()

    if settings is None:
        settings = load_normalization_settings()

    # Get settings with defaults
    weak_tokens = set(settings.get("weak_tokens", []))
    plural_map = settings.get("plural_singular_map", {})
    canonical_map = settings.get("canonical_retail_terms", {})
    enable_plural = settings.get("enable_plural_normalization", True)
    enable_weak = settings.get("enable_weak_token_filtering", True)
    enable_canonical = settings.get("enable_canonical_retail_terms", True)

    # Split into tokens (lowercase for consistent matching)
    tokens = name_core.lower().split()
    enhanced_tokens = []
    weak_tokens_found = set()

    for token in tokens:
        _original_token = token

        # Apply canonical retail term mapping
        if enable_canonical and token in canonical_map:
            token = canonical_map[token]

        # Apply plural to singular mapping
        if enable_plural and token in plural_map:
            token = plural_map[token]

        # Track weak tokens
        if enable_weak and token in weak_tokens:
            weak_tokens_found.add(token)
            # For weak tokens, we'll exclude them from Jaccard calculation
            # but keep them in the name for token_sort_ratio and token_set_ratio

        enhanced_tokens.append(token)

    enhanced_name_core = " ".join(enhanced_tokens)
    return enhanced_name_core, weak_tokens_found


def get_enhanced_tokens_for_jaccard(
    name_core: str,
    settings: Optional[dict[str, Any]] = None,
) -> set[str]:
    """Get token set for Jaccard calculation, excluding weak tokens.

    Args:
        name_core: Name core string
        settings: Normalization settings

    Returns:
        Set of tokens excluding weak tokens

    """
    if settings is None:
        settings = load_normalization_settings()

    _weak_tokens = set(settings.get("weak_tokens", []))
    enable_weak = settings.get("enable_weak_token_filtering", True)

    if not enable_weak:
        return set(name_core.split())

    # Apply enhancement and get weak tokens
    enhanced_name_core, weak_tokens_found = enhance_name_core(name_core, settings)

    # Return tokens excluding weak ones
    all_tokens = set(enhanced_name_core.split())
    return all_tokens - weak_tokens_found
