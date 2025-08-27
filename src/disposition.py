"""
Disposition logic for Company Junction deduplication.

This module handles:
- Record classification (Keep, Update, Delete, Verify)
- Blacklist detection
- Suffix mismatch handling
- LLM gate integration (stub for Phase 1)
"""

import pandas as pd
import re
from typing import Dict, List, Optional
import logging

logger = logging.getLogger(__name__)


# Blacklist of suspicious company names
BLACKLIST = [
    "pnc is not sure", "pnc is unsure", "unsure", "unknown", "no paystub", "no paystubs",
    "1099", "1099 pnc", "none", "n/a", "tbd", "test", "sample", "delete", "do not use",
    "not sure", "unsure", "unknown company", "no company", "no employer", "temp", "temporary"
]


def classify_disposition(row: pd.Series, group_meta: Dict, settings: Dict) -> str:
    """
    Classify disposition for a single record.
    
    Args:
        row: Record to classify
        group_meta: Group metadata (size, has_suffix_mismatch, etc.)
        settings: Configuration settings
        
    Returns:
        Disposition string: 'Keep', 'Update', 'Delete', or 'Verify'
    """
    # Check for blacklisted names
    if _is_blacklisted(row.get('Account Name', '')):
        return 'Delete'
    
    # Check for multiple names (requires splitting)
    if row.get('has_multiple_names', False):
        return 'Verify'
    
    # Check for alias matches
    alias_cross_refs = row.get('alias_cross_refs', [])
    if alias_cross_refs:
        return 'Verify'
    
    # Check for suffix mismatch in group
    if group_meta.get('has_suffix_mismatch', False):
        return 'Verify'
    
    # Check group size and primary status
    group_size = group_meta.get('group_size', 1)
    
    if group_size == 1:
        # Singleton - check if suspicious
        if _is_suspicious_singleton(row, settings):
            return 'Verify'
        return 'Keep'
    
    # Multi-record group
    is_primary = row.get('is_primary', False)
    
    if is_primary:
        return 'Keep'
    else:
        return 'Update'


def _load_manual_blacklist() -> List[str]:
    """
    Load manual blacklist terms from JSON file.
    
    Returns:
        List of blacklist terms, empty list if file doesn't exist
    """
    try:
        import json
        from pathlib import Path
        
        manual_dir = Path("data/manual")
        file_path = manual_dir / "manual_blacklist.json"
        
        if not file_path.exists():
            return []
        
        with open(file_path, 'r') as f:
            data = json.load(f)
            return data.get('terms', [])
    except Exception as e:
        logger.warning(f"Could not load manual blacklist: {e}")
        return []


def _load_manual_dispositions() -> Dict[str, str]:
    """
    Load manual disposition overrides from JSON file.
    
    Returns:
        Dictionary mapping record_id to override disposition
    """
    try:
        import json
        from pathlib import Path
        
        manual_dir = Path("data/manual")
        file_path = manual_dir / "manual_dispositions.json"
        
        if not file_path.exists():
            return {}
        
        with open(file_path, 'r') as f:
            dispositions = json.load(f)
            
        # Create mapping from record_id to override
        override_map = {}
        for disposition in dispositions:
            record_id = disposition.get('record_id')
            override = disposition.get('override')
            if record_id and override:
                override_map[record_id] = override
        
        return override_map
    except Exception as e:
        logger.warning(f"Could not load manual dispositions: {e}")
        return {}


def _is_blacklisted(name: str) -> bool:
    """
    Check if a company name is blacklisted.
    
    Args:
        name: Company name to check
        
    Returns:
        True if blacklisted
    """
    if not name or pd.isna(name):
        return False
    
    name_lower = str(name).lower()
    
    # Check for built-in blacklist substrings
    for blacklist_term in BLACKLIST:
        if blacklist_term in name_lower:
            return True
    
    # Check for manual blacklist terms
    manual_blacklist = _load_manual_blacklist()
    for term in manual_blacklist:
        if term in name_lower:
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


def _is_mostly_punctuation_or_stopwords(name: str) -> bool:
    """
    Check if name is mostly punctuation or stopwords.
    
    Args:
        name: Company name to check
        
    Returns:
        True if mostly punctuation or stopwords
    """
    if not name:
        return False
    
    # Common stopwords in company names
    stopwords = {
        'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with',
        'by', 'from', 'up', 'about', 'into', 'through', 'during', 'before', 'after',
        'above', 'below', 'between', 'among', 'within', 'without', 'against', 'toward',
        'towards', 'upon', 'over', 'under', 'behind', 'beneath', 'beside', 'beyond'
    }
    
    # Count alphanumeric characters
    alnum_count = len(re.findall(r'[a-zA-Z0-9]', name))
    total_chars = len(name.strip())
    
    if total_chars == 0:
        return True
    
    # If less than 30% alphanumeric, likely mostly punctuation
    if alnum_count / total_chars < 0.3:
        return True
    
    # Check if mostly stopwords
    words = re.findall(r'\b[a-zA-Z]+\b', name.lower())
    if words:
        stopword_count = sum(1 for word in words if word in stopwords)
        if stopword_count / len(words) > 0.8:  # More than 80% stopwords
            return True
    
    return False


def _is_suspicious_singleton(row: pd.Series, settings: Dict) -> bool:
    """
    Check if a singleton record is suspicious.
    
    Args:
        row: Record to check
        settings: Configuration settings
        
    Returns:
        True if suspicious
    """
    name = row.get('Account Name', '')
    
    # Check for suspicious patterns
    suspicious_patterns = [
        r'\b(unknown|unsure|not sure|no idea)\b',
        r'\b(test|sample|example|dummy)\b',
        r'\b(temp|temporary|temp agency)\b',
        r'\b(none|n/a|na|tbd|to be determined)\b',
        r'\b(delete|remove|do not use)\b'
    ]
    
    name_lower = str(name).lower()
    for pattern in suspicious_patterns:
        if re.search(pattern, name_lower):
            return True
    
    # Check LLM gate if enabled
    if settings.get('llm', {}).get('enabled', False):
        # Stub for Phase 1 - would call LLM classifier
        logger.debug("LLM gate would be called here in Phase 2")
    
    return False


def compute_group_metadata(df_groups: pd.DataFrame) -> Dict[int, Dict]:
    """
    Compute metadata for each group.
    
    Args:
        df_groups: DataFrame with group assignments
        
    Returns:
        Dictionary mapping group_id to metadata
    """
    group_metadata = {}
    
    for group_id in df_groups['group_id'].unique():
        if group_id == -1:  # Skip unassigned records
            continue
            
        group_mask = df_groups['group_id'] == group_id
        group_data = df_groups[group_mask]
        
        # Check for suffix mismatches
        suffix_classes = group_data['suffix_class'].unique()
        has_suffix_mismatch = len(suffix_classes) > 1
        
        # Check for blacklist hits
        blacklist_hits = group_data['Account Name'].apply(_is_blacklisted).sum()
        
        group_metadata[group_id] = {
            'group_size': len(group_data),
            'has_suffix_mismatch': has_suffix_mismatch,
            'blacklist_hits': blacklist_hits,
            'suffix_classes': list(suffix_classes),
            'has_primary': group_data['is_primary'].any()
        }
    
    return group_metadata


def apply_dispositions(df_groups: pd.DataFrame, settings: Dict) -> pd.DataFrame:
    """
    Apply disposition classification to all records.
    
    Args:
        df_groups: DataFrame with group assignments
        settings: Configuration settings
        
    Returns:
        DataFrame with disposition column added
    """
    logger.info("Applying disposition classification")
    
    # Load manual overrides
    manual_overrides = _load_manual_dispositions()
    override_count = len(manual_overrides)
    if override_count > 0:
        logger.info(f"Loaded {override_count} manual disposition overrides")
    
    # Compute group metadata
    group_metadata = compute_group_metadata(df_groups)
    
    # Apply dispositions
    result_df = df_groups.copy()
    result_df['Disposition'] = ''
    result_df['disposition_reason'] = ''
    
    for idx, row in result_df.iterrows():
        # Check for manual override first
        record_id = str(idx)
        if record_id in manual_overrides:
            override = manual_overrides[record_id]
            result_df.loc[idx, 'Disposition'] = override
            result_df.loc[idx, 'disposition_reason'] = f'manual_override:{override}'
            continue
        
        group_id = row['group_id']
        
        if group_id == -1:
            # Unassigned record - treat as singleton
            group_meta = {'group_size': 1, 'has_suffix_mismatch': False, 'blacklist_hits': 0}
        else:
            group_meta = group_metadata.get(group_id, {})
        
        disposition = classify_disposition(row, group_meta, settings)
        reason = get_disposition_reason(row, group_meta, settings)
        
        result_df.loc[idx, 'Disposition'] = disposition
        result_df.loc[idx, 'disposition_reason'] = reason
    
    # Log disposition summary
    disposition_counts = result_df['Disposition'].value_counts()
    logger.info(f"Disposition summary: {disposition_counts.to_dict()}")
    
    return result_df


def save_dispositions(df_dispositions: pd.DataFrame, output_path: str) -> None:
    """
    Save dispositions DataFrame to parquet file.
    
    Args:
        df_dispositions: DataFrame with dispositions
        output_path: Output file path
    """
    df_dispositions.to_parquet(output_path, index=False)
    logger.info(f"Saved dispositions to {output_path}")


def load_dispositions(input_path: str) -> pd.DataFrame:
    """
    Load dispositions DataFrame from parquet file.
    
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


def get_disposition_reason(row: pd.Series, group_meta: Dict, settings: Dict) -> str:
    """
    Get the reason for a disposition classification.
    
    Args:
        row: Record to classify
        group_meta: Group metadata
        settings: Configuration settings
        
    Returns:
        Reason string
    """
    # Check for blacklisted names
    if _is_blacklisted(row.get('Account Name', '')):
        return 'blacklisted_name'
    
    # Check for multiple names
    if row.get('has_multiple_names', False):
        return 'multi_name_string_requires_split'
    
    # Check for alias matches
    alias_cross_refs = row.get('alias_cross_refs', [])
    if alias_cross_refs:
        sources = list(set([ref.get('source', '') for ref in alias_cross_refs]))
        return f'alias_matches_{len(alias_cross_refs)}_groups_via_{sources}'
    
    # Check for suffix mismatch
    if group_meta.get('has_suffix_mismatch', False):
        return 'suffix_mismatch'
    
    # Check group size
    group_size = group_meta.get('group_size', 1)
    
    if group_size == 1:
        if _is_suspicious_singleton(row, settings):
            return 'suspicious_singleton'
        return 'clean_singleton'
    
    # Multi-record group
    is_primary = row.get('is_primary', False)
    
    if is_primary:
        return 'primary_record'
    else:
        return 'duplicate_record'
