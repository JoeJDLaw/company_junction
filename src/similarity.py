"""
Similarity scoring and candidate pairing for Company Junction deduplication.

This module handles:
- Candidate pair generation with blocking
- Similarity scoring using rapidfuzz
- Composite score calculation with penalties
- Suffix and numeric style matching
"""

import pandas as pd
import numpy as np
from rapidfuzz import fuzz
from typing import List, Tuple, Dict, Optional
import logging
from itertools import combinations
import re

logger = logging.getLogger(__name__)


def pair_scores(df_norm: pd.DataFrame, settings: Dict) -> pd.DataFrame:
    """
    Generate candidate pairs and compute similarity scores.
    
    Args:
        df_norm: DataFrame with normalized name columns
        settings: Configuration settings
        
    Returns:
        DataFrame with candidate pairs and scores
    """
    logger.info(f"Generating candidate pairs for {len(df_norm)} records")
    
    # Get configuration
    high_threshold = settings.get('similarity', {}).get('high', 92)
    medium_threshold = settings.get('similarity', {}).get('medium', 84)
    penalties = settings.get('similarity', {}).get('penalty', {})
    
    # Generate candidate pairs using blocking
    candidate_pairs = _generate_candidate_pairs(df_norm)
    
    if not candidate_pairs:
        logger.info("No candidate pairs found")
        return pd.DataFrame()
    
    # Compute similarity scores for each pair
    scores = []
    for idx_a, idx_b in candidate_pairs:
        score_data = _compute_pair_score(
            df_norm.iloc[idx_a], 
            df_norm.iloc[idx_b], 
            penalties
        )
        scores.append({
            'id_a': idx_a,
            'id_b': idx_b,
            **score_data
        })
    
    # Create DataFrame and filter by medium threshold
    pairs_df = pd.DataFrame(scores)
    if not pairs_df.empty:
        pairs_df = pairs_df[pairs_df['score'] >= medium_threshold].copy()
        pairs_df = pairs_df.sort_values('score', ascending=False)
    
    logger.info(f"Generated {len(pairs_df)} candidate pairs above medium threshold")
    return pairs_df


def _generate_candidate_pairs(df_norm: pd.DataFrame) -> List[Tuple[int, int]]:
    """
    Generate candidate pairs using blocking strategy.
    
    Args:
        df_norm: DataFrame with normalized name columns
        
    Returns:
        List of (idx_a, idx_b) tuples
    """
    pairs = []
    
    # Handle empty DataFrame
    if df_norm.empty or 'name_core' not in df_norm.columns:
        return pairs
    
    # Block by first token of name_core
    core_tokens = df_norm['name_core'].str.split().str[0].fillna('')
    unique_first_tokens = core_tokens.unique()
    
    for first_token in unique_first_tokens:
        if pd.isna(first_token) or first_token == '':
            continue
            
        # Get indices for records with this first token
        mask = core_tokens == first_token
        indices = df_norm[mask].index.tolist()
        
        # Generate pairs within this block
        if len(indices) > 1:
            block_pairs = list(combinations(indices, 2))
            pairs.extend(block_pairs)
    
    # Also block by account owner (if available) for additional pairs
    if 'Account Owner: Full Name' in df_norm.columns:
        owner_tokens = df_norm['Account Owner: Full Name'].str.split().str[0].fillna('')
        unique_owners = owner_tokens.unique()
        
        for owner in unique_owners:
            if pd.isna(owner) or owner == '':
                continue
                
            mask = owner_tokens == owner
            indices = df_norm[mask].index.tolist()
            
            if len(indices) > 1:
                owner_pairs = list(combinations(indices, 2))
                pairs.extend(owner_pairs)
    
    # Remove duplicates and return
    unique_pairs = list(set(pairs))
    logger.info(f"Generated {len(unique_pairs)} unique candidate pairs")
    return unique_pairs


def _compute_pair_score(row_a: pd.Series, row_b: pd.Series, penalties: Dict) -> Dict:
    """
    Compute similarity score for a pair of records.
    
    Args:
        row_a: First record
        row_b: Second record
        penalties: Penalty configuration
        
    Returns:
        Dictionary with score components
    """
    name_core_a = row_a['name_core']
    name_core_b = row_b['name_core']
    
    # Compute rapidfuzz ratios
    ratio_name = fuzz.token_sort_ratio(name_core_a, name_core_b)
    ratio_set = fuzz.token_set_ratio(name_core_a, name_core_b)
    
    # Compute Jaccard similarity
    tokens_a = set(name_core_a.split())
    tokens_b = set(name_core_b.split())
    
    if tokens_a and tokens_b:
        intersection = len(tokens_a & tokens_b)
        union = len(tokens_a | tokens_b)
        jaccard = intersection / union if union > 0 else 0
    else:
        jaccard = 0
    
    # Check numeric style match
    num_style_match = _check_numeric_style_match(name_core_a, name_core_b)
    
    # Check suffix match
    suffix_match = row_a['suffix_class'] == row_b['suffix_class']
    
    # Check punctuation mismatch
    punctuation_mismatch = _check_punctuation_mismatch(row_a, row_b)
    
    # Compute base score
    base = 0.45 * ratio_name + 0.35 * ratio_set + 20 * jaccard
    
    # Apply penalties
    if not num_style_match:
        base -= penalties.get('num_style_mismatch', 5)
    
    if not suffix_match:
        base -= penalties.get('suffix_mismatch', 25)
    
    if punctuation_mismatch:
        base -= penalties.get('punctuation_mismatch', 3)
    
    # Clip to 0-100 range
    score = max(0, min(100, round(base)))
    
    return {
        'score': score,
        'ratio_name': ratio_name,
        'ratio_set': ratio_set,
        'jaccard': jaccard,
        'num_style_match': num_style_match,
        'suffix_match': suffix_match,
        'punctuation_mismatch': punctuation_mismatch,
        'base_score': base
    }


def _check_numeric_style_match(name_a: str, name_b: str) -> bool:
    """
    Check if two names have matching numeric styles.
    
    Args:
        name_a: First normalized name
        name_b: Second normalized name
        
    Returns:
        True if numeric styles match
    """
    # Extract numeric patterns
    def extract_numeric_pattern(text):
        # Find patterns like "20 20", "100 200", etc.
        pattern = r'\d+\s+\d+'
        matches = re.findall(pattern, text)
        return set(matches)
    
    pattern_a = extract_numeric_pattern(name_a)
    pattern_b = extract_numeric_pattern(name_b)
    
    # If both have numeric patterns, they should match
    if pattern_a and pattern_b:
        return pattern_a == pattern_b
    
    # If neither has numeric patterns, consider it a match
    if not pattern_a and not pattern_b:
        return True
    
    # If only one has numeric patterns, it's a mismatch
    return False


def save_candidate_pairs(pairs_df: pd.DataFrame, output_path: str) -> None:
    """
    Save candidate pairs to parquet file.
    
    Args:
        pairs_df: DataFrame with candidate pairs
        output_path: Output file path
    """
    if not pairs_df.empty:
        pairs_df.to_parquet(output_path, index=False)
        logger.info(f"Saved {len(pairs_df)} candidate pairs to {output_path}")
    else:
        logger.warning("No candidate pairs to save")


def load_candidate_pairs(input_path: str) -> pd.DataFrame:
    """
    Load candidate pairs from parquet file.
    
    Args:
        input_path: Input file path
        
    Returns:
        DataFrame with candidate pairs
    """
    try:
        pairs_df = pd.read_parquet(input_path)
        logger.info(f"Loaded {len(pairs_df)} candidate pairs from {input_path}")
        return pairs_df
    except Exception as e:
        logger.error(f"Error loading candidate pairs: {e}")
        return pd.DataFrame()


def _check_punctuation_mismatch(row_a: pd.Series, row_b: pd.Series) -> bool:
    """
    Check if two records have conflicting punctuation patterns.
    
    Args:
        row_a: First record
        row_b: Second record
        
    Returns:
        True if punctuation patterns conflict
    """
    # Check for semicolon mismatch
    has_semicolon_a = row_a.get('has_semicolon', False)
    has_semicolon_b = row_b.get('has_semicolon', False)
    
    if has_semicolon_a != has_semicolon_b:
        return True
    
    # Check for parentheses mismatch
    has_parentheses_a = row_a.get('has_parentheses', False)
    has_parentheses_b = row_b.get('has_parentheses', False)
    
    if has_parentheses_a != has_parentheses_b:
        return True
    
    return False
