"""
Grouping functionality for Company Junction deduplication.

This module handles:
- Building duplicate groups using Union-Find algorithm
- Graph construction from candidate pairs
- Group assignment and metadata
"""

import pandas as pd
import numpy as np
from typing import List, Dict, Tuple, Optional
import logging

logger = logging.getLogger(__name__)


class UnionFind:
    """Union-Find data structure for efficient group management."""
    
    def __init__(self, n: int):
        """
        Initialize Union-Find with n elements.
        
        Args:
            n: Number of elements
        """
        self.parent = list(range(n))
        self.rank = [0] * n
    
    def find(self, x: int) -> int:
        """Find the root of element x with path compression."""
        if self.parent[x] != x:
            self.parent[x] = self.find(self.parent[x])
        return self.parent[x]
    
    def union(self, x: int, y: int) -> None:
        """Union two sets by rank."""
        root_x = self.find(x)
        root_y = self.find(y)
        
        if root_x == root_y:
            return
        
        if self.rank[root_x] < self.rank[root_y]:
            self.parent[root_x] = root_y
        elif self.rank[root_x] > self.rank[root_y]:
            self.parent[root_y] = root_x
        else:
            self.parent[root_y] = root_x
            self.rank[root_x] += 1
    
    def get_groups(self) -> Dict[int, List[int]]:
        """Get all groups as a dictionary mapping root to members."""
        groups = {}
        for i in range(len(self.parent)):
            root = self.find(i)
            if root not in groups:
                groups[root] = []
            groups[root].append(i)
        return groups


def build_groups(df_norm: pd.DataFrame, pairs_df: pd.DataFrame, settings: Dict) -> pd.DataFrame:
    """
    Build duplicate groups from candidate pairs.
    
    Args:
        df_norm: Normalized DataFrame
        pairs_df: Candidate pairs DataFrame
        settings: Configuration settings
        
    Returns:
        DataFrame with group assignments
    """
    logger.info(f"Building groups from {len(pairs_df)} candidate pairs")
    
    if pairs_df.empty:
        # No pairs means all records are singletons
        result_df = df_norm.copy()
        result_df['group_id'] = range(len(result_df))
        result_df['is_primary'] = True
        result_df['score_to_primary'] = 0.0
        return result_df
    
    # Get medium threshold for group building
    medium_threshold = settings.get('similarity', {}).get('medium', 84)
    
    # Filter pairs for group building (suffix_match=True and score>=medium)
    group_pairs = pairs_df[
        (pairs_df['suffix_match'] == True) & 
        (pairs_df['score'] >= medium_threshold)
    ].copy()
    
    logger.info(f"Using {len(group_pairs)} pairs for group building")
    
    # Initialize Union-Find
    uf = UnionFind(len(df_norm))
    
    # Union all qualifying pairs
    for _, pair in group_pairs.iterrows():
        uf.union(int(pair['id_a']), int(pair['id_b']))
    
    # Get groups
    groups = uf.get_groups()
    
    # Create result DataFrame
    result_df = df_norm.copy()
    result_df['group_id'] = -1
    result_df['is_primary'] = False
    result_df['score_to_primary'] = 0.0
    
    # Assign group IDs
    for group_id, members in groups.items():
        for member_idx in members:
            result_df.iloc[member_idx, result_df.columns.get_loc('group_id')] = group_id
    
    # Mark singletons (group size = 1)
    group_sizes = result_df['group_id'].value_counts()
    singletons = group_sizes[group_sizes == 1].index
    
    for singleton_id in singletons:
        mask = result_df['group_id'] == singleton_id
        result_df.loc[mask, 'is_primary'] = True
    
    logger.info(f"Built {len(groups)} groups ({len(singletons)} singletons)")
    return result_df


def compute_score_to_primary(df_groups: pd.DataFrame, pairs_df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute best score to primary for each record in a group.
    
    Args:
        df_groups: DataFrame with group assignments
        pairs_df: Candidate pairs DataFrame
        
    Returns:
        DataFrame with score_to_primary updated
    """
    if pairs_df.empty:
        return df_groups
    
    result_df = df_groups.copy()
    
    # For each group, find the best score to primary
    for group_id in result_df['group_id'].unique():
        if group_id == -1:  # Skip unassigned records
            continue
            
        group_mask = result_df['group_id'] == group_id
        group_indices = result_df[group_mask].index.tolist()
        
        # Find primary in this group
        primary_mask = result_df[group_mask]['is_primary']
        if not primary_mask.any():
            # No primary assigned yet, skip
            continue
            
        primary_idx = result_df[group_mask][primary_mask].index[0]
        
        # For each non-primary record, find best score to primary
        for record_idx in group_indices:
            if record_idx == primary_idx:
                continue
                
            # Look for pair between this record and primary
            pair_mask = (
                ((pairs_df['id_a'] == record_idx) & (pairs_df['id_b'] == primary_idx)) |
                ((pairs_df['id_a'] == primary_idx) & (pairs_df['id_b'] == record_idx))
            )
            
            if pair_mask.any():
                best_score = pairs_df[pair_mask]['score'].max()
                result_df.loc[record_idx, 'score_to_primary'] = best_score
            else:
                # No direct pair found, set to 0
                result_df.loc[record_idx, 'score_to_primary'] = 0.0
    
    return result_df


def save_groups(df_groups: pd.DataFrame, output_path: str) -> None:
    """
    Save groups DataFrame to parquet file.
    
    Args:
        df_groups: DataFrame with group assignments
        output_path: Output file path
    """
    df_groups.to_parquet(output_path, index=False)
    logger.info(f"Saved groups to {output_path}")


def load_groups(input_path: str) -> pd.DataFrame:
    """
    Load groups DataFrame from parquet file.
    
    Args:
        input_path: Input file path
        
    Returns:
        DataFrame with group assignments
    """
    try:
        df_groups = pd.read_parquet(input_path)
        logger.info(f"Loaded groups from {input_path}")
        return df_groups
    except Exception as e:
        logger.error(f"Error loading groups: {e}")
        return pd.DataFrame()
