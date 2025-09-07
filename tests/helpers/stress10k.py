"""Helper functions for loading and slicing the stress test dataset."""

from pathlib import Path
import pandas as pd
import re

# Resolve path relative to project root (where tests are run from)
CSV_PATH = Path("tests/data/companies_stress_10k.csv")


def load_slice(path=CSV_PATH, tags=None, gt_ids=None, cluster_ids=None):
    """
    Load a slice of the stress dataset.
    
    Args:
        path: Path to the CSV file (defaults to the standard stress dataset)
        tags: list[str] of tag tokens. Match if ALL tokens are present in scenario_tags.
        gt_ids: list[str] of ground_truth_entity_id values to include.
        cluster_ids: list[str] of cluster_id values to include.
    
    Returns:
        pandas.DataFrame: Filtered dataset slice
        
    Examples:
        # Load all adversarial venue cases
        df = load_slice(tags=["adversarial", "venue", "must_not_group"])
        
        # Load specific ground truth entities
        df = load_slice(gt_ids=["GT_APPLE", "GT_APPLE_BANK"])
        
        # Load triad clusters
        df = load_slice(tags=["triad"])
        
        # Load quad clusters
        df = load_slice(tags=["quad"])
        
        # Load outside-window edge dates
        ancient = load_slice(tags=["date", "edge", "ancient"])
        future = load_slice(tags=["date", "edge", "future"])
        
        # Load duplicate ID cases
        duplicates = load_slice(tags=["id", "duplicate"])
        
        # Load specific cluster
        df = load_slice(cluster_ids=["C_ORACLE_TRIAD"])
        
        # Load both filters
        df = load_slice(tags=["control"], gt_ids=["GT_JNJ"])
    """
    df = pd.read_csv(path)
    
    if tags:
        # Use delimiter-aware set matcher with AND semantics
        tokens = df["scenario_tags"].fillna("").str.split("|").apply(
            lambda ts: set(t.strip() for t in ts if t)
        )
        mask = tokens.apply(lambda s: set(tags).issubset(s))
        df = df[mask]
    
    if gt_ids:
        df = df[df["ground_truth_entity_id"].isin(gt_ids)]
    
    if cluster_ids:
        df = df[df["cluster_id"].isin(cluster_ids)]
    
    return df
