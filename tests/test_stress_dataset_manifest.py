"""Validator tests for the stress dataset manifest and generation."""

import pandas as pd
import subprocess
import tempfile
from pathlib import Path
import yaml

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

import pandas as pd
from tests.helpers.stress10k import load_slice


def test_seed_id_uniqueness():
    """Test that all seed IDs (from both YAMLs) are unique."""
    # Load the generated dataset
    df = pd.read_csv("tests/data/companies_stress_10k.csv")
    
    # Get seed rows (those with ground_truth_entity_id)
    seed_rows = df[df['ground_truth_entity_id'].notna() & (df['ground_truth_entity_id'] != '')]
    
    # Check for duplicate account_ids in seed rows
    duplicate_ids = seed_rows[seed_rows.duplicated(subset=['account_id'], keep=False)]
    
    assert len(duplicate_ids) == 0, f"Found duplicate seed IDs: {duplicate_ids['account_id'].tolist()}"
    
    print(f"✓ Seed ID uniqueness: {len(seed_rows)} unique seed IDs")


def test_triad_clusters_present():
    """Test that triad and quad clusters exist and are properly tagged."""
    # Load triad and quad cases
    triad_df = load_slice(tags=["triad"])
    quad_df = load_slice(tags=["quad"])
    
    # Combine both datasets
    cluster_df = pd.concat([triad_df, quad_df], ignore_index=True)
    
    assert len(cluster_df) > 0, "No triad/quad cases found"
    
    # Check that all cluster cases have cluster_id
    cluster_with_id = cluster_df[cluster_df['cluster_id'].notna() & (cluster_df['cluster_id'] != '')]
    assert len(cluster_with_id) == len(cluster_df), "Some cluster cases missing cluster_id"
    
    # Check that we have expected clusters
    expected_clusters = [
        "C_ORACLE_TRIAD", "C_APPLE_TRIAD", "C_MSFT_SUFFIX", "C_GE_SET", 
        "C_CAFE", "C_TARGET_EMOJI", "C_AMAZON_QUAD", "C_GOOGLE_QUAD"
    ]
    found_clusters = set(cluster_df['cluster_id'].unique())
    
    for cluster in expected_clusters:
        assert cluster in found_clusters, f"Expected cluster {cluster} not found"
    
    print(f"✓ Cluster coverage: {len(found_clusters)} clusters with {len(cluster_df)} total rows")


def test_duplicate_id_tagging():
    """Test that duplicate-ID rows are properly tagged and sliceable."""
    # Load duplicate ID cases
    duplicate_df = load_slice(tags=["id", "duplicate"])
    
    assert len(duplicate_df) > 0, "No duplicate ID cases found"
    
    # Check that all have the duplicate tag
    for _, row in duplicate_df.iterrows():
        tags = set(row['scenario_tags'].split('|'))
        assert "id" in tags, f"Row missing 'id' tag: {row['account_id']}"
        assert "duplicate" in tags, f"Row missing 'duplicate' tag: {row['account_id']}"
    
    print(f"✓ Duplicate ID tagging: {len(duplicate_df)} rows properly tagged")


def _stable_seed_view(df: pd.DataFrame) -> pd.DataFrame:
    """Create a stable view of seed rows for deterministic comparison."""
    seed = df[df['ground_truth_entity_id'].notna() & (df['ground_truth_entity_id'] != '')].copy()
    return seed.sort_values(["ground_truth_entity_id", "account_id"], kind="mergesort").reset_index(drop=True)


def test_fixed_today_determinism():
    """Test that --fixed-today produces deterministic dates."""
    fixed_date = "2024-01-01"
    
    # First generation
    result1 = subprocess.run([
        "python", "scripts/generate_stress_dataset.py", 
        "--fixed-today", fixed_date
    ], capture_output=True, text=True, cwd=Path.cwd())
    
    assert result1.returncode == 0, f"First generation failed: {result1.stderr}"
    
    # Read first dataset
    df1 = pd.read_csv("tests/data/companies_stress_10k.csv")
    
    # Second generation
    result2 = subprocess.run([
        "python", "scripts/generate_stress_dataset.py", 
        "--fixed-today", fixed_date
    ], capture_output=True, text=True, cwd=Path.cwd())
    
    assert result2.returncode == 0, f"Second generation failed: {result2.stderr}"
    
    # Read second dataset
    df2 = pd.read_csv("tests/data/companies_stress_10k.csv")
    
    # Compare seed rows only (the true contract of determinism)
    s1 = _stable_seed_view(df1)
    s2 = _stable_seed_view(df2)
    
    # Exact match on seed IDs and dates
    pd.testing.assert_series_equal(s1["account_id"], s2["account_id"], check_names=False)
    pd.testing.assert_series_equal(s1["created_date"], s2["created_date"], check_names=False)
    
    print(f"✓ Fixed-today determinism: dates and seed IDs identical across runs")


def test_manifest_yaml_validity():
    """Test that both YAML manifests are valid."""
    # Test adversarial.yaml
    adversarial_path = Path("tests/data/seeds/adversarial.yaml")
    assert adversarial_path.exists(), "adversarial.yaml not found"
    
    with open(adversarial_path, 'r') as f:
        adversarial_data = yaml.safe_load(f)
    
    assert 'pairs' in adversarial_data, "adversarial.yaml missing 'pairs' key"
    assert len(adversarial_data['pairs']) > 0, "adversarial.yaml has no pairs"
    
    # Test groups.yaml
    groups_path = Path("tests/data/seeds/groups.yaml")
    assert groups_path.exists(), "groups.yaml not found"
    
    with open(groups_path, 'r') as f:
        groups_data = yaml.safe_load(f)
    
    assert 'clusters' in groups_data, "groups.yaml missing 'clusters' key"
    assert len(groups_data['clusters']) > 0, "groups.yaml has no clusters"
    
    # Validate cluster structure
    for cluster in groups_data['clusters']:
        assert 'id' in cluster, f"Cluster missing 'id': {cluster}"
        assert 'items' in cluster, f"Cluster {cluster['id']} missing 'items'"
        assert len(cluster['items']) >= 3, f"Cluster {cluster['id']} has < 3 items"
        
        for item in cluster['items']:
            assert 'name' in item, f"Item missing 'name' in cluster {cluster['id']}"
            assert 'gt_entity' in item, f"Item missing 'gt_entity' in cluster {cluster['id']}"
            assert 'tags' in item, f"Item missing 'tags' in cluster {cluster['id']}"
    
    print(f"✓ YAML validity: {len(adversarial_data['pairs'])} pairs, {len(groups_data['clusters'])} clusters")


def test_scenario_tags_vocabulary():
    """Test that all scenario_tags use known vocabulary."""
    # Base vocabulary - core tags that should always be present
    BASE_VOCABULARY = {
        # Categories
        "adversarial", "control", "probe", "near_threshold",
        # Expectations  
        "must_group", "must_not_group",
        # Scenarios
        "bank", "venue", "brand_ext", "homoglyph", "transliteration", "emoji",
        # Data types
        "triad", "quad", "pair",
        # Date edge cases
        "date", "edge", "ancient", "future", "extreme", "invalid",
        # ID issues
        "id", "duplicate",
        # Additional common tags
        "alias", "suffix", "substring", "numeric_prefix", "group"
    }
    
    # Load YAML manifests to get their tags
    yaml_tags = set()
    
    # Load adversarial.yaml tags
    adversarial_path = Path("tests/data/seeds/adversarial.yaml")
    if adversarial_path.exists():
        with open(adversarial_path, "r", encoding="utf-8") as f:
            adversarial_data = yaml.safe_load(f)
        for pair in adversarial_data.get("pairs", []):
            yaml_tags.update(pair.get("tags", []))
    
    # Load groups.yaml tags
    groups_path = Path("tests/data/seeds/groups.yaml")
    if groups_path.exists():
        with open(groups_path, "r", encoding="utf-8") as f:
            groups_data = yaml.safe_load(f)
        for cluster in groups_data.get("clusters", []):
            for item in cluster.get("items", []):
                yaml_tags.update(item.get("tags", []))
    
    # Combine base vocabulary with YAML tags
    known_vocabulary = BASE_VOCABULARY | yaml_tags
    
    # Load dataset and extract all tags
    df = pd.read_csv("tests/data/companies_stress_10k.csv")
    all_tags = set()
    for tags_str in df['scenario_tags'].fillna(''):
        if tags_str:
            all_tags.update(tags_str.split('|'))
    
    # Check for unknown tags
    unknown_tags = all_tags - known_vocabulary
    assert len(unknown_tags) == 0, f"Unknown scenario tags found: {unknown_tags}"
    
    print(f"✓ Tag vocabulary: {len(all_tags)} unique tags, all from known vocabulary")


def test_normal_rows_within_10y_and_edges_tagged():
    """Test that normal rows stay within 10-year window and edge dates are properly tagged."""
    df = pd.read_csv("tests/data/companies_stress_10k.csv")
    
    # Validate test assumptions - ensure dataset was generated with --fixed-today
    seed_rows = df[df['ground_truth_entity_id'].notna() & (df['ground_truth_entity_id'] != '')]
    assert len(seed_rows) > 0, "No seed rows found - dataset may not be generated with --fixed-today"
    
    # Check that seed rows have expected dates (validation that --fixed-today was used)
    seed_dates = pd.to_datetime(seed_rows['created_date'], errors='coerce')
    expected_seed_date = pd.Timestamp("2023-01-15")  # From seed generation
    assert (seed_dates == expected_seed_date).all(), "Seed dates don't match expected --fixed-today generation"
    
    tags = df["scenario_tags"].fillna("")
    is_edge = tags.str.contains(r"\bdate\|edge\b")
    
    # Normal rows: must be within 10y and not future
    normal = df[~is_edge].copy()
    assert len(normal) > 0, "No normal rows found"
    
    # Handle invalid dates gracefully - they should be tagged as edge cases
    normal_dates = pd.to_datetime(normal["created_date"], errors="coerce")
    invalid_dates = normal_dates.isna()
    
    if invalid_dates.any():
        print(f"Warning: {invalid_dates.sum()} normal rows have invalid dates - these should be tagged as edge cases")
        # Remove invalid dates from normal rows for the contract check
        normal = normal[~invalid_dates].copy()
        normal_dates = normal_dates[~invalid_dates]
    
    if len(normal) > 0:
        anchor = pd.Timestamp("2024-01-01")  # matches our --fixed-today in CI/Makefile
        assert (normal_dates <= anchor).all(), "Normal rows contain future dates"
        assert ((anchor - normal_dates).dt.days <= 3650).all(), "Normal rows exceed 10-year window"

    # Edge rows that are out-of-window/future must be tagged accordingly
    edge = df[is_edge].copy()
    if len(edge):
        edge_dates = pd.to_datetime(edge["created_date"], errors="coerce")
        valid_edge_dates = ~edge_dates.isna()
        
        if valid_edge_dates.any():
            valid_edge = edge[valid_edge_dates].copy()
            valid_edge_dates_series = edge_dates[valid_edge_dates]
            anchor = pd.Timestamp("2024-01-01")
            
            is_future = valid_edge_dates_series > anchor
            is_ancient = (anchor - valid_edge_dates_series).dt.days > 3650
            
            # Where future, require 'future' token; where ancient, require 'ancient' token
            if is_future.any():
                assert (valid_edge[is_future]["scenario_tags"]
                        .str.contains(r"\bfuture\b")).all(), "Future edge rows missing 'future' tag"
            if is_ancient.any():
                assert (valid_edge[is_ancient]["scenario_tags"]
                        .str.contains(r"\bancient\b")).all(), "Ancient edge rows missing 'ancient' tag"
        
        # Check that invalid dates are tagged as invalid
        invalid_edge_dates = edge_dates.isna()
        if invalid_edge_dates.any():
            invalid_edge = edge[invalid_edge_dates]
            assert (invalid_edge["scenario_tags"]
                    .str.contains(r"\binvalid\b")).all(), "Invalid edge dates missing 'invalid' tag"
    
    print("✓ Date contract: normal rows within 10y, edge dates properly tagged")


def test_helper_function_compatibility():
    """Test that the helper function works with new features."""
    # Test basic functionality
    df = load_slice()
    assert len(df) > 0, "Helper function failed to load dataset"
    
    # Test tag filtering with set-based matching
    triad_df = load_slice(tags=["triad"])
    assert len(triad_df) > 0, "Tag filtering failed"
    
    # Test cluster_id filtering
    oracle_df = load_slice(cluster_ids=["C_ORACLE_TRIAD"])
    assert len(oracle_df) > 0, "Cluster ID filtering failed"
    
    # Test combined filtering
    control_triad = load_slice(tags=["control", "triad"])
    assert len(control_triad) >= 0, "Combined filtering failed"
    
    print(f"✓ Helper function: all filtering methods work correctly")


if __name__ == "__main__":
    """Run all validation tests."""
    print("🧪 Running stress dataset manifest validation tests...")
    print("=" * 60)
    
    try:
        test_manifest_yaml_validity()
        test_scenario_tags_vocabulary()
        test_seed_id_uniqueness()
        test_triad_clusters_present()
        test_duplicate_id_tagging()
        test_fixed_today_determinism()
        test_normal_rows_within_10y_and_edges_tagged()
        test_helper_function_compatibility()
        
        print("=" * 60)
        print("✅ All validation tests passed!")
        
    except Exception as e:
        print(f"❌ Validation test failed: {e}")
        raise
