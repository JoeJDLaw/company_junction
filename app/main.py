"""
Streamlit GUI for the Company Junction deduplication pipeline.

This app provides an interactive interface for:
- Loading review-ready data from pipeline output
- Filtering and reviewing duplicate groups
- Exporting filtered results
"""

import streamlit as st
import pandas as pd
import sys
from pathlib import Path
import json
import os

# Add src directory to path for imports
sys.path.append(str(Path(__file__).parent.parent / 'src'))

from utils import setup_logging


def load_review_data():
    """Load review-ready data from pipeline output."""
    # Try Parquet first (native types for alias fields)
    parquet_path = "data/processed/review_ready.parquet"
    csv_path = "data/processed/review_ready.csv"
    
    if os.path.exists(parquet_path):
        try:
            df = pd.read_parquet(parquet_path)
            st.success(f"Loaded {len(df)} records from Parquet file")
            return df
        except Exception as e:
            st.warning(f"Parquet load failed: {e}, falling back to CSV")
    
    # Fallback to CSV
    if os.path.exists(csv_path):
        try:
            df = pd.read_csv(csv_path)
            
            # Parse alias_cross_refs if it exists (CSV stores as string)
            if 'alias_cross_refs' in df.columns:
                df['alias_cross_refs'] = df['alias_cross_refs'].apply(parse_alias_cross_refs)
            
            st.success(f"Loaded {len(df)} records from CSV file")
            return df
        except Exception as e:
            st.error(f"Error loading review data: {e}")
            return None
    
    return None


def parse_alias_cross_refs(cross_refs_str):
    """Parse alias cross-references from string representation."""
    if pd.isna(cross_refs_str) or cross_refs_str == '[]':
        return []
    try:
        import ast
        return ast.literal_eval(cross_refs_str)
    except:
        return []


def load_settings(path="config/settings.yaml"):
    """Load settings from YAML file."""
    try:
        import yaml
        with open(path, "r") as fh:
            return yaml.safe_load(fh) or {}
    except Exception:
        return {}


def _safe_get(d, *keys, default=None):
    """Safely get nested dictionary values."""
    cur = d
    for k in keys:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur


def parse_merge_preview(preview_json):
    """Parse merge preview JSON for display."""
    if not preview_json or pd.isna(preview_json):
        return None
    
    try:
        return json.loads(preview_json)
    except:
        return None


def main():
    """Main Streamlit application."""
    st.set_page_config(
        page_title="Company Junction Deduplication Review",
        page_icon="🔗",
        layout="wide"
    )
    
    st.title("🔗 Company Junction Deduplication Review")
    st.markdown("Review and filter duplicate detection results from the pipeline.")
    
    # Setup logging
    setup_logging()
    
    # Load review data
    df = load_review_data()
    
    # Load settings for rules panel
    settings = load_settings()
    
    # Import manual data functions
    from manual_data import (
        load_manual_blacklist, add_manual_blacklist_term, remove_manual_blacklist_term,
        export_manual_data
    )
    
    if df is None:
        st.warning("""
        ## No Review Data Found
        
        Please run the pipeline first to generate review data:
        
        ```bash
        python src/cleaning.py --input data/raw/company_junction_range_01.csv --outdir data/processed --config config/settings.yaml
        ```
        
        This will create `data/processed/review_ready.csv` for review.
        """)
        return
    
    # Minimal Rules & Settings panel
    with st.expander("Rules & Settings", expanded=False):
        st.write("**Similarity Thresholds**")
        high_threshold = settings.get('similarity', {}).get('high', 'Not configured')
        medium_threshold = settings.get('similarity', {}).get('medium', 'Not configured')
        st.write(f"High: {high_threshold}, Medium: {medium_threshold}")
        
        st.write("**Alias Extraction Rules**")
        st.markdown("- Semicolons split multiple entities")
        st.markdown("- Numbered sequences like `(1)`, `(2)` denote separate entities") 
        st.markdown("- Parentheses evaluated conservatively (legal suffixes or multiple caps)")
        
        st.write("**Manual Overrides**")
        st.markdown("Manual overrides and manual blacklist (if present) are applied from `data/manual/` during pipeline runs.")
        
        # Export manual data
        dispositions_json, blacklist_json = export_manual_data()
        col1, col2 = st.columns(2)
        with col1:
            st.download_button(
                "Download Manual Dispositions",
                dispositions_json,
                file_name="manual_dispositions.json",
                mime="application/json"
            )
        with col2:
            st.download_button(
                "Download Manual Blacklist",
                blacklist_json,
                file_name="manual_blacklist.json",
                mime="application/json"
            )
    
    # Sidebar filters
    st.sidebar.header("Filters")
    
    # Min score filter
    min_score = st.sidebar.slider(
        "Minimum Score to Primary",
        min_value=0.0,
        max_value=100.0,
        value=0.0,
        step=1.0
    )
    
    # Disposition filter
    dispositions = df['Disposition'].unique()
    
    # Initialize session state for disposition filter
    if 'selected_disposition' not in st.session_state:
        st.session_state.selected_disposition = list(dispositions)
    
    selected_dispositions = st.sidebar.multiselect(
        "Disposition",
        options=dispositions,
        default=st.session_state.selected_disposition
    )
    
    # Update session state
    st.session_state.selected_disposition = selected_dispositions
    
    # Suffix mismatch filter
    show_suffix_mismatch = st.sidebar.checkbox("Show Suffix Mismatches Only", value=False)
    
    # Alias filter
    has_aliases = st.sidebar.checkbox("Has Aliases", value=False)
    
    # Manual Blacklist Editor
    with st.sidebar.expander("Manual Blacklist", expanded=False):
        st.write("**Add/Remove Blacklist Terms**")
        
        # Show current terms
        current_terms = load_manual_blacklist()
        if current_terms:
            st.write("**Current terms:**")
            for term in current_terms:
                col1, col2 = st.columns([3, 1])
                with col1:
                    st.write(f"• {term}")
                with col2:
                    if st.button("Remove", key=f"remove_{term}"):
                        remove_manual_blacklist_term(term)
                        st.rerun()
        else:
            st.write("No manual blacklist terms")
        
        # Add new term
        new_term = st.text_input("Add new term:", key="new_blacklist_term")
        if st.button("Add Term") and new_term.strip():
            add_manual_blacklist_term(new_term.strip())
            st.rerun()
    
    # Group size filter
    group_sizes = df['group_id'].value_counts()
    min_group_size = st.sidebar.slider(
        "Minimum Group Size",
        min_value=1,
        max_value=int(group_sizes.max()),
        value=1
    )
    
    # Apply filters
    filtered_df = df.copy()
    
    if min_score > 0:
        filtered_df = filtered_df[filtered_df['score_to_primary'] >= min_score]
    
    if selected_dispositions:
        filtered_df = filtered_df[filtered_df['Disposition'].isin(selected_dispositions)]
    
    if show_suffix_mismatch:
        # Filter for groups with suffix mismatches
        suffix_mismatch_groups = []
        for group_id in filtered_df['group_id'].unique():
            group_data = filtered_df[filtered_df['group_id'] == group_id]
            suffix_classes = group_data['suffix_class'].unique()
            if len(suffix_classes) > 1:
                suffix_mismatch_groups.append(group_id)
        filtered_df = filtered_df[filtered_df['group_id'].isin(suffix_mismatch_groups)]
    
    if has_aliases:
        # Filter for records with aliases
        if 'alias_cross_refs' in filtered_df.columns:
            filtered_df = filtered_df[filtered_df['alias_cross_refs'].apply(lambda x: len(x) > 0)]
    
    # Filter by group size
    group_sizes_filtered = filtered_df['group_id'].value_counts()
    valid_groups = group_sizes_filtered[group_sizes_filtered >= min_group_size].index
    filtered_df = filtered_df[filtered_df['group_id'].isin(valid_groups)]
    
    # Display summary
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Records", len(filtered_df))
    with col2:
        st.metric("Groups", len(filtered_df['group_id'].unique()))
    with col3:
        st.metric("Primary Records", filtered_df['is_primary'].sum())
    with col4:
        st.metric("Avg Score to Primary", f"{filtered_df['score_to_primary'].mean():.1f}")
    
    # Display disposition summary
    st.subheader("Disposition Summary")
    disposition_counts = filtered_df['Disposition'].value_counts()
    
    # Create compact table instead of chart
    disposition_table = pd.DataFrame({
        'Disposition': disposition_counts.index,
        'Count': disposition_counts.values,
        'Percent': (disposition_counts.values / len(filtered_df) * 100).round(1)
    })
    
    # Add clickable filter buttons
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        if st.button(f"Keep ({disposition_counts.get('Keep', 0)})"):
            st.session_state.selected_disposition = ['Keep']
    with col2:
        if st.button(f"Update ({disposition_counts.get('Update', 0)})"):
            st.session_state.selected_disposition = ['Update']
    with col3:
        if st.button(f"Delete ({disposition_counts.get('Delete', 0)})"):
            st.session_state.selected_disposition = ['Delete']
    with col4:
        if st.button(f"Verify ({disposition_counts.get('Verify', 0)})"):
            st.session_state.selected_disposition = ['Verify']
    
    # Show compact table
    st.dataframe(disposition_table, width='stretch', hide_index=True)
    
    # Display groups
    st.subheader("Duplicate Groups")
    
    # Sorting controls
    st.sidebar.write("**Sorting**")
    sort_by = st.sidebar.selectbox(
        "Sort Groups By",
        ["Group Size (Desc)", "Group Size (Asc)", "Max Score (Desc)", "Max Score (Asc)"],
        index=0
    )
    
    # Apply sorting to groups
    group_stats = []
    for group_id in filtered_df['group_id'].unique():
        group_data = filtered_df[filtered_df['group_id'] == group_id]
        max_score = group_data['score_to_primary'].max()
        group_stats.append({
            'group_id': group_id,
            'size': len(group_data),
            'max_score': max_score
        })
    
    group_stats_df = pd.DataFrame(group_stats)
    
    if "Group Size" in sort_by:
        if "(Desc)" in sort_by:
            sorted_groups = group_stats_df.sort_values('size', ascending=False)['group_id'].tolist()
        else:
            sorted_groups = group_stats_df.sort_values('size', ascending=True)['group_id'].tolist()
    else:  # Max Score
        if "(Desc)" in sort_by:
            sorted_groups = group_stats_df.sort_values('max_score', ascending=False)['group_id'].tolist()
        else:
            sorted_groups = group_stats_df.sort_values('max_score', ascending=True)['group_id'].tolist()
    
    # Pagination
    page_size = st.sidebar.selectbox("Page Size", [10, 25, 50, 100], index=1)
    total_groups = len(sorted_groups)
    
    if "page" not in st.session_state:
        st.session_state.page = 1
    
    max_page = max(1, (total_groups + page_size - 1) // page_size)
    
    col1, col2, col3 = st.columns(3)
    with col1:
        if st.button("Prev") and st.session_state.page > 1:
            st.session_state.page -= 1
    with col2:
        st.write(f"Page {st.session_state.page} / {max_page}")
    with col3:
        if st.button("Next") and st.session_state.page < max_page:
            st.session_state.page += 1
    
    start_idx = (st.session_state.page - 1) * page_size
    end_idx = start_idx + page_size
    
    # Get groups for current page
    page_groups = sorted_groups[start_idx:end_idx]
    
    # Group by group_id and display each group
    for group_id in page_groups:
        group_data = filtered_df[filtered_df['group_id'] == group_id].copy()
        
        # Parse merge preview for this group
        merge_preview = None
        for _, row in group_data.iterrows():
            if row['merge_preview_json']:
                merge_preview = parse_merge_preview(row['merge_preview_json'])
                break
        
        # Group header
        primary_record = group_data[group_data['is_primary']].iloc[0] if group_data['is_primary'].any() else group_data.iloc[0]
        
        with st.expander(f"Group {group_id}: {primary_record['Account Name']} ({len(group_data)} records)"):
            # Group Info at the top
            col1, col2, col3 = st.columns([2, 1, 1])
            
            with col1:
                # Group badges
                suffix_classes = group_data['suffix_class'].unique()
                if len(suffix_classes) > 1:
                    st.error("⚠️ Suffix Mismatch")
                else:
                    st.success(f"✅ {suffix_classes[0]}")
                
                # Blacklist hits
                blacklist_hits = group_data['Account Name'].str.lower().str.contains('|'.join([
                    'pnc is not sure', 'unsure', 'unknown', '1099', 'none', 'n/a', 'test'
                ])).sum()
                
                if blacklist_hits > 0:
                    st.warning(f"⚠️ {blacklist_hits} blacklist hits")
            
            with col2:
                # Primary selection info
                if merge_preview and 'primary_record' in merge_preview:
                    primary_info = merge_preview['primary_record']
                    st.write(f"**Primary:** {primary_info.get('account_id', 'N/A')}")
                    st.write(f"**Rank:** {primary_info.get('relationship_rank', 'N/A')}")
            
            with col3:
                # Manual override dropdown
                from manual_data import add_manual_disposition, get_manual_override_for_record
                
                # Get current override for the primary record
                primary_record_id = primary_record.name
                current_override = get_manual_override_for_record(primary_record_id)
                
                if current_override:
                    st.info(f"**Override:** {current_override}")
                
                override_options = ["No Override", "Keep", "Delete", "Update", "Verify"]
                selected_override = st.selectbox(
                    "Manual Override",
                    override_options,
                    index=override_options.index(current_override) if current_override else 0,
                    key=f"override_{group_id}"
                )
                
                if selected_override != "No Override" and selected_override != current_override:
                    if st.button("Apply Override", key=f"apply_{group_id}"):
                        add_manual_disposition(
                            record_id=str(primary_record_id),
                            account_id=primary_record.get('Account ID', ''),
                            account_name=primary_record.get('Account Name', ''),
                            name_core=primary_record.get('name_core', ''),
                            override=selected_override,
                            reason="Manual override from UI"
                        )
                        st.rerun()
            
            # Display group table below
            st.write("**Records:**")
            display_cols = [
                'Account Name', 'Account ID', 'Relationship', 'Disposition',
                'is_primary', 'score_to_primary', 'suffix_class'
            ]
            display_cols = [col for col in display_cols if col in group_data.columns]
            
            # Configure column display for better readability
            column_config = {
                'Account Name': st.column_config.TextColumn(
                    'Account Name',
                    width='large',
                    help='Company name',
                    max_chars=None
                ),
                'Account ID': st.column_config.TextColumn(
                    'Account ID',
                    width='medium'
                ),
                'Relationship': st.column_config.TextColumn(
                    'Relationship',
                    width='medium'
                ),
                'Disposition': st.column_config.SelectboxColumn(
                    'Disposition',
                    width='small',
                    options=['Keep', 'Update', 'Delete', 'Verify']
                ),
                'is_primary': st.column_config.CheckboxColumn(
                    'Primary',
                    width='small'
                ),
                'score_to_primary': st.column_config.NumberColumn(
                    'Score',
                    width='small',
                    format='%.1f'
                ),
                'suffix_class': st.column_config.TextColumn(
                    'Suffix',
                    width='small'
                )
            }
            
            st.dataframe(
                group_data[display_cols],
                width='stretch',
                column_config=column_config,
                hide_index=True
            )
            
            # Alias information (if present)
            if 'alias_cross_refs' in group_data.columns:
                alias_records = group_data[group_data['alias_cross_refs'].apply(lambda x: len(x) > 0)]
                if not alias_records.empty:
                    st.write("**Alias Cross-links:**")
                    for _, record in alias_records.iterrows():
                        cross_refs = record['alias_cross_refs']
                        if cross_refs:
                            st.write(f"📎 {len(cross_refs)} cross-links")
                            with st.expander("View cross-links"):
                                for ref in cross_refs:
                                    st.write(f"• {ref.get('alias', '')} → Group {ref.get('group_id', '')} (score: {ref.get('score', '')}, source: {ref.get('source', '')})")
            
            # Display merge preview if available
            if merge_preview and 'field_comparisons' in merge_preview:
                st.write("**Field Conflicts:**")
                
                conflicts = []
                for field, comparison in merge_preview['field_comparisons'].items():
                    if comparison.get('has_conflict', False):
                        conflicts.append({
                            'field': field,
                            'primary_value': comparison.get('primary_value', ''),
                            'alternatives': comparison.get('alternative_values', [])
                        })
                
                if conflicts:
                    for conflict in conflicts:
                        st.write(f"**{conflict['field']}:**")
                        st.write(f"  Primary: {conflict['primary_value']}")
                        st.write(f"  Alternatives: {', '.join(conflict['alternatives'])}")
                else:
                    st.success("✅ No field conflicts")
    
    # Export functionality
    st.subheader("Export")
    
    if st.button("Export Filtered Data"):
        # Create download link
        csv = filtered_df.to_csv(index=False)
        st.download_button(
            label="Download CSV",
            data=csv,
            file_name=f"filtered_review_data_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv"
        )


if __name__ == "__main__":
    main()
