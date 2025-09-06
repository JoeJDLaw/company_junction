"""Group list component for Phase 1.18.1 refactor.

This module handles the paginated display of groups with navigation controls.
"""

import logging
import os
from typing import Any

import streamlit as st

from src.services.group_service import GroupService
from src.utils.cache_keys import build_cache_key
from src.utils.fragment_utils import fragment
from src.utils.group_pagination import (
    PageFetchTimeout,
    get_groups_page,
    get_total_groups_count,
)
from src.utils.state_utils import get_backend_state, get_page_state, set_page_state

logger = logging.getLogger(__name__)


def _render_cluster_details(group_info: dict[str, Any], selected_run_id: str) -> None:
    """Render cluster-specific details instead of traditional group details."""
    try:
        # Load account data to show cluster members
        from src.utils.artifact_management import get_artifact_paths
        artifact_paths = get_artifact_paths(selected_run_id)
        review_path = artifact_paths.get("review_ready_parquet")
        
        if review_path and os.path.exists(review_path):
            import pandas as pd
            review_df = pd.read_parquet(review_path)
            
            # Get cluster members
            cluster_members = group_info.get("members", [])
            if cluster_members:
                # Filter review data to only cluster members
                cluster_data = review_df[review_df['account_id'].isin(cluster_members)].copy()
                
                if not cluster_data.empty:
                    # Sort by account name for better display
                    cluster_data = cluster_data.sort_values('account_name')
                    
                    # Display cluster members in a table
                    st.markdown("**Cluster Members:**")
                    
                    # Show key columns
                    display_columns = ['account_name', 'account_id']
                    if 'disposition' in cluster_data.columns:
                        display_columns.append('disposition')
                    if 'suffix_class' in cluster_data.columns:
                        display_columns.append('suffix_class')
                    
                    # Filter to only existing columns
                    available_columns = [col for col in display_columns if col in cluster_data.columns]
                    if available_columns:
                        st.dataframe(
                            cluster_data[available_columns], 
                            hide_index=True,
                            use_container_width=True
                        )
                    else:
                        st.dataframe(cluster_data, hide_index=True, use_container_width=True)
                    
                    # Show cluster statistics
                    st.markdown("**Cluster Statistics:**")
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Total Members", len(cluster_data))
                    with col2:
                        if 'disposition' in cluster_data.columns:
                            disposition_counts = cluster_data['disposition'].value_counts()
                            st.metric("Most Common Disposition", disposition_counts.index[0] if len(disposition_counts) > 0 else "N/A")
                    with col3:
                        if 'suffix_class' in cluster_data.columns:
                            suffix_counts = cluster_data['suffix_class'].value_counts()
                            st.metric("Most Common Suffix", suffix_counts.index[0] if len(suffix_counts) > 0 else "N/A")
                else:
                    st.warning("No data found for cluster members")
            else:
                st.info("No cluster members to display")
        else:
            st.warning("Could not load review data for cluster details")
            
    except Exception as e:
        st.error(f"Error loading cluster details: {e}")


def render_similarity_clusters(
    selected_run_id: str,
    sort_by: str,
    page: int,
    page_size: int,
    filters: dict[str, Any],
    settings: dict[str, Any],
) -> tuple[list[dict[str, Any]], int, int]:
    """Render similarity-based clusters with pagination.
    
    Args:
        selected_run_id: The selected run ID
        sort_by: The sort key
        page: The current page number
        page_size: The page size
        filters: The filters dictionary
        settings: Application settings
        
    Returns:
        Tuple of (page_clusters, total_clusters, max_page)
    """
    # Get clustering parameters from filters
    threshold = filters.get("similarity_threshold", 92) / 100.0  # Convert to [0,1]
    policy = filters.get("clustering_policy", "complete")
    min_cluster_size = filters.get("min_cluster_size", 2)
    
    # Initialize group service
    group_service = GroupService(settings)
    
    try:
        # Get clustering results
        clustering_result = group_service.get_similarity_clusters(
            run_id=selected_run_id,
            threshold=threshold,
            policy=policy,
            min_cluster_size=min_cluster_size,
        )
        
        # Load account names from review data to display proper names instead of IDs
        try:
            from src.utils.artifact_management import get_artifact_paths
            artifact_paths = get_artifact_paths(selected_run_id)
            review_path = artifact_paths.get("review_ready_parquet")
            
            if review_path and os.path.exists(review_path):
                import pandas as pd
                review_df = pd.read_parquet(review_path)
                # Create a mapping from account_id to account_name
                account_name_map = dict(zip(review_df['account_id'], review_df['account_name']))
            else:
                account_name_map = {}
        except Exception as e:
            logger.warning(f"Could not load account names: {e}")
            account_name_map = {}
        
        # Convert clusters to group-like format for compatibility
        clusters_data = []
        for i, cluster in enumerate(clustering_result.clusters):
            # Get the first member's account name, fallback to ID if not found
            first_member_id = cluster.members[0] if cluster.members else "Unknown"
            primary_name = account_name_map.get(first_member_id, first_member_id)
            
            cluster_info = {
                "group_id": f"cluster_{cluster.id}",
                "group_size": cluster.size,
                "primary_name": primary_name,
                "max_score": cluster.min_pairwise_sim * 100,  # Convert back to [0,100]
                "disposition": "Unknown",  # Clusters don't have dispositions yet
                "cluster_id": cluster.id,
                "members": cluster.members,
                "min_pairwise_sim": cluster.min_pairwise_sim,
            }
            clusters_data.append(cluster_info)
        
        # Add outliers as a special "cluster"
        if clustering_result.outliers:
            outliers_info = {
                "group_id": "outliers",
                "group_size": len(clustering_result.outliers),
                "primary_name": "Outliers",
                "max_score": 0.0,
                "disposition": "Unknown",
                "cluster_id": -1,
                "members": clustering_result.outliers,
                "min_pairwise_sim": 0.0,
            }
            clusters_data.append(outliers_info)
        
        # Apply sorting
        if sort_by == "Group Size (Desc)":
            clusters_data.sort(key=lambda x: x["group_size"], reverse=True)
        elif sort_by == "Group Size (Asc)":
            clusters_data.sort(key=lambda x: x["group_size"])
        elif sort_by == "Similarity Score (Desc)":
            clusters_data.sort(key=lambda x: x["max_score"], reverse=True)
        elif sort_by == "Similarity Score (Asc)":
            clusters_data.sort(key=lambda x: x["max_score"])
        elif sort_by == "Account Name (Asc)":
            clusters_data.sort(key=lambda x: x["primary_name"])
        elif sort_by == "Account Name (Desc)":
            clusters_data.sort(key=lambda x: x["primary_name"], reverse=True)
        
        # Apply pagination
        total_clusters = len(clusters_data)
        max_page = max(1, (total_clusters + page_size - 1) // page_size)
        
        start_idx = (page - 1) * page_size
        end_idx = min(start_idx + page_size, total_clusters)
        page_clusters = clusters_data[start_idx:end_idx]
        
        return page_clusters, total_clusters, max_page
        
    except Exception as e:
        st.error(f"Error loading similarity clusters: {e}")
        return [], 0, 1


def render_group_list(
    selected_run_id: str,
    sort_by: str,
    page: int,
    page_size: int,
    filters: dict[str, Any],
) -> tuple[list[dict[str, Any]], int, int]:
    """Render the paginated group list with fragments.

    Args:
        selected_run_id: The selected run ID
        sort_by: The sort key
        page: The current page number
        page_size: The page size
        filters: The filters dictionary

    Returns:
        Tuple of (page_groups, total_groups, max_page)

    """
    # Get backend state
    backend_state = get_backend_state(st.session_state)
    # backend = backend_state.groups.get(selected_run_id, "pyarrow")  # Not used in current implementation

    # Get total groups count
    total_groups = get_total_groups_count(selected_run_id, filters)
    max_page = max(1, (total_groups + page_size - 1) // page_size)

    # Ensure page is within bounds
    page_state = get_page_state(st.session_state)
    if page_state.number > max_page:
        page_state.number = max_page
        set_page_state(st.session_state, page_state)
    elif page_state.number < 1:
        page_state.number = 1
        set_page_state(st.session_state, page_state)

    # Build cache key for this specific page request (for logging/debugging)
    _ = build_cache_key(
        selected_run_id,
        sort_by,
        page_state.number,
        page_size,
        filters,
        backend_state.groups.get(selected_run_id, "pyarrow"),
    )

    try:
        page_groups, actual_total = get_groups_page(
            selected_run_id,
            sort_by,
            page_state.number,
            page_size,
            filters,
        )

        # Update total if different (should be the same, but handle edge cases)
        if actual_total != total_groups:
            total_groups = actual_total
            max_page = max(1, (total_groups + page_size - 1) // page_size)

    except PageFetchTimeout:
        # Handle timeout specifically
        st.error("⚠️ Page load timed out after 30 seconds. The dataset is very large.")

        # Offer user options
        col1, col2 = st.columns(2)
        with col1:
            if st.button("🔄 Try Again", key=f"try_again_{selected_run_id}"):
                st.rerun()
        with col2:
            if st.button(
                "📉 Reduce Page Size to 50",
                key=f"reduce_page_size_{selected_run_id}",
            ):
                page_state.size = 50
                set_page_state(st.session_state, page_state)
                st.rerun()

        st.info(
            "💡 **Tip:** For large datasets, try reducing the page size or applying filters to reduce the data load.",
        )
        return [], 0, 1

    except Exception as e:
        # Fallback to current behavior if pagination fails
        st.warning(f"Pagination failed, falling back to current behavior: {e}")
        st.error(
            f'Groups pagination fallback | run_id={selected_run_id} error="{e!s}"',
        )
        return [], 0, 1

    # Page navigation
    col1, col2, col3 = st.columns(3)
    with col1:
        if (
            st.button("◀ Prev", key=f"prev_page_{selected_run_id}")
            and page_state.number > 1
        ):
            page_state.number -= 1
            set_page_state(st.session_state, page_state)
    with col2:
        st.write(f"Page {page_state.number} / {max_page}")
    with col3:
        if (
            st.button("Next ▶", key=f"next_page_{selected_run_id}")
            and page_state.number < max_page
        ):
            page_state.number += 1
            set_page_state(st.session_state, page_state)

    # Show pagination caption
    start_idx = (page_state.number - 1) * page_size
    end_idx = min(start_idx + page_size, total_groups)
    st.caption(f"Showing {start_idx + 1}–{end_idx} of {total_groups} groups")

    # Phase 1.26.2: Cache management moved to maintenance sidebar for better organization
    # Cache clearing is now handled in the maintenance component with helpful tooltips

    # Phase 1.22.1: Show performance indicator when using fast path
    try:
        import os

        from src.utils.artifact_management import get_artifact_paths

        artifact_paths = get_artifact_paths(selected_run_id)
        group_stats_path = artifact_paths.get("group_stats_parquet")

        if group_stats_path and os.path.exists(group_stats_path):
            # Show active threshold in the banner
            active_threshold = int(filters.get("min_edge_strength", 0) or 0)
            st.success(
                f"⚡ **Fast stats mode**: Using pre-computed group statistics for instant loading · "
                f"Similarity ≥ {active_threshold:.0f}%",
                icon="⚡",
            )
        else:
            st.info("📊 **Standard mode**: Computing group statistics on-demand")
    except Exception:
        pass  # Don't break the UI if this fails

    return page_groups, total_groups, max_page


@fragment
def render_group_list_fragment(
    selected_run_id: str,
    sort_by: str,
    page: int,
    page_size: int,
    filters: dict[str, Any],
    settings: dict[str, Any] = None,
) -> None:
    """Render the group list within a fragment to prevent page-wide blocking.

    Args:
        selected_run_id: The selected run ID
        sort_by: The sort key
        page: The current page number
        page_size: The page size
        filters: The filters dictionary

    """
    # Show view mode help
    view_mode = filters.get("view_mode", "Edge Groups")
    if view_mode == "Similarity Clusters":
        with st.expander("ℹ️ About Similarity Clusters", expanded=False):
            st.markdown("""
            **Similarity Clusters** use mathematical clustering algorithms to group records based on pairwise similarity scores.
            
            **Clustering Policies:**
            - **Complete-linkage (strict)**: Every pair of records in a cluster must have similarity ≥ threshold
            - **Single-linkage (looser)**: Any connection path above threshold is sufficient (can create chains)
            
            **Key Differences from Edge Groups:**
            - **Symmetric**: No "primary" record bias - clusters are formed based on all pairwise similarities
            - **Configurable**: Adjust similarity threshold and clustering policy
            - **Mathematically sound**: Uses established clustering algorithms
            - **Outliers**: Records that don't meet clustering criteria are shown separately
            
            **Tips:**
            - Start with Complete-linkage for strict grouping
            - Use Single-linkage to explore looser connections
            - Adjust Min Cluster Size to filter out small clusters
            - Lower similarity thresholds will create larger, more inclusive clusters
            """)
    
    # Add CSS for larger, more readable expander headers
    st.markdown(
        """
        <style>
        .streamlit-expanderHeader {
            font-size: 1.6rem !important;
            font-weight: 600 !important;
            line-height: 1.6 !important;
            padding: 1.25rem 1.5rem !important;
            margin-bottom: 1rem !important;
        }
        .streamlit-expanderContent {
            padding: 1.5rem !important;
        }
        /* Make the expander header text more readable */
        .streamlit-expanderHeader p {
            font-size: 1.6rem !important;
            font-weight: 600 !important;
            margin: 0 !important;
            line-height: 1.6 !important;
        }
        /* Ensure the markdown container text is large enough */
        .st-emotion-cache-gx6i9d p {
            font-size: 1.6rem !important;
            font-weight: 600 !important;
            line-height: 1.6 !important;
        }
        /* Remove horizontal dividers between groups */
        .stDivider {
            display: none !important;
        }
        /* Add more spacing between expanders instead */
        .streamlit-expander {
            margin-bottom: 1.5rem !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    # Duplicate render guard for the list
    key = f"group_list_rendered:{selected_run_id}:{page}:{sort_by}"
    if not st.session_state.get(key):
        st.session_state[key] = True

    # Determine view mode and render accordingly
    view_mode = filters.get("view_mode", "Edge Groups")
    
    if view_mode == "Similarity Clusters":
        # Render similarity clusters
        page_groups, total_groups, max_page = render_similarity_clusters(
            selected_run_id,
            sort_by,
            page,
            page_size,
            filters,
            settings or {},
        )
    else:
        # Render traditional edge groups
        page_groups, total_groups, max_page = render_group_list(
            selected_run_id,
            sort_by,
            page,
            page_size,
            filters,
        )

    # Group by group_id and display each group
    for i, group_info in enumerate(page_groups):
        group_id = group_info["group_id"]
        group_size = group_info["group_size"]
        primary_name = group_info["primary_name"]

        # Extract additional fields for better decision making
        max_score = group_info.get("max_score", 0.0)
        disposition = group_info.get("disposition", "Unknown")

        # Create a more informative expander title with key fields
        primary_name_display = primary_name or "Unknown"

        # Dynamic font scaling based on similarity score (0.9rem to 1.6rem)
        similarity_score = float(max_score or 0)
        font_size = (
            0.9 + (similarity_score / 100.0) * 0.7
        )  # 0.9rem at 0%, 1.6rem at 100%
        font_size = max(0.9, min(1.6, font_size))  # Clamp between 0.9 and 1.6

        # Create expander title based on view mode
        if view_mode == "Similarity Clusters":
            if group_id == "outliers":
                expander_title = f"Outliers · {group_size} records"
            else:
                # For clusters, show min pairwise similarity
                min_sim = group_info.get("min_pairwise_sim", 0.0)
                threshold = filters.get("similarity_threshold", 92)
                expander_title = (
                    f"Cluster {group_info.get('cluster_id', '?')} · {primary_name_display} · {group_size} records"
                )
                if min_sim > 0:
                    expander_title += f" · Min Pairwise ≥ {int(round(min_sim * 100))}%"
        else:
            # Traditional edge groups
            expander_title = (
                f"Group {group_id} · {primary_name_display} · {group_size} records"
            )
            if max_score is not None and max_score > 0:
                # Show max score but clarify it's the highest edge score in the group
                expander_title += f" · Max Edge {int(round(max_score))}%"
            if disposition and disposition != "Unknown":
                expander_title += f" · {disposition}"

        with st.expander(expander_title, expanded=False):
            # Add dynamic font scaling for the primary name inside the expander
            st.markdown(
                f"<div style='font-size:{font_size:.1f}rem; font-weight:700; margin-bottom:0.5rem;'>{primary_name_display}</div>",
                unsafe_allow_html=True,
            )
            # Show active threshold for this group
            active_threshold = int(filters.get("min_edge_strength", 0) or 0)
            st.caption(
                f"Showing groups with Similarity Score ≥ {active_threshold:.0f}%",
            )

            # Display key group information for quick review
            col1, col2, col3 = st.columns(3)
            with col1:
                if view_mode == "Similarity Clusters":
                    st.metric("Cluster Size", group_size)
                else:
                    st.metric("Group Size", group_size)
            with col2:
                if view_mode == "Similarity Clusters":
                    if group_id == "outliers":
                        st.metric("Min Pairwise", "N/A", help="Outliers don't meet clustering criteria")
                    else:
                        min_sim = group_info.get("min_pairwise_sim", 0.0)
                        if min_sim > 0:
                            st.metric("Min Pairwise", f"{min_sim * 100:.1f}%", help="Minimum similarity between any two records in this cluster")
                        else:
                            st.metric("Min Pairwise", "N/A")
                else:
                    if max_score > 0:
                        # Show max score with better context
                        st.metric("Max Edge Score", f"{max_score:.1f}%", help="Highest similarity score between any two records in this group")
                        # Add a note about score distribution if we have more info
                        if group_size > 2:
                            st.caption("(scores vary within group)")
                    else:
                        st.metric("Max Edge Score", "N/A")
            with col3:
                if view_mode == "Similarity Clusters":
                    if group_id == "outliers":
                        st.info("🔍 Outliers")
                    else:
                        policy = filters.get("clustering_policy", "complete")
                        policy_display = "Complete-linkage" if policy == "complete" else "Single-linkage"
                        st.info(f"📊 {policy_display}")
                else:
                    if disposition and disposition != "Unknown":
                        # Color-code dispositions for quick visual identification
                        if disposition == "Keep":
                            st.success(f"✅ {disposition}")
                        elif disposition == "Update":
                            st.warning(f"⚠️ {disposition}")
                        elif disposition == "Delete":
                            st.error(f"🗑️ {disposition}")
                        elif disposition == "Verify":
                            st.info(f"🔍 {disposition}")
                        else:
                            st.write(f"📋 {disposition}")
                    else:
                        st.write("📋 No disposition")

            # Render group details
            if view_mode == "Similarity Clusters":
                # For clusters, render cluster-specific details
                _render_cluster_details(group_info, selected_run_id)
            else:
                # For traditional edge groups, use the existing group details
                from app.components.group_details import render_group_details

                render_group_details(
                    selected_run_id,
                    group_id,
                    group_size,
                    primary_name,
                    expander_title,
                    create_expander=False,
                )

        # Visual separators removed - using CSS margin instead for cleaner spacing
