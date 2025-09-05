"""
Export component for Phase 1.18.1 refactor.

This module handles data export functionality.
"""

import streamlit as st
import pandas as pd
from src.utils.schema_utils import to_display


def render_export(filtered_df: pd.DataFrame, similarity_threshold: int = 100) -> None:
    """Render export functionality with similarity threshold parity."""
    st.subheader("Export")

    # Phase 1.35.2: Export parity with similarity threshold
    if similarity_threshold < 100:
        st.info(f"📊 **Export Parity**: CSV will contain only groups with edge strength ≥ {similarity_threshold}% (same as current view)")
    else:
        st.info("📊 **Export Parity**: CSV will contain all groups (exact matches only)")

    if st.button("Export Filtered Data", key="export_filtered_data"):
        # Apply display labels for user-friendly CSV export
        df_display = to_display(filtered_df)
        
        # Convert DataFrame to CSV
        csv = df_display.to_csv(index=False)

        # Create filename with similarity threshold for clarity
        if similarity_threshold < 100:
            filename = f"filtered_groups_threshold_{similarity_threshold}.csv"
        else:
            filename = "filtered_groups_exact_only.csv"

        # Create download button
        st.download_button(
            label="Download CSV",
            data=csv,
            file_name=filename,
            mime="text/csv",
            key="download_csv",
        )

    # Additional export options
    st.write("**Export Options:**")
    st.info("Additional export formats will be implemented in a future phase.")
