"""
Export component for Phase 1.18.1 refactor.

This module handles data export functionality.
"""

import streamlit as st
import pandas as pd


def render_export(filtered_df: pd.DataFrame) -> None:
    """Render export functionality."""
    st.subheader("Export")

    if st.button("Export Filtered Data", key="export_filtered_data"):
        # Convert DataFrame to CSV
        csv = filtered_df.to_csv(index=False)

        # Create download button
        st.download_button(
            label="Download CSV",
            data=csv,
            file_name="filtered_groups.csv",
            mime="text/csv",
            key="download_csv",
        )

    # Additional export options
    st.write("**Export Options:**")
    st.info("Additional export formats will be implemented in a future phase.")
