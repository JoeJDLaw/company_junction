"""Fragment API utilities for Phase 1.18.3.

Provides a unified fragment decorator that automatically chooses between
st.fragment (≥ 1.29) and st.experimental_fragment (< 1.29).
"""

import streamlit as st
from packaging.version import Version

from src.utils.logging_utils import get_logger

logger = get_logger(__name__)

# Detect which fragment API to use
_USE_STABLE_FRAGMENT = Version(st.__version__) >= Version("1.29.0")

# Choose fragment decorator based on version
if _USE_STABLE_FRAGMENT:
    fragment = st.fragment
else:
    # Fallback to experimental_fragment if available, otherwise use fragment
    try:
        fragment = st.experimental_fragment  # type: ignore[attr-defined]
    except AttributeError:
        # If experimental_fragment doesn't exist, use fragment
        fragment = st.fragment

# Log the choice once at module import
logger.info(
    f"Using fragment API: {'st.fragment' if _USE_STABLE_FRAGMENT else 'st.experimental_fragment'} | streamlit={st.__version__}",
)
