"""
UI session state management for ui_helpers refactor.

This module provides a session state adapter with dict fallback for tests.
"""

from typing import Any, Optional, Dict
# TODO: Import from opt_deps when implemented
# from .opt_deps import STREAMLIT

class SessionState:
    """Session state adapter with dict fallback for tests."""
    def __init__(self, use_streamlit: bool = True):
        # TODO: Implement actual streamlit detection
        self._use_streamlit = use_streamlit  # and STREAMLIT is not None
        self._fallback: Dict[str, Any] = {}

    def get(self, key: str, default: Any = None) -> Any:
        """Get value from session state."""
        if self._use_streamlit:
            try:
                import streamlit as st
                return st.session_state.get(key, default)
            except ImportError:
                pass
        return self._fallback.get(key, default)

    def set(self, key: str, value: Any) -> None:
        """Set value in session state."""
        if self._use_streamlit:
            try:
                import streamlit as st
                st.session_state[key] = value
                return
            except ImportError:
                pass
        self._fallback[key] = value

    def set_backend_choice(self, run_id: str, backend: str) -> None:
        """Set backend choice for a specific run."""
        key = f"groups.backend:{run_id}"
        self.set(key, backend)

# Global instance - configurable for tests
session = SessionState()

# Constants
BACKEND_KEY = "cj.backend.groups"
