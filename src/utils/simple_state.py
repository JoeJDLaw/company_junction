"""
Simplified session state management for Phase 1.38.1 UI cleanup.

This module provides a unified, simplified approach to session state management
that consolidates the multiple state classes into a single, easy-to-use interface.
"""

from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field


@dataclass
class AppState:
    """
    Unified application state that consolidates all UI state management.
    
    This replaces the multiple separate state classes (PageState, BackendState, 
    DetailsState, etc.) with a single, cohesive state object.
    """
    
    # Pagination state
    page_number: int = 1
    page_size: int = 50
    
    # Backend selection per run
    backend_choices: Dict[str, str] = field(default_factory=dict)  # run_id -> backend
    
    # Group details state (lazy loading)
    details_requested: Dict[Tuple[str, str], bool] = field(default_factory=dict)  # (run_id, group_id) -> bool
    details_loaded: Dict[Tuple[str, str], bool] = field(default_factory=dict)    # (run_id, group_id) -> bool
    details_data: Dict[Tuple[str, str], List[Dict[str, Any]]] = field(default_factory=dict)  # (run_id, group_id) -> data
    
    # Alias cross-references state
    aliases_requested: Dict[Tuple[str, str], bool] = field(default_factory=dict)  # (run_id, group_id) -> bool
    aliases_data: Dict[Tuple[str, str], List[Dict[str, Any]]] = field(default_factory=dict)  # (run_id, group_id) -> data
    
    # Filter state
    filter_signature: str = ""
    
    # Cache management
    cache_clear_requested_for_run: Optional[str] = None
    
    # UI-specific state
    similarity_threshold: float = 100.0
    previous_sort_key: str = "Group Size (Desc)"


def get_app_state(session_state: Any) -> AppState:
    """
    Get the unified application state from session state.
    
    Args:
        session_state: Streamlit session state object
        
    Returns:
        AppState object with all current state
    """
    return AppState(
        # Pagination
        page_number=session_state.get("cj.page.number", 1),
        page_size=session_state.get("cj.page.size", 50),
        
        # Backend choices
        backend_choices=session_state.get("cj.backend.groups", {}),
        
        # Details state
        details_requested=session_state.get("cj.details.requested", {}),
        details_loaded=session_state.get("cj.details.loaded", {}),
        details_data=session_state.get("cj.details.data", {}),
        
        # Aliases state
        aliases_requested=session_state.get("cj.aliases.requested", {}),
        aliases_data=session_state.get("cj.aliases.data", {}),
        
        # Filter state
        filter_signature=session_state.get("cj.filters.signature", ""),
        
        # Cache state
        cache_clear_requested_for_run=session_state.get("cj.cache.clear_requested_for_run_id", None),
        
        # UI state
        similarity_threshold=session_state.get("cj.ui.similarity_threshold", 100.0),
        previous_sort_key=session_state.get("cj.ui.previous_sort_key", "Group Size (Desc)"),
    )


def set_app_state(session_state: Any, app_state: AppState) -> None:
    """
    Set the unified application state in session state.
    
    Args:
        session_state: Streamlit session state object
        app_state: AppState object to store
    """
    # Pagination
    session_state["cj.page.number"] = app_state.page_number
    session_state["cj.page.size"] = app_state.page_size
    
    # Backend choices
    session_state["cj.backend.groups"] = app_state.backend_choices
    
    # Details state
    session_state["cj.details.requested"] = app_state.details_requested
    session_state["cj.details.loaded"] = app_state.details_loaded
    session_state["cj.details.data"] = app_state.details_data
    
    # Aliases state
    session_state["cj.aliases.requested"] = app_state.aliases_requested
    session_state["cj.aliases.data"] = app_state.aliases_data
    
    # Filter state
    session_state["cj.filters.signature"] = app_state.filter_signature
    
    # Cache state
    session_state["cj.cache.clear_requested_for_run_id"] = app_state.cache_clear_requested_for_run
    
    # UI state
    session_state["cj.ui.similarity_threshold"] = app_state.similarity_threshold
    session_state["cj.ui.previous_sort_key"] = app_state.previous_sort_key


def clear_app_state(session_state: Any) -> None:
    """
    Clear all application state from session state.
    
    Args:
        session_state: Streamlit session state object
    """
    # Clear all keys that start with "cj."
    keys_to_remove = [key for key in session_state.keys() if key.startswith("cj.")]
    for key in keys_to_remove:
        del session_state[key]


# Convenience functions for common operations
def get_backend_for_run(app_state: AppState, run_id: str, default: str = "pyarrow") -> str:
    """Get the backend choice for a specific run."""
    return app_state.backend_choices.get(run_id, default)


def set_backend_for_run(app_state: AppState, run_id: str, backend: str) -> None:
    """Set the backend choice for a specific run."""
    app_state.backend_choices[run_id] = backend


def is_details_requested(app_state: AppState, run_id: str, group_id: str) -> bool:
    """Check if details have been requested for a group."""
    return app_state.details_requested.get((run_id, group_id), False)


def set_details_requested(app_state: AppState, run_id: str, group_id: str, requested: bool = True) -> None:
    """Mark details as requested for a group."""
    app_state.details_requested[(run_id, group_id)] = requested


def is_details_loaded(app_state: AppState, run_id: str, group_id: str) -> bool:
    """Check if details have been loaded for a group."""
    return app_state.details_loaded.get((run_id, group_id), False)


def set_details_loaded(app_state: AppState, run_id: str, group_id: str, data: List[Dict[str, Any]]) -> None:
    """Mark details as loaded and store the data."""
    key = (run_id, group_id)
    app_state.details_loaded[key] = True
    app_state.details_data[key] = data


def get_details_data(app_state: AppState, run_id: str, group_id: str) -> Optional[List[Dict[str, Any]]]:
    """Get the loaded details data for a group."""
    return app_state.details_data.get((run_id, group_id))


def reset_page_to_one(app_state: AppState) -> None:
    """Reset pagination to page 1."""
    app_state.page_number = 1


def update_page_size(app_state: AppState, new_size: int) -> None:
    """Update the page size and reset to page 1."""
    app_state.page_size = new_size
    app_state.page_number = 1
