"""
Session state utilities for Phase 1.18.1 refactor.

This module provides typed helpers for managing namespaced session state keys
without importing Streamlit directly. It accepts dict-like objects for flexibility.
"""

from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field


@dataclass
class PageState:
    """Page state for pagination controls."""

    number: int = 1
    size: int = 50


@dataclass
class BackendState:
    """Backend selection state per run."""

    groups: Dict[str, str] = field(default_factory=dict)


@dataclass
class DetailsState:
    """Group details state."""

    requested: Dict[Tuple[str, str], bool] = field(
        default_factory=dict
    )  # (run_id, group_id) -> bool
    loaded: Dict[Tuple[str, str], bool] = field(
        default_factory=dict
    )  # (run_id, group_id) -> bool
    data: Dict[Tuple[str, str], Dict[str, Any]] = field(
        default_factory=dict
    )  # (run_id, group_id) -> data


@dataclass
class ExplainState:
    """Explain metadata state."""

    requested: Dict[Tuple[str, str], bool] = field(
        default_factory=dict
    )  # (run_id, group_id) -> bool
    data: Dict[Tuple[str, str], Any] = field(
        default_factory=dict
    )  # (run_id, group_id) -> data


@dataclass
class AliasesState:
    """Alias cross-references state."""

    requested: Dict[Tuple[str, str], bool] = field(
        default_factory=dict
    )  # (run_id, group_id) -> bool
    data: Dict[Tuple[str, str], List[Dict[str, Any]]] = field(
        default_factory=dict
    )  # (run_id, group_id) -> data


@dataclass
class FiltersState:
    """Filter state."""

    signature: str = ""


@dataclass
class CacheState:
    """Cache management state."""

    clear_requested_for_run_id: Optional[str] = None


def get_page_state(state: Any) -> PageState:
    """Get page state from session state."""
    return PageState(
        number=state.get("cj.page.number", 1), size=state.get("cj.page.size", 50)
    )


def set_page_state(state: Any, page_state: PageState) -> None:
    """Set page state in session state."""
    state["cj.page.number"] = page_state.number
    state["cj.page.size"] = page_state.size


def get_backend_state(state: Any) -> BackendState:
    """Get backend state from session state."""
    return BackendState(groups=state.get("cj.backend.groups", {}))


def set_backend_state(state: Any, backend_state: BackendState) -> None:
    """Set backend state in session state."""
    state["cj.backend.groups"] = backend_state.groups


def get_details_state(state: Any) -> DetailsState:
    """Get details state from session state."""
    return DetailsState(
        requested=state.get("cj.details.requested", {}),
        data=state.get("cj.details.data", {}),
    )


def set_details_state(state: Any, details_state: DetailsState) -> None:
    """Set details state in session state."""
    state["cj.details.requested"] = details_state.requested
    state["cj.details.data"] = details_state.data


def get_explain_state(state: Any) -> ExplainState:
    """Get explain state from session state."""
    return ExplainState(
        requested=state.get("cj.explain.requested", {}),
        data=state.get("cj.explain.data", {}),
    )


def set_explain_state(state: Any, explain_state: ExplainState) -> None:
    """Set explain state in session state."""
    state["cj.explain.requested"] = explain_state.requested
    state["cj.explain.data"] = explain_state.data


def get_aliases_state(state: Any) -> AliasesState:
    """Get aliases state from session state."""
    return AliasesState(
        requested=state.get("cj.aliases.requested", {}),
        data=state.get("cj.aliases.data", {}),
    )


def set_aliases_state(state: Any, aliases_state: AliasesState) -> None:
    """Set aliases state in session state."""
    state["cj.aliases.requested"] = aliases_state.requested
    state["cj.aliases.data"] = aliases_state.data


def get_filters_state(state: Any) -> FiltersState:
    """Get filters state from session state."""
    return FiltersState(signature=state.get("cj.filters.signature", ""))


def set_filters_state(state: Any, filters_state: FiltersState) -> None:
    """Set filters state in session state."""
    state["cj.filters.signature"] = filters_state.signature


def get_cache_state(state: Any) -> CacheState:
    """Get cache state from session state."""
    return CacheState(
        clear_requested_for_run_id=state.get("cj.cache.clear_requested_for_run_id")
    )


def set_cache_state(state: Any, cache_state: CacheState) -> None:
    """Set cache state in session state."""
    state["cj.cache.clear_requested_for_run_id"] = (
        cache_state.clear_requested_for_run_id
    )


# Legacy key migration helpers
def migrate_legacy_keys(state: Any) -> None:
    """Migrate legacy session state keys to namespaced versions."""
    # Migrate page state
    if "page" in state and "cj.page.number" not in state:
        state["cj.page.number"] = state["page"]
    if "groups_backend" in state and "cj.backend.groups" not in state:
        state["cj.backend.groups"] = state["groups_backend"]
    if "previous_filters_key" in state and "cj.filters.signature" not in state:
        state["cj.filters.signature"] = state["previous_filters_key"]

    # Migrate backend state (one-time migration)
    if "groups_backend" in state:
        # Copy legacy backend state to namespaced version
        if "cj.backend.groups" not in state:
            state["cj.backend.groups"] = state["groups_backend"]
        # Remove legacy key after migration
        del state["groups_backend"]


def clear_legacy_keys(state: Any) -> None:
    """Clear legacy session state keys after migration."""
    legacy_keys = [
        "page",
        "groups_backend",
        "previous_filters_key",
        "group_details_loaded_",
        "group_details_",
        "explain_metadata_",
        "explain_loaded_",
        "alias_cross_refs_",
        "alias_loaded_",
        "group_list_rendered:",
    ]
    keys_to_remove = []
    for key in state.keys():
        for legacy_key in legacy_keys:
            if key.startswith(legacy_key):
                keys_to_remove.append(key)
                break
    for key in keys_to_remove:
        del state[key]
