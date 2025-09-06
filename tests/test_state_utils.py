"""Tests for src.utils.state_utils module.

This module tests the session state utilities for Phase 1.18.1 refactor.
"""

from typing import Any

from src.utils.state_utils import (
    AliasesState,
    BackendState,
    CacheState,
    DetailsState,
    ExplainState,
    FiltersState,
    PageState,
    clear_legacy_keys,
    get_aliases_state,
    get_backend_state,
    get_cache_state,
    get_details_state,
    get_explain_state,
    get_filters_state,
    get_page_state,
    migrate_legacy_keys,
    set_aliases_state,
    set_backend_state,
    set_cache_state,
    set_details_state,
    set_explain_state,
    set_filters_state,
    set_page_state,
)


def test_page_state_defaults() -> None:
    """Test PageState dataclass defaults."""
    state = PageState()
    assert state.number == 1
    assert state.size == 50


def test_backend_state_defaults() -> None:
    """Test BackendState dataclass defaults."""
    state = BackendState()
    assert state.groups == {}


def test_details_state_defaults() -> None:
    """Test DetailsState dataclass defaults."""
    state = DetailsState()
    assert state.requested == {}
    assert state.data == {}


def test_explain_state_defaults() -> None:
    """Test ExplainState dataclass defaults."""
    state = ExplainState()
    assert state.requested == {}
    assert state.data == {}


def test_aliases_state_defaults() -> None:
    """Test AliasesState dataclass defaults."""
    state = AliasesState()
    assert state.requested == {}
    assert state.data == {}


def test_filters_state_defaults() -> None:
    """Test FiltersState dataclass defaults."""
    state = FiltersState()
    assert state.signature == ""


def test_cache_state_defaults() -> None:
    """Test CacheState dataclass defaults."""
    state = CacheState()
    assert state.clear_requested_for_run_id is None


def test_get_set_page_state() -> None:
    """Test get_page_state and set_page_state functions."""
    session_state: dict[str, Any] = {}

    # Test defaults
    page_state = get_page_state(session_state)
    assert page_state.number == 1
    assert page_state.size == 50

    # Test setting values
    new_state = PageState(number=5, size=100)
    set_page_state(session_state, new_state)
    assert session_state["cj.page.number"] == 5
    assert session_state["cj.page.size"] == 100


def test_get_set_backend_state() -> None:
    """Test get_backend_state and set_backend_state functions."""
    session_state: dict[str, Any] = {}

    # Test defaults
    backend_state = get_backend_state(session_state)
    assert backend_state.groups == {}

    # Test setting values
    new_state = BackendState(groups={"run1": "duckdb"})
    set_backend_state(session_state, new_state)
    assert session_state["cj.backend.groups"] == {"run1": "duckdb"}


def test_get_set_details_state() -> None:
    """Test get_details_state and set_details_state functions."""
    session_state: dict[str, Any] = {}

    # Test defaults
    details_state = get_details_state(session_state)
    assert details_state.requested == {}
    assert details_state.data == {}

    # Test setting values
    new_state = DetailsState(
        requested={("run1", "group1"): True},
        data={("run1", "group1"): {"test": "data"}},
    )
    set_details_state(session_state, new_state)
    assert session_state["cj.details.requested"] == {("run1", "group1"): True}
    assert session_state["cj.details.data"] == {("run1", "group1"): {"test": "data"}}


def test_get_set_explain_state() -> None:
    """Test get_explain_state and set_explain_state functions."""
    session_state: dict[str, Any] = {}

    # Test defaults
    explain_state = get_explain_state(session_state)
    assert explain_state.requested == {}
    assert explain_state.data == {}

    # Test setting values
    new_state = ExplainState(
        requested={("run1", "group1"): True},
        data={("run1", "group1"): [{"test": "data"}]},
    )
    set_explain_state(session_state, new_state)
    assert session_state["cj.explain.requested"] == {("run1", "group1"): True}
    assert session_state["cj.explain.data"] == {("run1", "group1"): [{"test": "data"}]}


def test_get_set_aliases_state() -> None:
    """Test get_aliases_state and set_aliases_state functions."""
    session_state: dict[str, Any] = {}

    # Test defaults
    aliases_state = get_aliases_state(session_state)
    assert aliases_state.requested == {}
    assert aliases_state.data == {}

    # Test setting values
    new_state = AliasesState(
        requested={("run1", "group1"): True},
        data={("run1", "group1"): [{"test": "data"}]},
    )
    set_aliases_state(session_state, new_state)
    assert session_state["cj.aliases.requested"] == {("run1", "group1"): True}
    assert session_state["cj.aliases.data"] == {("run1", "group1"): [{"test": "data"}]}


def test_get_set_filters_state() -> None:
    """Test get_filters_state and set_filters_state functions."""
    session_state: dict[str, Any] = {}

    # Test defaults
    filters_state = get_filters_state(session_state)
    assert filters_state.signature == ""

    # Test setting values
    new_state = FiltersState(signature="test_signature")
    set_filters_state(session_state, new_state)
    assert session_state["cj.filters.signature"] == "test_signature"


def test_get_set_cache_state() -> None:
    """Test get_cache_state and set_cache_state functions."""
    session_state: dict[str, Any] = {}

    # Test defaults
    cache_state = get_cache_state(session_state)
    assert cache_state.clear_requested_for_run_id is None

    # Test setting values
    new_state = CacheState(clear_requested_for_run_id="run1")
    set_cache_state(session_state, new_state)
    assert session_state["cj.cache.clear_requested_for_run_id"] == "run1"


def test_migrate_legacy_keys() -> None:
    """Test migrate_legacy_keys function."""
    session_state: dict[str, Any] = {
        "page": 5,
        "groups_backend": {"run1": "duckdb"},
        "previous_filters_key": "old_signature",
    }

    migrate_legacy_keys(session_state)

    assert session_state["cj.page.number"] == 5
    assert session_state["cj.backend.groups"] == {"run1": "duckdb"}
    assert session_state["cj.filters.signature"] == "old_signature"


def test_clear_legacy_keys() -> None:
    """Test clear_legacy_keys function."""
    session_state: dict[str, Any] = {
        "page": 5,
        "groups_backend": {"run1": "duckdb"},
        "previous_filters_key": "old_signature",
        "group_details_loaded_group1": True,
        "explain_metadata_group1": {"test": "data"},
        "alias_cross_refs_group1": [{"test": "data"}],
        "group_list_rendered:run1": True,
        "valid_key": "should_remain",
    }

    clear_legacy_keys(session_state)

    # Legacy keys should be removed
    assert "page" not in session_state
    assert "groups_backend" not in session_state
    assert "previous_filters_key" not in session_state
    assert "group_details_loaded_group1" not in session_state
    assert "explain_metadata_group1" not in session_state
    assert "alias_cross_refs_group1" not in session_state
    assert "group_list_rendered:run1" not in session_state

    # Valid keys should remain
    assert "valid_key" in session_state
    assert session_state["valid_key"] == "should_remain"
