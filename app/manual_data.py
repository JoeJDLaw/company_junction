"""
Manual data management for Phase 1.7.

This module handles manual overrides and blacklist management
with JSON persistence and audit trails.
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any
import streamlit as st
from src.utils.schema_utils import ACCOUNT_ID, ACCOUNT_NAME


def ensure_manual_directory() -> Path:
    """Ensure the manual data directory exists."""
    manual_dir = Path("data/manual")
    manual_dir.mkdir(parents=True, exist_ok=True)
    return manual_dir


def load_manual_dispositions() -> List[Dict[str, Any]]:
    """Load manual disposition overrides from JSON file."""
    manual_dir = ensure_manual_directory()
    file_path = manual_dir / "manual_dispositions.json"

    if not file_path.exists():
        return []

    try:
        with open(file_path, "r") as f:
            data = json.load(f)
            if isinstance(data, list):
                return data
            else:
                st.error("Invalid manual dispositions format")
                return []
    except Exception as e:
        st.error(f"Error loading manual dispositions: {e}")
        return []


def save_manual_dispositions(dispositions: List[Dict[str, Any]]) -> bool:
    """Save manual disposition overrides to JSON file."""
    manual_dir = ensure_manual_directory()
    file_path = manual_dir / "manual_dispositions.json"

    try:
        with open(file_path, "w") as f:
            json.dump(dispositions, f, indent=2)
        return True
    except Exception as e:
        st.error(f"Error saving manual dispositions: {e}")
        return False


def add_manual_disposition(
    record_id: str,
    account_id: str,
    account_name: str,
    name_core: str,
    override: str,
    reason: str = "",
) -> bool:
    """Add a manual disposition override."""
    dispositions = load_manual_dispositions()

    # Remove existing override for this record_id if it exists
    dispositions = [d for d in dispositions if d.get("record_id") != record_id]

    # Add new override
    new_override = {
        "record_id": record_id,
        ACCOUNT_ID: account_id,
        ACCOUNT_NAME: account_name,
        "name_core": name_core,
        "override": override,
        "reason": reason,
        "ts": datetime.now().isoformat(),
    }

    dispositions.append(new_override)

    if save_manual_dispositions(dispositions):
        st.success(f"Override saved: {record_id} → {override}")
        return True
    return False


def load_manual_blacklist() -> List[str]:
    """Load manual blacklist terms from JSON file."""
    manual_dir = ensure_manual_directory()
    file_path = manual_dir / "manual_blacklist.json"

    if not file_path.exists():
        return []

    try:
        with open(file_path, "r") as f:
            data = json.load(f)
            terms = data.get("terms", [])
            if isinstance(terms, list):
                return terms
            else:
                st.error("Invalid blacklist format")
                return []
    except Exception as e:
        st.error(f"Error loading manual blacklist: {e}")
        return []


def save_manual_blacklist(terms: List[str]) -> bool:
    """Save manual blacklist terms to JSON file."""
    manual_dir = ensure_manual_directory()
    file_path = manual_dir / "manual_blacklist.json"

    data = {"terms": terms, "last_updated": datetime.now().isoformat()}

    try:
        with open(file_path, "w") as f:
            json.dump(data, f, indent=2)
        return True
    except Exception as e:
        st.error(f"Error saving manual blacklist: {e}")
        return False


def add_manual_blacklist_term(term: str) -> bool:
    """Add a term to the manual blacklist."""
    terms = load_manual_blacklist()
    term_lower = term.lower().strip()

    if term_lower not in terms:
        terms.append(term_lower)
        if save_manual_blacklist(terms):
            st.success(f"Added blacklist term: {term}")
            return True
    else:
        st.warning(f"Term already exists: {term}")

    return False


def remove_manual_blacklist_term(term: str) -> bool:
    """Remove a term from the manual blacklist."""
    terms = load_manual_blacklist()
    term_lower = term.lower().strip()

    if term_lower in terms:
        terms.remove(term_lower)
        if save_manual_blacklist(terms):
            st.success(f"Removed blacklist term: {term}")
            return True

    return False


def get_manual_override_for_record(record_id: str) -> Optional[str]:
    """Get manual override for a specific record."""
    dispositions = load_manual_dispositions()
    for disposition in dispositions:
        if disposition.get("record_id") == record_id:
            return disposition.get("override")
    return None


def export_manual_data() -> tuple[str, str]:
    """Export manual data files for download."""
    ensure_manual_directory()

    # Export dispositions
    dispositions = load_manual_dispositions()
    dispositions_json = json.dumps(dispositions, indent=2)

    # Export blacklist
    blacklist_terms = load_manual_blacklist()
    blacklist_data = {
        "terms": blacklist_terms,
        "exported_at": datetime.now().isoformat(),
    }
    blacklist_json = json.dumps(blacklist_data, indent=2)

    return dispositions_json, blacklist_json
