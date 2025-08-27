# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.2.0] - 2025-08-27

### Added
- **Review UX improvements**: Disposition table replaces bar chart, group-level sorting controls
- **Manual disposition overrides**: Group-level dropdown for Keep/Delete/Update/Verify with JSON persistence
- **Manual blacklist editor**: Add/remove pattern-based rules for automatic Delete classification
- **Audit trail**: Timestamps and export functionality for manual changes
- **Better group layout**: Group info moved to top with badges, improved table readability

### Changed
- Streamlit UI layout reorganized for better workflow efficiency
- Account Name column now wraps fully for better readability
- Sorting options for groups (by size, score) and records (by name)
- Manual data stored in JSON format for better structure and validation

### Technical
- New `app/manual_data.py` module for manual override and blacklist management
- Enhanced `src/disposition.py` with optional manual file loading and override application
- Manual data directory `data/manual/` with git-ignored JSON files
- Unit tests for manual override and blacklist functionality
- Pipeline gracefully handles missing or malformed manual files

## [1.1.0] - 2025-08-27

### Added
- **Conservative alias extraction** (semicolon, numbered sequences; parentheses only when content contains a legal suffix or multiple capitalized words)
- **Alias matching** with high-confidence gating (suffix match + score ≥ high), cross-links only; writes `alias_matches.parquet` and minimal metadata to `review_ready.csv`
- **Minimal UI** support: alias badge/expander and "Has aliases" filter in Streamlit
- **Performance safeguards** for alias comparisons (config cap)

### Changed
- Parentheses handling is **preserved and flagged** by default (no blanket alias creation)
- Punctuation normalization remains conservative (no global comma/period stripping)

## [1.0.0] - 2025-08-27

### Added
- Legal-aware normalization (`src/normalize.py`)
- Similarity scoring (`src/similarity.py`)
- Grouping & survivorship (`src/grouping.py`, `src/survivorship.py`)
- Disposition logic (`src/disposition.py`)
- CLI orchestrator (`src/cleaning.py`)
- Streamlit review UI (`app/main.py`)
- Config updates (`config/settings.yaml`, `config/relationship_ranks.csv`)
- Unit tests across modules

### Changed
- README.md and cursor_rules.md updated to reflect Phase 1 rules

## [Unreleased]

### Added
- Initial project scaffolding with Cookiecutter Data Science structure
- Basic data cleaning pipeline with duplicate detection
- Streamlit GUI for interactive data processing
- Salesforce CLI integration framework
- Utility functions for file management and validation
- Configuration system with `config/settings.yaml` and `config/logging.conf`
- Test fixtures with sample data in `tests/fixtures/`
- Enhanced documentation with prerequisites, usage examples, and contributing guidelines
- Tightened development guardrails in `cursor_rules.md`

### Changed

### Deprecated

### Removed

### Fixed

### Security
