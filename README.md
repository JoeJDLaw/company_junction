# Company Junction Deduplication Pipeline

## Project Status
**Phase 1 complete** ✅ — The system identifies likely duplicate Accounts, proposes a primary record using Relationship rank + Created Date, and assigns Disposition (`Keep`, `Update`, `Delete`, `Verify`). No Salesforce writes are performed in this phase.

**Phase 1.5 (refinements)**: Conservative alias extraction (semicolon & numbered sequences; parentheses only when content looks like a company), high-confidence alias matching (suffix match + score ≥ high), minimal Streamlit alias UI.

**Phase 1.7 (UX & Manual Controls)**: Review UX improvements (disposition table, sorting, better layout), manual disposition overrides, manual blacklist editor, JSON persistence with audit trail.

**Docs:** see `docs/DLaw_Company_Junction_Dedup_Plan.md` for the detailed plan and acceptance criteria.

**Next:** Phase 2 (future) will add Split detection & parsing, optional LLM "real-company" classifier, and Salesforce sync steps.

---

## Phase 1 Quick Start

### 1) Install
```bash
pip install -r requirements.txt
```

### 2) Run the pipeline
```bash
python src/cleaning.py \
  --input data/raw/company_junction_range_01.csv \
  --outdir data/processed \
  --config config/settings.yaml
```

### 3) Review results in Streamlit (read-only)
```bash
streamlit run app/main.py
```

### 4) Where to look
- **Interim artifacts:** `data/interim/`
  - `accounts_normalized.parquet`
  - `candidate_pairs.parquet` (if created)
  - `groups.parquet`
  - `dispositions.parquet`
  - `alias_matches.parquet` (Phase 1.5)
- **Manual data:** `data/manual/` (Phase 1.7)
  - `manual_dispositions.json` (overrides)
  - `manual_blacklist.json` (pattern rules)
- **Final review file:** `data/processed/review_ready.csv`
- **Config:** `config/settings.yaml`, `config/relationship_ranks.csv`

---

## Overview
This project provides a mini data pipeline with a Streamlit GUI for cleaning Salesforce export data. It identifies duplicates based on name, merges field values into a master record, and supports syncing back to Salesforce via CLI.

## Workflow
1. Export reports from Salesforce into `data/raw/`
2. Run the cleaning pipeline (in `src/`)
3. Review duplicates and approve merges in the Streamlit app (`app/main.py`)
4. Save final cleaned dataset to `data/processed/`
5. Use Salesforce CLI to update master records and delete duplicates

## Folder Structure
- **app/**: Streamlit GUI interface  
- **src/**: Pipeline code (cleaning, utils, Salesforce integration)  
- **data/raw/**: Original Salesforce exports  
- **data/interim/**: Temporary processing outputs  
- **data/processed/**: Final cleaned datasets  
- **tests/**: Unit tests  
- **docs/**: Additional documentation  
- **README.md**: This file (verbose project overview)  
- **cursor_rules.md**: Lean source of truth for Cursor  

## Getting Started
1. Install dependencies: `pip install -r requirements.txt`  
2. Run the Streamlit app: `streamlit run app/main.py`  
3. Upload your Salesforce CSV and review results interactively.  

## Prerequisites
- Python 3.10+
- Salesforce CLI installed and authenticated

## Usage Examples
```bash
# Run cleaning pipeline directly
python src/cleaning.py --input data/raw/my_export.csv --output data/interim/cleaned.csv

# Run Streamlit app
streamlit run app/main.py
```

## Contributing
- Follow PEP 8.
- Run tests with `pytest` before committing.
- Update `CHANGELOG.md` for significant changes.

## Current Phase Summary
- Legal-aware normalization preserves suffix differences (INC vs LLC, etc.)
- Similarity scoring via RapidFuzz with configurable thresholds (`high=92`, `medium=84`)
- Grouping with connected components (edges require suffix match + score ≥ medium)
- Survivorship by Relationship rank → Created Date → Account ID
- Disposition per record: `Keep`, `Update`, `Delete`, `Verify`
- No Salesforce writes in Phase 1

### Phase 1.5 Highlights
- Conservative **alias extraction** (semicolon, numbered sequences; filtered parentheses)
- **Alias matching** is cross-link only (no regrouping), requires high-confidence match

### Phase 1.7 Highlights
- **Review UX**: Disposition table replaces chart, group-level sorting, better layout
- **Manual overrides**: Group-level disposition overrides with JSON persistence
- **Manual blacklist**: Pattern-based rules for automatic Delete classification
- **Audit trail**: Timestamps and export functionality for manual changes
- **Minimal Streamlit** updates to surface aliases without overwhelming the UI

## License
MIT License - see LICENSE file for details.