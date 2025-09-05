# Company Junction — Deduplication Pipeline & UI

A fast, reproducible pipeline for finding duplicate Salesforce records, picking a primary, and producing a review‑ready file and parquet artifacts. This README is **task‑oriented**: plain‑English explanations and copy‑pasteable commands.

---

## Contents

- [What you get](#what-you-get)
- [Prerequisites](#prerequisites)
- [Install](#install)
- [Your data](#your-data)
- [Run the pipeline (quick starts)](#run-the-pipeline-quick-starts)
- [All flags & examples](#all-flags--examples)
- [Understanding stages & artifacts](#understanding-stages--artifacts)
- [Backend parity (why we run two engines sometimes)](#backend-parity-why-we-run-two-engines-sometimes)
- [Troubleshooting](#troubleshooting)
- [FAQ](#faq)
- [Contributing](#contributing)
- [License](#license)

---

## What you get

- **Review file** (CSV + Parquet) with a disposition per record: `Keep`, `Update`, `Delete`, `Verify`
- **Group stats** parquet for a snappy UI experience
- **Intermediate artifacts** for debugging and audit
- **Streamlit UI** to explore/verify results (read‑only)

---

## Prerequisites

- macOS / Linux (Windows WSL works)
- **Python 3.10+**
- (Optional) Salesforce CLI if you plan to script SFDC operations later

We strongly recommend a **virtual environment** per project.

---

## Install

```bash
# 1) Create and activate a venv
python -m venv .venv
source .venv/bin/activate

# 2) Install dependencies
python -m pip install -r requirements.txt

# 3) (Optional) editable install for local imports
python -m pip install -e .
```

> Tip: if you switch Python versions, recreate the venv.

---

## Your data

Place your Salesforce export(s) in `data/raw/`. CSV, XLSX, and XLS are supported.

The pipeline auto‑maps columns, but you can override names at the CLI (see **All flags** below).  
Schema decisions are saved per‑run to `data/processed/{run_id}/schema_mapping.json`.

---

## Run the pipeline (quick starts)

These examples are **safe to copy/paste**. Replace the input path as needed.

> Clamp BLAS threads on macOS to prevent oversubscription:
```bash
export OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 VECLIB_MAXIMUM_THREADS=1 NUMEXPR_NUM_THREADS=1
```

### A) Minimal end‑to‑end run

```bash
RUN_ID="cj$(date +%Y%m%d%H%M%S)"
python src/cleaning.py \
  --input data/raw/company_junction_range_01.csv \
  --outdir data/processed \
  --config config/settings.yaml \
  --run-id "$RUN_ID"
```

### B) With progress bars and deterministic fresh run

```bash
RUN_ID="cj$(date +%Y%m%d%H%M%S)"
python src/cleaning.py \
  --input data/raw/company_junction_range_01.csv \
  --outdir data/processed \
  --config config/settings.yaml \
  --run-id "$RUN_ID" \
  --progress \
  --no-resume
```

### C) Performance‑tuned (Apple Silicon example)

```bash
source .venv/bin/activate
export OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 VECLIB_MAXIMUM_THREADS=1 NUMEXPR_NUM_THREADS=1
RUN_ID="cj$(date +%Y%m%d%H%M%S)"

python src/cleaning.py \
  --input data/raw/company_junction_range_01.csv \
  --outdir data/processed \
  --config config/settings.yaml \
  --run-id "$RUN_ID" \
  --workers 10 \
  --chunk-size 2000 \
  --parallel-backend loky \
  --progress \
  --no-resume
```

### D) Launch the Streamlit UI

```bash
# Option 1: wrapper (preferred)
python run_streamlit.py

# Option 2: direct (Ctrl+C may show a CancelledError)
streamlit run app/main.py
```

---

## All flags & examples

`python src/cleaning.py --help` will list everything. Here are the **common flags** with copy‑paste examples.

### Inputs & basic control

- `--input PATH` – required CSV/XLS(X)
- `--outdir DIR` – output root (default: `data/processed`)
- `--config PATH` – YAML settings (default: `config/settings.yaml`)
- `--run-id STR` – name the run; used for foldering & pointers

```bash
python src/cleaning.py \
  --input data/raw/my_export.csv \
  --outdir data/processed \
  --config config/settings.yaml \
  --run-id "cj_demo"
```

### Progress & resumes

- `--progress` – tqdm bars (if installed)
- `--no-resume` – force fresh run (ignore previous state)
- `--resume-from STAGE` – resume at a later stage
- `--force` – allow resume even if inputs changed (hash mismatch)
- `--state-path PATH` – custom state file (default: `data/interim/{run_id}/pipeline_state.json`)

Valid `STAGE` values: `normalization`, `filtering`, `exact_equals`, `candidate_generation`, `grouping`, `survivorship`, `disposition`, `alias_matching`, `final_output`.

```bash
# Resume from grouping (skips earlier stages)
python src/cleaning.py --input data/raw/my_export.csv --outdir data/processed --config config/settings.yaml --run-id "cj_demo" --resume-from grouping
```

### Column mapping (schema)

The resolver tries: **CLI overrides → filename template → synonyms → heuristics**.

```bash
# Map actual headers → canonical names
python src/cleaning.py \
  --input data/raw/weird_headers.csv \
  --outdir data/processed \
  --config config/settings.yaml \
  --run-id "cj_map" \
  --col account_name="Account Name" account_id="Account ID"
```

### Parallelism & chunks

- `--workers N` – process count (auto if omitted)
- `--no-parallel` – single‑process
- `--parallel-backend {loky,threading}` – default `loky`
- `--chunk-size N` – chunk size for parallel operations

```bash
python src/cleaning.py \
  --input data/raw/my_export.csv \
  --outdir data/processed \
  --config config/settings.yaml \
  --run-id "cj_perf" \
  --workers 8 --chunk-size 1500 --parallel-backend loky
```

### Environment toggles (optional)

- `CJ_GROUP_STATS_PERSIST_ARTIFACTS=true|false` – force writing group‑stats artifacts
- `CJ_GROUP_STATS_RUN_PARITY=true|false` – run pandas vs DuckDB parity check

```bash
CJ_GROUP_STATS_PERSIST_ARTIFACTS=true CJ_GROUP_STATS_RUN_PARITY=true \
python src/cleaning.py --input data/raw/my_export.csv --outdir data/processed --config config/settings.yaml --run-id "cj_parity"
```

### Logging

Logging defaults come from `config/settings.yaml`:

```yaml
logging:
  level: "INFO"
  file: "pipeline.log"
```

Tail logs while a run executes:

```bash
tail -f pipeline.log
```

---

## Understanding stages & artifacts

Each run creates **interim** and **processed** folders under your `--run-id`.

**Stages (in order):**

1. **normalization** – clean and tokenize names  
2. **filtering** – drop empty/noisy names (writes an audit parquet)  
3. **exact_equals** – merges strict duplicates early for speed  
4. **candidate_generation** – create & score candidate pairs  
5. **grouping** – union‑find with edge‑gating to build groups  
6. **survivorship** – pick a single primary per group  
7. **disposition** – classify each record (`Keep/Update/Delete/Verify`)  
8. **alias_matching** – produce alias cross‑references (no regrouping)  
9. **final_output** – write review files and summaries

**Key artifacts (typical):**
