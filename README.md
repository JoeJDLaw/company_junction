# 🏢 Company Junction — Deduplication Pipeline & UI

> **Fast, reproducible pipeline for finding duplicate Salesforce records, picking a primary, and producing review‑ready files and parquet artifacts.**

📋 **This README is task‑oriented**: plain‑English explanations and copy‑pasteable commands.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

## [NAVIGATION] 📑 Quick Navigation

| 🎯 **Get Started** | ⚙️ **Configuration** | 🔧 **Advanced** |
|-------------------|---------------------|-----------------|
| [🚀 What you get](#-what-you-get) | [🏃‍♂️ Run the pipeline](#-run-the-pipeline-quick-starts) | [🧹 Cleanup & Run Management](#-cleanup--run-management) |
| [📋 Prerequisites](#-prerequisites) | [⚙️ All flags & examples](#️-all-flags--examples) | [🔍 Understanding stages & artifacts](#-understanding-stages--artifacts) |
| [💾 Install](#-install) | [🔄 Backend parity](#-backend-parity) | [🛠️ Troubleshooting](#️-troubleshooting) |
| [📁 Your data](#-your-data) | | [❓ FAQ](#-faq) |

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

## [GET STARTED] 🎯 What you get
_Key outputs from the pipeline_
────────────────────────────────────────────────────────────────────────────

> **✨ Key Outputs**

- 📄 **Review file** (CSV + Parquet) with dispositions: `Keep`, `Update`, `Delete`, `Verify`
- 📊 **Group stats** parquet for snappy UI experience  
- 🔍 **Intermediate artifacts** for debugging and audit
- 🖥️ **Streamlit UI** to explore/verify results (read‑only)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

## [GET STARTED] 📋 Prerequisites
_Required before first run_
────────────────────────────────────────────────────────────────────────────

> **🖥️ System Requirements**

- ✅ **macOS / Linux** (Windows WSL works)
- 🐍 **Python 3.10+**
- 🔧 **(Optional)** Salesforce CLI for future SFDC operations

> 💡 **We strongly recommend a virtual environment per project.**

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

## [GET STARTED] 💾 Install
_One-time setup process_
────────────────────────────────────────────────────────────────────────────

> **🚀 Quick Setup**

```bash
# 1️⃣ Create and activate a venv
python -m venv .venv
source .venv/bin/activate

# 2️⃣ Install dependencies
python -m pip install -r requirements.txt

# 3️⃣ (Optional) editable install for local imports
python -m pip install -e .
```

> ⚠️ **Tip**: If you switch Python versions, recreate the venv.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

## [GET STARTED] 📁 Your data
_Where to put your input files_
────────────────────────────────────────────────────────────────────────────

> **📂 Data Placement**

Place your Salesforce export(s) in `data/raw/`. 

**Supported formats**: CSV, XLSX, and XLS

> 🔄 **Auto-mapping**: The pipeline auto‑maps columns, but you can override names at the CLI (see **All flags** below).  
> 💾 **Schema decisions** are saved per‑run to `data/processed/{run_id}/schema_mapping.json`.

## [CONFIGURATION] 🏃‍♂️ Run the pipeline (quick starts)
_Copy-paste examples for immediate use_
────────────────────────────────────────────────────────────────────────────

> **🚀 Ready-to-use examples** — safe to copy/paste. Replace the input path as needed.

> ⚠️ **macOS Users**: Clamp BLAS threads to prevent oversubscription:
```bash
export OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 VECLIB_MAXIMUM_THREADS=1 NUMEXPR_NUM_THREADS=1
```

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

### 🎯 A) Minimal end‑to‑end run

```bash
RUN_ID="cj$(date +%Y%m%d%H%M%S)"
python src/cleaning.py \
  --input data/raw/company_junction_range_01.csv \
  --outdir data/processed \
  --config config/settings.yaml \
  --run-id "$RUN_ID"
```

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

### 📊 B) With progress bars and deterministic fresh run

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

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

### ⚡ C) Performance‑tuned (Apple Silicon example)

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

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

### 🖥️ D) Launch the Streamlit UI

```bash
# 🎯 Option 1: wrapper (preferred)
python run_streamlit.py

# 🔧 Option 2: direct (Ctrl+C may show a CancelledError)
streamlit run app/main.py
```

## [CONFIGURATION] ⚙️ All flags & examples
_Complete command-line reference_
────────────────────────────────────────────────────────────────────────────

> **📚 Complete reference** — `python src/cleaning.py --help` lists everything. Here are the **common flags** with copy‑paste examples.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

### 📥 Inputs & basic control
_These are the most common flags you'll actually use._

| Flag | Description | Default |
|------|-------------|---------|
| `--input PATH` | 📄 **Required CSV/XLS(X)** | — |
| `--outdir DIR` | 📁 Output root | `data/processed` |
| `--config PATH` | ⚙️ YAML settings | `config/settings.yaml` |
| `--run-id STR` | 🏷️ Name the run; used for foldering & pointers | Auto-generated |

```bash
python src/cleaning.py \
  --input data/raw/my_export.csv \
  --outdir data/processed \
  --config config/settings.yaml \
  --run-id "cj_demo"
```

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

### 🔄 Progress & resumes
_Control how the pipeline runs and resumes._

| Flag | Description |
|------|-------------|
| `--progress` | 📊 tqdm bars (if installed) |
| `--no-resume` | 🔄 **Force fresh run** (ignore previous state) |
| `--resume-from STAGE` | 🎯 Resume at a later stage |
| `--force` | ⚠️ Allow resume even if inputs changed (hash mismatch) |
| `--state-path PATH` | 📄 Custom state file |

> **🎯 Valid `STAGE` values**: `normalization`, `filtering`, `candidate_generation`, `grouping`, `survivorship`, `disposition`, `alias_matching`, `final_output`

```bash
# Resume from grouping (skips earlier stages)
python src/cleaning.py --input data/raw/my_export.csv --outdir data/processed --config config/settings.yaml --run-id "cj_demo" --resume-from grouping
```

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

### 🗂️ Column mapping (schema)
_Map your CSV headers to expected column names._

> **🔄 Resolution order**: CLI overrides → filename template → synonyms → heuristics

```bash
# Map actual headers → canonical names
python src/cleaning.py \
  --input data/raw/weird_headers.csv \
  --outdir data/processed \
  --config config/settings.yaml \
  --run-id "cj_map" \
  --col account_name="Account Name" account_id="Account ID"
```

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

### ⚡ Parallelism & chunks
_Performance tuning for different system sizes._

| Flag | Description | Default |
|------|-------------|---------|
| `--workers N` | 👥 Process count | Auto |
| `--no-parallel` | 🔒 **Single‑process** |
| `--parallel-backend {loky,threading}` | 🔧 Backend | `loky` |
| `--chunk-size N` | 📦 Chunk size for parallel operations | `1000` |

```bash
python src/cleaning.py \
  --input data/raw/my_export.csv \
  --outdir data/processed \
  --config config/settings.yaml \
  --run-id "cj_perf" \
  --workers 8 --chunk-size 1500 --parallel-backend loky
```

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

### 🔧 Environment toggles (optional)

| Variable | Description |
|----------|-------------|
| `CJ_GROUP_STATS_PERSIST_ARTIFACTS=true\|false` | 📊 Force writing group‑stats artifacts |
| `CJ_GROUP_STATS_RUN_PARITY=true\|false` | 🔍 Run pandas vs DuckDB parity check |

```bash
CJ_GROUP_STATS_PERSIST_ARTIFACTS=true CJ_GROUP_STATS_RUN_PARITY=true \
python src/cleaning.py --input data/raw/my_export.csv --outdir data/processed --config config/settings.yaml --run-id "cj_parity"
```

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

### 📝 Logging

> **⚙️ Logging defaults** come from `config/settings.yaml`:

```yaml
logging:
  level: "INFO"
  file: "pipeline.log"
```

> **👀 Tail logs** while a run executes:

```bash
tail -f pipeline.log
```

[↑ Back to top](#-company-junction--deduplication-pipeline--ui)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

## [ADVANCED] 🧹 Cleanup & Run Management

> **🗂️ Pipeline creates run artifacts that can accumulate over time. Use the cleanup utility to manage them safely.**

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

### 🚀 Quick Cleanup Commands

```bash
# 📋 List all runs grouped by type
python tools/pipeline_cleanup.py --list

# 👀 Preview what would be deleted (recommended first step)
python tools/pipeline_cleanup.py --delete-tests --dry-run
python tools/pipeline_cleanup.py --delete-prod --dry-run
python tools/pipeline_cleanup.py --delete-all --dry-run
python tools/pipeline_cleanup.py --delete-run RUN_ID --dry-run

# 🗑️ Actually delete (requires confirmation)
python tools/pipeline_cleanup.py --delete-tests
python tools/pipeline_cleanup.py --delete-prod
python tools/pipeline_cleanup.py --delete-all
python tools/pipeline_cleanup.py --delete-run RUN_ID
```

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

### 🏷️ Run Types

| Type | Description | Usage |
|------|-------------|-------|
| **`dev`** | 🛠️ Development runs | Default |
| **`test`** | 🧪 Test runs | `--run-type test` |
| **`prod`** | 🚀 Production runs | `--run-type prod` |

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

### 🛡️ Safety Features

- ✅ **Dry-run by default** - Always preview before deleting
- ✅ **Confirmation prompts** - Explicit user confirmation required
- ✅ **Fuse protection** - Respects `PHASE1_DESTRUCTIVE_FUSE` environment variable
- ✅ **Running run protection** - Blocks deletion of active pipeline runs
- ✅ **Latest run protection** - Never deletes the latest successful run

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

### 🖥️ Streamlit UI Integration

> **⚙️ Run deletion** is available in the Streamlit UI under "⚙️ Advanced: Maintenance" when:
- `ui.enable_run_deletion: true` in `config/settings.yaml`
- `ui.admin_mode: true` in `config/settings.yaml`

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

### 🏷️ Setting Run Type

> **🎯 Use the `--run-type` flag** when running the pipeline:

```bash
# 🛠️ Development run (default)
python src/cleaning.py --input data.csv --outdir data/processed

# 🧪 Test run
python src/cleaning.py --input data.csv --outdir data/processed --run-type test

# 🚀 Production run
python src/cleaning.py --input data.csv --outdir data/processed --run-type prod
```

> **💾 The run type** is stored in both `review_meta.json` and `run_index.json` for cleanup operations.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

### 📊 Exit Codes

| Code | Meaning |
|------|---------|
| **0** | ✅ No candidates found for deletion |
| **2** | 👀 Candidates found (dry-run mode) |
| **>0** | ❌ Errors occurred during execution |

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

### 🔄 Legacy Run Handling

> **⚠️ Runs created before the run type system** are treated as `dev` runs with a warning logged:

```
WARNING: Legacy run cj20250905190659 missing run_type, treating as 'dev'
```

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

### 💡 Best Practices

1. **👀 Always use `--dry-run` first** to preview what will be deleted
2. **🏷️ Use specific run types** when running the pipeline for easier cleanup
3. **🚀 Keep production runs** longer than test runs
4. **📊 Monitor disk usage** and clean up regularly
5. **🖥️ Use the Streamlit UI** for interactive cleanup when available

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

### 🛠️ Troubleshooting

#### 🔒 Permission Errors

> **If you encounter permission errors**, ensure the cleanup utility has write access:

```bash
# Check permissions
ls -la data/processed/
ls -la data/interim/

# Fix permissions if needed
chmod -R 755 data/processed/
chmod -R 755 data/interim/
```

#### 🔥 Fuse Disabled

> **If the fuse is disabled**, you'll see:

```
Delete runs disabled: Phase 1 destructive fuse not enabled
```

> **Enable it with:**

```bash
export PHASE1_DESTRUCTIVE_FUSE=true
```

#### 🔍 Running Process Detection

> **If the utility incorrectly detects a running process**, check for active processes:

```bash
# Check for active cleaning.py processes
ps aux | grep cleaning.py
```

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

### 🔄 Migration from Old Cleanup Script

> **⚠️ The old `tools/cleanup_test_artifacts.py` has been deprecated.** Use the new `pipeline_cleanup.py` instead:

```bash
# ❌ Old way (deprecated)
python tools/cleanup_test_artifacts.py --types test,dev --older-than 7

# ✅ New way (current)
python tools/pipeline_cleanup.py --delete-tests --dry-run
python tools/pipeline_cleanup.py --delete-tests
```

> **📚 See `deprecated/README.md`** for more migration information.

[↑ Back to top](#-company-junction--deduplication-pipeline--ui)

## [ADVANCED] 🔍 Understanding stages & artifacts

> **📁 Each run creates `interim` and `processed` folders under your `--run-id`.**

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

### 🔄 Pipeline Stages (in order)

| Stage | Description | Purpose |
|-------|-------------|---------|
| 1️⃣ **normalization** | 🧹 Clean and tokenize names | Data preparation |
| 2️⃣ **filtering** | 🗑️ Drop empty/noisy names | Quality control |
| 3️⃣ **candidate_generation** | 🎯 Create & score candidate pairs | Similarity detection |
| 4️⃣ **grouping** | 🔗 Union‑find with edge‑gating | Group formation |
| 5️⃣ **survivorship** | 👑 Pick a single primary per group | Primary selection |
| 6️⃣ **disposition** | 🏷️ Classify each record | Decision making |
| 7️⃣ **alias_matching** | 🔗 Produce alias cross‑references | Relationship mapping |
| 8️⃣ **final_output** | 📄 Write review files and summaries | Output generation |

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

### 📦 Key Artifacts (typical)

| File | Description | Location |
|------|-------------|----------|
| 📄 **review_ready.csv** | Main review file with dispositions | `data/processed/{run_id}/` |
| ⚡ **review_ready.parquet** | Parquet version for performance | `data/processed/{run_id}/` |
| 📊 **group_stats.parquet** | Group-level statistics | `data/processed/{run_id}/` |
| 🔍 **group_details.parquet** | Detailed group information | `data/processed/{run_id}/` |
| ⚙️ **review_meta.json** | Run metadata and configuration | `data/processed/{run_id}/` |
| 🗂️ **interim files** | Intermediate files for debugging and resume | `data/interim/{run_id}/` |

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

## [ADVANCED] 🔄 Backend parity (why we run two engines sometimes)

> **🔧 The pipeline supports both pandas and DuckDB backends for different operations:**

| Backend | Use Case | Performance |
|---------|----------|-------------|
| **🦆 DuckDB** (default) | Fast analytics queries, group statistics, large dataset operations | ⚡ High |
| **🐼 pandas** | Complex data transformations, string processing, legacy compatibility | 🔧 Flexible |

> **🔍 Parity validation** ensures both backends produce identical results. Enable with:
```bash
CJ_GROUP_STATS_RUN_PARITY=true python src/cleaning.py ...
```

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

## [ADVANCED] 🛠️ Troubleshooting

> **🚨 Common Issues & Solutions**

### 🔍 **Adversarial False Positives (Production Blocker)**

If you're experiencing incorrect company groupings (e.g., "Apple Inc" being grouped with "Apple Bank Inc"), see the comprehensive troubleshooting plan:

**📋 [Phase 2.0.2 Adversarial False Positives Plan](docs/plans/Phase2.0.2-Adversarial-FalsePositives.md)**

This document provides:
- Root cause analysis of false-positive cases
- Detailed implementation strategy for distractor token handling
- Test plans and acceptance criteria
- Configuration options for fine-tuning similarity scoring

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

### ❌ Common Issues

#### 🔧 **Pipeline fails with "No module named 'src'"**
```bash
# Install in editable mode
python -m pip install -e .
```

#### 💾 **Memory issues on large datasets**
```bash
# Reduce chunk size and workers
python src/cleaning.py --chunk-size 500 --workers 4 ...
```

#### 🔄 **Resume fails with hash mismatch**
```bash
# Force resume (use with caution)
python src/cleaning.py --force --resume-from grouping ...
```

#### 🖥️ **Streamlit UI shows "No runs found"**
- ✅ Check `data/processed/latest.json` points to a valid run
- ✅ Verify run artifacts exist in `data/processed/{run_id}/`

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

### ⚡ Performance Tuning

#### 🍎 **For Apple Silicon (M1/M2)**
```bash
export OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 VECLIB_MAXIMUM_THREADS=1 NUMEXPR_NUM_THREADS=1
python src/cleaning.py --workers 10 --chunk-size 2000 --parallel-backend loky ...
```

#### 📊 **For large datasets (>100K records)**
```bash
python src/cleaning.py --workers 8 --chunk-size 1500 --parallel-backend loky ...
```

#### 📝 **For small datasets (<10K records)**
```bash
python src/cleaning.py --no-parallel ...
```

[↑ Back to top](#-company-junction--deduplication-pipeline--ui)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

## [REFERENCE] ❓ FAQ

> **🤔 Frequently Asked Questions**

| Question | Answer |
|----------|--------|
| **🔄 Can I resume a failed pipeline run?** | ✅ Yes! Use `--resume-from STAGE` or let the pipeline auto-detect the resume point. |
| **📄 What file formats are supported?** | 📊 CSV, XLSX, and XLS files. The pipeline auto-detects format and handles encoding. |
| **🗂️ How do I map custom column names?** | 🎯 Use `--col` flags: `--col account_name="Company Name" account_id="ID"` |
| **🖥️ Can I run the pipeline without the UI?** | ✅ Yes! The pipeline produces review files that can be opened in Excel or any CSV viewer. |
| **🏷️ What's the difference between run types?** | 🛠️ `dev` (default), 🧪 `test`, and 🚀 `prod` runs are categorized for cleanup operations only. |
| **🧹 How do I clean up old runs?** | 📋 Use `python tools/pipeline_cleanup.py --list` to see runs, then `--delete-tests` or `--delete-all`. |

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

## [REFERENCE] 🤝 Contributing

> **🚀 How to contribute to this project**

### 📋 Steps to Contribute

1. **🍴 Fork the repository**
2. **🌿 Create a feature branch**: `git checkout -b feature/amazing-feature`
3. **✅ Follow the coding standards**:
   - 🎨 Run `black` for formatting
   - 🔍 Run `ruff` for linting  
   - 🔧 Run `mypy` for type checking
   - 🧪 Run `pytest` for tests
4. **💾 Commit your changes**: `git commit -m 'Add amazing feature'`
5. **📤 Push to the branch**: `git push origin feature/amazing-feature`
6. **🔄 Open a Pull Request**

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

### 🛠️ Development Setup

```bash
# Install development dependencies
python -m pip install -e ".[dev]"

# Run all checks
black src/ tests/
ruff check src/ tests/
mypy src/
pytest tests/
```

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

## [REFERENCE] 📄 License

> **📜 This project is licensed under the MIT License** - see the [LICENSE](LICENSE) file for details.
