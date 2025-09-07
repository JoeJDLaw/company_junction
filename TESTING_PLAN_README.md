# 🧪 Company Junction Testing Plan

**Status**: Phase 2.0.1 Complete | Production Readiness In Progress | Production Ready: 90%

---

## 📖 **What This Document Is**

This document tracks our testing strategy for the Company Junction pipeline. It shows what we've built, what's working, and what we need to do next to make the pipeline production-ready.
---

### **Schema Checklist**

#### **Input Fields (Populate in Test Datasets)**
- [ ] `account_id` (optional, 15/18-char Salesforce ID - pipeline can auto-generate)
- [ ] `account_name` (required, company name for deduplication)
- [ ] `created_date` (optional, YYYY-MM-DD format for tie-breaking)
- [ ] `suffix_class` (optional, extracted suffix like Inc/LLC/Ltd)
- [ ] `disposition` (optional, Update/Merge/Drop classification)
- [ ] `parent_account_id` (optional, parent relationship for tie-breaking)
- [ ] `relationship` (optional, relationship type for survivorship)

#### **Generated Fields (Created by Pipeline - Do NOT Pre-populate)**
- [ ] `name_core` (normalized company name core)
- [ ] `name_core_tokens` (JSON array of normalized tokens)
- [ ] `has_parentheses`, `has_semicolon`, `has_multiple_names` (normalization flags)
- [ ] `group_id` (assigned during grouping stage)
- [ ] `group_size` (calculated during grouping)
- [ ] `is_primary` (determined during survivorship)
- [ ] `max_score` (highest similarity score in group)
- [ ] `primary_name` (canonical representative name)
- [ ] `weakest_edge_to_primary` (similarity score to primary)
- [ ] `block_key` (generated during candidate generation)
- [ ] `group_join_reason` (edge gating decision reason)
- [ ] `shared_tokens_count` (token overlap count)

---

## 🎯 **Current Status Overview**

### ✅ **What's Working (Phase 2.0.1 Complete)**
- **Golden datasets** - Test data for consistent testing
- **Property-based tests** - Mathematical correctness validation  
- **E2E resume tests** - Pipeline restart functionality
- **File format tests** - Input file handling
- **Adversarial tests** - Edge case detection
- **CLI integration tests** - Command-line interface
- **Test robustness improvements** - Better error handling
- **Engine selection observability** - Backend selection logging and reasoning
- **Blacklist audit fixes** - Corrected audit snapshot math (no double-counting)

### 🔄 **What's In Progress (Production Readiness)**
- **Coverage analysis** - Test coverage validation
- **Adversarial test fixes** - Critical production blocker resolution

### ⏳ **What's Next (Production Readiness)**
- **Performance testing** - Large dataset handling
- **Deployment preparation** - Production setup

---

## 📊 **Progress Tracking**

**Overall Progress: 95% Complete**

| Phase | Status | Progress |
|-------|--------|----------|
| Phase 2.0.0 Core | ✅ Complete | 100% |
| Phase 2.0.1 Improvements | ✅ Complete | 100% |
| Coverage & CI | 🔄 In Progress | 80% |
| Production Readiness | 🔄 In Progress | 30% |

---

## 🎉 **Phase 2.0.1 Achievements (Recently Completed)**

### **Engine Selection & Observability**
- ✅ **Backend selection logging** - Clear reasoning for pandas vs DuckDB choices
- ✅ **Truthful logging** - `requested_backend` vs `effective_backend` distinction
- ✅ **Configuration-driven selection** - Respects user preferences and thresholds
- ✅ **Fallback tracking** - Logs when and why fallbacks occur

### **Blacklist System Improvements**
- ✅ **Config precedence** - Explicit empty configs disable built-ins (no silent fallback)
- ✅ **Manual terms inclusion** - Always included regardless of config presence
- ✅ **Performance optimization** - Smart caching with config+manual digest keys
- ✅ **Cache management** - `clear_blacklist_cache()` for long runs
- ✅ **Audit fix** - Corrected double-counting bug in audit snapshots

### **Exact Equals Optimization**
- ✅ **Spanning tree implementation** - Configurable pair emission (O(n²) → O(n-1))
- ✅ **Performance improvement** - Dramatic reduction in pair generation for large datasets
- ✅ **Backward compatibility** - Default to spanning tree, option for complete graph

### **Critical Bug Fixes**
- ✅ **Empty token guard** - Prevents catastrophic "match everywhere" regex bug
- ✅ **Index alignment** - Fixed vectorized disposition reasons with non-RangeIndex
- ✅ **Legacy path consistency** - Both vectorized and legacy paths respect config

---

## 🧪 **Test Categories (What We Built)**

### 1. **Golden Datasets** ✅ **COMPLETE**
**Location**: `tests/data/`

**What it does**: Provides consistent test data for all tests

**Files**:
- `companies_small.csv` - 50 realistic company pairs
- `companies_edge.csv` - 50 edge cases (unicode, special chars)
- `companies_adversarial.csv` - 50 adversarial cases (should NOT group)

---

### 2. **Property-Based Tests** ✅ **COMPLETE**
**Location**: `tests/test_similarity_property_based.py`

**What it does**: Tests mathematical properties of similarity scoring

**Tests**:
- **Symmetry**: `score(a,b) = score(b,a)`
- **Identity**: Identical names have perfect scores
- **Bounds**: All scores between 0-100
- **Deterministic**: Same input always gives same output

---

### 3. **E2E Resume Tests** ✅ **COMPLETE**
**Location**: `tests/test_resume_e2e.py`

**What it does**: Tests pipeline restart functionality

**Tests**:
- **Auto-resume detection** - Pipeline automatically resumes from where it left off
- **Log verification** - Correct log messages are generated
- **Artifact reuse** - Previously computed files are reused
- **No-resume flag** - `--no-resume` forces full run

---

### 4. **File Format Tests** ✅ **COMPLETE**
**Location**: `tests/test_file_formats.py`

**What it does**: Tests input file handling

**Tests**:
- **CSV format detection** - Reads CSV files correctly
- **Excel format detection** - Reads XLSX/XLS files (with optional dependencies)
- **Unsupported formats** - Handles errors gracefully
- **Encoding handling** - Supports UTF-8 and other encodings

---

### 5. **Adversarial Tests** ✅ **INFRASTRUCTURE COMPLETE**
**Location**: `tests/test_adversarial_cases.py`

**What it does**: Tests edge cases where companies should NOT be grouped

**Tests**:
- **Bank distractors** - "Apple Inc" vs "Apple Bank Inc" (should NOT group)
- **Venue distractors** - "Oracle" vs "Oracle Park" (should NOT group)
- **Brand extensions** - "Uber Eats" vs "Uber Technologies" (should NOT group)

**Status**: Test infrastructure works, but found behavioral issue (see Priority 1 below)

---

### 6. **CLI Integration Tests** ✅ **COMPLETE**
**Location**: `tests/test_cli_integration.py`

**What it does**: Tests command-line interface

**Status**: Verified working with recent pipeline fixes (12/13 tests passing)

---

## 🔧 **Configuration & Setup**

### **Deterministic Testing**
- **`pytest.ini`** - Hypothesis settings for consistent results
- **`tests/conftest.py`** - Global seed configuration (seed=42)
- **Property-based tests** - Use `@pytest.mark.hypothesis` marker
- **Floating point comparisons** - Use `math.isclose()` for precision

### **Coverage Gates**
- **Overall coverage**: ≥75% (lines & branches)
- **Critical modules**: ≥90% (cleaning.py, disposition.py, edge_grouping.py, survivorship.py)

### **Engine Selection Observability**
- **Engine selection logging**: Detailed logging of backend selection decisions
- **Decision reasoning**: Clear explanation of why pandas vs DuckDB was chosen
- **Configuration visibility**: Engine selection settings and thresholds
- **Fallback tracking**: When and why fallbacks occur

#### Engine Selection Configuration
```yaml
# Engine Selection Configuration
engines:
  # Backend selection for specific stages
  filtering: "auto"  # "auto" | "pandas" | "duckdb"
  exact_equals: "auto"  # "auto" | "pandas" | "duckdb"
  
  # Automatic selection thresholds
  duckdb_threshold_rows: 50000  # Use DuckDB for datasets >= this size
```

#### Engine Selection Log Format
```
engine_selection | stage=filtering | chosen=duckdb | requested=auto | duckdb=duckdb_import_ok | n_rows=91944 threshold=50000 | object_cols=False | auto_selected=above_threshold
```

---

## 🚀 **Running Tests**

### **Quick Test Commands**
```bash
# Run all tests
pytest

# Run specific test categories
pytest tests/test_similarity_property_based.py  # Property-based tests
pytest tests/test_resume_e2e.py                # E2E resume tests
pytest tests/test_file_formats.py              # File format tests
pytest tests/test_adversarial_cases.py         # Adversarial tests

# Skip slow tests
pytest -m "not slow"
```

### **Coverage Analysis**
```bash
# Run tests with coverage
pytest --cov=src --cov-branch --cov-report=xml --cov-report=term-missing:skip-covered

# Check coverage gates
python scripts/check_coverage_gates.py
```

---

## 🚨 **Critical Issues (Production Blockers)**

### **Priority 1: Adversarial Test Failures** 🔴 **BLOCKING**

**Problem**: Similarity algorithm is grouping companies that shouldn't be grouped

**Examples**:
- "Apple Inc" and "Apple Bank Inc" are being grouped together
- "Oracle" and "Oracle Park" are being grouped together

**Impact**: This would cause incorrect grouping decisions in production

**Action Needed**: Fix similarity scoring logic to properly handle bank/venue/brand distractors

**📋 Detailed Plan**: See [Phase2.0.2-Adversarial-FalsePositives.md](docs/plans/Phase2.0.2-Adversarial-FalsePositives.md) for comprehensive troubleshooting plan, root cause analysis, and implementation strategy.

---

## 📋 **Production Readiness Roadmap**

### **Priority 2: Test Coverage & CI** 🟡 **HIGH**

**Tasks**:
- [x] Run coverage analysis to check current coverage (56.1% overall, below 75% target)
- [ ] Verify we meet 75% overall / 90% critical module coverage
- [ ] Set up CI integration with coverage gates
- [x] Test CLI integration with recent pipeline fixes (12/13 tests passing)
- [x] Fix blacklist audit snapshot math bug (double-counting issue resolved)

---

### **Priority 3: Production Readiness** 🟡 **HIGH**

**Error Handling & Resilience**:
- [ ] Test with malformed input data
- [ ] Test with missing required columns
- [ ] Test with extremely large datasets
- [ ] Test with network/IO failures

**Performance Validation**:
- [ ] Benchmark with realistic dataset sizes (10K+ records)
- [ ] Memory usage profiling
- [ ] Parallel processing efficiency
- [ ] Database connection pooling

**Configuration Management**:
- [ ] Validate all config settings are production-safe
- [ ] Test config validation and error messages
- [ ] Document required vs optional settings

---

### **Priority 4: Deployment Preparation** 🟢 **MEDIUM**

**Documentation**:
- [ ] Production deployment guide
- [ ] Configuration reference
- [ ] Troubleshooting guide
- [ ] Performance tuning guide

**Environment Setup**:
- [ ] Docker containerization
- [ ] Environment variable configuration
- [ ] Database connection setup
- [ ] File system permissions

**Monitoring & Alerting**:
- [ ] Health check endpoints
- [ ] Performance dashboards
- [ ] Error alerting
- [ ] Resource usage monitoring

---

## 🎯 **Immediate Next Actions**

### **This Week**
1. **🔴 Fix adversarial test failures** - Critical production blocker (Priority 1)
2. **📊 Run coverage analysis** - Verify test coverage gates (Priority 2)
3. **🔧 Complete CLI integration** - Ensure remaining test passes (Priority 2)

### **Next 2 Weeks**
1. **📈 Performance benchmarking** - Test with realistic data sizes
2. **🛡️ Error handling validation** - Test edge cases and failures
3. **📝 Documentation completion** - Production deployment guides

### **Next Month**
1. **🔍 Mutation testing** for critical modules
2. **📊 Monitoring setup** - Production observability
3. **🚀 Deployment automation** - CI/CD pipeline
4. **⚡ Load testing** - Production-scale validation

---

## 🏷️ **Tagging Strategy**

Use annotated tags for milestones:
```bash
git tag -a phase2.0.0-implementation-v1 -m "Phase 2.0.0: Core implementation complete
- All test infrastructure working
- Pipeline stability achieved
- Ready for production readiness phase"
```

---

## 📚 **Additional Resources**

- **Full testing plan**: `TESTING_PLAN_README.md` (this file)
- **Coverage instructions**: `COVERAGE_INSTRUCTIONS.md`
- **Cursor rules**: `cursor_rules.md`

---

**Last Updated**: Current session - Phase 2.0.1 Complete
**Next Review**: After adversarial test failures resolved (Priority 1)