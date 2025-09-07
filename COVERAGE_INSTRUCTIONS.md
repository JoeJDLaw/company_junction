# 📊 Coverage Report Instructions

## 🚀 Quick Start

To generate the HTML coverage report locally:

```bash
# Activate virtual environment
source .venv/bin/activate

# Run tests with coverage
pytest --cov=src --cov-branch --cov-report=html

# Open HTML report
open htmlcov/index.html
```

## 📁 Coverage Artifacts

### **Included in PR**
- `coverage.xml` - For CI integration and coverage reporting
- `.coveragerc` - Coverage configuration with proper exclusions

### **Generated Locally (Don't Commit)**
- `htmlcov/` - Interactive HTML coverage report
  - `htmlcov/index.html` - Main coverage dashboard
  - `htmlcov/src/` - Per-module coverage details

## 🎯 Coverage Targets

### **Current Baseline**
- **Overall Coverage**: 45.2% (3,044 lines covered out of 6,703 total)
- **Branch Coverage**: 225 branches partially covered out of 2,130 total

### **Phase 2 Targets**
- **Project Baseline**: ≥75% (lines & branches)
- **Critical Paths**: 90-95% (cleaning.py, disposition.py, edge_grouping.py, survivorship.py)
- **Similarity Module**: ≥90% + property-based tests

## 🚨 Critical Gaps (Triage-Now)

| File | Coverage | Priority | Action Required |
|------|----------|----------|-----------------|
| `src/cleaning.py` | 5.1% | **CRITICAL** | Add E2E pipeline tests |
| `src/disposition.py` | 6.9% | **CRITICAL** | Add decision logic tests |
| `src/manual_io.py` | 0.0% | **LOW** | Deprecate or add tests |
| `src/performance.py` | 23.4% | **MEDIUM** | Add performance tests |
| `src/services/group_service.py` | 20.0% | **MEDIUM** | Add service tests |
| `src/utils/run_management.py` | 0.0% | **LOW** | Deprecate or add tests |
| `src/utils/union_find.py` | 0.0% | **LOW** | Deprecate or add tests |
| `src/utils/parity_validator.py` | 0.0% | **LOW** | Deprecate or add tests |
| `src/utils/parquet_size_reporter.py` | 0.0% | **LOW** | Deprecate or add tests |
| `src/utils/simple_state.py` | 0.0% | **LOW** | Deprecate or add tests |

## 🔧 Coverage Configuration

The `.coveragerc` file includes proper exclusions:

```ini
[run]
branch = True
source = src
omit =
    tests/*
    docs/*
    tools/*
    scripts/*
    */migrations/*
    */generated/*
    */vendor/*
    */third_party/*
    setup.py
    */__main__.py
    src/*/experimental/*
    src/*/legacy/*

[report]
skip_covered = True
show_missing = True
precision = 1
```

## 📈 CI Integration

The `coverage.xml` file is included for CI integration:

```yaml
# Example CI workflow
- name: Coverage Gate
  run: |
    pytest --cov=src --cov-branch --cov-report=xml
    coverage report --fail-under=75 --show-missing
```

## 🎯 Next Steps

1. **Review the testing plan** in `TESTING_PLAN_README.md`
2. **Generate HTML report** locally using instructions above
3. **Identify specific gaps** in critical modules
4. **Address adversarial false positives** - See [Phase2.0.2-Adversarial-FalsePositives.md](docs/plans/Phase2.0.2-Adversarial-FalsePositives.md) for detailed plan
5. **Begin Phase 1 implementation** after plan approval

## 🚨 Critical Coverage Gaps (Adversarial Testing)

**Priority**: The adversarial false-positive issue requires new similarity scoring logic that must be thoroughly tested:

- **New distractor detection logic** - Requires ≥95% coverage
- **Non-distractor evidence validation** - Requires ≥95% coverage  
- **Penalty application and logging** - Requires ≥95% coverage
- **Configuration validation** - Requires ≥95% coverage

**Impact**: These new code paths are critical for production safety and must be covered before deployment.

---

*This file will be updated as coverage targets are achieved and new insights are gained.*
