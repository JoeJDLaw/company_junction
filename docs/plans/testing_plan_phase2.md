# 🧪 Phase 2.0.0 Testing Plan

**Version**: 1.0  
**Date**: 2025-09-06  
**Status**: Draft - Awaiting Approval  

---

## 📊 Coverage Baseline Report

### Current Coverage Status
- **Overall Coverage**: 45.2% (3,044 lines covered out of 6,703 total)
- **Branch Coverage**: 225 branches partially covered out of 2,130 total
- **Target**: 75% baseline, 90-95% for critical paths

### Critical Coverage Gaps

#### 🚨 **Critical Paths (Target: 90-95%)**
| Module | Current | Target | Priority | Notes |
|--------|---------|--------|----------|-------|
| `src/cleaning.py` | 5.1% | 90% | **CRITICAL** | Main pipeline entry point |
| `src/disposition.py` | 6.9% | 90% | **CRITICAL** | Final decision logic |
| `src/similarity/scoring.py` | 94.4% | 95% | **HIGH** | Already good, minor gaps |
| `src/edge_grouping.py` | 67.8% | 90% | **HIGH** | Grouping algorithm |
| `src/survivorship.py` | 40.1% | 90% | **HIGH** | Primary selection logic |

#### 📈 **Supporting Modules (Target: 75%)**
| Module | Current | Target | Priority | Notes |
|--------|---------|--------|----------|-------|
| `src/alias_matching.py` | 35.0% | 75% | **MEDIUM** | Alias cross-references |
| `src/normalize.py` | 81.4% | 85% | **MEDIUM** | Already good |
| `src/utils/mini_dag.py` | 73.7% | 80% | **MEDIUM** | Resume functionality |
| `src/utils/group_pagination.py` | 31.0% | 75% | **MEDIUM** | UI pagination |

#### 🔧 **Utility Modules (Target: 75%)**
| Module | Current | Target | Priority | Notes |
|--------|---------|--------|----------|-------|
| `src/utils/cache_utils.py` | 30.5% | 75% | **LOW** | Cache management |
| `src/utils/io_utils.py` | 23.8% | 75% | **LOW** | File I/O operations |

### Coverage Exclusions Applied
- ✅ Tests directory (`tests/*`)
- ✅ Documentation (`docs/*`)
- ✅ Tools and scripts (`tools/*`, `scripts/*`)
- ✅ Generated/migration code (`*/migrations/*`, `*/generated/*`)
- ✅ Setup and entry points (`setup.py`, `*/__main__.py`)

---

## 📋 Test Inventory & Categorization

### Current Test Files (58 total)

#### 🧪 **Unit Tests (45 files)**
| Category | Files | Coverage Focus | Status |
|----------|-------|----------------|--------|
| **Similarity & Scoring** | 15 files | Comprehensive | ✅ **EXCELLENT** |
| - `test_similarity*.py` | 8 files | Core similarity logic | ✅ Well covered |
| - `test_scoring*.py` | 7 files | Scoring components | ✅ Well covered |
| **Normalization** | 1 file | Name normalization | ✅ Good |
| **Grouping & Survivorship** | 3 files | Group formation | ⚠️ **NEEDS WORK** |
| **Alias Matching** | 3 files | Alias cross-references | ⚠️ **NEEDS WORK** |
| **Utilities** | 15 files | Various utilities | ⚠️ **MIXED** |
| **CLI & Integration** | 3 files | Command-line interface | ⚠️ **NEEDS WORK** |
| **Linting & Quality** | 5 files | Code quality checks | ✅ Good |

#### 🔗 **Integration Tests (8 files)**
| Test File | Purpose | Status | Coverage |
|-----------|---------|--------|----------|
| `test_mini_dag_resume.py` | Resume functionality | ⚠️ **INCOMPLETE** | Unit-level only |
| `test_mini_dag_resume_contract.py` | Resume contracts | ⚠️ **INCOMPLETE** | Mocked only |
| `test_cleanup_api.py` | Cleanup operations | ✅ Good | API level |
| `test_pipeline_cleanup_cli.py` | CLI cleanup | ✅ Good | CLI level |
| `test_parallel_execution.py` | Parallel processing | ✅ Good | Unit-level |
| `test_group_artifacts_scoped.py` | Artifact management | ✅ Good | Unit-level |
| `test_cleanup_empty_state.py` | Empty state handling | ✅ Good | Unit-level |
| `test_survivorship_equivalence.py` | Survivorship logic | ✅ Good | Unit-level |

#### 🚫 **Missing Test Categories**
- ❌ **End-to-End Tests**: No full pipeline tests with real data
- ❌ **Resume E2E Tests**: No verification of resume behavior with logs/artifacts
- ❌ **Property-Based Tests**: No Hypothesis-based invariant testing
- ❌ **Log Verification Tests**: No tests verifying log output correctness

---

## 🎯 FAQ Traceability Matrix

| FAQ Claim | Current Test Coverage | Status | Required Action |
|-----------|----------------------|--------|-----------------|
| **🔄 Can I resume a failed pipeline run?** | `test_mini_dag_resume*.py` (mocked) | ❌ **INSUFFICIENT** | Add E2E resume tests |
| **📄 What file formats are supported?** | No direct tests | ❌ **MISSING** | Add format detection tests |
| **🗂️ How do I map custom column names?** | `test_schema_*.py` (partial) | ⚠️ **PARTIAL** | Enhance schema tests |
| **🖥️ Can I run the pipeline without the UI?** | No direct tests | ❌ **MISSING** | Add CLI-only tests |
| **🏷️ What's the difference between run types?** | `test_cleanup_*.py` (partial) | ⚠️ **PARTIAL** | Add run type tests |
| **🧹 How do I clean up old runs?** | `test_pipeline_cleanup_cli.py` | ✅ **GOOD** | No action needed |

---

## 🧠 Comprehensive Logic Coverage Strategy

### Similarity & Scoring Logic (Target: 95% coverage + property-based tests)

#### **Realistic Name Combination Testing**
```python
# Golden test cases for realistic scenarios
test_cases = [
    # Should group (high similarity)
    ("Acme Corporation", "Acme Corp", "keep"),
    ("Microsoft Inc", "Microsoft Corporation", "keep"),
    ("Apple LLC", "Apple Inc", "keep"),
    
    # Should not group (low similarity)
    ("Acme Corporation", "Beta Industries", "delete"),
    ("Microsoft Inc", "Apple Inc", "delete"),
    
    # Edge cases (verification needed)
    ("Acme Corp", "Acme LLC", "merge"),  # Different suffixes
    ("99 Cents Store", "99¢ Store", "keep"),  # Unicode variants
]
```

#### **Property-Based Testing with Hypothesis**
```python
@given(st.text(min_size=1, max_size=50))
@settings(deadline=None, max_examples=200, derandomize=True)
def test_scoring_symmetry(name1, name2):
    """Score(A,B) should equal Score(B,A)"""
    score_ab = compute_similarity_score(name1, name2)
    score_ba = compute_similarity_score(name2, name1)
    assert abs(score_ab - score_ba) < 0.001

@given(st.text(min_size=1, max_size=50))
def test_scoring_idempotence(name):
    """Score(A,A) should always be 100"""
    score = compute_similarity_score(name, name)
    assert score == 100.0
```

### Disposition Logic Validation (Target: 90% coverage)

#### **Decision Tree Testing**
```python
def test_disposition_decision_tree():
    """Test all disposition paths with realistic scenarios"""
    # Test keep scenarios
    assert get_disposition(score=95, group_size=1) == "keep"
    assert get_disposition(score=85, group_size=2, is_primary=True) == "keep"
    
    # Test merge scenarios  
    assert get_disposition(score=75, group_size=3, is_primary=False) == "merge"
    
    # Test delete scenarios
    assert get_disposition(score=45, group_size=1) == "delete"
```

### Edge Scoring vs Similarity Scoring Correctness

#### **Algorithm Consistency Testing**
```python
def test_edge_vs_similarity_scoring_consistency():
    """Edge scoring and similarity scoring should be consistent"""
    pairs = generate_realistic_pairs()
    for pair in pairs:
        edge_score = compute_edge_score(pair)
        similarity_score = compute_similarity_score(pair)
        
        # Both should agree on grouping decision
        edge_decision = edge_score >= EDGE_THRESHOLD
        similarity_decision = similarity_score >= SIMILARITY_THRESHOLD
        assert edge_decision == similarity_decision
```

---

## 🚀 Integration & E2E Testing Plan

### Resume Functionality E2E Tests

#### **Test Scenarios**
1. **Full Pipeline Resume**
   ```python
   def test_resume_from_intermediate_stage():
       """Test resuming from grouping stage"""
       # Run pipeline to grouping stage
       run_pipeline(input_file, stop_at="grouping")
       
       # Verify intermediate artifacts exist
       assert exists("data/interim/run_id/candidate_pairs.parquet")
       
       # Resume from grouping
       run_pipeline(input_file, resume_from="grouping")
       
       # Verify logs show resume behavior
       assert "Auto-resume decision: resume_from='grouping'" in logs
       assert "SMART_DETECT" in logs
       
       # Verify artifacts were reused, not regenerated
       assert file_mtime("data/interim/run_id/candidate_pairs.parquet") == original_mtime
   ```

2. **Auto-Resume Detection**
   ```python
   def test_auto_resume_detection():
       """Test automatic resume point detection"""
       # Run partial pipeline
       run_pipeline(input_file, stop_at="survivorship")
       
       # Run again without resume flag
       run_pipeline(input_file)  # Should auto-detect resume point
       
       # Verify auto-resume behavior
       assert "Auto-resume decision: resume_from='disposition'" in logs
   ```

3. **Output Consistency**
   ```python
   def test_resume_output_consistency():
       """Resumed run should produce identical output to fresh run"""
       # Fresh run
       run_pipeline(input_file)
       fresh_output = read_output_files()
       
       # Partial run + resume
       run_pipeline(input_file, stop_at="grouping")
       run_pipeline(input_file, resume_from="grouping")
       resumed_output = read_output_files()
       
       # Should be identical
       assert_dataframes_equal(fresh_output, resumed_output)
   ```

### Real Data Integration Tests

#### **Test Data Strategy**
- **Golden datasets**: `tests/data/companies_small.csv` (20-50 rows)
- **Edge case datasets**: `tests/data/companies_edge.csv` (suffixes, unicode, etc.)
- **Faker generator**: Deterministic seed for property-based tests

#### **E2E Test Scenarios**
1. **Full Pipeline with Real CSV**
2. **Full Pipeline with Real XLSX**
3. **Column Mapping with Custom Names**
4. **Large Dataset Performance**

---

## 📈 Proposed Test Categories

### 1. **Unit Tests** (Enhance existing)
- **Similarity & Scoring**: Add property-based tests with Hypothesis
- **Disposition Logic**: Add comprehensive decision tree tests
- **Edge Grouping**: Add realistic grouping scenario tests
- **Utilities**: Improve coverage for critical utilities

### 2. **Integration Tests** (Expand existing)
- **Resume Functionality**: Add E2E resume tests with log verification
- **Schema Resolution**: Add custom column mapping tests
- **File Format Support**: Add CSV/XLSX detection tests
- **Run Type Management**: Add run type categorization tests

### 3. **End-to-End Tests** (New category)
- **Full Pipeline**: Test complete pipeline with real data
- **Resume E2E**: Test resume behavior with logs and artifacts
- **CLI-Only Mode**: Test pipeline without UI
- **Performance**: Test with realistic dataset sizes

### 4. **Property-Based Tests** (New category)
- **Scoring Invariants**: Symmetry, monotonicity, idempotence
- **Normalization Equivalence**: "Corp." vs "Corporation"
- **Threshold Boundaries**: Disposition decision boundaries
- **Edge Cases**: Realistic name variations

### 5. **Log Verification Tests** (New category)
- **Resume Logs**: Verify correct resume decision logging
- **Backend Selection**: Verify backend choice logging
- **Error Handling**: Verify error message logging
- **Progress Reporting**: Verify progress log format

---

## 🎯 Coverage Targets & Risk Rationale

### **Project Baseline: ≥75%** (Lines & Branches)
- **Rationale**: Salesforce minimum standard
- **Focus**: All runtime code paths
- **Exclusions**: Tests, docs, tools, generated code

### **Critical Paths: 90-95%** (Lines & Branches)
- **Rationale**: Business logic must be bulletproof
- **Modules**: `cleaning.py`, `disposition.py`, `edge_grouping.py`, `survivorship.py`
- **Focus**: Decision-making logic, not trivial utilities

### **Similarity Module: ≥90%** (Lines & Branches) + Property-Based Tests
- **Rationale**: Core algorithm must be mathematically sound
- **Focus**: Scoring logic, grouping decisions, edge cases
- **Additional**: Hypothesis-based invariant testing

---

## 🔄 Recommendations

### **Incremental Improvement Approach** ✅
**Rationale**: Aligns with `cursor_rules.md` and reduces risk

**Pros**:
- Preserves existing valuable tests
- Lower risk of regression
- Maintains test history and context
- Easier to review and validate

**Cons**:
- May retain some technical debt
- Slower initial progress

**Action Plan**:
1. **Phase 1**: Fix critical gaps (cleaning.py, disposition.py)
2. **Phase 2**: Add missing test categories (E2E, property-based)
3. **Phase 3**: Enhance existing tests (similarity, grouping)
4. **Phase 4**: Improve utility coverage

### **Alternative: Start Fresh** ❌
**Rationale**: Not recommended due to high risk and rule conflicts

**Pros**:
- Clean slate approach
- No legacy test baggage

**Cons**:
- High risk of losing valuable test cases
- Violates `cursor_rules.md` incremental approach
- Significant time investment
- Potential for introducing new bugs

---

## ⚖️ Rule Conflicts & Deviations

### **No Conflicts Identified** ✅
The testing plan aligns perfectly with `cursor_rules.md`:

- **Rule 6 (Test Coverage)**: Covers all required areas
- **Rule 7 (Tooling & CI)**: Maintains pytest requirements  
- **Rule 8 (Change Management)**: Supports incremental improvement
- **Rule 10 (Deprecation)**: No file deletions planned

### **Potential Future Considerations**
- **Mutation Testing**: May require additional tooling setup
- **Property-Based Testing**: May need Hypothesis configuration
- **E2E Tests**: May require test data management strategy

---

## 📋 Plan Summary (Repeat-Back)

### **Execution Checklist**

#### **Phase 1: Critical Gap Fixes** (Week 1)
- [ ] Add E2E resume tests with log verification
- [ ] Add file format detection tests (CSV/XLSX)
- [ ] Add CLI-only mode tests
- [ ] Improve disposition.py coverage to 90%
- [ ] Improve cleaning.py coverage to 90%

#### **Phase 2: Missing Test Categories** (Week 2)
- [ ] Set up Hypothesis for property-based testing
- [ ] Add scoring invariant tests (symmetry, monotonicity)
- [ ] Add normalization equivalence tests
- [ ] Add threshold boundary tests
- [ ] Create golden test datasets

#### **Phase 3: Enhanced Logic Coverage** (Week 3)
- [ ] Add realistic name combination tests
- [ ] Add edge scoring vs similarity scoring consistency tests
- [ ] Add comprehensive disposition decision tree tests
- [ ] Add grouping vs non-grouping scenario tests
- [ ] Improve edge_grouping.py coverage to 90%

#### **Phase 4: Utility & Polish** (Week 4)
- [ ] Improve utility module coverage to 75%
- [ ] Add log verification tests
- [ ] Add performance tests with realistic data
- [ ] Set up mutation testing for critical modules
- [ ] Final coverage validation and reporting

### **Estimated Effort**
- **Total Time**: 4 weeks
- **Critical Paths**: 2 weeks
- **Enhancement**: 2 weeks
- **Validation**: Ongoing

### **Success Criteria**
- [ ] Overall coverage ≥75%
- [ ] Critical paths ≥90%
- [ ] Similarity module ≥90% + property-based tests
- [ ] All FAQ claims backed by tests
- [ ] E2E resume functionality verified
- [ ] Property-based tests for scoring invariants

---

## 🚦 Approval Gate

**This plan is ready for review and approval.**

**Next Steps**:
1. Review this comprehensive testing plan
2. Provide feedback on priorities and approach
3. Approve with explicit "APPROVED: plan v1" response
4. Begin implementation with Phase 1

**No code changes will be made until explicit approval is received.**

---

## 📋 Addendum: Execution Details

### 🔧 Environment Configuration

#### **Dependencies**
- ✅ `hypothesis` - Property-based testing framework
- ✅ `mutmut` - Mutation testing tool  
- ✅ `faker` - Test data generation
- ✅ `.coveragerc` - Branch coverage with sensible exclusions (use as-is)

#### **Coverage Configuration**
```bash
# Use existing .coveragerc (do not modify)
pytest --cov=src --cov-branch --cov-report=xml --cov-report=html
```

### 🔍 Resume Log Assertions (Exact Patterns)

#### **Structured Log Patterns**
```python
# Auto-resume decision logging
resume_patterns = {
    "auto_resume_detected": r"Auto-resume decision: resume_from='(\w+)' \| last_completed='(\w+)' \| input_hash=PASS \| reason=SMART_DETECT",
    "auto_resume_failed": r"Auto-resume decision: input_hash=FAIL - forcing full run due to input/config changes \| reason=HASH_MISMATCH",
    "no_previous_run": r"Auto-resume decision: no previous run found - starting fresh \| reason=NO_PREVIOUS_RUN",
    "manual_resume": r"Auto-resume decision: resume_from='(\w+)' \| reason=MANUAL_SPECIFIED",
    "no_resume_flag": r"Auto-resume decision: --no-resume specified - forcing full run \| reason=NO_RESUME_FLAG"
}

# Stage execution logging
stage_patterns = {
    "stage_skipped": r"Stage '(\w+)' already completed - skipping",
    "stage_started": r"Starting stage: (\w+)",
    "stage_completed": r"Completed stage: (\w+)"
}
```

#### **Test Assertions**
```python
def test_resume_log_assertions():
    """Verify exact log patterns for resume behavior"""
    # Run pipeline with resume
    run_pipeline(input_file, resume_from="grouping")
    
    # Assert specific log patterns
    assert_log_contains("Auto-resume decision: resume_from='grouping'")
    assert_log_contains("reason=MANUAL_SPECIFIED")
    assert_log_contains("Stage 'normalization' already completed - skipping")
    assert_log_contains("Stage 'filtering' already completed - skipping")
```

### 📁 Artifact Reuse Verification

#### **Authoritative Artifacts**
```python
# Primary artifacts to verify reuse
AUTHORITATIVE_ARTIFACTS = {
    "candidate_pairs.parquet": "data/interim/{run_id}/candidate_pairs.parquet",
    "groups.parquet": "data/interim/{run_id}/groups.parquet", 
    "survivors.parquet": "data/interim/{run_id}/survivors.parquet",
    "pipeline_state.json": "data/interim/{run_id}/pipeline_state.json"
}

def verify_artifact_reuse(run_id: str, stage: str):
    """Verify artifacts were reused, not regenerated"""
    for artifact_name, artifact_path in AUTHORITATIVE_ARTIFACTS.items():
        if should_exist_before_stage(artifact_name, stage):
            path = artifact_path.format(run_id=run_id)
            
            # Check file exists
            assert os.path.exists(path), f"Artifact {artifact_name} missing"
            
            # Verify content hash (not just mtime)
            original_hash = get_file_content_hash(path)
            # ... run pipeline ...
            new_hash = get_file_content_hash(path)
            
            assert original_hash == new_hash, f"Artifact {artifact_name} was regenerated"
            
            # Verify file size + mtime combo
            original_stat = os.stat(path)
            # ... run pipeline ...
            new_stat = os.stat(path)
            
            assert original_stat.st_size == new_stat.st_size, f"Artifact {artifact_name} size changed"
            assert original_stat.st_mtime == new_stat.st_mtime, f"Artifact {artifact_name} mtime changed"
```

### 📊 Golden Datasets Schema

#### **Location & Naming**
```
tests/data/
├── companies_small.csv      # 20-50 rows, curated pairs/near-pairs/non-pairs
├── companies_edge.csv       # Edge cases: suffixes, punctuation, unicode, acronyms
└── companies_generated/     # Faker-generated datasets (deterministic seed)
    ├── companies_1000.csv
    └── companies_10000.csv
```

#### **Schema: companies_small.csv**
```csv
account_id,account_name,expected_group_id,expected_disposition,test_category
"001Hs000054S8kI","Acme Corporation","group_1","keep","should_group"
"001Hs000054SAQt","Acme Corp","group_1","keep","should_group"
"001Hs000054S8kI2","Acme LLC","group_1","keep","should_group"
"001Hs000054S8kI3","Beta Industries","group_2","keep","should_not_group"
"001Hs000054S8kI4","Microsoft Inc","group_3","keep","suffix_variants"
"001Hs000054S8kI5","Microsoft Corporation","group_3","keep","suffix_variants"
```

#### **Schema: companies_edge.csv**
```csv
account_id,account_name,expected_group_id,expected_disposition,test_category
"001Hs000054S8kI6","99 Cents Store","group_4","keep","unicode_variants"
"001Hs000054S8kI7","99¢ Store","group_4","keep","unicode_variants"
"001Hs000054S8kI8","AT&T Inc","group_5","keep","special_chars"
"001Hs000054S8kI9","AT and T Inc","group_5","keep","special_chars"
"001Hs000054S8kI10","IBM","group_6","keep","acronyms"
"001Hs000054S8kI11","International Business Machines","group_6","keep","acronyms"
```

### 🎲 Deterministic Test Strategy

#### **pytest.ini Configuration**
```ini
[tool:pytest]
# Deterministic testing
addopts = --strict-markers --strict-config
markers =
    hypothesis: marks tests as property-based (deselect with '-m "not hypothesis"')
    integration: marks tests as integration tests
    e2e: marks tests as end-to-end tests

# Hypothesis settings
hypothesis_settings = hypothesis.settings(deadline=None, max_examples=200, derandomize=True, database=None)
```

#### **conftest.py Seed Strategy**
```python
import pytest
import random
import numpy as np
from hypothesis import settings, seed

# Global deterministic seed
DETERMINISTIC_SEED = 42

@pytest.fixture(autouse=True)
def set_deterministic_seed():
    """Set deterministic seed for all tests"""
    random.seed(DETERMINISTIC_SEED)
    np.random.seed(DETERMINISTIC_SEED)
    yield
    # Reset after test
    random.seed()
    np.random.seed()

# Hypothesis settings for all property-based tests
settings.register_profile("deterministic", 
    deadline=None, 
    max_examples=200, 
    derandomize=True,
    database=None
)
settings.load_profile("deterministic")
```

### 🚦 CI Coverage Gates

#### **Overall Coverage Gate**
```yaml
# .github/workflows/coverage.yml
- name: Coverage Gate
  run: |
    pytest --cov=src --cov-branch --cov-report=xml --cov-report=term-missing:skip-covered
    coverage report --fail-under=75 --show-missing
```

#### **Critical Modules Gate (Post-Phase 2)**
```yaml
- name: Critical Modules Coverage
  run: |
    # Check individual critical modules
    coverage report --include="src/cleaning.py" --fail-under=90
    coverage report --include="src/disposition.py" --fail-under=90  
    coverage report --include="src/edge_grouping.py" --fail-under=90
    coverage report --include="src/survivorship.py" --fail-under=90
```

#### **Commands & Artifacts**
```bash
# Generate coverage reports
pytest --cov=src --cov-branch --cov-report=xml --cov-report=html --cov-report=term-missing:skip-covered

# Coverage artifacts
coverage.xml          # For CI integration
htmlcov/              # Local HTML report (don't commit)
coverage_term.txt     # Terminal report
```

### 🧬 Mutation Testing Scope

#### **Initial Scope**
```bash
# Target modules only
mutmut run --paths-to-mutate=src/similarity/,src/disposition.py

# Results interpretation
mutmut results
# Track surviving mutants, no hard gate in Phase 2
# Focus on killing mutants in scoring and disposition logic
```

#### **Success Criteria**
- **Phase 2**: Track surviving mutants, aim for <20% survival rate
- **Phase 3**: Consider hard gate if survival rate >10%

### 📋 FAQ Traceability Matrix (Expanded)

| FAQ Claim | Test ID | Status | Implementation |
|-----------|---------|--------|----------------|
| **🔄 Can I resume a failed pipeline run?** | `test_resume_e2e_full_pipeline()` | ❌ Missing | `tests/test_resume_e2e.py` |
| | `test_auto_resume_detection()` | ❌ Missing | `tests/test_resume_e2e.py` |
| | `test_resume_log_verification()` | ❌ Missing | `tests/test_resume_e2e.py` |
| **📄 What file formats are supported?** | `test_csv_format_detection()` | ❌ Missing | `tests/test_file_formats.py` |
| | `test_xlsx_format_detection()` | ❌ Missing | `tests/test_file_formats.py` |
| | `test_xls_format_detection()` | ❌ Missing | `tests/test_file_formats.py` |
| **🗂️ How do I map custom column names?** | `test_schema_resolver_custom_mapping()` | ⚠️ Partial | `tests/test_schema_resolver.py` |
| | `test_cli_column_overrides()` | ❌ Missing | `tests/test_cli_integration.py` |
| **🖥️ Can I run the pipeline without the UI?** | `test_cli_only_mode()` | ❌ Missing | `tests/test_cli_integration.py` |
| | `test_output_files_generated()` | ❌ Missing | `tests/test_cli_integration.py` |
| **🏷️ What's the difference between run types?** | `test_run_type_categorization()` | ⚠️ Partial | `tests/test_cleanup_api.py` |
| | `test_run_type_cleanup_behavior()` | ⚠️ Partial | `tests/test_pipeline_cleanup_cli.py` |
| **🧹 How do I clean up old runs?** | `test_cleanup_list_command()` | ✅ Good | `tests/test_pipeline_cleanup_cli.py` |
| | `test_cleanup_delete_commands()` | ✅ Good | `tests/test_pipeline_cleanup_cli.py` |

### 🔄 PR Workflow & Naming

#### **Branch Naming Convention**
```
tests/phase2/<topic>
Examples:
- tests/phase2/resume-e2e
- tests/phase2/property-based-scoring  
- tests/phase2/golden-datasets
- tests/phase2/coverage-gates
```

#### **PR Checklist**
- [ ] Golden datasets updated (if applicable)
- [ ] Deterministic seeds configured
- [ ] Coverage gates passing (≥75% overall)
- [ ] Critical modules coverage (≥90% if applicable)
- [ ] FAQ traceability links updated
- [ ] Property-based tests deterministic
- [ ] E2E tests use realistic data
- [ ] Log assertions use exact patterns
- [ ] Artifact reuse verification implemented

### ⚖️ Edge vs Similarity Decision Validation

#### **Consistency Boundary Testing**
```python
def test_edge_vs_similarity_consistency():
    """Validate consistency between edge and similarity scoring"""
    
    # Canonical cases that should NOT group despite high lexical overlap
    non_grouping_cases = [
        ("Apple Inc", "Apple Computer Inc", "Different business focus"),
        ("Microsoft Corporation", "Microsoft Research", "Different entity types"),
        ("Google LLC", "Google Cloud", "Parent vs subsidiary")
    ]
    
    for name1, name2, reason in non_grouping_cases:
        edge_score = compute_edge_score(name1, name2)
        similarity_score = compute_similarity_score(name1, name2)
        
        # Both should agree on non-grouping decision
        edge_decision = edge_score >= EDGE_THRESHOLD
        similarity_decision = similarity_score >= SIMILARITY_THRESHOLD
        
        # Should not group despite lexical similarity
        assert not edge_decision, f"Edge scoring incorrectly groups: {name1} vs {name2} ({reason})"
        assert not similarity_decision, f"Similarity scoring incorrectly groups: {name1} vs {name2} ({reason})"
        
        # Disposition should resolve to "delete" or "keep" (not "merge")
        disposition = get_disposition(edge_score, similarity_score, group_size=1)
        assert disposition in ["keep", "delete"], f"Unexpected disposition: {disposition} for {name1} vs {name2}"
```

#### **Decision Resolution Logic**
```python
def test_disposition_resolution_logic():
    """Test how disposition resolves edge vs similarity disagreements"""
    
    # Case 1: High similarity, low edge (should be "keep" - singleton)
    high_sim_low_edge = get_disposition(edge_score=60, similarity_score=90, group_size=1)
    assert high_sim_low_edge == "keep"
    
    # Case 2: Low similarity, high edge (should be "merge" - in group)
    low_sim_high_edge = get_disposition(edge_score=85, similarity_score=70, group_size=3)
    assert low_sim_high_edge == "merge"
    
    # Case 3: Both low (should be "delete")
    both_low = get_disposition(edge_score=50, similarity_score=50, group_size=1)
    assert both_low == "delete"
```

### 📊 Baseline Verification Results

#### **Coverage HTML Artifact**
```bash
# Generate HTML coverage report
pytest --cov=src --cov-branch --cov-report=html

# HTML report location
htmlcov/index.html  # Main coverage report
htmlcov/src/        # Per-module coverage details
```

#### **Files with <10% Coverage (Triage-Now)**
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

---

*This document will be updated as the testing plan evolves and new insights are gained.*
