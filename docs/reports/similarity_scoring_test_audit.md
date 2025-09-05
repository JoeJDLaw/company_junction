# Similarity Scoring Test Audit Report

**Date**: 2025-09-05  
**Purpose**: Comprehensive audit of existing similarity scoring tests to identify gaps and requirements for complete test coverage

## Executive Summary

The similarity scoring system has **partial test coverage** but is missing several critical test categories. We have good coverage of basic functionality and some edge cases, but lack comprehensive testing of:

1. **Base signal correctness** (token_sort_ratio, token_set_ratio, Jaccard behavior)
2. **Penalty system** (suffix, numeric-style, punctuation mismatches)
3. **Bulk vs parallel parity** (gate behavior and numerical consistency)
4. **Edge case robustness** (Unicode, empty tokens, whitespace variants)
5. **Configuration-driven behavior** (penalty toggles, normalization settings)

## Current Test Coverage Analysis

### ✅ **Well Covered Areas**

#### **Basic Integration Tests**
- `test_similarity_fix.py`: 99 Cents grouping scenarios, enhanced normalization
- `test_similarity_fixes.py`: Lowercasing, allowlisted bigrams, duplicate deduplication
- `test_similarity_improvements.py`: Safety rails, sharding, strategy logging
- `test_similarity_refactor.py`: No mutation, sort order, stop tokens
- `test_similarity_scores_columns.py`: Output column structure

#### **Blocking & Candidate Generation**
- Allowlist/denylist behavior
- Sharding safety rails
- Bigram prepass
- Duplicate pair prevention

#### **Basic Scoring Components**
- Some `compute_score_components` usage
- Basic penalty application (suffix mismatch)
- Enhanced normalization integration

### ❌ **Missing Critical Test Areas**

#### **A. Base Signal Correctness** (0% coverage)
- **token_sort_ratio sensitivity**: No tests for order-insensitive behavior
- **token_set_ratio resilience**: No tests for subset handling
- **Jaccard with enhanced tokens**: Limited testing of weak token removal
- **Plural→singular mapping**: No dedicated tests
- **Canonical retail terms**: No dedicated tests

#### **B. Penalty System** (20% coverage)
- **Suffix mismatch**: Basic test exists but limited scenarios
- **Numeric-style mismatch**: No tests for digit pattern differences
- **Punctuation mismatch**: One basic test, no comprehensive coverage
- **Penalty boolean flags**: No verification of flag setting

#### **C. Bulk vs Parallel Parity** (10% coverage)
- **Gate behavior**: One basic test, no comprehensive gate testing
- **Numerical consistency**: No tests ensuring bulk ≈ parallel scores
- **Gate cutoff accuracy**: No tests around cutoff boundaries

#### **D. Edge Case Robustness** (5% coverage)
- **Missing suffix_class**: No tests for "NONE" default handling
- **Empty/short names**: No tests for edge cases
- **Unicode handling**: No tests for smart quotes, different hyphen types
- **Whitespace variants**: No tests for multiple spaces, leading/trailing

#### **E. Determinism & Sorting** (30% coverage)
- **Deterministic outputs**: Basic test exists
- **Sort order contract**: Basic test exists
- **No DataFrame mutation**: Test exists

#### **F. Configuration-Driven Behavior** (0% coverage)
- **Penalty value changes**: No tests for config-driven penalty adjustments
- **Normalization toggles**: No tests for enable/disable flags
- **Threshold usage**: No tests for medium threshold filtering

#### **G. Column Safety** (50% coverage)
- **String dtypes**: Basic test exists
- **No mutation**: Test exists

#### **H. Integration with Blocking** (40% coverage)
- **Realistic pairs**: Some integration tests exist
- **Expected outcomes**: Limited coverage

## Detailed Gap Analysis

### **Missing Test Files Needed**

1. **`tests/test_scoring_components.py`**
   - Base signal correctness tests
   - Enhanced normalization behavior
   - Jaccard token filtering

2. **`tests/test_scoring_penalties.py`**
   - Comprehensive penalty system tests
   - Boolean flag verification
   - Exact penalty application

3. **`tests/test_scoring_bulk_parity.py`**
   - Bulk vs parallel numerical consistency
   - Gate behavior around cutoff
   - Performance path verification

4. **`tests/test_scoring_robustness.py`**
   - Edge cases (Unicode, empty, whitespace)
   - Missing suffix handling
   - Hyphenation variants

5. **`tests/test_scoring_contracts.py`**
   - Determinism verification
   - Sort order contracts
   - No mutation guarantees

6. **`tests/test_scoring_config_toggles.py`**
   - Configuration-driven behavior
   - Penalty value changes
   - Normalization setting toggles

### **Specific Test Cases Missing**

#### **Base Signal Tests**
```python
# token_sort_ratio sensitivity
("acme holdings", "holdings acme") → high token_sort
("acme shop", "acme store") → lower token_sort vs set

# token_set_ratio resilience  
("acme store", "acme store west") → high token_set (subset)
("acme", "acme the store") → token_set stays high despite "the"

# Jaccard with enhanced tokens
("99 cents only store", "99 cents store") → Jaccard=1.0
("stores", "store") → tokens equal after normalization
("shop", "store") → mapped to same canonical token
```

#### **Penalty Tests**
```python
# Suffix mismatch
("acme inc", "acme llc") → exact penalty subtraction

# Numeric-style mismatch
("7 eleven", "seven eleven") → penalty applied
("7 eleven 123", "7 eleven") → penalty applied

# Punctuation mismatch
("7-eleven", "7 eleven") → penalty applied
("bob's", "bobs") → penalty applied
```

#### **Bulk vs Parallel Tests**
```python
# Gate behavior
pairs around gate_cutoff (70-75) → correct gating
bulk results ≈ parallel results within tolerance

# Gate correctness
pairs below cutoff → never in bulk results
pairs above cutoff → present and scored
```

#### **Edge Case Tests**
```python
# Missing suffix_class
("acme", "acme") with suffix_class=None → defaults to "NONE"

# Unicode handling
("7–eleven", "7-eleven") → consistent behavior
("bob's", "bob's") → smart quotes handled

# Whitespace variants
("  acme  ", "acme") → normalized correctly
```

## Recommendations

### **Immediate Actions**

1. **Create missing test files** following the suggested structure
2. **Implement comprehensive base signal tests** for all scoring components
3. **Add penalty system tests** with exact numerical verification
4. **Build bulk vs parallel parity tests** for numerical consistency
5. **Add edge case robustness tests** for production readiness

### **Test Quality Standards**

- **Exact assertions** for deterministic behavior (penalties, sorting)
- **Range/tolerance assertions** for RapidFuzz ratios
- **Presence/absence assertions** for gate behavior
- **No mutation verification** for input DataFrames
- **Configuration-driven testing** for all tunable parameters

### **Coverage Targets**

- **Lines in `compute_score_components()`**: All branches covered
- **Penalty application logic**: All penalty types tested
- **Bulk vs parallel paths**: Both execution paths verified
- **Enhanced normalization**: All normalization features tested
- **Edge cases**: Unicode, empty, whitespace scenarios covered

## Conclusion

The current test suite provides **good foundation coverage** but lacks the **comprehensive, production-ready testing** needed for a critical similarity scoring system. The missing tests represent **significant risk** for:

- **Numerical accuracy** (penalty calculations, bulk vs parallel consistency)
- **Edge case handling** (Unicode, empty inputs, configuration changes)
- **Production robustness** (determinism, performance path correctness)

**Priority**: **HIGH** - These tests should be implemented before any production deployment of the similarity scoring system.

---

**Next Steps**: 
1. Review this audit with the team
2. Prioritize test implementation based on risk assessment
3. Implement tests following the suggested structure
4. Verify 100% coverage of critical scoring paths
