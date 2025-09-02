# Cursor Rules for Company Junction

## Core Principles

### 1. **No Hardcoded Values**
- **Sort Options Must Be Centralized**: All sort key mappings must use a single source of truth (e.g., `get_order_by()` helper function)
- **No Per-Function Maps**: Avoid duplicate sort key mappings in different functions
- **Default Values From Config**: Default sorts, fallbacks, and preferences must come from configuration files, not hardcoded values
- **Cache Keys Must Include Source**: Cache keys must include data source (stats vs review_ready) and backend information

### 2. **Configuration-Driven Behavior**
- **UI Sort Defaults**: Default sort behavior must be configurable via `config/settings.yaml`
- **Backend Selection**: Backend preferences must be configurable, not hardcoded
- **Fallback Logic**: Error handling and fallbacks must use configuration values
- **Performance Thresholds**: All performance thresholds must be configurable

### 3. **Unified Logging & Debugging**
- **Distinct Path Logging**: Each backend path must have unique, unambiguous log messages
- **Sort Key Tracking**: Every function call must log `sort_key='...' | order_by='...' | backend=...`
- **Fallback Transparency**: Explicit logging of fallback reasons and resulting paths
- **Cache Key Visibility**: Log cache key components including source and backend

### 4. **Data Source Consistency**
- **Parquet Fingerprinting**: Separate fingerprints for different data sources to prevent cache mixing
- **Source-Aware Caching**: Cache keys must distinguish between stats and review_ready data
- **Backend Consistency**: Same sort key mapping must work across all backend implementations
- **Schema Validation**: Ensure constants resolve correctly across all data sources

## Implementation Standards

### **Sort Key Mapping**
```python
# ✅ CORRECT: Centralized helper function
def get_order_by(sort_key: str) -> str:
    order_by_map = {
        "Account Name (Asc)": f"{PRIMARY_NAME} ASC",
        "Account Name (Desc)": f"{PRIMARY_NAME} DESC",
        # ... other mappings
    }
    # Use config default, not hardcoded fallback
    return order_by_map.get(sort_key, config.get("ui.sort.default"))

# ❌ WRONG: Per-function hardcoded maps
def some_function():
    if "Group Size" in sort_key:  # Hardcoded logic
        order_by = "group_size DESC"  # Hardcoded fallback
```

### **Cache Key Generation**
```python
# ✅ CORRECT: Include source and backend
def build_cache_key(..., source: str, backend: str) -> str:
    key_components = [
        run_id, source, backend, parquet_fingerprint,
        sort_key, page, page_size, filters_signature
    ]

# ❌ WRONG: Missing source/backend information
def build_cache_key(...) -> str:
    key_components = [run_id, sort_key, page, page_size]  # Missing source
```

### **Configuration Structure**
```yaml
# ✅ CORRECT: Configurable defaults
ui:
  sort:
    default: "group_size DESC"  # Configurable default
  use_duckdb_for_groups: false  # Configurable preference

# ❌ WRONG: Hardcoded in code
DEFAULT_SORT = "group_size DESC"  # Hardcoded constant
```

### **Logging Standards**
```python
# ✅ CORRECT: Distinct, unambiguous messages
logger.info(f"groups_page_from_stats_duckdb | sort_key='{sort_key}' | order_by='{order_by}' | backend=duckdb")
logger.info(f"groups_page_duckdb | sort_key='{sort_key}' | order_by='{order_by}' | backend=duckdb")

# ❌ WRONG: Ambiguous, generic messages
logger.info(f"DuckDB query built | order_by='{order_by}'")  # Which function?
```

## Testing Requirements

### **Sort Key Mapping Tests**
- Verify all sort keys map to correct ORDER BY clauses
- Test unknown sort keys log errors and use config defaults
- Ensure same mapping works across all backend implementations

### **Cache Key Tests**
- Verify cache keys differ when source differs (stats vs review_ready)
- Confirm cache keys include backend information
- Test cache key changes when sort, filters, or page changes

### **Configuration Tests**
- Verify default sort comes from config, not hardcoded values
- Test fallback behavior uses configuration values
- Ensure all performance thresholds are configurable

### **Logging Tests**
- Verify distinct log messages for each backend path
- Confirm sort key and ORDER BY logging in all functions
- Test fallback reason logging and path transparency

## Compliance Checklist

- [ ] **No hardcoded sort key mappings** - All sorting uses centralized helper
- [ ] **No hardcoded default values** - All defaults come from configuration
- [ ] **No hardcoded fallback logic** - All fallbacks use config values
- [ ] **Cache keys include source and backend** - No cache mixing between sources
- [ ] **Distinct logging for each path** - Clear identification of backend selection
- [ ] **Sort key tracking in all functions** - Consistent logging format
- [ ] **Configuration-driven behavior** - No hardcoded preferences or thresholds
- [ ] **Unified sort key mapping** - Same behavior across all backends
- [ ] **Source-aware caching** - Separate fingerprints for different data sources
- [ ] **Constants properly resolved** - All imports and references work correctly