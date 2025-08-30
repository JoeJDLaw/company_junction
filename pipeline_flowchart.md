# Company Junction Deduplication Pipeline - Complete Flowchart

## System Architecture Overview

```mermaid
graph TB
    %% Entry Points
    CLI[src/cleaning.py<br/>main()] --> RUN_PIPELINE[run_pipeline()]
    STREAMLIT[app/main.py<br/>main()] --> LOAD_REVIEW[load_review_data()]
    
    %% Main Pipeline Flow
    RUN_PIPELINE --> LOAD_DATA[load_salesforce_data()]
    LOAD_DATA --> VALIDATE[validate_required_columns()]
    VALIDATE --> NORMALIZE[normalize_dataframe()]
    NORMALIZE --> SIMILARITY[pair_scores()]
    SIMILARITY --> GROUPING[create_groups_with_edge_gating()]
    GROUPING --> SURVIVORSHIP[select_primary_records()]
    SURVIVORSHIP --> MERGE_PREVIEW[generate_merge_preview()]
    MERGE_PREVIEW --> DISPOSITION[apply_dispositions()]
    DISPOSITION --> ALIAS_MATCHING[compute_alias_matches()]
    ALIAS_MATCHING --> FINAL_OUTPUT[Create review_ready.csv]
    
    %% Data Flow
    LOAD_DATA --> |CSV/Excel| RAW_DATA[data/raw/]
    NORMALIZE --> |Parquet| NORM_DATA[data/interim/accounts_normalized.parquet]
    SIMILARITY --> |Parquet| PAIRS_DATA[data/interim/candidate_pairs.parquet]
    GROUPING --> |Parquet| GROUPS_DATA[data/interim/groups.parquet]
    SURVIVORSHIP --> |Parquet| SURV_DATA[data/interim/survivorship.parquet]
    DISPOSITION --> |Parquet| DISP_DATA[data/interim/dispositions.parquet]
    ALIAS_MATCHING --> |Parquet| ALIAS_DATA[data/interim/alias_matches.parquet]
    FINAL_OUTPUT --> |CSV/Parquet| PROCESSED_DATA[data/processed/review_ready.*]
    
    %% Configuration
    CONFIG[config/settings.yaml] --> RUN_PIPELINE
    RANKS[config/relationship_ranks.csv] --> SURVIVORSHIP
    
    %% Manual Data
    MANUAL_IO[src/manual_io.py] --> DISPOSITION
    MANUAL_BLACKLIST[data/manual/manual_blacklist.json] --> MANUAL_IO
    MANUAL_OVERRIDES[data/manual/manual_dispositions.json] --> MANUAL_IO
    
    %% Streamlit Interface
    LOAD_REVIEW --> |Load| PROCESSED_DATA
    STREAMLIT --> MANUAL_IO
    STREAMLIT --> EXPORT[export_manual_data()]
    
    %% Performance & Audit
    PERF[src/performance.py<br/>PerformanceTracker] --> RUN_PIPELINE
    AUDIT[_create_audit_snapshot()] --> RUN_PIPELINE
    AUDIT --> |JSON| META_DATA[data/processed/review_meta.json]
    
    %% Styling
    classDef entryPoint fill:#e1f5fe
    classDef coreModule fill:#f3e5f5
    classDef dataFile fill:#e8f5e8
    classDef configFile fill:#fff3e0
    classDef utilModule fill:#fce4ec
    
    class CLI,STREAMLIT entryPoint
    class RUN_PIPELINE,NORMALIZE,SIMILARITY,GROUPING,SURVIVORSHIP,DISPOSITION,ALIAS_MATCHING coreModule
    class RAW_DATA,NORM_DATA,PAIRS_DATA,GROUPS_DATA,SURV_DATA,DISP_DATA,ALIAS_DATA,PROCESSED_DATA,META_DATA dataFile
    class CONFIG,RANKS,MANUAL_BLACKLIST,MANUAL_OVERRIDES configFile
    class MANUAL_IO,PERF,AUDIT utilModule
```

## Detailed Module Functions

### 1. Main Pipeline (src/cleaning.py)
```mermaid
graph TD
    MAIN[main()] --> ARGS[Parse CLI arguments]
    ARGS --> RUN_PIPELINE[run_pipeline()]
    
    RUN_PIPELINE --> LOAD_DATA[load_salesforce_data()]
    LOAD_DATA --> VALIDATE[validate_required_columns()]
    VALIDATE --> NORM[normalize_dataframe()]
    NORM --> PAIRS[pair_scores()]
    PAIRS --> GROUPS[create_groups_with_edge_gating()]
    GROUPS --> PRIMARY[select_primary_records()]
    PRIMARY --> MERGE[generate_merge_preview()]
    MERGE --> DISP[apply_dispositions()]
    DISP --> ALIAS[compute_alias_matches()]
    ALIAS --> FINAL[Create review_ready.csv]
    
    %% Helper functions
    RUN_PIPELINE --> ASSERT[_assert_pairs_cover_accounts()]
    RUN_PIPELINE --> AUDIT[_create_audit_snapshot()]
    RUN_PIPELINE --> PERF[_create_performance_summary_enhanced()]
```

### 2. Normalization Module (src/normalize.py)
```mermaid
graph TD
    NORM_DF[normalize_dataframe()] --> NORM_NAME[normalize_name()]
    
    NORM_NAME --> NAME_BASE[_create_name_base()]
    NORM_NAME --> NUMERIC[_unify_numeric_style()]
    NORM_NAME --> MULTIPLE[_detect_multiple_names()]
    NORM_NAME --> ALIAS_EXTRACT[_extract_alias_candidates()]
    NORM_NAME --> SUFFIX[extract_suffix()]
    
    ALIAS_EXTRACT --> VALID_PAREN[_is_valid_parentheses_alias()]
    ALIAS_EXTRACT --> NORM_ALIAS[_normalize_alias()]
    
    SUFFIX --> SUFFIX_TOKENS[extract_suffix_from_tokens()]
    
    NORM_DF --> EXCEL_DATE[excel_serial_to_datetime()]
```

### 3. Similarity Module (src/similarity.py)
```mermaid
graph TD
    PAIR_SCORES[pair_scores()] --> GEN_PAIRS[_generate_candidate_pairs()]
    PAIR_SCORES --> COMPUTE_SCORE[_compute_pair_score()]
    
    COMPUTE_SCORE --> NUMERIC_MATCH[_check_numeric_style_match()]
    COMPUTE_SCORE --> PUNCT_MATCH[_check_punctuation_mismatch()]
    
    PAIR_SCORES --> SAVE_PAIRS[save_candidate_pairs()]
    PAIR_SCORES --> LOAD_PAIRS[load_candidate_pairs()]
    
    GEN_PAIRS --> STOP_TOKENS[get_stop_tokens()]
```

### 4. Grouping Module (src/grouping.py)
```mermaid
graph TD
    CREATE_GROUPS[create_groups_with_edge_gating()] --> CAN_JOIN[can_join_group()]
    CREATE_GROUPS --> CANOPY[apply_canopy_bound()]
    CREATE_GROUPS --> STANDARD[create_groups_standard()]
    
    CAN_JOIN --> EDGE_GATING[Edge gating logic]
    CANOPY --> SIZE_LIMIT[Group size limits]
```

### 5. Survivorship Module (src/survivorship.py)
```mermaid
graph TD
    SELECT_PRIMARY[select_primary_records()] --> SELECT_FROM_GROUP[_select_primary_from_group()]
    SELECT_PRIMARY --> MERGE_PREVIEW[generate_merge_preview()]
    
    MERGE_PREVIEW --> GROUP_MERGE[_generate_group_merge_preview()]
    
    SELECT_PRIMARY --> SAVE_SURV[save_survivorship_results()]
    SELECT_PRIMARY --> LOAD_SURV[load_survivorship_results()]
```

### 6. Disposition Module (src/disposition.py)
```mermaid
graph TD
    APPLY_DISP[apply_dispositions()] --> CLASSIFY[classify_disposition()]
    APPLY_DISP --> GROUP_META[compute_group_metadata()]
    
    CLASSIFY --> IS_BLACKLISTED[_is_blacklisted()]
    CLASSIFY --> IS_BLACKLISTED_IMPROVED[_is_blacklisted_improved()]
    CLASSIFY --> IS_SUSPICIOUS[_is_suspicious_singleton()]
    
    IS_BLACKLISTED --> COMPILE_REGEX[_compile_blacklist_regex()]
    IS_BLACKLISTED --> LOAD_MANUAL_BL[_load_manual_blacklist()]
    
    APPLY_DISP --> SAVE_DISP[save_dispositions()]
    APPLY_DISP --> LOAD_DISP[load_dispositions()]
    APPLY_DISP --> GET_REASON[get_disposition_reason()]
    
    GET_BLACKLIST[get_blacklist_terms()]
```

### 7. Alias Matching Module (src/alias_matching.py)
```mermaid
graph TD
    COMPUTE_ALIAS[compute_alias_matches()] --> NORM_ALIAS[_normalize_alias()]
    COMPUTE_ALIAS --> SCORE_ALIAS[_score_alias_against_records()]
    
    COMPUTE_ALIAS --> CREATE_CROSS[create_alias_cross_refs()]
    COMPUTE_ALIAS --> SAVE_ALIAS[save_alias_matches()]
    COMPUTE_ALIAS --> LOAD_ALIAS[load_alias_matches()]
```

### 8. Manual I/O Module (src/manual_io.py)
```mermaid
graph TD
    ENSURE_DIR[ensure_manual_directory()] --> ATOMIC_WRITE[_atomic_write_json()]
    
    LOAD_BLACKLIST[load_manual_blacklist()] --> SAVE_BLACKLIST[save_manual_blacklist()]
    LOAD_OVERRIDES[load_manual_overrides()] --> SAVE_OVERRIDES[save_manual_overrides()]
    
    UPSERT_OVERRIDE[upsert_manual_override()] --> REMOVE_OVERRIDE[remove_manual_override()]
    GET_OVERRIDE[get_manual_override()]
```

### 9. Streamlit App (app/main.py)
```mermaid
graph TD
    MAIN_APP[main()] --> LOAD_REVIEW[load_review_data()]
    MAIN_APP --> LOAD_SETTINGS[load_settings()]
    MAIN_APP --> PARSE_ALIAS[parse_alias_cross_refs()]
    MAIN_APP --> PARSE_MERGE[parse_merge_preview()]
    
    LOAD_REVIEW --> PARQUET[Load Parquet]
    LOAD_REVIEW --> CSV[Load CSV]
    
    MAIN_APP --> EXPORT_DATA[export_manual_data()]
    MAIN_APP --> FILTERS[Apply filters]
    MAIN_APP --> PAGINATION[Pagination]
    MAIN_APP --> MANUAL_OVERRIDES[Manual overrides]
```

### 10. Utils Package (src/utils/)

#### 10.1 I/O Utils (src/utils/io_utils.py)
```mermaid
graph TD
    GET_FILE_INFO[get_file_info()] --> LIST_FILES[list_data_files()]
    LOAD_SETTINGS[load_settings()] --> LOAD_RANKS[load_relationship_ranks()]
```

#### 10.2 Path Utils (src/utils/path_utils.py)
```mermaid
graph TD
    GET_ROOT[get_project_root()] --> ENSURE_DIR[ensure_directory_exists()]
    ENSURE_DIR --> GET_PATHS[get_data_paths()]
```

#### 10.3 Logging Utils (src/utils/logging_utils.py)
```mermaid
graph TD
    SETUP_LOGGING[setup_logging()]
```

#### 10.4 Performance Utils (src/utils/perf_utils.py)
```mermaid
graph TD
    LOG_PERF[log_perf()] --> Context Manager
```

#### 10.5 DTypes Utils (src/utils/dtypes.py)
```mermaid
graph TD
    APPLY_DTYPES[apply_dtypes()] --> ASSERT_COLS[assert_no_unexpected_object_columns()]
    ASSERT_COLS --> DROP_COLS[drop_intermediate_columns()]
    DROP_COLS --> OPTIMIZE[optimize_dataframe_memory()]
    OPTIMIZE --> DETECT_SCHEMA[_detect_schema()]
    DETECT_SCHEMA --> GET_DTYPES[get_dtypes_for_schema()]
```

#### 10.6 ID Utils (src/utils/id_utils.py)
```mermaid
graph TD
    CHUNK_CHECKSUM[_chunk_checksum()] --> SFID15_TO_18[sfid15_to_18()]
    SFID15_TO_18 --> NORM_SFID[normalize_sfid_series()]
    NORM_SFID --> VALIDATE_SFID[validate_sfid_format()]
```

#### 10.7 Hash Utils (src/utils/hash_utils.py)
```mermaid
graph TD
    CONFIG_HASH[config_hash()] --> STABLE_GROUP[stable_group_id()]
    STABLE_GROUP --> COMPUTE_HASH[_compute_config_hash()]
```

#### 10.8 Validation Utils (src/utils/validation_utils.py)
```mermaid
graph TD
    VALIDATE_DF[validate_dataframe()]
```

### 11. Performance Module (src/performance.py)
```mermaid
graph TD
    PERF_TRACKER[PerformanceTracker] --> RECORD_TIMING[record_timing()]
    PERF_TRACKER --> RECORD_MEMORY[record_memory()]
    PERF_TRACKER --> END_RUN[end_run()]
    
    SAVE_PERF[save_performance_summary()] --> GROUP_HIST[compute_group_size_histogram()]
    GROUP_HIST --> BLOCK_TOKENS[compute_block_top_tokens()]
```

### 12. Manual Data App (app/manual_data.py)
```mermaid
graph TD
    ENSURE_DIR[ensure_manual_directory()] --> LOAD_DISP[load_manual_dispositions()]
    LOAD_DISP --> SAVE_DISP[save_manual_dispositions()]
    SAVE_DISP --> ADD_DISP[add_manual_disposition()]
    
    LOAD_BLACKLIST[load_manual_blacklist()] --> SAVE_BLACKLIST[save_manual_blacklist()]
    SAVE_BLACKLIST --> ADD_BLACKLIST[add_manual_blacklist_term()]
    ADD_BLACKLIST --> REMOVE_BLACKLIST[remove_manual_blacklist_term()]
    
    GET_OVERRIDE[get_manual_override_for_record()] --> EXPORT_DATA[export_manual_data()]
```

## Data Flow Summary

```mermaid
graph LR
    subgraph "Input"
        RAW[data/raw/*.csv]
        CONFIG[config/settings.yaml]
        RANKS[config/relationship_ranks.csv]
    end
    
    subgraph "Pipeline Processing"
        NORM[normalize_dataframe]
        SIM[pair_scores]
        GROUP[create_groups_with_edge_gating]
        SURV[select_primary_records]
        DISP[apply_dispositions]
        ALIAS[compute_alias_matches]
    end
    
    subgraph "Interim Data"
        NORM_DATA[accounts_normalized.parquet]
        PAIRS_DATA[candidate_pairs.parquet]
        GROUPS_DATA[groups.parquet]
        SURV_DATA[survivorship.parquet]
        DISP_DATA[dispositions.parquet]
        ALIAS_DATA[alias_matches.parquet]
    end
    
    subgraph "Output"
        REVIEW[review_ready.csv/.parquet]
        META[review_meta.json]
        PERF[perf_summary.json]
    end
    
    subgraph "Manual Data"
        MANUAL_BL[manual_blacklist.json]
        MANUAL_OV[manual_dispositions.json]
    end
    
    RAW --> NORM
    CONFIG --> NORM
    RANKS --> SURV
    
    NORM --> NORM_DATA
    NORM_DATA --> SIM
    SIM --> PAIRS_DATA
    PAIRS_DATA --> GROUP
    GROUP --> GROUPS_DATA
    GROUPS_DATA --> SURV
    SURV --> SURV_DATA
    SURV_DATA --> DISP
    DISP --> DISP_DATA
    DISP_DATA --> ALIAS
    ALIAS --> ALIAS_DATA
    
    DISP_DATA --> REVIEW
    ALIAS_DATA --> REVIEW
    
    MANUAL_BL --> DISP
    MANUAL_OV --> DISP
    
    REVIEW --> META
    REVIEW --> PERF
```

## Configuration Files

### config/settings.yaml
- **similarity**: High/medium thresholds, penalties
- **grouping**: Edge-gating settings, canopy bounds
- **survivorship**: Tie-breaker order
- **io**: File format preferences
- **salesforce**: Integration settings

### config/relationship_ranks.csv
- **Relationship types**: 1099 forms, paychecks, emails, etc.
- **Rank values**: 10 (highest) to 60 (lowest)
- **Priority order**: Used for primary record selection

## Key Algorithms

1. **Blocking Strategy**: Token-based candidate pair generation
2. **Similarity Scoring**: RapidFuzz with composite penalties
3. **Edge-Gating**: Suffix match + minimum similarity requirement
4. **Connected Components**: Graph-based group formation
5. **Survivorship Rules**: Relationship rank → Created date → Account ID
6. **Disposition Logic**: Blacklist → Group size → Primary status → Manual overrides
7. **Alias Detection**: Conservative extraction with high-confidence matching

## Performance Optimizations

1. **Memory Management**: Lean dtypes, intermediate column cleanup
2. **Blocking**: Reduces comparison pairs from O(n²) to O(n log n)
3. **Canopy Clustering**: Limits group sizes for scalability
4. **Parquet Format**: Columnar storage for large datasets
5. **Performance Tracking**: Comprehensive timing and memory monitoring
