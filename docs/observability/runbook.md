# Company Junction Observability Runbook

## 🚨 **Critical Alerts**

### High Latency Alert
**Alert:** `groups_page_latency_seconds p95 > 2.0s`

**Symptoms:**
- Users experiencing slow page loads
- High p95 latency on groups page operations

**Immediate Actions:**
1. Check current backend distribution: `backend_choice_total`
2. If PyArrow-heavy, consider forcing DuckDB: Set `ui_perf.force_duckdb = true`
3. If DuckDB-heavy, check for connection issues or resource constraints
4. Monitor timeout rate: `timeouts_total`

**Escalation:**
- If latency > 5s: Force DuckDB backend immediately
- If timeouts > 10/min: Check infrastructure resources

### High Error Rate Alert
**Alert:** `error_rate > 5%`

**Symptoms:**
- Users seeing error pages
- High failure rate on requests

**Immediate Actions:**
1. Check error logs for specific failure patterns
2. Verify parquet file integrity and schema
3. Check for resource exhaustion (memory, connections)
4. Consider rolling back to previous deployment

**Escalation:**
- If error rate > 20%: Emergency rollback
- If error rate > 50%: Disable affected endpoints

### Page Size Clamping Spike
**Alert:** `page_size_clamped_total rate > 100/min`

**Symptoms:**
- Users hitting page size limits frequently
- Possible misconfiguration or traffic pattern change

**Immediate Actions:**
1. Check if `ui.max_page_size` setting is too low
2. Review recent configuration changes
3. Consider increasing `max_page_size` if legitimate use case
4. Monitor for potential abuse patterns

## 🔧 **Operational Procedures**

### Force Backend Selection

#### Force DuckDB (for performance issues)
```yaml
# In config/settings.yaml
ui_perf:
  force_duckdb: true
  force_pyarrow: false
```

#### Force PyArrow (for testing/comparison)
```yaml
# In config/settings.yaml
ui_perf:
  force_pyarrow: true
  force_duckdb: false
```

#### Emergency Environment Override
```bash
# Set environment variables for immediate effect
export CJ_FORCE_DUCKDB=1
export CJ_FORCE_PYARROW=0
```

### Rollback Procedure

#### Quick Rollback (if recent deployment)
1. Revert to previous git commit
2. Redeploy application
3. Monitor metrics for recovery

#### Configuration Rollback
1. Restore previous `config/settings.yaml`
2. Restart application
3. Verify metrics return to baseline

### Performance Tuning

#### Increase Page Size Limits
```yaml
# In config/settings.yaml
ui:
  max_page_size: 500  # Increase from default 250
```

#### Adjust Timeout Settings
```yaml
# In config/settings.yaml
ui:
  timeout_seconds: 30  # Increase from default 10
  max_pyarrow_groups_seconds: 15  # Increase from default 5
```

#### Optimize DuckDB Threads
```yaml
# In config/settings.yaml
ui:
  duckdb_threads: 8  # Increase from default 4
```

## 📊 **Key Metrics to Monitor**

### Performance Metrics
- `groups_page_latency_seconds` - Request latency by backend
- `details_page_latency_seconds` - Details page latency
- `backend_choice_total` - Backend selection distribution

### Error Metrics
- `groups_page_requests_total{ok="false"}` - Failed requests
- `details_page_requests_total{ok="false"}` - Failed details requests
- `timeouts_total` - Timeout occurrences

### Capacity Metrics
- `page_size_clamped_total` - Page size limit hits
- `duckdb_active_connections` - Active database connections

## 🎯 **SLO Targets**

### Latency SLOs
- **Groups Page p95:** < 2.0s
- **Details Page p95:** < 1.0s
- **Groups Page p99:** < 5.0s
- **Details Page p99:** < 3.0s

### Availability SLOs
- **Error Rate:** < 1%
- **Timeout Rate:** < 0.1%
- **Uptime:** > 99.9%

### Capacity SLOs
- **Page Size Clamping:** < 10% of requests
- **Backend Distribution:** Balanced (no single backend > 90%)

## 🔍 **Troubleshooting Guide**

### High Latency on PyArrow
1. Check if large parquet files are being processed
2. Consider forcing DuckDB for better performance
3. Verify PyArrow version compatibility
4. Check system memory usage

### High Latency on DuckDB
1. Check active connection count
2. Verify thread pool configuration
3. Check for long-running queries
4. Consider connection pool tuning

### Schema Errors
1. Run contract tests: `pytest tests/contracts/`
2. Verify parquet file schema matches expectations
3. Check for upstream data pipeline changes
4. Review recent deployments

### Memory Issues
1. Monitor `duckdb_active_connections`
2. Check for memory leaks in long-running processes
3. Consider reducing `duckdb_threads`
4. Verify parquet file sizes

## 📞 **Escalation Contacts**

- **Primary On-Call:** [Your Team]
- **Secondary On-Call:** [Backup Team]
- **Infrastructure Team:** [Infra Team]
- **Data Pipeline Team:** [Data Team]

## 📝 **Post-Incident Actions**

1. **Document the incident** in incident tracking system
2. **Update runbook** with any new procedures discovered
3. **Review metrics** to identify root cause
4. **Implement preventive measures** to avoid recurrence
5. **Schedule post-mortem** if severity warrants
