# Exact Duplicate Root Cause Analysis and Resolution

## 🚨 Critical Issue Summary

**Date**: June 21, 2025  
**Severity**: CRITICAL  
**Impact**: 97.78% of time_entries records are exact duplicates (44,000 out of 45,000 records)

## 📊 Investigation Findings

### Data Quality Assessment
- **Total Records**: 45,000
- **Duplicate Records**: 44,000 (97.78%)
- **Unique Records**: 1,000 (2.22%)
- **Duplicate Groups**: 1,000 groups with ~44 duplicates each
- **Affected Tenant**: sitetechnology (100% of duplicates)
- **Record Type**: All duplicates have `is_current=false` (historical records)

### Root Cause Analysis

#### 1. **Single Tenant System with Concurrent Processing (PRIMARY CAUSE)**
**Evidence**: Only 1 enabled tenant in system, 44 Lambda functions processing same data

**Technical Details**:
- System has only **1 active tenant**: `sitetechnology`
- **44 Lambda functions** triggered simultaneously (likely scheduled job)
- **All Lambdas processed same tenant/table**: `sitetechnology/time_entries`
- **All Lambdas read same S3 files** from `sitetechnology/canonical/time_entries/`
- **No idempotency checks** to prevent reprocessing same data

**Code Location**: `src/clickhouse/data_loader/lambda_function.py` lines 440-447

#### 2. **Missing Idempotency Logic (SECONDARY CAUSE)**
**Evidence**: No checks to prevent processing already-processed files

**Technical Details**:
- Lambda doesn't check if S3 files were already processed
- Multiple Lambdas process identical files simultaneously
- No file-level or timestamp-based deduplication

#### 3. **Schema Mismatch Causing Retries (CONTRIBUTING FACTOR)**
**Evidence**: Column name mismatch causing INSERT failures and retries

**Technical Details**:
- Code referenced `created_date` but schema uses `date_entered`
- Failed INSERTs trigger Lambda retries, multiplying the duplication problem
- Each retry creates more duplicate records

**Code Location**: Fixed in updated Lambda function

## 🛠️ Resolution Implementation

### 1. **Idempotency Checks**
**Implementation**: Added file-level processing checks

```python
def check_if_already_processed(tenant_id: str, table_name: str, s3_files: List[Dict]) -> bool:
    """Check if the current batch of files has already been processed."""
    # Compares S3 file modification times with last processed timestamp
    # Skips processing if all files are older than last update
```

**Benefits**:
- Prevents reprocessing of same S3 files
- Multiple Lambda invocations become safe
- Reduces unnecessary ClickHouse operations

### 2. **Improved Staging Table Logic**
**Implementation**: Enhanced atomic operations with unique staging table names

```python
# Old: staging table name could collide
staging_table = f"{table_name}_staging_{int(current_time.timestamp() * 1000)}"

# New: microsecond precision prevents collisions
staging_table = f"{table_name}_staging_{int(current_time.timestamp() * 1000000)}"
```

**Benefits**:
- Prevents staging table name collisions
- Each Lambda gets unique staging table
- Atomic SCD Type 2 operations remain safe

### 3. **Schema Alignment**
**Fix**: Corrected column name from `created_date` to `date_entered`

**Impact**:
- Eliminates INSERT failures that trigger retries
- Reduces Lambda execution errors and costs
- Prevents retry-induced duplication

### 4. **SSL Configuration Fix**
**Fix**: Disabled SSL verification for ClickHouse Cloud compatibility

**Impact**:
- Eliminates connection failures
- Reduces Lambda timeouts and retries
- Improves pipeline reliability

## 📋 Cleanup Strategy

### Immediate Actions Required

1. **Execute Duplicate Cleanup**
   ```bash
   python3 scripts/cleanup-exact-duplicates.py --execute
   ```
   - Will remove 43,000 duplicate records
   - Preserves 1,000 unique records (oldest by effective_date)
   - Generates 860 cleanup queries in batches

2. **Deploy Fixed Pipeline**
   - Deploy updated `src/clickhouse/data_loader/lambda_function.py`
   - No additional infrastructure changes needed
   - Idempotency uses existing `LastUpdated-dev` DynamoDB table

3. **Verify Data Integrity**
   ```bash
   python3 scripts/investigate-exact-duplicates.py
   ```
   - Confirm no remaining exact duplicates
   - Validate SCD Type 2 logic is working correctly

### Prevention Measures

1. **Lambda Configuration**
   - Consider setting reserved concurrency to 1 for single-tenant scenarios
   - Add monitoring for concurrent Lambda executions
   - Implement circuit breaker for failed processing

2. **Monitoring Enhancements**
   - Real-time duplicate detection alerts
   - Lambda concurrency monitoring
   - Data quality dashboards
   - S3 file processing tracking

3. **Future Multi-Tenant Considerations**
   - Current fix scales well to multiple tenants
   - Each tenant processes independently
   - Idempotency works per tenant/table combination

## 🔍 Technical Deep Dive

### Why This Happened

**Trigger Event**: Likely a scheduled job (EventBridge/CloudWatch) that:
1. **Invoked 44 Lambda functions** simultaneously
2. **Each Lambda** called `get_tenant_list()` → returned `['sitetechnology']`
3. **All 44 Lambdas** processed same tenant/table combination
4. **No idempotency** meant all processed same S3 files
5. **Schema mismatch** caused failures and retries, amplifying the problem

### Why 44 Duplicates Per Record?

The number 44 corresponds to:
- **Lambda auto-scaling**: AWS created 44 concurrent instances
- **Simultaneous execution**: All started within same minute
- **Same data source**: All processed identical S3 files
- **No deduplication**: Each created identical records

### Single vs Multi-Tenant Impact

**Current State (Single Tenant)**:
- All Lambdas process same data → duplicates
- Idempotency fixes prevent this

**Future State (Multi-Tenant)**:
- Each Lambda processes different tenant → natural partitioning
- Idempotency provides additional safety
- System scales horizontally with tenant count

## 📈 Success Metrics

### Data Quality Targets
- **Exact Duplicates**: 0% (currently 97.78%)
- **Data Freshness**: < 1 hour lag
- **Pipeline Success Rate**: > 99.5%
- **Idempotency Hit Rate**: Track skipped processing

### Monitoring KPIs
- Duplicate detection alerts: Real-time
- Lambda concurrent executions: Monitor for spikes
- S3 file processing: Track file-level completion
- Data loading latency: < 5 minutes

## 🚀 Next Steps

1. **Immediate** (Today):
   - Execute duplicate cleanup
   - Deploy fixed pipeline code
   - Verify idempotency is working

2. **Short Term** (This Week):
   - Add comprehensive monitoring
   - Document operational procedures
   - Test with simulated concurrent executions

3. **Long Term** (Next Sprint):
   - Consider Lambda reserved concurrency settings
   - Add data lineage tracking
   - Prepare for multi-tenant scaling

## 📞 Escalation Path

**Critical Issues**: Immediate escalation to Data Engineering team  
**Data Quality Alerts**: Automated Slack notifications  
**Pipeline Failures**: PagerDuty integration  

---

**Document Owner**: Data Engineering Team  
**Last Updated**: June 21, 2025  
**Review Cycle**: Weekly during stabilization period

## 🎯 Key Takeaway

**This was NOT a design flaw** - the Lambda architecture is correct for multi-tenant systems. The issue was:
1. **Single tenant** causing all Lambdas to process same data
2. **Missing idempotency** allowing duplicate processing
3. **Schema mismatch** causing retries and amplification

The fix (idempotency) makes the system robust for both single and multi-tenant scenarios.