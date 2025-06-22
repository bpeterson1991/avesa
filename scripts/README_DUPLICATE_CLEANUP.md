# Duplicate Cleanup Tools

This directory contains production-ready tools for detecting and cleaning up data duplicates in ClickHouse.

## 🔍 Investigation Tools

### [`investigate-exact-duplicates.py`](investigate-exact-duplicates.py)
Detects exact duplicates where records have identical tenant_id, ID, last_updated, and is_current values.

```bash
# Investigate exact duplicates
python3 scripts/investigate-exact-duplicates.py --save-report

# Use environment variables for credentials
python3 scripts/investigate-exact-duplicates.py --credentials env
```

### [`investigate-scd-violations.py`](investigate-scd-violations.py)
Detects SCD Type 2 violations where records have same tenant_id, ID, last_updated but different is_current values.

```bash
# Investigate SCD violations
python3 scripts/investigate-scd-violations.py --save-report
```

### [`analyze-duplicate-root-cause.py`](analyze-duplicate-root-cause.py)
Analyzes patterns in duplicates to identify root causes (timing, concurrency, pipeline issues).

```bash
# Analyze root cause patterns
python3 scripts/analyze-duplicate-root-cause.py --save-report
```

## 🧹 Cleanup Tools

### [`cleanup-exact-duplicates.py`](cleanup-exact-duplicates.py)
Removes exact duplicates while preserving the oldest record in each duplicate group.

```bash
# Dry run (default)
python3 scripts/cleanup-exact-duplicates.py --save-report

# Execute cleanup (requires confirmation)
python3 scripts/cleanup-exact-duplicates.py --execute --save-report
```

**Safety Features:**
- Dry run by default
- Requires exact confirmation: "DELETE DUPLICATES"
- Preserves oldest record by effective_date
- Batch processing to avoid query limits

### [`cleanup-scd-violations.py`](cleanup-scd-violations.py)
Fixes SCD Type 2 violations by removing records with is_current=false when a record with is_current=true exists for the same ID+last_updated.

```bash
# Dry run (default)
python3 scripts/cleanup-scd-violations.py --save-report

# Execute cleanup (requires confirmation)
python3 scripts/cleanup-scd-violations.py --execute --save-report
```

**Safety Features:**
- Dry run by default
- Requires exact confirmation: "FIX SCD VIOLATIONS"
- Preserves is_current=true records
- Maintains SCD Type 2 integrity

## 📋 Recommended Workflow

### 1. Investigation Phase
```bash
# Check for exact duplicates
python3 scripts/investigate-exact-duplicates.py --save-report

# Check for SCD violations
python3 scripts/investigate-scd-violations.py --save-report

# Analyze root causes
python3 scripts/analyze-duplicate-root-cause.py --save-report
```

### 2. Cleanup Phase
```bash
# Clean exact duplicates first (usually the bulk of the problem)
python3 scripts/cleanup-exact-duplicates.py --execute --save-report

# Then clean SCD violations
python3 scripts/cleanup-scd-violations.py --execute --save-report
```

### 3. Verification Phase
```bash
# Verify cleanup was successful
python3 scripts/investigate-exact-duplicates.py
python3 scripts/investigate-scd-violations.py
```

## 🔧 Configuration

### AWS Credentials
Tools use AWS Secrets Manager by default. Set these environment variables:

```bash
export AWS_PROFILE=AdministratorAccess-123938354448
export AWS_SDK_LOAD_CONFIG=1
export CLICKHOUSE_SECRET_NAME=clickhouse-connection-dev
```

### Environment Variables (Alternative)
Use `--credentials env` flag with these variables:

```bash
export CLICKHOUSE_HOST=your-clickhouse-host
export CLICKHOUSE_PORT=8443
export CLICKHOUSE_USER=your-username
export CLICKHOUSE_PASSWORD=your-password
export CLICKHOUSE_DATABASE=default
```

## 📊 Exit Codes

- **0**: Success, no issues found
- **1**: Issues found (duplicates/violations detected)
- **2**: Error (connection failure, etc.)

## 📄 Reports

All tools support `--save-report` flag to generate detailed JSON reports with:
- Investigation results
- Cleanup actions taken
- Performance metrics
- Error details

Reports are saved with timestamp: `{tool_name}_{YYYYMMDD_HHMMSS}.json`

## 🚨 Safety Notes

- **Always run investigation tools first** to understand the scope
- **Use dry run mode** to preview changes before execution
- **Take database backups** before running cleanup in production
- **Monitor performance** during cleanup operations
- **Verify results** after cleanup completion

## 📚 Documentation

See [`docs/COMPREHENSIVE_DUPLICATE_CLEANUP_STRATEGY.md`](../docs/COMPREHENSIVE_DUPLICATE_CLEANUP_STRATEGY.md) for complete strategy and procedures.