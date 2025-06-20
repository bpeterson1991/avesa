# Table Naming Convention Documentation

## Overview

This document establishes the explicit table naming convention for the AVESA data pipeline to prevent endpoint-to-table name confusion and ensure consistency across all components.

## Problem Statement

Previously, table names were derived from API endpoint paths, which led to inconsistencies:
- Endpoint `time/entries` was sometimes mapped to table name `entries` instead of `time_entries`
- This caused S3 path inconsistencies and canonical transformation issues
- Different components used different naming conventions

## Solution: Explicit Table Naming

### 1. Endpoint Configuration

All service endpoint configurations now include explicit `table_name` fields:

```json
{
  "endpoints": {
    "time/entries": {
      "enabled": true,
      "table_name": "time_entries",
      "canonical_table": "time_entries",
      "description": "Time tracking entries"
    }
  }
}
```

### 2. Table Naming Rules

1. **Use explicit `table_name` field** in endpoint configurations
2. **Use snake_case** for all table names (e.g., `time_entries`, not `time-entries`)
3. **Be descriptive** and avoid abbreviations when possible
4. **Maintain consistency** between raw and canonical table names

### 3. Standard Table Names

| Endpoint | Table Name | Description |
|----------|------------|-------------|
| `service/tickets` | `tickets` | Service tickets and requests |
| `time/entries` | `time_entries` | Time tracking entries |
| `company/companies` | `companies` | Customer companies and accounts |
| `company/contacts` | `contacts` | Company contacts and users |
| `procurement/products` | `products` | Product catalog items |
| `finance/agreements` | `agreements` | Service agreements and contracts |
| `project/projects` | `projects` | Project management data |
| `system/members` | `members` | System users and technicians |

## Implementation Details

### 1. S3 Path Structure

Raw data paths use explicit table names:
```
{tenant_id}/raw/{service_name}/{table_name}/{date}/{timestamp}.json
```

Example:
```
sitetechnology/raw/connectwise/time_entries/2025-06-19/2025-06-19T20:13:00Z.json
```

### 2. Canonical Data Paths

Canonical data paths also use explicit table names:
```
{tenant_id}/canonical/{table_name}/{date}/{timestamp}.parquet
```

Example:
```
sitetechnology/canonical/time_entries/2025-06-19/2025-06-19T20:13:00Z.parquet
```

### 3. Code Implementation

#### Utility Functions

```python
def get_table_name_from_endpoint_config(endpoint_config: Dict[str, Any], endpoint_path: str) -> str:
    """Get explicit table name from endpoint configuration."""
    # Check for explicit table_name field
    table_name = endpoint_config.get('table_name')
    if table_name:
        return table_name
    
    # Fallback to canonical_table if table_name not present
    canonical_table = endpoint_config.get('canonical_table')
    if canonical_table:
        return canonical_table
    
    # Last resort: derive from endpoint path (with warning)
    derived_name = endpoint_path.split('/')[-1]
    print(f"WARNING: No explicit table_name found for {endpoint_path}, using derived name: {derived_name}")
    return derived_name
```

#### Loading Endpoint Configuration

```python
def load_endpoint_configuration(service_name: str) -> Dict[str, Any]:
    """Load endpoint configuration for a service."""
    config_path = f'mappings/integrations/{service_name}_endpoints.json'
    with open(config_path, 'r') as f:
        return json.load(f)
```

## Migration Guide

### For Existing Data

1. **Check existing S3 data** for inconsistent naming
2. **Update references** from `entries` to `time_entries` in:
   - Test scripts
   - Documentation
   - Lambda function configurations
   - Canonical transform mappings

### For New Services

1. **Always include `table_name`** in endpoint configurations
2. **Follow snake_case convention** for table names
3. **Test both raw and canonical paths** to ensure consistency

## Validation

### Automated Checks

The pipeline includes validation to ensure:
1. All endpoint configurations have explicit `table_name` fields
2. Table names follow snake_case convention
3. S3 paths use correct table names
4. Canonical transformations reference correct table names

### Manual Verification

Use the following commands to verify table naming consistency:

```bash
# Check endpoint configurations
python tests/validate-canonical-setup.py

# Verify S3 data structure
python tests/test-end-to-end-pipeline.py --check-recent

# Test canonical transformations
python tests/test-canonical-transform-pipeline.py --table time_entries
```

## Benefits

1. **Consistency**: All components use the same table names
2. **Clarity**: Explicit naming prevents confusion
3. **Maintainability**: Easy to understand and modify
4. **Scalability**: Clear pattern for adding new tables
5. **Debugging**: Easier to trace data through the pipeline

## Future Considerations

1. **Service-specific naming**: Consider prefixes for multi-service environments
2. **Versioning**: Plan for table schema evolution
3. **Validation**: Automated checks for naming convention compliance
4. **Documentation**: Keep this document updated as new tables are added