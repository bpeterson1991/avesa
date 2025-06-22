# Slowly Changing Dimension (SCD) Configuration Guide

## Overview

Slowly Changing Dimensions (SCD) are a data warehousing concept that defines how to handle changes to dimension data over time. This guide explains the SCD types implemented in our canonical mapping system and when to use each type.

## SCD Types

### Type 1 - Overwrite (Simple Upsert)

**Configuration**: `"scd_type": "type_1"`

**Behavior**: When a record is updated, the existing record is overwritten with the new values. No historical data is preserved.

**Use Cases**:
- Data where historical changes are not important
- Reference data that needs to reflect current state only
- Data with frequent updates where history would create excessive storage overhead
- Correcting data errors where the old value should be completely replaced

**Tables Using Type 1**:
- [`companies.json`](../mappings/canonical/companies.json) - Company information changes are typically corrections or updates that should reflect current state
- [`contacts.json`](../mappings/canonical/contacts.json) - Contact information updates usually represent current contact details
- [`time_entries.json`](../mappings/canonical/time_entries.json) - Time entry corrections should overwrite incorrect data

**Example**:
```
Before: Company ABC, Phone: 555-1234
After:  Company ABC, Phone: 555-5678 (old phone number is lost)
```

### Type 2 - Historical Tracking (Versioning)

**Configuration**: `"scd_type": "type_2"`

**Behavior**: When a record is updated, the existing record is marked as historical and a new record is created with the updated values. This preserves the complete history of changes.

**Implementation Details**:
- Each record includes versioning fields:
  - `effective_date`: When this version became active
  - `expiration_date`: When this version was superseded (NULL for current version)
  - `is_current`: Boolean flag indicating the current active version
  - `version_number`: Sequential version identifier

**Use Cases**:
- Data where historical changes are critical for analysis
- Audit requirements that mandate change tracking
- Business processes that need to understand state at specific points in time
- Regulatory compliance requiring historical data preservation

**Tables Using Type 2**:
- [`tickets.json`](../mappings/canonical/tickets.json) - Ticket status changes, priority updates, and assignment changes need historical tracking for analysis and reporting

**Example**:
```
Version 1: Ticket #123, Status: Open,     Priority: Low,    effective_date: 2024-01-01, expiration_date: 2024-01-15, is_current: false
Version 2: Ticket #123, Status: In Progress, Priority: High, effective_date: 2024-01-15, expiration_date: NULL,       is_current: true
```

## Configuration Structure

Each canonical mapping file includes an `scd_type` field at the root level:

```json
{
  "scd_type": "type_1",  // or "type_2"
  "connectwise": {
    // mapping configuration
  },
  "servicenow": {
    // mapping configuration
  }
}
```

## Implementation Considerations

### Type 1 Implementation
- Simple upsert operations based on primary key
- Minimal storage overhead
- Fast query performance
- No additional schema complexity

### Type 2 Implementation
- Requires additional versioning columns in target tables
- More complex insert/update logic
- Increased storage requirements
- Queries must filter for current records unless historical analysis is needed
- Requires careful handling of foreign key relationships

## Best Practices

### Choosing SCD Type

**Use Type 1 when**:
- Historical changes are not business-critical
- Storage efficiency is a priority
- Query simplicity is important
- Data changes represent corrections rather than legitimate state changes

**Use Type 2 when**:
- Historical analysis is required
- Audit trails are mandatory
- Business processes depend on understanding data evolution
- Regulatory compliance requires change tracking

### Performance Considerations

**Type 1**:
- Faster writes (simple updates)
- Smaller table sizes
- Simpler query patterns

**Type 2**:
- More complex write operations
- Larger table sizes due to historical records
- Requires indexing on versioning columns
- Queries should include `is_current = true` filters for current data

### Data Quality

**Type 1**:
- Ensure data validation before overwriting
- Consider backup strategies for critical corrections
- Monitor for unintended data loss

**Type 2**:
- Validate effective/expiration date logic
- Ensure proper handling of concurrent updates
- Monitor for orphaned historical records

## Migration Considerations

When changing SCD types:

1. **Type 1 to Type 2**: Requires schema changes and data migration to add versioning columns
2. **Type 2 to Type 1**: Consider data archival strategy for historical records before conversion

## Monitoring and Maintenance

### Type 1 Tables
- Monitor update frequency and patterns
- Track data quality metrics
- Alert on unexpected bulk updates

### Type 2 Tables
- Monitor table growth rates
- Track version distribution
- Alert on excessive versioning (potential data quality issues)
- Implement archival strategies for old versions

## Related Documentation

- [Table Naming Convention](TABLE_NAMING_CONVENTION.md)
- [Data Movement Strategy](DATA_MOVEMENT_STRATEGY_CLICKHOUSE.md)
- [Performance Optimization Architecture](PERFORMANCE_OPTIMIZATION_ARCHITECTURE.md)