# Development Tools

This directory contains development utilities and tools for the Avesa ClickHouse implementation.

## Files

### `insert-sample-data.js`
**Purpose**: Inserts sample data directly into ClickHouse tables for testing and development.

**Usage**:
```bash
# From project root
node dev-tools/insert-sample-data.js
```

**Requirements**:
- AWS credentials configured
- ClickHouse credentials in AWS Secrets Manager
- Environment variables set (AWS_REGION, CLICKHOUSE_SECRET_NAME)

**What it does**:
- Connects to ClickHouse using AWS Secrets Manager credentials
- Inserts sample data for companies, contacts, tickets, and time_entries tables
- Verifies data insertion with count queries
- Uses tenant_id 'sitetechnology' for all sample data

## Development Files Kept in Original Locations

The following development files remain in their original locations as they are actively used:

- `src/clickhouse/api/debug-server-startup.js` - Debug script for API server startup issues
- `src/clickhouse/api/mock-server.js` - Mock API server for frontend development
- `src/clickhouse/api/config/clickhouse-dev.js` - Mock ClickHouse client for development

## Notes

- All tools in this directory are for development and testing purposes only
- Do not use these tools in production environments
- Ensure proper AWS credentials and permissions before running any scripts