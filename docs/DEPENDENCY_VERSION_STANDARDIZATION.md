# Dependency Version Standardization

## Overview

This document explains the version standardization decisions made across the Avesa project to ensure consistency, compatibility, and maintainability of dependencies.

## Standardization Principles

### 1. **Consistency Across Components**
- All Lambda functions use identical versions of shared dependencies
- Infrastructure and application code use compatible versions
- Version conflicts are eliminated through careful resolution

### 2. **Latest Stable Versions**
- Use the most recent stable versions that maintain compatibility
- Prioritize security updates and bug fixes
- Avoid bleeding-edge versions that may introduce instability

### 3. **Explicit Version Pinning**
- Lambda functions use exact versions (`==`) for predictable deployments
- Root requirements use minimum versions (`>=`) for flexibility
- Infrastructure uses exact versions for reproducible builds

## Version Decisions

### AWS SDK (boto3/botocore)

**Standardized Version**: `boto3==1.38.39`, `botocore==1.38.39`

**Rationale**:
- Latest stable version as of standardization
- Provides newest AWS service features and security updates
- Consistent across all Lambda functions and infrastructure
- Eliminates version conflicts between components

**Files Updated**:
- `src/clickhouse/schema_init/requirements.txt` (updated from 1.34.0)
- `src/backfill/requirements.txt` (updated from >=1.26.0)
- `src/optimized/orchestrator/requirements.txt` (updated from >=1.29.0)
- `infrastructure/requirements.txt` (updated from >=1.26.0)
- `requirements.txt` (updated from >=1.34.0)

### ClickHouse Connector

**Standardized Version**: `clickhouse-connect==0.8.17`

**Rationale**:
- Stable version tested across all ClickHouse functions
- No changes needed - already consistent
- Provides reliable ClickHouse connectivity and query execution

**Files Using This Version**:
- `src/clickhouse/schema_init/requirements.txt`
- `src/clickhouse/data_loader/requirements.txt`
- `src/clickhouse/scd_processor/requirements.txt`

### Data Processing Libraries

**Standardized Versions**: `pandas==2.2.3`, `pyarrow==18.1.0`

**Rationale**:
- Compatible versions optimized for performance
- pandas 2.2.3 provides latest features while maintaining stability
- pyarrow 18.1.0 offers improved Parquet processing performance
- Used consistently in data processing Lambda functions

**Files Using These Versions**:
- `src/clickhouse/data_loader/requirements.txt`
- `src/clickhouse/scd_processor/requirements.txt`
- `requirements.txt` (minimum versions for development)

### AWS CDK

**Standardized Version**: `aws-cdk-lib==2.100.0`

**Rationale**:
- Stable baseline version used across infrastructure
- Provides all required CDK features for the project
- Exact version ensures reproducible infrastructure deployments

**Files Using This Version**:
- `infrastructure/requirements.txt`
- `requirements.txt` (minimum version for development)

## Update Procedures

### For Lambda Functions
1. Update the specific Lambda's `requirements.txt` file
2. Test the Lambda function with new dependencies
3. Deploy and verify functionality
4. Update other Lambda functions to maintain consistency

### For Infrastructure
1. Update `infrastructure/requirements.txt`
2. Test CDK synthesis and deployment in development
3. Verify all stacks deploy successfully
4. Update root `requirements.txt` to maintain alignment

### For Development Environment
1. Update root `requirements.txt` with minimum compatible versions
2. Test all development workflows (testing, linting, etc.)
3. Update CI/CD pipelines if necessary
4. Document any breaking changes

## Compatibility Matrix

| Component | boto3 | pandas | pyarrow | clickhouse-connect | aws-cdk-lib |
|-----------|-------|--------|---------|-------------------|-------------|
| ClickHouse Schema Init | 1.38.39 | - | - | 0.8.17 | - |
| ClickHouse Data Loader | 1.38.39 | 2.2.3 | 18.1.0 | 0.8.17 | - |
| ClickHouse SCD Processor | 1.38.39 | 2.2.3 | 18.1.0 | 0.8.17 | - |
| Backfill Functions | 1.38.39 | - | - | - | - |
| Orchestrator | 1.38.39 | - | - | - | - |
| Infrastructure | 1.38.39 | - | - | - | 2.100.0 |
| Development | ≥1.38.39 | ≥2.2.3 | ≥18.1.0 | - | ≥2.100.0 |

## Safety Measures

### Version Pinning Strategy
- **Lambda Functions**: Exact versions (`==`) for predictable runtime behavior
- **Infrastructure**: Exact versions (`==`) for reproducible deployments
- **Development**: Minimum versions (`>=`) for flexibility while maintaining compatibility

### Testing Requirements
- All version updates must pass existing test suites
- Integration tests verify cross-component compatibility
- Performance tests ensure no regression with new versions

### Rollback Procedures
- Keep previous working versions documented
- Test rollback procedures in development environment
- Maintain deployment scripts for quick version rollbacks

## Future Considerations

### Regular Update Schedule
- Review dependency versions quarterly
- Monitor security advisories for critical updates
- Plan major version upgrades during maintenance windows

### Compatibility Monitoring
- Track AWS service API changes that may affect boto3 usage
- Monitor ClickHouse connector updates for performance improvements
- Watch for pandas/pyarrow compatibility issues

### Documentation Maintenance
- Update this document when versions change
- Maintain changelog of version updates and reasons
- Document any compatibility issues discovered

## Implementation Status

✅ **COMPLETED**: Lambda Requirements Standardization
- All Lambda functions now use boto3==1.38.39
- Consistent pandas/pyarrow versions where applicable
- Added comprehensive documentation to all requirements files

✅ **COMPLETED**: Root Requirements Alignment
- Updated root requirements.txt with latest compatible versions
- Aligned with infrastructure requirements
- Maintained development flexibility with minimum versions

✅ **COMPLETED**: Infrastructure Requirements Update
- Updated infrastructure requirements to use standardized boto3 version
- Maintained exact CDK version for stability
- Added documentation explaining version choices

## Maintenance Notes

- This standardization eliminates version conflicts identified in the codebase audit
- All changes maintain backward compatibility with existing functionality
- No breaking changes introduced - only version updates and documentation improvements
- Future updates should follow the procedures outlined in this document