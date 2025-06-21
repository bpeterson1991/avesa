# AVESA Tests Directory

This directory contains all testing, validation, and verification scripts for the AVESA Multi-Tenant Data Pipeline.

**Last Updated:** June 21, 2025
**Status:** Reorganized from `/scripts` directory for better separation of concerns

## Test Categories

### Pipeline Testing Scripts

#### [`validate-pipeline.py`](validate-pipeline.py) ⭐ **COMPREHENSIVE VALIDATION SUITE**
Unified pipeline validation script with multiple validation modes.

```bash
# Full validation (recommended for production readiness)
python tests/validate-pipeline.py --mode full

# Quick health checks (fast connectivity tests)
python tests/validate-pipeline.py --mode quick

# Data integrity focus (SCD validation, multi-tenant isolation)
python tests/validate-pipeline.py --mode data

# Performance metrics (query performance, response times)
python tests/validate-pipeline.py --mode performance
```

#### [`end-to-end-pipeline-test.py`](end-to-end-pipeline-test.py)
Comprehensive test of the entire AVESA data pipeline.

```bash
python tests/end-to-end-pipeline-test.py --environment dev --region us-east-2
```

#### [`targeted-pipeline-test.py`](targeted-pipeline-test.py)
Focused test of canonical transform and ClickHouse loading.

```bash
python tests/targeted-pipeline-test.py --environment dev
```

### Component Testing Scripts

#### [`test_clickhouse_connection.py`](test_clickhouse_connection.py)
Tests ClickHouse Cloud connectivity using AWS Secrets Manager.

```bash
python tests/test_clickhouse_connection.py
```

#### [`test-api-endpoints.py`](test-api-endpoints.py)
Tests AVESA API endpoints with synchronized ClickHouse schema.

```bash
python tests/test-api-endpoints.py --environment dev
```

### Validation Scripts

#### [`verify-clickhouse-deployment.py`](verify-clickhouse-deployment.py)
Verifies ClickHouse infrastructure deployment.

```bash
python tests/verify-clickhouse-deployment.py --environment dev
```

#### [`validate-dependency-standardization.py`](validate-dependency-standardization.py)
Validates dependency version standardization.

```bash
python tests/validate-dependency-standardization.py
```

#### [`validate-security-setup.py`](validate-security-setup.py)
Validates security configuration and removes hardcoded credentials.

```bash
python tests/validate-security-setup.py --environment dev
```

## Unit Tests

### Core Component Tests

#### [`test_dynamic_orchestrator.py`](test_dynamic_orchestrator.py)
Unit tests for the dynamic orchestrator component.

#### [`test_dynamic_processors.py`](test_dynamic_processors.py)
Unit tests for dynamic processors.

#### [`test_mapping_file_distribution.py`](test_mapping_file_distribution.py)
Tests for mapping file distribution strategy.

#### [`test_new_service_integration.py`](test_new_service_integration.py)
Tests for new service integration capabilities.

#### [`test_shared_utils.py`](test_shared_utils.py)
Tests for shared utility functions.

## Running Tests

### Prerequisites

```bash
# Install test dependencies
pip install -r requirements.txt

# Set up AWS credentials
export AWS_PROFILE=your-profile
export AWS_DEFAULT_REGION=us-east-2
```

### Test Execution

```bash
# Run all unit tests
python -m pytest tests/test_*.py -v

# Run pipeline validation
python tests/validate-pipeline.py --mode full

# Run end-to-end tests
python tests/end-to-end-pipeline-test.py --environment dev

# Run specific component tests
python tests/test_clickhouse_connection.py
python tests/test-api-endpoints.py
```

### Test Categories

- **Unit Tests** (`test_*.py`) - Component-level testing
- **Integration Tests** (`*-test.py`) - Cross-component testing
- **Validation Scripts** (`validate-*.py`) - Configuration and setup validation
- **Verification Scripts** (`verify-*.py`) - Deployment and infrastructure verification

## Test Environment Setup

### Development Environment

```bash
# Start development environment
../scripts/start-development-environment.sh real

# Run tests against development environment
python tests/validate-pipeline.py --mode full
```

### Production Validation

```bash
# Validate production deployment
python tests/verify-clickhouse-deployment.py --environment prod
python tests/validate-security-setup.py --environment prod
```

## Contributing

When adding new tests:

1. **Follow naming conventions** (`test_*.py` for unit tests, `*-test.py` for integration tests)
2. **Include docstrings** explaining test purpose and expected behavior
3. **Add error handling** and proper assertions
4. **Update this README** with new test information
5. **Ensure tests are idempotent** and can run multiple times safely

## Related Documentation

- [Scripts Directory](../scripts/README.md) - Core operational scripts
- [Tools Directory](../tools/README.md) - Development and rebuild utilities
- [Deployment Guide](../docs/DEPLOYMENT_GUIDE.md) - Deployment procedures
- [Testing Strategy](../docs/TESTING_STRATEGY.md) - Comprehensive testing approach