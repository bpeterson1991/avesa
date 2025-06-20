# Mapping File Distribution Strategy

## Overview

This document describes the comprehensive strategy for distributing mapping files across all AVESA environments (dev, staging, production) to ensure reliable dynamic service discovery and canonical table resolution.

## Problem Statement

The AVESA pipeline relies on mapping files for:
- **Service Discovery**: Identifying available integration services
- **Endpoint Configuration**: Defining API endpoints and their configurations
- **Canonical Mapping**: Transforming raw data into canonical format
- **Dynamic Orchestration**: Building processing workflows based on available services

**Previous Issues:**
- Mapping files only available in dev environment (local filesystem)
- Lambda functions in staging/production couldn't access mapping files
- Inconsistent loading strategies between different components
- S3-based loading without proper distribution mechanism

## Solution: Bundled Mapping Files Strategy

### Approach

We implemented **Option A: Bundle mapping files with Lambda packages** for maximum reliability and consistency.

### Key Components

#### 1. Enhanced Packaging Script

**File**: [`scripts/package-lightweight-lambdas.py`](../scripts/package-lightweight-lambdas.py)

**Changes:**
- Automatically includes entire `mappings/` directory in Lambda packages
- Copies all mapping files to package root for consistent access
- Validates mapping file count during packaging
- Maintains directory structure: `mappings/canonical/`, `mappings/services/`, `mappings/integrations/`

#### 2. Multi-Source Loading Strategy

**File**: [`src/shared/utils.py`](../src/shared/utils.py)

**Loading Priority:**
1. **Bundled mappings** (Lambda package): `./mappings/`
2. **Local development** (relative path): `../../mappings/`
3. **S3 fallback** (if BUCKET_NAME env var set): `s3://bucket/mappings/`
4. **Default mappings** (hardcoded fallbacks)

**Updated Functions:**
- `load_canonical_mapping()` - Loads canonical table mappings
- `load_service_configuration()` - Loads service configurations
- `load_endpoint_configuration()` - Loads endpoint configurations
- `discover_available_services()` - Discovers services from bundled/local mappings
- `discover_canonical_tables()` - Discovers canonical tables from bundled/local mappings

#### 3. Consistent Lambda Function Updates

**Files Updated:**
- [`src/canonical_transform/lambda_function.py`](../src/canonical_transform/lambda_function.py) - Uses shared utilities
- [`infrastructure/stacks/performance_optimization_stack.py`](../infrastructure/stacks/performance_optimization_stack.py) - Uses packaged Lambda code
- [`scripts/deploy.sh`](../scripts/deploy.sh) - Includes mapping files in all Lambda packages

## Implementation Details

### Mapping File Structure

```
mappings/
├── canonical/           # Canonical table mappings
│   ├── companies.json
│   ├── contacts.json
│   ├── tickets.json
│   └── time_entries.json
├── services/           # Service configurations
│   ├── connectwise.json
│   ├── salesforce.json
│   └── servicenow.json
├── integrations/       # Endpoint configurations
│   ├── connectwise_endpoints.json
│   ├── salesforce_endpoints.json
│   └── servicenow_endpoints.json
└── backfill_config.json
```

### Lambda Package Structure

After packaging, Lambda functions contain:

```
lambda-package/
├── lambda_function.py
├── config_simple.py
├── logger.py
├── utils.py
├── aws_clients.py
└── mappings/           # Bundled mapping files
    ├── canonical/
    ├── services/
    ├── integrations/
    └── backfill_config.json
```

### Loading Logic Flow

```python
def load_canonical_mapping(canonical_table: str) -> Dict[str, Any]:
    # 1. Try bundled mappings (Lambda environment)
    bundled_path = os.path.join(os.path.dirname(__file__), 'mappings', 'canonical', f'{canonical_table}.json')
    if os.path.exists(bundled_path):
        return load_from_file(bundled_path)
    
    # 2. Try local development mappings
    local_path = os.path.join(os.path.dirname(__file__), '..', '..', 'mappings', 'canonical', f'{canonical_table}.json')
    if os.path.exists(local_path):
        return load_from_file(local_path)
    
    # 3. Try S3 fallback (if configured)
    if os.environ.get('BUCKET_NAME'):
        return load_from_s3(f"mappings/canonical/{canonical_table}.json")
    
    # 4. Return empty dict if not found
    return {}
```

## Deployment Process

### Updated Deployment Steps

1. **Package Lambda Functions**:
   ```bash
   python3 scripts/package-lightweight-lambdas.py --function canonical
   ```

2. **Deploy Infrastructure**:
   ```bash
   cd infrastructure && cdk deploy --context environment=staging
   ```

3. **Verify Mapping Access**:
   ```bash
   python3 tests/test_mapping_file_distribution.py
   ```

### Environment-Specific Behavior

| Environment | Primary Source | Fallback | Notes |
|-------------|---------------|----------|-------|
| **Development** | Local filesystem | Bundled (if packaged) | Direct file access |
| **Staging** | Bundled in Lambda | S3 (if configured) | Reliable package-based |
| **Production** | Bundled in Lambda | S3 (if configured) | Reliable package-based |

## Testing Strategy

### Comprehensive Test Suite

**File**: [`tests/test_mapping_file_distribution.py`](../tests/test_mapping_file_distribution.py)

**Test Coverage:**
1. **Local Development Environment** - Verifies filesystem access
2. **Lambda Package Environment** - Tests bundled file access
3. **Fallback Behavior** - Ensures graceful handling of missing files
4. **Mapping File Consistency** - Validates JSON structure

**Run Tests:**
```bash
python3 tests/test_mapping_file_distribution.py
```

### Expected Results

```
Testing Mapping File Distribution System
==================================================
✓ Local Development Environment: PASS
✓ Lambda Package Environment: PASS  
✓ Fallback Behavior: PASS
✓ Mapping File Consistency: PASS

🎉 All mapping file distribution tests passed!
✅ Mapping files are properly distributed and accessible in all environments
```

## Benefits

### Reliability
- **Consistent Access**: Mapping files available in all environments
- **No External Dependencies**: No reliance on S3 for core functionality
- **Atomic Deployment**: Mapping files deployed with Lambda code

### Performance
- **Fast Access**: Local file system access (no network calls)
- **Reduced Latency**: No S3 API calls during Lambda execution
- **Cold Start Optimization**: Files immediately available

### Maintainability
- **Single Source of Truth**: Mapping files in version control
- **Automatic Distribution**: Packaging script handles distribution
- **Environment Consistency**: Same files across all environments

## Troubleshooting

### Common Issues

#### 1. Mapping Files Not Found
**Symptoms**: Empty service discovery, failed canonical transformations
**Solution**: 
- Verify packaging script includes mapping files
- Check Lambda package contents: `unzip -l package.zip | grep mappings`
- Run distribution tests: `python3 tests/test_mapping_file_distribution.py`

#### 2. Outdated Mapping Files
**Symptoms**: Missing services, incorrect transformations
**Solution**:
- Redeploy Lambda functions to pick up latest mapping files
- Verify mapping file timestamps in package

#### 3. JSON Parsing Errors
**Symptoms**: Failed to load mapping configurations
**Solution**:
- Run consistency test: `python3 tests/test_mapping_file_distribution.py`
- Validate JSON syntax in mapping files

### Debugging Commands

```bash
# Test local mapping access
python3 tests/test_dynamic_service_discovery.py

# Test packaged mapping access
python3 tests/test_mapping_file_distribution.py

# Verify package contents
unzip -l lambda-packages/canonical-transform.zip | grep mappings

# Check mapping file count
find mappings -name "*.json" | wc -l
```

## Future Enhancements

### Potential Improvements

1. **Mapping File Versioning**: Track mapping file versions for rollback capability
2. **Dynamic Updates**: Hot-reload mapping files without redeployment
3. **Validation Pipeline**: Automated mapping file validation in CI/CD
4. **Performance Monitoring**: Track mapping file access patterns

### Migration Path

For future S3-based distribution (if needed):
1. Maintain bundled files as primary source
2. Add S3 sync in deployment pipeline
3. Update loading logic to prefer S3 for specific use cases
4. Implement caching layer for S3-based access

## Conclusion

The bundled mapping files strategy provides a robust, reliable, and performant solution for distributing mapping files across all AVESA environments. This approach ensures:

- ✅ **Consistent availability** in dev, staging, and production
- ✅ **Fast access** without network dependencies  
- ✅ **Atomic deployment** with Lambda functions
- ✅ **Comprehensive testing** for all scenarios
- ✅ **Easy troubleshooting** with clear debugging steps

The implementation successfully resolves the mapping file distribution issue and provides a solid foundation for dynamic service discovery across all environments.