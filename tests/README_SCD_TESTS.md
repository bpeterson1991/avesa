# SCD-Aware Test Suite

This directory contains an enhanced test suite that properly validates mixed SCD Type 1 and Type 2 behavior across the AVESA pipeline.

## Overview

The SCD-aware test suite ensures that:
- **Type 1 tables** properly overwrite existing records (no historical versions)
- **Type 2 tables** maintain historical records with proper SCD fields
- **Mixed SCD scenarios** work correctly across different table types
- **Schema-configuration alignment** is validated

## Current SCD Configuration

Based on the canonical mapping files:

| Table | SCD Type | Behavior |
|-------|----------|----------|
| `companies` | Type 1 | Simple upsert - overwrites existing records |
| `contacts` | Type 1 | Simple upsert - overwrites existing records |
| `tickets` | Type 2 | Historical tracking - maintains all versions |
| `time_entries` | Type 1 | Simple upsert - overwrites existing records |

## Test Files

### Core Test Files

1. **`test_scd_behavior.py`** - Comprehensive SCD behavior validation
   - Tests Type 1 upsert behavior (overwrites existing records)
   - Tests Type 2 historical tracking (creates new versions)
   - Tests mixed SCD scenarios
   - Validates schema-configuration alignment

2. **`test_scd_config.py`** - SCD configuration management tests
   - Tests SCDConfigManager functionality
   - Validates SCD type determination
   - Tests configuration validation
   - Tests convenience functions

3. **`validate-pipeline.py`** - Enhanced pipeline validation (UPDATED)
   - Now includes SCD-aware validation logic
   - Uses SCDConfigManager to determine table SCD types
   - Validates different behaviors for Type 1 vs Type 2 tables
   - Updated `verify_scd_structure()` method to be SCD-type aware

### Test Runners

4. **`run_scd_tests.py`** - Comprehensive SCD test runner
   - Runs all SCD-related tests
   - Provides multiple test modes
   - Generates detailed reports
   - Validates configuration consistency

## Running the Tests

### Quick Start

```bash
# Run all SCD tests
python tests/run_scd_tests.py

# Run specific test categories
python tests/run_scd_tests.py --mode behavior
python tests/run_scd_tests.py --mode config
python tests/run_scd_tests.py --mode pipeline
python tests/run_scd_tests.py --mode consistency
```

### Individual Test Files

```bash
# Run SCD behavior tests
python -m pytest tests/test_scd_behavior.py -v

# Run SCD configuration tests
python -m pytest tests/test_scd_config.py -v

# Run enhanced pipeline validation
python tests/validate-pipeline.py --mode data
```

## Test Modes

### Full Mode (Default)
Runs all SCD-related tests:
- SCD configuration tests
- SCD behavior validation
- Configuration consistency checks
- Schema alignment validation
- Enhanced pipeline validation

### Behavior Mode
Focuses on SCD behavior validation:
- Type 1 upsert behavior
- Type 2 historical tracking
- Mixed SCD scenarios
- Data structure validation

### Config Mode
Tests SCD configuration management:
- Configuration loading
- SCD type determination
- Validation logic
- Error handling

### Pipeline Mode
Runs enhanced pipeline validation:
- SCD-aware data integrity checks
- Table-specific validation logic
- S3 canonical data structure validation

### Consistency Mode
Validates configuration consistency:
- Cross-system SCD configuration alignment
- Schema-configuration matching
- Business rule compliance

## Key Test Scenarios

### Type 1 Behavior Validation
```python
# Tests that Type 1 tables (companies, contacts, time_entries):
# - Overwrite existing records on update
# - Do not maintain historical versions
# - Have no duplicate IDs for the same tenant
# - Do not have SCD tracking fields
```

### Type 2 Behavior Validation
```python
# Tests that Type 2 tables (tickets):
# - Maintain historical records
# - Have proper SCD tracking fields
# - Have exactly one current record per ID
# - Properly manage effective dates
# - Set is_current flags correctly
```

### Mixed SCD Scenarios
```python
# Tests that mixed SCD processing:
# - Correctly identifies table SCD types
# - Applies appropriate processing logic
# - Maintains data integrity across table types
# - Handles configuration changes gracefully
```

## Enhanced Pipeline Validation Changes

The `validate-pipeline.py` file has been enhanced with SCD awareness:

### Key Changes

1. **SCD Manager Integration**
   ```python
   from shared.scd_config import SCDConfigManager
   self.scd_manager = SCDConfigManager()
   ```

2. **SCD-Aware ClickHouse Validation**
   - Determines SCD type for each table
   - Uses appropriate validation logic for Type 1 vs Type 2
   - Validates SCD field presence/absence based on table type

3. **Enhanced SCD Structure Verification**
   - Table-specific validation based on SCD type
   - Type 1: Validates absence of SCD fields
   - Type 2: Validates presence and correctness of SCD fields

4. **Improved Reporting**
   - Reports SCD type for each table
   - Provides SCD-specific validation results
   - Identifies configuration mismatches

## Expected Test Results

### Successful Test Run
```
🧪 SCD-AWARE TEST SUITE
===============================================================================

📋 Running SCD Configuration Tests
✅ SCD configuration tests passed
✅ Passed 15 SCD configuration tests

📋 Running SCD Behavior Tests  
✅ SCD behavior tests passed
✅ Passed 12 SCD behavior tests

📋 Validating SCD Configuration Consistency
✅ companies: type_1 (consistent)
✅ contacts: type_1 (consistent)
✅ tickets: type_2 (consistent)
✅ time_entries: type_1 (consistent)
✅ SCD configuration is consistent across all tables

📋 Validating Schema-Configuration Alignment
✅ companies: schema aligned with type_1 configuration
✅ contacts: schema aligned with type_1 configuration
✅ tickets: schema aligned with type_2 configuration
✅ time_entries: schema aligned with type_1 configuration
✅ All table schemas are aligned with their SCD configuration

🎯 SCD TEST SUITE STATUS: ALL_PASSED
🎉 SCD TEST SUITE: SUCCESS
✅ All SCD-aware tests passed!
```

## Troubleshooting

### Common Issues

1. **Import Errors**
   ```bash
   # Ensure src is in Python path
   export PYTHONPATH="${PYTHONPATH}:$(pwd)/src"
   ```

2. **Missing Dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configuration Issues**
   - Verify canonical mapping files exist in `mappings/canonical/`
   - Check that SCD types are properly configured
   - Ensure AWS credentials are available for S3 access

### Debug Mode

Run tests with verbose output:
```bash
python tests/run_scd_tests.py --mode full -v
python -m pytest tests/test_scd_behavior.py -v -s
```

## Integration with CI/CD

Add to your CI/CD pipeline:
```yaml
- name: Run SCD Tests
  run: |
    python tests/run_scd_tests.py --mode full
    
- name: Validate Pipeline with SCD Awareness
  run: |
    python tests/validate-pipeline.py --mode data
```

## Future Enhancements

1. **Real ClickHouse Integration**
   - Connect to actual ClickHouse instances
   - Validate real data structures
   - Test actual SCD processing

2. **Performance Testing**
   - SCD processing performance benchmarks
   - Large dataset validation
   - Concurrent processing tests

3. **Data Quality Validation**
   - SCD field data quality checks
   - Historical data integrity validation
   - Cross-table relationship validation

## Contributing

When adding new SCD-related functionality:

1. Update the appropriate test files
2. Add new test cases for edge cases
3. Update this README with new test scenarios
4. Ensure all tests pass before submitting

## Related Documentation

- [SCD Configuration Guide](../docs/SCD_CONFIGURATION_GUIDE.md)
- [SCD Implementation Summary](../docs/SCD_IMPLEMENTATION_SUMMARY.md)
- [Main Test README](README.md)