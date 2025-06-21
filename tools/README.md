# AVESA Tools Directory

This directory contains development tools, rebuild utilities, and maintenance scripts for the AVESA Multi-Tenant Data Pipeline.

**Last Updated:** June 21, 2025
**Status:** Created during Phase 4 reorganization for better separation of concerns

## Development Tools

### Lambda Function Management

#### [`rebuild-clickhouse-lambdas.py`](rebuild-clickhouse-lambdas.py)
Rebuilds and updates ClickHouse Lambda functions.

```bash
# Rebuild all ClickHouse Lambda functions
python tools/rebuild-clickhouse-lambdas.py --environment dev

# Rebuild specific Lambda function
python tools/rebuild-clickhouse-lambdas.py --function clickhouse-scd-processor --environment dev

# Rebuild with verbose output
python tools/rebuild-clickhouse-lambdas.py --environment dev --verbose
```

**Features:**
- Rebuilds Lambda deployment packages
- Updates function code and configuration
- Supports environment-specific deployments
- Comprehensive logging and error handling

## Usage Guidelines

### Prerequisites

```bash
# Install required dependencies
pip install -r requirements.txt

# Set up AWS credentials
export AWS_PROFILE=your-profile
export AWS_DEFAULT_REGION=us-east-2
```

### Tool Categories

- **Rebuild Tools** - Scripts for rebuilding and updating components
- **Development Utilities** - Helper scripts for development workflows
- **Maintenance Scripts** - Tools for system maintenance and cleanup

## Development Workflow

### Lambda Function Updates

```bash
# After making changes to Lambda function code
python tools/rebuild-clickhouse-lambdas.py --environment dev

# Test the updated functions
python ../tests/test-api-endpoints.py --environment dev
```

### Integration with Other Directories

- **Scripts** (`../scripts/`) - Core operational scripts
- **Tests** (`../tests/`) - Testing and validation scripts
- **Source** (`../src/`) - Lambda function source code

## Contributing

When adding new tools:

1. **Follow naming conventions** (`kebab-case.py` for Python tools)
2. **Include usage documentation** in script headers
3. **Add error handling** and validation
4. **Update this README** with new tool information
5. **Test thoroughly** in development environment
6. **Ensure compatibility** with existing infrastructure

### Tool Categories

**✅ Current Tools:**
- Lambda function rebuild utilities
- Development workflow helpers

**🔄 Planned Tools:**
- Database schema migration utilities
- Performance profiling tools
- Automated testing helpers
- Infrastructure validation tools

## Related Documentation

- [Scripts Directory](../scripts/README.md) - Core operational scripts
- [Tests Directory](../tests/README.md) - Testing and validation scripts
- [Deployment Guide](../docs/DEPLOYMENT_GUIDE.md) - Deployment procedures
- [Development Guide](../docs/DEVELOPMENT_GUIDE.md) - Development workflows