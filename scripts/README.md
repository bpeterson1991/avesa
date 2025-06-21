# AVESA Scripts Directory

This directory contains automated scripts for setting up and managing the AVESA Multi-Tenant Data Pipeline optimized infrastructure.

**Last Updated:** June 21, 2025
**Status:** Phase 4 Scripts Reorganization completed - 59% reduction in /scripts directory, proper separation of concerns

## Recently Completed: Phase 4 Scripts Reorganization ✅

**MAJOR CLEANUP COMPLETED:** Comprehensive script consolidation and reorganization reducing maintenance overhead by 59%.

### Reorganization Summary:
- **Started with:** 39 scripts in `/scripts`
- **Removed:** 16 obsolete/completed scripts
- **Moved to /tmp:** 2 one-time setup scripts
- **Moved to /tests:** 8 test/validation scripts
- **Moved to /tools:** 1 rebuild/utility script
- **Consolidated:** 8 scripts → 3 unified scripts
- **Final `/scripts` count:** 16 scripts (59% reduction from original 39)

### Directory Structure:
- **[`/scripts`](.)** - Core operational scripts (deployment, setup, data loading)
- **[`/tests`](../tests/)** - All testing, validation, and verification scripts
- **[`/tools`](../tools/)** - Rebuild utilities and development tools
- **`/tmp`** - One-time setup scripts (completed)

### Phase 3 Consolidation Details:

#### 🗑️ **Removed Scripts (16 total):**
**Obsolete/Completed Tasks:**
- `complete-data-loading.py` - Task completed
- `run-schema-migration.py` - Superseded by unified schema manager
- `migrate-companies-schema.sql` - Migration completed
- `fix-canonical-scd-transform.py` - Fix applied
- `configure-clickhouse-s3.py` - Security risk, superseded by secure version
- `validate-infrastructure-consolidation.py` - Consolidation completed

**Server Startup Scripts (consolidated):**
- `start-api-server.sh` → [`start-development-environment.sh`](start-development-environment.sh)
- `start-dev-servers.sh` → [`start-development-environment.sh`](start-development-environment.sh)
- `start-fullstack.sh` → [`start-development-environment.sh`](start-development-environment.sh)
- `start-working-servers.sh` → [`start-development-environment.sh`](start-development-environment.sh)

**Validation Scripts (consolidated):**
- `complete-end-to-end-validation.py` → [`validate-pipeline.py`](validate-pipeline.py)
- `final-pipeline-validation.py` → [`validate-pipeline.py`](validate-pipeline.py)

**Schema Management Scripts (consolidated):**
- `aws-clickhouse-schema-migration.py` → [`manage-clickhouse-schema.py`](manage-clickhouse-schema.py)
- `comprehensive-schema-migration.py` → [`manage-clickhouse-schema.py`](manage-clickhouse-schema.py)

#### 📦 **Moved to /tmp (2 total):**
- `setup-clickhouse-iam.py` - One-time setup completed
- `setup-credential-rotation.py` - Optional feature, rarely used

#### ⭐ **New Unified Scripts (3 total):**
1. **[`start-development-environment.sh`](start-development-environment.sh)** - Unified development server launcher
2. **[`manage-clickhouse-schema.py`](manage-clickhouse-schema.py)** - Unified ClickHouse schema management

#### 📦 **Scripts Moved to New Directories (9 total):**
**Moved to [`/tests`](../tests/):**
- `validate-pipeline.py` → [`tests/validate-pipeline.py`](../tests/validate-pipeline.py) - Comprehensive validation suite
- `end-to-end-pipeline-test.py` → [`tests/end-to-end-pipeline-test.py`](../tests/end-to-end-pipeline-test.py)
- `targeted-pipeline-test.py` → [`tests/targeted-pipeline-test.py`](../tests/targeted-pipeline-test.py)
- `test_clickhouse_connection.py` → [`tests/test_clickhouse_connection.py`](../tests/test_clickhouse_connection.py)
- `test-api-endpoints.py` → [`tests/test-api-endpoints.py`](../tests/test-api-endpoints.py)
- `validate-dependency-standardization.py` → [`tests/validate-dependency-standardization.py`](../tests/validate-dependency-standardization.py)
- `validate-security-setup.py` → [`tests/validate-security-setup.py`](../tests/validate-security-setup.py)
- `verify-clickhouse-deployment.py` → [`tests/verify-clickhouse-deployment.py`](../tests/verify-clickhouse-deployment.py)

**Moved to [`/tools`](../tools/):**
- `rebuild-clickhouse-lambdas.py` → [`tools/rebuild-clickhouse-lambdas.py`](../tools/rebuild-clickhouse-lambdas.py)

### Previous Phase 2 Consolidation:

**MAJOR CLEANUP COMPLETED:** Consolidated 8+ duplicate load-canonical-data scripts into a single, configurable script.

### Before Consolidation:
- `load-canonical-data.py` (base version)
- `load-canonical-data-simple.py` (simplified approach)
- `load-canonical-data-direct.py` (direct loading approach)
- `load-canonical-data-with-creds.py` (with credentials handling)
- `load-canonical-data-with-schema-mapping.py` (with schema mapping)
- `load-canonical-data-corrected.py` (corrected version)
- `load-canonical-data-fixed.py` (fixed version)
- `load-canonical-data-python.py` (python-specific version)

### After Consolidation:
- **Single consolidated script:** [`load-canonical-data.py`](load-canonical-data.py) ⭐ **CONSOLIDATED LOADER**

**Backup Location:** All original scripts moved to `/tmp/avesa-script-cleanup-backup/` for safety.

## New Unified Scripts ⭐

### Development Environment Management

#### [`start-development-environment.sh`](start-development-environment.sh) ⭐ **UNIFIED DEVELOPMENT LAUNCHER**
Consolidated development server startup script with multiple modes.

```bash
# Start with real ClickHouse data (recommended for testing)
./scripts/start-development-environment.sh real

# Start with mock data (fastest for frontend development)
./scripts/start-development-environment.sh mock

# Start API server only with real ClickHouse data
./scripts/start-development-environment.sh api-only

# Start full-stack with hot reload (best for development)
./scripts/start-development-environment.sh frontend
```

**Features:**
- **4 Launch Modes:** Real data, mock data, API-only, and frontend development
- **AWS Credential Management:** Automatic AWS profile detection and configuration
- **Dependency Installation:** Automatic npm dependency installation
- **Process Management:** Graceful startup, monitoring, and cleanup
- **Error Handling:** Fallback to mock server if real ClickHouse fails
- **Browser Integration:** Automatic browser opening on macOS

**Replaces 4 Previous Scripts:**
- `start-api-server.sh` - API server with real ClickHouse data
- `start-dev-servers.sh` - Simple development server startup
- `start-fullstack.sh` - Full-stack development with hot reload
- `start-working-servers.sh` - Mock API and React frontend

### Pipeline Validation

#### [`validate-pipeline.py`](validate-pipeline.py) ⭐ **COMPREHENSIVE VALIDATION SUITE**
Unified pipeline validation script with multiple validation modes.

```bash
# Full validation (recommended for production readiness)
python scripts/validate-pipeline.py --mode full

# Quick health checks (fast connectivity tests)
python scripts/validate-pipeline.py --mode quick

# Data integrity focus (SCD validation, multi-tenant isolation)
python scripts/validate-pipeline.py --mode data

# Performance metrics (query performance, response times)
python scripts/validate-pipeline.py --mode performance
```

**Features:**
- **4 Validation Modes:** Full, quick, data-focused, and performance testing
- **ClickHouse Integration:** Direct ClickHouse connectivity and data validation
- **API Testing:** Comprehensive API endpoint validation
- **Frontend Validation:** React application connectivity testing
- **Multi-tenant Testing:** Tenant isolation and data integrity validation
- **Comprehensive Reporting:** JSON reports with detailed metrics and timestamps

**Replaces 2 Previous Scripts:**
- `complete-end-to-end-validation.py` - End-to-end pipeline validation
- `final-pipeline-validation.py` - Final validation with reporting

### Schema Management

#### [`manage-clickhouse-schema.py`](manage-clickhouse-schema.py) ⭐ **UNIFIED SCHEMA MANAGER**
Comprehensive ClickHouse schema management with multiple operation modes.

```bash
# Migrate schema to match canonical mappings (recommended)
python scripts/manage-clickhouse-schema.py --mode migrate --credentials aws

# Validate current schema against mappings
python scripts/manage-clickhouse-schema.py --mode validate --credentials aws

# Analyze schema differences and generate reports
python scripts/manage-clickhouse-schema.py --mode analyze --credentials aws

# Use environment variables for credentials instead of AWS Secrets Manager
python scripts/manage-clickhouse-schema.py --mode migrate --credentials env
```

**Features:**
- **4 Operation Modes:** Migrate, validate, analyze, and initialize
- **Dual Credential Support:** AWS Secrets Manager or environment variables
- **Intelligent Type Mapping:** Automatic ClickHouse type determination
- **Schema Analysis:** Detailed difference analysis and reporting
- **Data Integrity Validation:** SCD Type 2 validation and record counting
- **Utility Creation:** Automatic schema status views and analysis tools

**Replaces 2 Previous Scripts:**
- `aws-clickhouse-schema-migration.py` - AWS Secrets Manager schema migration
- `comprehensive-schema-migration.py` - Environment variable schema migration

## Currently Active Scripts

### Data Loading Scripts

#### [`load-canonical-data.py`](load-canonical-data.py) ⭐ **CONSOLIDATED CANONICAL DATA LOADER**
Consolidated ClickHouse canonical data loader with multiple approaches.

```bash
# Most reliable approach (recommended)
python scripts/load-canonical-data.py --mode python --tenant-id sitetechnology

# Fastest approach (if ClickHouse has S3 access)
python scripts/load-canonical-data.py --mode direct --with-creds

# Clear existing data first
python scripts/load-canonical-data.py --mode python --clear-existing

# Dry run to see what would be done
python scripts/load-canonical-data.py --mode python --dry-run
```

**Features:**
- **2 Loading Modes:** Direct (ClickHouse S3 function) and Python (download & insert)
- **Configurable Options:** Credentials, tenant selection, data clearing
- **Proper Null Handling:** Safe data type conversion for ClickHouse compatibility
- **Comprehensive Logging:** Detailed progress and error reporting
- **Dry Run Support:** Preview operations without executing

**Consolidates 8 Previous Scripts:**
- Combines all functionality from duplicate scripts
- Simplified command-line interface
- Maintains backward compatibility for all use cases
- Reduces maintenance overhead by 87.5% (8 scripts → 1 script)

### Core Deployment Scripts

#### [`deploy.sh`](deploy.sh) ⭐ **PRIMARY DEPLOYMENT SCRIPT**
Deployment script for all environments using the parallel processing architecture.

```bash
# Deploy to development
./scripts/deploy.sh --environment dev

# Deploy to production
./scripts/deploy.sh --environment prod --profile avesa-production
```

**Features:**
- Deploys infrastructure using [`infrastructure/app.py`](../infrastructure/app.py)
- Includes Step Functions state machines for parallel processing
- Enhanced Lambda functions with lightweight packaging
- Comprehensive monitoring and dashboards

## AWS IAM Setup Scripts for GitHub Actions

These scripts automate the setup of cross-account IAM roles and users needed for GitHub Actions production deployment with the optimized architecture.

### Quick Start

For a complete automated setup, run:

```bash
./scripts/setup-github-actions-aws.sh
```

This interactive script will guide you through the entire process.

### Individual Scripts

#### 1. Production Account Setup

[`setup-production-iam.sh`](setup-production-iam.sh) - Creates the deployment role in your production AWS account for optimized architecture deployment.

```bash
./scripts/setup-production-iam.sh \
  --prod-account-id 123456789012 \
  --dev-account-id 987654321098
```

**What it creates:**
- `GitHubActionsDeploymentRole` - Cross-account deployment role
- Trust policy allowing development account access
- PowerUserAccess + custom IAM permissions
- External ID for additional security

#### 2. Development Account Setup

[`setup-development-iam.sh`](setup-development-iam.sh) - Creates the GitHub Actions user in your development account for optimized deployments.

```bash
./scripts/setup-development-iam.sh \
  --prod-account-id 123456789012 \
  --dev-account-id 987654321098
```

**What it creates:**
- `github-actions-deployer` IAM user
- Access keys for GitHub secrets
- Policy to assume production role
- Automatic role assumption testing

#### 3. Combined Setup

[`setup-github-actions-aws.sh`](setup-github-actions-aws.sh) - Interactive script that runs both setups.

```bash
./scripts/setup-github-actions-aws.sh
```

**Features:**
- Interactive prompts for account IDs
- Validates AWS CLI configuration
- Runs both production and development setup
- Provides GitHub secrets configuration
- Creates rollback scripts

#### 4. Validation Script

[`validate-github-actions-setup.sh`](validate-github-actions-setup.sh) - Tests the complete setup.

```bash
./scripts/validate-github-actions-setup.sh
```

**What it tests:**
- Cross-account role assumption
- AWS service permissions
- CDK bootstrap status
- GitHub Actions simulation

#### 5. sts:TagSession Permission Fix

[`fix-sts-tagsession-permission.sh`](fix-sts-tagsession-permission.sh) - Fixes missing sts:TagSession permission.

```bash
./scripts/fix-sts-tagsession-permission.sh \
  --dev-account-id 123938354448 \
  --prod-account-id 987654321098
```

**What it fixes:**
- Adds missing `sts:TagSession` permission to existing policies
- Resolves "User is not authorized to perform: sts:TagSession" errors
- Updates the `AVESAAssumeProductionRole` policy
- Enables successful GitHub Actions role assumption

### Prerequisites

Before running these scripts, ensure you have:

1. **AWS CLI** installed and configured
2. **jq** installed for JSON processing
3. **Access to both AWS accounts** with appropriate permissions
4. **IAM permissions** to create roles, users, and policies

#### Installation Commands

```bash
# macOS
brew install awscli jq

# Ubuntu/Debian
sudo apt-get install awscli jq

# CentOS/RHEL
sudo yum install awscli jq
```

### Usage Examples

#### Complete Setup (Recommended)

```bash
# Interactive setup - will prompt for all required information
./scripts/setup-github-actions-aws.sh

# Non-interactive setup
./scripts/setup-github-actions-aws.sh \
  --prod-account-id 123456789012 \
  --dev-account-id 987654321098 \
  --non-interactive
```

#### Individual Component Setup

```bash
# Setup production account only
./scripts/setup-production-iam.sh \
  --prod-account-id 123456789012 \
  --dev-account-id 987654321098 \
  --profile avesa-production

# Setup development account only
./scripts/setup-development-iam.sh \
  --prod-account-id 123456789012 \
  --dev-account-id 987654321098 \
  --profile my-dev-profile

# Validate the complete setup
./scripts/validate-github-actions-setup.sh \
  --access-key-id AKIA... \
  --secret-access-key ... \
  --role-arn arn:aws:iam::123456789012:role/GitHubActionsDeploymentRole
```

### GitHub Secrets Configuration

After running the setup scripts, add these secrets to your GitHub repository:

| Secret Name | Description | Example Value |
|-------------|-------------|---------------|
| `AWS_ACCESS_KEY_ID_PROD` | Development account access key | `AKIA...` |
| `AWS_SECRET_ACCESS_KEY_PROD` | Development account secret key | `wJalrXUtnFEMI/K7MDENG...` |
| `AWS_PROD_DEPLOYMENT_ROLE_ARN` | Production deployment role ARN | `arn:aws:iam::123456789012:role/GitHubActionsDeploymentRole` |

### Security Features

#### Cross-Account Security
- **External ID**: Additional security layer for role assumption
- **Least Privilege**: Minimal required permissions
- **Account Isolation**: Development and production account separation

#### Credential Management
- **Temporary Credentials**: GitHub Actions uses temporary role credentials
- **No Long-term Production Access**: Development account cannot directly access production
- **Automatic Rotation**: Support for regular access key rotation

### Troubleshooting

#### Common Issues

**❌ "User is not authorized to perform: sts:TagSession"**
```bash
# Fix missing sts:TagSession permission
./scripts/fix-sts-tagsession-permission.sh \
  --dev-account-id YOUR_DEV_ACCOUNT \
  --prod-account-id YOUR_PROD_ACCOUNT
```

**❌ "Could not assume role"**
```bash
# Check role ARN and trust policy
aws iam get-role --role-name GitHubActionsDeploymentRole
```

**❌ "Access Denied"**
```bash
# Verify IAM permissions
aws iam simulate-principal-policy \
  --policy-source-arn arn:aws:iam::ACCOUNT:user/github-actions-deployer \
  --action-names sts:AssumeRole sts:TagSession
```

**❌ "Invalid credentials"**
```bash
# Test basic access
aws sts get-caller-identity
```

#### Validation Commands

```bash
# Test role assumption manually
aws sts assume-role \
  --role-arn arn:aws:iam::PROD_ACCOUNT:role/GitHubActionsDeploymentRole \
  --role-session-name test-session \
  --external-id avesa-github-actions-2024

# Validate GitHub Actions setup
./scripts/validate-github-actions-setup.sh --verbose
```

### Rollback Procedures

The setup scripts create rollback scripts in `/tmp/`:

```bash
# Rollback production setup
/tmp/rollback-production-iam.sh

# Rollback development setup
/tmp/rollback-development-iam.sh
```

**Manual Rollback:**

```bash
# Remove development user
aws iam delete-access-key --user-name github-actions-deployer --access-key-id AKIA...
aws iam detach-user-policy --user-name github-actions-deployer --policy-arn arn:aws:iam::DEV_ACCOUNT:policy/AVESAAssumeProductionRole
aws iam delete-policy --policy-arn arn:aws:iam::DEV_ACCOUNT:policy/AVESAAssumeProductionRole
aws iam delete-user --user-name github-actions-deployer

# Remove production role
aws iam detach-role-policy --role-name GitHubActionsDeploymentRole --policy-arn arn:aws:iam::aws:policy/PowerUserAccess
aws iam detach-role-policy --role-name GitHubActionsDeploymentRole --policy-arn arn:aws:iam::PROD_ACCOUNT:policy/AVESADeploymentPolicy
aws iam delete-policy --policy-arn arn:aws:iam::PROD_ACCOUNT:policy/AVESADeploymentPolicy
aws iam delete-role --role-name GitHubActionsDeploymentRole
```

## Service Management Scripts

### Lambda Function Management

#### [`package-lightweight-lambdas.py`](package-lightweight-lambdas.py) ⭐ **OPTIMIZED PACKAGING**
Package optimized Lambda functions with lightweight dependencies.

```bash
# Package all optimized functions
python scripts/package-lightweight-lambdas.py --function all

# Package specific optimized function
python scripts/package-lightweight-lambdas.py --function optimized-orchestrator
python scripts/package-lightweight-lambdas.py --function optimized-processors
```

**Features:**
- 99.9% package size reduction using AWS Pandas layers
- Optimized for the new parallel processing architecture
- Supports all optimized Lambda components

### Tenant and Service Management

#### [`setup-service.py`](setup-service.py)
Configure tenant services for the optimized pipeline.

```bash
# Setup ConnectWise for a tenant (works with optimized architecture)
python scripts/setup-service.py \
  --tenant-id "example-tenant" \
  --service connectwise \
  --environment dev
```

#### [`trigger-backfill.py`](trigger-backfill.py)
Trigger data backfill for tenants using optimized processing.

```bash
# Trigger backfill for specific tenant (uses optimized pipeline)
python scripts/trigger-backfill.py \
  --tenant-id "example-tenant" \
  --environment dev
```

### Testing and Validation Scripts

#### [`test-end-to-end-pipeline.py`](test-end-to-end-pipeline.py) ⭐ **OPTIMIZED TESTING**
Test the complete optimized pipeline end-to-end.

```bash
# Test optimized pipeline
python scripts/test-end-to-end-pipeline.py --environment dev --region us-east-2
```

#### [`test-lambda-functions.py`](test-lambda-functions.py)
Test individual Lambda functions (updated for optimized architecture).

```bash
# Test optimized Lambda functions
python scripts/test-lambda-functions.py --environment dev --region us-east-2 --verbose
```

### Maintenance Scripts

#### [`cleanup-stuck-jobs.py`](cleanup-stuck-jobs.py)
Clean up stuck processing jobs in the optimized pipeline.

```bash
# Clean up stuck jobs
python scripts/cleanup-stuck-jobs.py --environment dev
```

## Script Dependencies

### Python Scripts
- Python 3.9+
- boto3
- Required packages in [`requirements.txt`](../requirements.txt)
- AWS Pandas layers (for optimized Lambda functions)

### Bash Scripts
- AWS CLI v2
- jq (JSON processor)
- Standard Unix utilities (grep, sed, awk)

## Environment Variables

### AWS Configuration
```bash
export AWS_PROFILE=your-profile
export AWS_DEFAULT_REGION=us-east-2
```

### GitHub Actions Secrets (for optimized deployments)
```bash
export AWS_ACCESS_KEY_ID_PROD=AKIA...
export AWS_SECRET_ACCESS_KEY_PROD=...
export AWS_PROD_DEPLOYMENT_ROLE_ARN=arn:aws:iam::123456789012:role/GitHubActionsDeploymentRole
```

### Optimized Architecture Variables
```bash
# For optimized CDK deployment
export CDK_DEFAULT_ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
export CDK_PROD_ACCOUNT=your-prod-account-id
```

## Best Practices

### Security
1. **Rotate access keys** every 90 days
2. **Use least privilege** IAM policies
3. **Monitor CloudTrail** for unusual activity
4. **Store secrets securely** in GitHub only
5. **Use optimized architecture** for enhanced security

### Operations
1. **Test in development** before production using optimized pipeline
2. **Use validation scripts** before deployment
3. **Document all changes** in commit messages
4. **Monitor optimized pipeline** performance via CloudWatch dashboards

### Monitoring
1. **Enable CloudTrail** logging
2. **Set up CloudWatch** alarms for optimized components
3. **Monitor GitHub Actions** logs for optimized deployments
4. **Review IAM policies** regularly
5. **Use optimized monitoring dashboards** for real-time insights

## Support

For issues with these scripts:

1. **Check prerequisites** are installed
2. **Validate AWS credentials** and permissions
3. **Run validation script** for detailed diagnostics
4. **Review troubleshooting section** above
5. **Check project documentation** in [`docs/`](../docs/)

### Related Documentation

- [Deployment Guide (Updated)](../docs/DEPLOYMENT_GUIDE.md) - Optimized deployment procedures
- [Dev Environment Setup (Updated)](../docs/DEPLOYMENT_GUIDE.md#development-environment-setup) - Optimized development setup
- [Migration Strategy (Completed)](../docs/MIGRATION_STRATEGY.md) - Migration completion details
- [Phase 1 Implementation (Completed)](../docs/PHASE_1_IMPLEMENTATION_README.md) - Implementation status
- [AWS Credentials Setup Guide](../docs/AWS_CREDENTIALS_GUIDE.md)
- [GitHub Secrets Quick Setup](../docs/GITHUB_SECRETS_QUICK_SETUP.md)
- [sts:TagSession Permission Fix](../docs/STS_TAGSESSION_FIX.md)
- [Production Environment Setup](../docs/PROD_ENVIRONMENT_SETUP_GUIDE.md)

## Contributing

When adding new scripts:

1. **Follow naming conventions** (`kebab-case.sh` or `snake_case.py`)
2. **Include usage documentation** in script headers
3. **Add error handling** and validation
4. **Update this README** with new script information
5. **Test thoroughly** in development environment with optimized architecture
6. **Ensure compatibility** with optimized infrastructure components

### Script Categories

**✅ Active Scripts** (in `scripts/` directory):
- Deployment and infrastructure management
- Service and tenant configuration
- Testing and validation
- Maintenance and cleanup

## License

These scripts are part of the AVESA project and follow the same licensing terms.