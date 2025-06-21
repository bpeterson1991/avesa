# AVESA Pipeline Security Implementation Guide

This guide documents the comprehensive security implementation for the AVESA data pipeline, including credential management, access controls, and security best practices.

## Overview

The AVESA pipeline has been secured with the following key improvements:

1. **Removed all hardcoded credentials** from scripts and configuration files
2. **Implemented AWS Secrets Manager** for secure credential storage
3. **Added IAM role-based access control** for AWS services
4. **Created automated credential rotation** capabilities
5. **Implemented comprehensive security validation** tools

## Security Architecture

### Credential Management

#### AWS Secrets Manager Integration
- All sensitive credentials are stored in AWS Secrets Manager
- Secrets are encrypted at rest using AWS KMS
- Access is controlled through IAM policies
- Automatic rotation is supported

#### ClickHouse Credentials
- Stored in secret: `avesa/clickhouse/{environment}`
- Contains: host, port, username, password, SSL settings
- Accessed only through the credential manager

#### AWS Credentials
- Uses IAM roles and profiles instead of hardcoded keys
- Supports temporary credentials with session tokens
- Automatic credential refresh

### Access Control

#### IAM Roles
- Service-specific roles for Lambda functions
- Cross-account roles for multi-environment access
- Least privilege principle applied

#### Environment Isolation
- Separate credentials per environment (dev, staging, prod)
- Environment-specific resource naming
- Isolated S3 buckets and Lambda functions

## Implementation Components

### 1. Credential Manager (`src/shared/credential_manager.py`)

Central component for secure credential management:

```python
from shared.credential_manager import get_credential_manager

# Get credential manager
manager = get_credential_manager()

# Retrieve ClickHouse credentials
clickhouse_creds = manager.get_clickhouse_credentials('dev')

# Get AWS credentials for a service
aws_creds = manager.get_aws_credentials_for_service('s3', 'dev')

# Validate credentials
is_valid = manager.validate_credentials('clickhouse', 'dev')
```

### 2. Secure ClickHouse Configuration (`scripts/configure-clickhouse-s3-secure.py`)

Replaces the insecure version with:
- Credentials from Secrets Manager
- Environment-specific configuration
- Credential validation
- Secure S3 function creation

### 3. Credential Setup (`scripts/setup-clickhouse-credentials.py`)

Interactive and automated credential setup:

```bash
# Interactive setup
python scripts/setup-clickhouse-credentials.py --interactive --environment dev

# From environment variables
export CLICKHOUSE_HOST="your-host.clickhouse.cloud"
export CLICKHOUSE_PASSWORD="your-secure-password"
python scripts/setup-clickhouse-credentials.py --from-env --environment dev

# Validate existing credentials
python scripts/setup-clickhouse-credentials.py --validate --environment dev

# Rotate password
python scripts/setup-clickhouse-credentials.py --rotate --environment dev
```

### 4. Automated Credential Rotation (`scripts/setup-credential-rotation.py`)

Implements automated credential rotation:

```bash
# Set up rotation (every 90 days)
python scripts/setup-credential-rotation.py --rotation-days 90

# Set up manual trigger
python scripts/setup-credential-rotation.py --manual-trigger

# Test rotation function
python scripts/setup-credential-rotation.py --test-only
```

### 5. Security Validation (`scripts/validate-security-setup.py`)

Comprehensive security validation:

```bash
# Run full security validation
python scripts/validate-security-setup.py

# Generate JSON report
python scripts/validate-security-setup.py --json-output

# Save detailed report
python scripts/validate-security-setup.py --report-file security-report.json
```

## Security Features

### Hardcoded Credential Removal

All hardcoded credentials have been removed from:
- ✅ `scripts/configure-clickhouse-s3.py` → Replaced with secure version
- ✅ `scripts/end-to-end-pipeline-test.py` → Updated to use credential manager
- ✅ Lambda functions → Use IAM roles and Secrets Manager
- ✅ Configuration files → Environment-specific settings

### Credential Validation

Automatic validation of:
- AWS credential availability and permissions
- ClickHouse connection and authentication
- S3 bucket access
- Lambda function permissions

### Rotation Capabilities

- **Automated rotation**: Scheduled via EventBridge
- **Manual rotation**: Triggered via SNS
- **Validation**: Test new credentials before activation
- **Rollback**: Maintain previous credentials for emergency rollback

### Monitoring and Auditing

- CloudWatch logs for all credential operations
- CloudTrail logging for API access
- Security validation reports
- Automated alerting for credential issues

## Setup Instructions

### 1. Initial Setup

```bash
# 1. Set up ClickHouse credentials
python scripts/setup-clickhouse-credentials.py --interactive --environment dev

# 2. Configure secure ClickHouse S3 integration
python scripts/configure-clickhouse-s3-secure.py --environment dev --setup-tables

# 3. Set up credential rotation
python scripts/setup-credential-rotation.py --rotation-days 90 --manual-trigger

# 4. Validate security setup
python scripts/validate-security-setup.py
```

### 2. Environment-Specific Setup

For each environment (dev, staging, prod):

```bash
# Set up credentials for environment
python scripts/setup-clickhouse-credentials.py --environment staging

# Configure ClickHouse integration
python scripts/configure-clickhouse-s3-secure.py --environment staging

# Validate setup
python scripts/validate-security-setup.py
```

### 3. Testing

```bash
# Test secure pipeline
python scripts/end-to-end-pipeline-test.py

# Test credential rotation
python scripts/setup-credential-rotation.py --test-only

# Validate all security measures
python scripts/validate-security-setup.py --report-file security-validation.json
```

## Security Best Practices

### 1. Credential Management
- ✅ Store all credentials in AWS Secrets Manager
- ✅ Use IAM roles instead of user credentials where possible
- ✅ Implement automatic credential rotation
- ✅ Validate credentials before use
- ✅ Monitor credential access

### 2. Access Control
- ✅ Apply least privilege principle
- ✅ Use environment-specific roles
- ✅ Implement cross-account access controls
- ✅ Regular access reviews

### 3. Monitoring
- ✅ Enable CloudTrail logging
- ✅ Monitor credential usage
- ✅ Set up security alerts
- ✅ Regular security audits

### 4. Development Practices
- ✅ Never commit credentials to version control
- ✅ Use environment variables for local development
- ✅ Implement security scanning in CI/CD
- ✅ Regular security training

## Troubleshooting

### Common Issues

#### 1. "Unable to locate credentials" Error

**Cause**: AWS credentials not properly configured

**Solution**:
```bash
# Check current AWS identity
aws sts get-caller-identity

# Validate AWS credentials
python scripts/validate-security-setup.py

# Set up proper AWS profile
aws configure --profile avesa-dev
```

#### 2. ClickHouse Connection Failed

**Cause**: ClickHouse credentials not in Secrets Manager

**Solution**:
```bash
# Set up ClickHouse credentials
python scripts/setup-clickhouse-credentials.py --interactive

# Validate credentials
python scripts/setup-clickhouse-credentials.py --validate
```

#### 3. Lambda Function Access Denied

**Cause**: Lambda function lacks proper IAM permissions

**Solution**:
- Check Lambda function IAM role
- Ensure role has Secrets Manager access
- Validate environment-specific permissions

### Security Validation Failures

Run the security validator to identify issues:

```bash
python scripts/validate-security-setup.py --json-output
```

Common validation failures and solutions:

1. **Hardcoded credentials found**: Remove and store in Secrets Manager
2. **AWS credentials invalid**: Check IAM role/profile configuration
3. **ClickHouse credentials missing**: Run credential setup script
4. **Rotation not configured**: Set up credential rotation

## Migration from Insecure Setup

### 1. Backup Current Configuration

```bash
# Backup current scripts (if needed)
cp scripts/configure-clickhouse-s3.py scripts/configure-clickhouse-s3.py.backup
```

### 2. Set Up Secure Credentials

```bash
# Set up ClickHouse credentials in Secrets Manager
python scripts/setup-clickhouse-credentials.py --interactive
```

### 3. Update Scripts

- Use `scripts/configure-clickhouse-s3-secure.py` instead of the old version
- Update any custom scripts to use the credential manager
- Test with `scripts/end-to-end-pipeline-test.py`

### 4. Validate Migration

```bash
# Run security validation
python scripts/validate-security-setup.py

# Test pipeline functionality
python scripts/end-to-end-pipeline-test.py
```

## Compliance and Auditing

### Security Standards

The implementation follows these security standards:
- AWS Well-Architected Security Pillar
- NIST Cybersecurity Framework
- SOC 2 Type II requirements
- Industry best practices for credential management

### Audit Trail

All security-related activities are logged:
- Credential access (CloudTrail)
- Secret rotation (CloudWatch)
- Security validation (Application logs)
- Access patterns (AWS Config)

### Regular Reviews

Implement regular security reviews:
- Monthly credential audits
- Quarterly access reviews
- Annual security assessments
- Continuous monitoring

## Support and Escalation

### Security Issues

For security issues:
1. Run security validation: `python scripts/validate-security-setup.py`
2. Check CloudWatch logs for errors
3. Review CloudTrail for unauthorized access
4. Contact security team if needed

### Emergency Procedures

In case of credential compromise:
1. Immediately rotate affected credentials
2. Review access logs
3. Update affected systems
4. Document incident

## Related Documentation

- [AWS Credentials Setup Guide](AWS_CREDENTIALS_GUIDE.md)
- [Deployment Guide](DEPLOYMENT_GUIDE.md)
- [ClickHouse Implementation Guide](CLICKHOUSE_GUIDE.md)
- [AWS Security Best Practices](https://docs.aws.amazon.com/security/)

## Conclusion

The AVESA pipeline now implements comprehensive security measures including:
- Secure credential management
- Automated rotation capabilities
- Comprehensive validation tools
- Environment isolation
- Monitoring and auditing

This implementation ensures that the pipeline meets enterprise security standards while maintaining operational efficiency.