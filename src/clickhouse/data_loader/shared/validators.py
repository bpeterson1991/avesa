"""
Data Validators - Centralized validation and quality checks

This module provides:
- Centralized credential validation functions
- Service configuration validation
- Data quality checks and metrics
- Tenant configuration validation
- API response validation
"""

import logging
import re
from typing import Dict, Any, List, Optional, Union
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


class ValidationError(Exception):
    """Custom exception for validation errors."""
    pass


class CredentialValidator:
    """
    Centralized credential validation for various services.
    
    Consolidates validation logic that was duplicated across:
    - src/backfill/lambda_function.py:263-281
    - src/shared/config_simple.py:96-116
    - scripts/setup-service.py:139-166
    """
    
    # Service-specific required fields
    SERVICE_REQUIRED_FIELDS = {
        'connectwise': ['company_id', 'public_key', 'private_key', 'client_id'],
        'salesforce': ['username', 'password', 'security_token', 'client_id', 'client_secret'],
        'servicenow': ['username', 'password', 'instance_url'],
        'azure': ['tenant_id', 'client_id', 'client_secret'],
        'google': ['client_id', 'client_secret', 'refresh_token']
    }
    
    # Optional fields that enhance functionality
    SERVICE_OPTIONAL_FIELDS = {
        'connectwise': ['api_base_url', 'timeout', 'max_retries'],
        'salesforce': ['api_version', 'sandbox'],
        'servicenow': ['api_version', 'timeout'],
        'azure': ['resource', 'api_version'],
        'google': ['scope', 'api_version']
    }
    
    @staticmethod
    def validate_connectwise(credentials: Dict[str, Any]) -> bool:
        """
        Validate ConnectWise credentials have required fields.
        
        Consolidates validation logic from:
        - src/backfill/lambda_function.py:263-281
        - src/shared/config_simple.py:96-116
        
        Args:
            credentials: Credentials dictionary
            
        Returns:
            True if valid, False otherwise
            
        Raises:
            ValidationError: If validation fails with detailed error message
        """
        required_fields = CredentialValidator.SERVICE_REQUIRED_FIELDS['connectwise']
        
        missing_fields = []
        invalid_fields = []
        
        for field in required_fields:
            value = credentials.get(field)
            if not value:
                missing_fields.append(field)
            elif not isinstance(value, str) or not value.strip():
                invalid_fields.append(field)
        
        # Additional ConnectWise-specific validations
        if 'company_id' in credentials:
            company_id = credentials['company_id']
            if not re.match(r'^[a-zA-Z0-9_-]+$', company_id):
                invalid_fields.append('company_id (invalid format)')
        
        if 'client_id' in credentials:
            client_id = credentials['client_id']
            # ConnectWise client IDs are typically UUIDs
            if not re.match(r'^[0-9a-fA-F-]{36}$', client_id):
                logger.warning(f"ConnectWise client_id doesn't match UUID format: {client_id}")
        
        # Check for common URL field
        if 'api_base_url' in credentials:
            api_url = credentials['api_base_url']
            if api_url and not api_url.startswith(('http://', 'https://')):
                invalid_fields.append('api_base_url (must start with http:// or https://)')
        
        if missing_fields or invalid_fields:
            error_parts = []
            if missing_fields:
                error_parts.append(f"Missing required fields: {', '.join(missing_fields)}")
            if invalid_fields:
                error_parts.append(f"Invalid fields: {', '.join(invalid_fields)}")
            
            error_message = '; '.join(error_parts)
            logger.error(f"ConnectWise credential validation failed: {error_message}")
            raise ValidationError(f"ConnectWise credential validation failed: {error_message}")
        
        logger.debug("ConnectWise credentials validated successfully")
        return True
    
    @staticmethod
    def validate_salesforce(credentials: Dict[str, Any]) -> bool:
        """
        Validate Salesforce credentials have required fields.
        
        Args:
            credentials: Credentials dictionary
            
        Returns:
            True if valid, False otherwise
            
        Raises:
            ValidationError: If validation fails
        """
        required_fields = CredentialValidator.SERVICE_REQUIRED_FIELDS['salesforce']
        
        missing_fields = []
        invalid_fields = []
        
        for field in required_fields:
            value = credentials.get(field)
            if not value:
                missing_fields.append(field)
            elif not isinstance(value, str) or not value.strip():
                invalid_fields.append(field)
        
        # Salesforce-specific validations
        if 'username' in credentials:
            username = credentials['username']
            if '@' not in username:
                invalid_fields.append('username (must be email format)')
        
        if missing_fields or invalid_fields:
            error_parts = []
            if missing_fields:
                error_parts.append(f"Missing required fields: {', '.join(missing_fields)}")
            if invalid_fields:
                error_parts.append(f"Invalid fields: {', '.join(invalid_fields)}")
            
            error_message = '; '.join(error_parts)
            logger.error(f"Salesforce credential validation failed: {error_message}")
            raise ValidationError(f"Salesforce credential validation failed: {error_message}")
        
        logger.debug("Salesforce credentials validated successfully")
        return True
    
    @staticmethod
    def validate_servicenow(credentials: Dict[str, Any]) -> bool:
        """
        Validate ServiceNow credentials have required fields.
        
        Args:
            credentials: Credentials dictionary
            
        Returns:
            True if valid, False otherwise
            
        Raises:
            ValidationError: If validation fails
        """
        required_fields = CredentialValidator.SERVICE_REQUIRED_FIELDS['servicenow']
        
        missing_fields = []
        invalid_fields = []
        
        for field in required_fields:
            value = credentials.get(field)
            if not value:
                missing_fields.append(field)
            elif not isinstance(value, str) or not value.strip():
                invalid_fields.append(field)
        
        # ServiceNow-specific validations
        if 'instance_url' in credentials:
            instance_url = credentials['instance_url']
            if not instance_url.startswith(('http://', 'https://')):
                invalid_fields.append('instance_url (must start with http:// or https://)')
            elif not re.match(r'https?://[a-zA-Z0-9.-]+\.service-now\.com', instance_url):
                logger.warning(f"ServiceNow instance URL doesn't match expected format: {instance_url}")
        
        if missing_fields or invalid_fields:
            error_parts = []
            if missing_fields:
                error_parts.append(f"Missing required fields: {', '.join(missing_fields)}")
            if invalid_fields:
                error_parts.append(f"Invalid fields: {', '.join(invalid_fields)}")
            
            error_message = '; '.join(error_parts)
            logger.error(f"ServiceNow credential validation failed: {error_message}")
            raise ValidationError(f"ServiceNow credential validation failed: {error_message}")
        
        logger.debug("ServiceNow credentials validated successfully")
        return True
    
    @staticmethod
    def validate_service_credentials(service: str, credentials: Dict[str, Any]) -> bool:
        """
        Generic service credential validation.
        
        Consolidates validation logic from scripts/setup-service.py:139-166
        
        Args:
            service: Service name (e.g., 'connectwise', 'salesforce')
            credentials: Credentials dictionary
            
        Returns:
            True if valid, False otherwise
            
        Raises:
            ValidationError: If validation fails
        """
        service = service.lower()
        
        # Dispatch to service-specific validator
        validators = {
            'connectwise': CredentialValidator.validate_connectwise,
            'salesforce': CredentialValidator.validate_salesforce,
            'servicenow': CredentialValidator.validate_servicenow
        }
        
        if service not in validators:
            # Generic validation for unknown services
            return CredentialValidator._validate_generic_service(service, credentials)
        
        return validators[service](credentials)
    
    @staticmethod
    def _validate_generic_service(service: str, credentials: Dict[str, Any]) -> bool:
        """
        Generic validation for services without specific validators.
        
        Args:
            service: Service name
            credentials: Credentials dictionary
            
        Returns:
            True if valid, False otherwise
            
        Raises:
            ValidationError: If validation fails
        """
        if not credentials:
            raise ValidationError(f"No credentials provided for service: {service}")
        
        # Check for common credential patterns
        common_fields = ['username', 'password', 'api_key', 'token', 'client_id', 'client_secret']
        found_fields = [field for field in common_fields if field in credentials and credentials[field]]
        
        if not found_fields:
            raise ValidationError(f"No valid credential fields found for service: {service}")
        
        logger.debug(f"Generic validation passed for service: {service}")
        return True
    
    @staticmethod
    def get_required_fields(service: str) -> List[str]:
        """
        Get required credential fields for a service.
        
        Args:
            service: Service name
            
        Returns:
            List of required field names
        """
        service = service.lower()
        return CredentialValidator.SERVICE_REQUIRED_FIELDS.get(service, [])
    
    @staticmethod
    def get_optional_fields(service: str) -> List[str]:
        """
        Get optional credential fields for a service.
        
        Args:
            service: Service name
            
        Returns:
            List of optional field names
        """
        service = service.lower()
        return CredentialValidator.SERVICE_OPTIONAL_FIELDS.get(service, [])


class DataQualityValidator:
    """
    Data quality validation and metrics.
    """
    
    @staticmethod
    def validate_record_completeness(record: Dict[str, Any], required_fields: List[str]) -> Dict[str, Any]:
        """
        Validate that a record has all required fields.
        
        Args:
            record: Data record to validate
            required_fields: List of required field names
            
        Returns:
            Validation result with details
        """
        missing_fields = []
        empty_fields = []
        
        for field in required_fields:
            if field not in record:
                missing_fields.append(field)
            elif record[field] is None or (isinstance(record[field], str) and not record[field].strip()):
                empty_fields.append(field)
        
        is_valid = len(missing_fields) == 0 and len(empty_fields) == 0
        
        return {
            'is_valid': is_valid,
            'missing_fields': missing_fields,
            'empty_fields': empty_fields,
            'completeness_score': 1.0 - (len(missing_fields) + len(empty_fields)) / len(required_fields)
        }
    
    @staticmethod
    def validate_data_types(record: Dict[str, Any], field_types: Dict[str, type]) -> Dict[str, Any]:
        """
        Validate data types in a record.
        
        Args:
            record: Data record to validate
            field_types: Dictionary mapping field names to expected types
            
        Returns:
            Validation result with details
        """
        type_errors = []
        
        for field, expected_type in field_types.items():
            if field in record and record[field] is not None:
                if not isinstance(record[field], expected_type):
                    type_errors.append({
                        'field': field,
                        'expected_type': expected_type.__name__,
                        'actual_type': type(record[field]).__name__,
                        'value': str(record[field])[:100]  # Truncate long values
                    })
        
        return {
            'is_valid': len(type_errors) == 0,
            'type_errors': type_errors,
            'type_accuracy': 1.0 - len(type_errors) / len(field_types) if field_types else 1.0
        }
    
    @staticmethod
    def validate_date_fields(record: Dict[str, Any], date_fields: List[str]) -> Dict[str, Any]:
        """
        Validate date fields in a record.
        
        Args:
            record: Data record to validate
            date_fields: List of field names that should contain dates
            
        Returns:
            Validation result with details
        """
        date_errors = []
        
        for field in date_fields:
            if field in record and record[field] is not None:
                try:
                    # Try to parse as ISO format datetime
                    if isinstance(record[field], str):
                        datetime.fromisoformat(record[field].replace('Z', '+00:00'))
                    elif not isinstance(record[field], datetime):
                        date_errors.append({
                            'field': field,
                            'value': str(record[field]),
                            'error': 'Not a valid date format'
                        })
                except (ValueError, TypeError) as e:
                    date_errors.append({
                        'field': field,
                        'value': str(record[field]),
                        'error': str(e)
                    })
        
        return {
            'is_valid': len(date_errors) == 0,
            'date_errors': date_errors,
            'date_accuracy': 1.0 - len(date_errors) / len(date_fields) if date_fields else 1.0
        }


class TenantConfigValidator:
    """
    Tenant configuration validation.
    """
    
    @staticmethod
    def validate_tenant_config(config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate tenant configuration structure.
        
        Args:
            config: Tenant configuration dictionary
            
        Returns:
            Validation result with details
        """
        required_fields = ['tenant_id', 'company_name', 'enabled']
        optional_fields = ['created_at', 'updated_at', 'custom_config']
        
        validation_result = {
            'is_valid': True,
            'errors': [],
            'warnings': []
        }
        
        # Check required fields
        for field in required_fields:
            if field not in config:
                validation_result['errors'].append(f"Missing required field: {field}")
                validation_result['is_valid'] = False
            elif not config[field]:
                validation_result['errors'].append(f"Empty required field: {field}")
                validation_result['is_valid'] = False
        
        # Validate tenant_id format
        if 'tenant_id' in config:
            tenant_id = config['tenant_id']
            if not re.match(r'^[a-zA-Z0-9_-]+$', tenant_id):
                validation_result['errors'].append("tenant_id contains invalid characters")
                validation_result['is_valid'] = False
        
        # Validate enabled field
        if 'enabled' in config and not isinstance(config['enabled'], bool):
            validation_result['errors'].append("enabled field must be boolean")
            validation_result['is_valid'] = False
        
        return validation_result


class DataValidator:
    """
    Main data validator class that combines all validation functionality.
    Provides a unified interface for data validation across the application.
    """
    
    def __init__(self, strict_mode: bool = False):
        """
        Initialize the data validator.
        
        Args:
            strict_mode: Whether to use strict validation rules
        """
        self.strict_mode = strict_mode
        self.credential_validator = CredentialValidator()
        self.quality_validator = DataQualityValidator()
        self.tenant_validator = TenantConfigValidator()
    
    def validate_company_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate company data structure and content.
        
        Args:
            data: Company data to validate
            
        Returns:
            Validation result with details
        """
        required_fields = ['id', 'name']
        optional_fields = ['created_date', 'updated_date', 'address', 'phone']
        
        result = {
            'is_valid': True,
            'errors': [],
            'warnings': []
        }
        
        # Check required fields
        for field in required_fields:
            if field not in data:
                result['errors'].append(f"Missing required field: {field}")
                result['is_valid'] = False
            elif not data[field]:
                result['errors'].append(f"Empty required field: {field}")
                result['is_valid'] = False
        
        # Validate ID format
        if 'id' in data and data['id']:
            if not str(data['id']).strip():
                result['errors'].append("ID cannot be empty")
                result['is_valid'] = False
        
        # Validate name
        if 'name' in data and data['name']:
            if not isinstance(data['name'], str) or len(data['name'].strip()) < 1:
                result['errors'].append("Company name must be a non-empty string")
                result['is_valid'] = False
        
        # Validate dates if present
        date_fields = ['created_date', 'updated_date']
        date_validation = self.quality_validator.validate_date_fields(data, date_fields)
        if not date_validation['is_valid']:
            result['errors'].extend([error['error'] for error in date_validation['date_errors']])
            result['is_valid'] = False
        
        return result
    
    def validate_contact_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate contact data structure and content.
        
        Args:
            data: Contact data to validate
            
        Returns:
            Validation result with details
        """
        required_fields = ['id', 'first_name', 'last_name']
        
        result = {
            'is_valid': True,
            'errors': [],
            'warnings': []
        }
        
        # Check required fields
        for field in required_fields:
            if field not in data:
                result['errors'].append(f"Missing required field: {field}")
                result['is_valid'] = False
            elif not data[field]:
                result['errors'].append(f"Empty required field: {field}")
                result['is_valid'] = False
        
        # Validate email if present
        if 'email' in data and data['email']:
            email = data['email']
            if not re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', email):
                result['errors'].append("Invalid email format")
                result['is_valid'] = False
        
        return result
    
    def validate_ticket_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate ticket data structure and content.
        
        Args:
            data: Ticket data to validate
            
        Returns:
            Validation result with details
        """
        required_fields = ['id', 'subject']
        
        result = {
            'is_valid': True,
            'errors': [],
            'warnings': []
        }
        
        # Check required fields
        for field in required_fields:
            if field not in data:
                result['errors'].append(f"Missing required field: {field}")
                result['is_valid'] = False
            elif not data[field]:
                result['errors'].append(f"Empty required field: {field}")
                result['is_valid'] = False
        
        return result
    
    def validate_time_entry_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate time entry data structure and content.
        
        Args:
            data: Time entry data to validate
            
        Returns:
            Validation result with details
        """
        required_fields = ['id', 'hours']
        
        result = {
            'is_valid': True,
            'errors': [],
            'warnings': []
        }
        
        # Check required fields
        for field in required_fields:
            if field not in data:
                result['errors'].append(f"Missing required field: {field}")
                result['is_valid'] = False
            elif not data[field]:
                result['errors'].append(f"Empty required field: {field}")
                result['is_valid'] = False
        
        # Validate hours
        if 'hours' in data and data['hours'] is not None:
            try:
                hours = float(data['hours'])
                if hours < 0:
                    result['errors'].append("Hours cannot be negative")
                    result['is_valid'] = False
                elif hours > 24:
                    result['warnings'].append("Hours greater than 24 may indicate data quality issue")
            except (ValueError, TypeError):
                result['errors'].append("Hours must be a valid number")
                result['is_valid'] = False
        
        return result
    
    def validate_credentials(self, service: str, credentials: Dict[str, Any]) -> bool:
        """
        Validate service credentials.
        
        Args:
            service: Service name
            credentials: Credentials to validate
            
        Returns:
            True if valid, False otherwise
        """
        return self.credential_validator.validate_service_credentials(service, credentials)
    
    def validate_tenant_configuration(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate tenant configuration.
        
        Args:
            config: Tenant configuration to validate
            
        Returns:
            Validation result with details
        """
        return self.tenant_validator.validate_tenant_config(config)


# Convenience functions for backward compatibility
def validate_connectwise_credentials(credentials: Dict[str, Any]) -> bool:
    """Backward compatibility function for ConnectWise validation."""
    return CredentialValidator.validate_connectwise(credentials)


def validate_tenant_config(config: Dict[str, Any]) -> bool:
    """Backward compatibility function for tenant config validation."""
    result = TenantConfigValidator.validate_tenant_config(config)
    return result['is_valid']