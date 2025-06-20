#!/usr/bin/env python3
"""
Test script to demonstrate adding a new service without code changes.
This creates a hypothetical "HubSpot" service to prove the system is truly dynamic.
"""

import sys
import os
import json
import tempfile
import shutil

# Add the src directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src', 'shared'))

def create_hubspot_service_config():
    """Create a HubSpot service configuration."""
    return {
        "name": "HubSpot",
        "auth_mode": "oauth2",
        "required_fields": [
            "access_token",
            "portal_id"
        ],
        "default_tables": [
            "contacts",
            "companies",
            "deals",
            "tickets"
        ],
        "description": "HubSpot CRM integration",
        "lambda_function_prefix": "avesa-hubspot-ingestion"
    }

def create_hubspot_endpoints_config():
    """Create HubSpot endpoints configuration."""
    return {
        "service_name": "hubspot",
        "api_version": "v3",
        "endpoints": {
            "crm/v3/objects/contacts": {
                "enabled": True,
                "table_name": "contacts",
                "sync_frequency": "30min",
                "page_size": 100,
                "order_by": "lastmodifieddate",
                "incremental_field": "lastmodifieddate",
                "description": "HubSpot contacts"
            },
            "crm/v3/objects/companies": {
                "enabled": True,
                "table_name": "companies",
                "sync_frequency": "60min",
                "page_size": 100,
                "order_by": "hs_lastmodifieddate",
                "incremental_field": "hs_lastmodifieddate",
                "description": "HubSpot companies"
            },
            "crm/v3/objects/deals": {
                "enabled": True,
                "table_name": "deals",
                "sync_frequency": "30min",
                "page_size": 100,
                "order_by": "hs_lastmodifieddate",
                "incremental_field": "hs_lastmodifieddate",
                "description": "HubSpot deals"
            },
            "crm/v3/objects/tickets": {
                "enabled": True,
                "table_name": "tickets",
                "sync_frequency": "15min",
                "page_size": 100,
                "order_by": "hs_lastmodifieddate",
                "incremental_field": "hs_lastmodifieddate",
                "description": "HubSpot support tickets"
            }
        },
        "authentication": {
            "type": "oauth2",
            "token_field": "access_token"
        },
        "rate_limiting": {
            "requests_per_minute": 100,
            "burst_limit": 10,
            "retry_strategy": "exponential_backoff"
        }
    }

def update_canonical_mappings_for_hubspot():
    """Update canonical mappings to include HubSpot."""
    
    # Get the mappings directory
    mappings_dir = os.path.join(os.path.dirname(__file__), '..', 'mappings', 'canonical')
    
    # Update companies.json
    companies_file = os.path.join(mappings_dir, 'companies.json')
    with open(companies_file, 'r') as f:
        companies_mapping = json.load(f)
    
    companies_mapping['hubspot'] = {
        "crm/v3/objects/companies": {
            "id": "id",
            "company_name": "name",
            "company_identifier": "domain",
            "address_line1": "address",
            "city": "city",
            "state": "state",
            "zip": "zip",
            "country": "country",
            "phone_number": "phone",
            "website": "website",
            "number_of_employees": "numberofemployees",
            "last_updated": "hs_lastmodifieddate"
        }
    }
    
    # Update contacts.json
    contacts_file = os.path.join(mappings_dir, 'contacts.json')
    with open(contacts_file, 'r') as f:
        contacts_mapping = json.load(f)
    
    contacts_mapping['hubspot'] = {
        "crm/v3/objects/contacts": {
            "id": "id",
            "company_id": "associatedcompanyid",
            "first_name": "firstname",
            "last_name": "lastname",
            "title": "jobtitle",
            "address_line1": "address",
            "city": "city",
            "state": "state",
            "zip": "zip",
            "country": "country",
            "default_phone_number": "phone",
            "default_email_address": "email",
            "last_updated": "lastmodifieddate"
        }
    }
    
    # Update tickets.json
    tickets_file = os.path.join(mappings_dir, 'tickets.json')
    with open(tickets_file, 'r') as f:
        tickets_mapping = json.load(f)
    
    tickets_mapping['hubspot'] = {
        "crm/v3/objects/tickets": {
            "id": "id",
            "ticket_number": "hs_ticket_id",
            "summary": "subject",
            "description": "content",
            "status": "hs_ticket_status",
            "priority": "hs_ticket_priority",
            "company_id": "hs_associated_company_id",
            "contact_id": "hs_associated_contact_id",
            "created_date": "createdate",
            "last_updated": "hs_lastmodifieddate"
        }
    }
    
    return companies_mapping, contacts_mapping, tickets_mapping

def test_hubspot_integration():
    """Test adding HubSpot service without code changes."""
    print("=== Testing HubSpot Service Integration ===")
    
    # Get the mappings directory
    base_dir = os.path.join(os.path.dirname(__file__), '..')
    services_dir = os.path.join(base_dir, 'mappings', 'services')
    integrations_dir = os.path.join(base_dir, 'mappings', 'integrations')
    canonical_dir = os.path.join(base_dir, 'mappings', 'canonical')
    
    # Backup original files
    backup_files = []
    
    try:
        # Create HubSpot service configuration
        hubspot_service_file = os.path.join(services_dir, 'hubspot.json')
        with open(hubspot_service_file, 'w') as f:
            json.dump(create_hubspot_service_config(), f, indent=2)
        backup_files.append(hubspot_service_file)
        
        # Create HubSpot endpoints configuration
        hubspot_endpoints_file = os.path.join(integrations_dir, 'hubspot_endpoints.json')
        with open(hubspot_endpoints_file, 'w') as f:
            json.dump(create_hubspot_endpoints_config(), f, indent=2)
        backup_files.append(hubspot_endpoints_file)
        
        # Update canonical mappings
        companies_mapping, contacts_mapping, tickets_mapping = update_canonical_mappings_for_hubspot()
        
        # Backup and update canonical files
        for filename, mapping in [
            ('companies.json', companies_mapping),
            ('contacts.json', contacts_mapping),
            ('tickets.json', tickets_mapping)
        ]:
            canonical_file = os.path.join(canonical_dir, filename)
            backup_file = canonical_file + '.backup'
            shutil.copy2(canonical_file, backup_file)
            backup_files.append(backup_file)
            
            with open(canonical_file, 'w') as f:
                json.dump(mapping, f, indent=2)
        
        # Now test the dynamic system with HubSpot
        from utils import (
            discover_available_services,
            build_service_table_configurations,
            get_canonical_table_for_endpoint
        )
        
        # Test service discovery
        services = discover_available_services()
        print(f"Services discovered: {services}")
        
        if 'hubspot' not in services:
            print("❌ HubSpot not discovered in services")
            return False
        
        print("✅ HubSpot service discovered")
        
        # Test table configuration building
        hubspot_tables = build_service_table_configurations('hubspot')
        print(f"HubSpot tables configured: {len(hubspot_tables)}")
        
        for table in hubspot_tables:
            print(f"  - {table['table_name']} ({table['endpoint']}) -> {table.get('canonical_table', 'N/A')}")
        
        # Test canonical mapping
        test_mappings = [
            ('hubspot', 'crm/v3/objects/contacts'),
            ('hubspot', 'crm/v3/objects/companies'),
            ('hubspot', 'crm/v3/objects/tickets')
        ]
        
        mapping_success = 0
        for service, endpoint in test_mappings:
            canonical_table = get_canonical_table_for_endpoint(service, endpoint)
            print(f"  {service}/{endpoint} -> {canonical_table}")
            if canonical_table:
                mapping_success += 1
        
        print(f"✅ HubSpot canonical mappings: {mapping_success}/{len(test_mappings)} successful")
        
        # Test processor integration
        print("\n=== Testing Processor Integration with HubSpot ===")
        
        # Mock environment for processor testing
        from unittest.mock import patch
        
        with patch.dict(os.environ, {
            'BUCKET_NAME': 'test-bucket',
            'TENANT_SERVICES_TABLE': 'test-tenant-services',
            'LAST_UPDATED_TABLE': 'test-last-updated',
            'ENVIRONMENT': 'test'
        }):
            sys.path.insert(0, os.path.join(base_dir, 'src', 'optimized', 'processors'))
            from tenant_processor import TenantProcessor
            
            processor = TenantProcessor()
            hubspot_config = processor._get_service_config('hubspot')
            
            if hubspot_config:
                print("✅ Tenant processor can load HubSpot configuration")
                tables = hubspot_config.get('tables', [])
                print(f"  HubSpot tables: {len(tables)}")
                for table in tables:
                    print(f"    - {table['table_name']} -> {table.get('canonical_table', 'N/A')}")
            else:
                print("❌ Tenant processor failed to load HubSpot configuration")
                return False
        
        print("\n✅ HubSpot integration successful - no code changes required!")
        return True
        
    except Exception as e:
        print(f"❌ Error during HubSpot integration test: {e}")
        return False
        
    finally:
        # Clean up - remove created files and restore backups
        for backup_file in backup_files:
            if backup_file.endswith('.backup'):
                original_file = backup_file.replace('.backup', '')
                if os.path.exists(backup_file):
                    shutil.move(backup_file, original_file)
            else:
                if os.path.exists(backup_file):
                    os.remove(backup_file)

if __name__ == "__main__":
    print("Testing New Service Integration (HubSpot)")
    print("=" * 50)
    
    success = test_hubspot_integration()
    
    print("\n" + "=" * 50)
    if success:
        print("✅ NEW SERVICE INTEGRATION TEST PASSED")
        print("The system successfully integrated HubSpot without any code changes!")
    else:
        print("❌ NEW SERVICE INTEGRATION TEST FAILED")
    
    sys.exit(0 if success else 1)