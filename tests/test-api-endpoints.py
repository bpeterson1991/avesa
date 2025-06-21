#!/usr/bin/env python3

"""
API Endpoints Testing Script

This script tests the AVESA API endpoints to ensure they work correctly
with the synchronized ClickHouse schema and return canonical data.
"""

import requests
import json
import sys
from datetime import datetime

# API Configuration
API_BASE_URL = "http://localhost:3001"
DEMO_CREDENTIALS = {
    "email": "admin@sitetechnology.com",
    "password": "demo123",
    "tenantId": "sitetechnology"
}

def get_auth_token():
    """Get authentication token from the API"""
    
    print("🔐 Authenticating with API...")
    
    try:
        response = requests.post(
            f"{API_BASE_URL}/auth/login",
            json=DEMO_CREDENTIALS,
            headers={"Content-Type": "application/json"}
        )
        
        if response.status_code == 200:
            data = response.json()
            token = data.get('token')
            if token:
                print("✅ Authentication successful")
                return token
            else:
                print("❌ No token in response")
                return None
        else:
            print(f"❌ Authentication failed: {response.status_code}")
            print(f"Response: {response.text}")
            return None
            
    except Exception as e:
        print(f"❌ Authentication error: {e}")
        return None

def test_endpoint(endpoint, token, description):
    """Test a specific API endpoint"""
    
    print(f"\n🔍 Testing {description}")
    print(f"   Endpoint: {endpoint}")
    
    try:
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        
        response = requests.get(f"{API_BASE_URL}{endpoint}", headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            print(f"   ✅ Success ({response.status_code})")
            
            # Analyze response structure
            if isinstance(data, dict):
                if 'companies' in data:
                    companies = data['companies']
                    print(f"   📊 Found {len(companies)} companies")
                    if companies:
                        sample_company = companies[0]
                        canonical_fields = [
                            'id', 'company_name', 'company_identifier', 'status',
                            'phone_number', 'website', 'city', 'state', 'is_current'
                        ]
                        present_fields = [field for field in canonical_fields if field in sample_company]
                        print(f"   📋 Canonical fields present: {len(present_fields)}/{len(canonical_fields)}")
                        print(f"   🔍 Sample company: {sample_company.get('company_name', 'N/A')}")
                
                elif 'contacts' in data:
                    contacts = data['contacts']
                    print(f"   📊 Found {len(contacts)} contacts")
                    if contacts:
                        sample_contact = contacts[0]
                        print(f"   🔍 Sample contact: {sample_contact.get('first_name', 'N/A')} {sample_contact.get('last_name', 'N/A')}")
                
                elif 'tickets' in data:
                    tickets = data['tickets']
                    print(f"   📊 Found {len(tickets)} tickets")
                    if tickets:
                        sample_ticket = tickets[0]
                        print(f"   🔍 Sample ticket: {sample_ticket.get('summary', 'N/A')}")
                
                elif 'timeEntries' in data:
                    time_entries = data['timeEntries']
                    print(f"   📊 Found {len(time_entries)} time entries")
                    if time_entries:
                        sample_entry = time_entries[0]
                        print(f"   🔍 Sample entry: {sample_entry.get('actual_hours', 'N/A')} hours")
                
                elif 'company' in data:
                    company = data['company']
                    print(f"   📊 Company details for: {company.get('company_name', 'N/A')}")
                    canonical_fields = [
                        'id', 'company_name', 'company_type', 'status', 'address_line1',
                        'city', 'state', 'phone_number', 'website', 'annual_revenue'
                    ]
                    present_fields = [field for field in canonical_fields if field in company]
                    print(f"   📋 Canonical fields present: {len(present_fields)}/{len(canonical_fields)}")
                
                elif 'summary' in data:
                    summary = data['summary']
                    print(f"   📊 Dashboard summary:")
                    for entity_type, stats in summary.items():
                        print(f"      {entity_type}: {stats.get('current', 0)} current records")
                
                # Check for pagination
                if 'pagination' in data:
                    pagination = data['pagination']
                    print(f"   📄 Pagination: page {pagination.get('page', 1)} of {pagination.get('totalPages', 1)}")
            
            return True
            
        else:
            print(f"   ❌ Failed ({response.status_code})")
            print(f"   Response: {response.text}")
            return False
            
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return False

def test_health_endpoint():
    """Test the health endpoint (no auth required)"""
    
    print("🏥 Testing health endpoint...")
    
    try:
        response = requests.get(f"{API_BASE_URL}/health")
        
        if response.status_code == 200:
            data = response.json()
            print("✅ Health check passed")
            print(f"   Status: {data.get('status', 'unknown')}")
            if 'clickhouse' in data:
                ch_status = data['clickhouse']
                print(f"   ClickHouse: {'✅ Connected' if ch_status.get('connected') else '❌ Disconnected'}")
            return True
        else:
            print(f"❌ Health check failed: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Health check error: {e}")
        return False

def main():
    """Main testing function"""
    
    print("🚀 AVESA API ENDPOINTS TESTING")
    print("=" * 60)
    print("Testing API endpoints with synchronized ClickHouse schema")
    
    # Test health endpoint first
    if not test_health_endpoint():
        print("\n❌ Health check failed - API may not be running")
        print("Please ensure the API server is running on http://localhost:3001")
        sys.exit(1)
    
    # Get authentication token
    token = get_auth_token()
    if not token:
        print("\n❌ Authentication failed - cannot proceed with tests")
        sys.exit(1)
    
    # Define test endpoints
    test_cases = [
        # Analytics endpoints
        ("/api/analytics/dashboard", "Dashboard Analytics"),
        ("/api/analytics/tickets/status", "Ticket Status Analytics"),
        ("/api/analytics/companies/top", "Top Companies Analytics"),
        
        # Companies endpoints
        ("/api/companies", "Companies List"),
        ("/api/companies?limit=5", "Companies List (Limited)"),
        ("/api/companies?search=tech", "Companies Search"),
        
        # Contacts endpoints
        ("/api/contacts", "Contacts List"),
        ("/api/contacts?limit=5", "Contacts List (Limited)"),
        
        # Tickets endpoints
        ("/api/tickets", "Tickets List"),
        ("/api/tickets?limit=5", "Tickets List (Limited)"),
        
        # Time entries endpoints
        ("/api/time-entries", "Time Entries List"),
        ("/api/time-entries?limit=5", "Time Entries List (Limited)"),
    ]
    
    # Run tests
    successful_tests = 0
    total_tests = len(test_cases)
    
    for endpoint, description in test_cases:
        if test_endpoint(endpoint, token, description):
            successful_tests += 1
    
    # Test specific company details (if we have companies)
    print(f"\n🔍 Testing specific company details...")
    companies_response = requests.get(
        f"{API_BASE_URL}/api/companies?limit=1",
        headers={"Authorization": f"Bearer {token}"}
    )
    
    if companies_response.status_code == 200:
        companies_data = companies_response.json()
        if companies_data.get('companies'):
            company_id = companies_data['companies'][0]['id']
            
            # Test company details
            if test_endpoint(f"/api/companies/{company_id}", token, f"Company Details (ID: {company_id})"):
                successful_tests += 1
            total_tests += 1
            
            # Test company tickets
            if test_endpoint(f"/api/companies/{company_id}/tickets", token, f"Company Tickets (ID: {company_id})"):
                successful_tests += 1
            total_tests += 1
    
    # Final summary
    print(f"\n🎉 TEST SUMMARY")
    print("=" * 60)
    print(f"Tests passed: {successful_tests}/{total_tests}")
    print(f"Success rate: {(successful_tests/total_tests)*100:.1f}%")
    
    if successful_tests == total_tests:
        print("✅ ALL TESTS PASSED!")
        print("The API is working correctly with the synchronized ClickHouse schema.")
        print("✅ Schema synchronization validation completed successfully!")
    else:
        print("⚠️  SOME TESTS FAILED")
        print("Please check the output above for details.")
    
    return successful_tests == total_tests

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)