#!/usr/bin/env python3
"""
Test script to verify Lambda imports work correctly
"""

import json
import os
import sys
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional

# Add shared module to path
sys.path.append('/opt/python')
sys.path.append(os.path.join(os.path.dirname(__file__), 'shared'))
sys.path.append('./src/shared')

try:
    from shared.config import Config, TenantConfig, ConnectWiseCredentials
    from shared.aws_clients import get_dynamodb_client, get_s3_client, get_secrets_client
    from shared.logger import PipelineLogger
    from shared.utils import flatten_json, get_timestamp, get_s3_key, validate_tenant_config, chunk_list, safe_get
    print("✅ Primary imports successful")
except ImportError as e:
    print(f"❌ Primary imports failed: {e}")
    # Fallback for local imports - try shared directory in Lambda package
    try:
        from shared.config import Config, TenantConfig, ConnectWiseCredentials
        from shared.aws_clients import get_dynamodb_client, get_s3_client, get_secrets_client
        from shared.logger import PipelineLogger
        from shared.utils import flatten_json, get_timestamp, get_s3_key, validate_tenant_config, chunk_list, safe_get
        print("✅ Fallback shared imports successful")
    except ImportError as e2:
        print(f"❌ Fallback shared imports failed: {e2}")
        # Final fallback for development environment
        try:
            import sys
            import os
            sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'shared'))
            from config import Config, TenantConfig, ConnectWiseCredentials
            from aws_clients import get_dynamodb_client, get_s3_client, get_secrets_client
            from logger import PipelineLogger
            from utils import flatten_json, get_timestamp, get_s3_key, validate_tenant_config, chunk_list, safe_get
            print("✅ Final fallback imports successful")
        except ImportError as e3:
            print(f"❌ All imports failed: {e3}")
            sys.exit(1)

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Test Lambda handler."""
    print("🚀 Lambda handler started")
    
    try:
        logger = PipelineLogger("test")
        config = Config.from_environment()
        print("✅ Config and logger initialized successfully")
        
        return {
            'statusCode': 200,
            'body': {
                'message': 'Test successful - imports working correctly',
                'timestamp': get_timestamp()
            }
        }
        
    except Exception as e:
        print(f"❌ Lambda handler failed: {str(e)}")
        return {
            'statusCode': 500,
            'body': {
                'message': 'Test failed',
                'error': str(e)
            }
        }

if __name__ == '__main__':
    # Test locally
    print("Testing imports locally...")
    result = lambda_handler({'test': True}, None)
    print(f"Result: {result}")