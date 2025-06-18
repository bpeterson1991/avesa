"""
AWS client utilities for the ConnectWise data pipeline.
"""

import boto3
from typing import Optional
from botocore.config import Config as BotoConfig


def get_dynamodb_client(region_name: Optional[str] = None):
    """Get DynamoDB client with optimized configuration."""
    config = BotoConfig(
        region_name=region_name,
        retries={
            'max_attempts': 3,
            'mode': 'adaptive'
        },
        max_pool_connections=50
    )
    return boto3.client('dynamodb', config=config)


def get_s3_client(region_name: Optional[str] = None):
    """Get S3 client with optimized configuration."""
    config = BotoConfig(
        region_name=region_name,
        retries={
            'max_attempts': 3,
            'mode': 'adaptive'
        },
        max_pool_connections=50,
        s3={
            'max_concurrent_requests': 10,
            'max_bandwidth': None,
            'use_accelerate_endpoint': False,
            'addressing_style': 'auto'
        }
    )
    return boto3.client('s3', config=config)


def get_secrets_client(region_name: Optional[str] = None):
    """Get Secrets Manager client with optimized configuration."""
    config = BotoConfig(
        region_name=region_name,
        retries={
            'max_attempts': 3,
            'mode': 'adaptive'
        }
    )
    return boto3.client('secretsmanager', config=config)


def get_cloudwatch_client(region_name: Optional[str] = None):
    """Get CloudWatch client for custom metrics."""
    config = BotoConfig(
        region_name=region_name,
        retries={
            'max_attempts': 3,
            'mode': 'adaptive'
        }
    )
    return boto3.client('cloudwatch', config=config)