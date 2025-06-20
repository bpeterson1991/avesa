"""
Unit tests for shared utilities.
"""

import pytest
from datetime import datetime, timezone, timedelta
from src.shared.utils import (
    flatten_json, get_timestamp, get_s3_key, chunk_list,
    safe_get, normalize_datetime, calculate_data_freshness
)


class TestFlattenJson:
    """Test cases for flatten_json function."""
    
    def test_simple_dict(self):
        """Test flattening a simple dictionary."""
        data = {"name": "John", "age": 30}
        result = flatten_json(data)
        assert result == {"name": "John", "age": 30}
    
    def test_nested_dict(self):
        """Test flattening a nested dictionary."""
        data = {
            "user": {
                "name": "John",
                "details": {
                    "age": 30,
                    "city": "New York"
                }
            }
        }
        expected = {
            "user__name": "John",
            "user__details__age": 30,
            "user__details__city": "New York"
        }
        result = flatten_json(data)
        assert result == expected
    
    def test_with_arrays(self):
        """Test flattening dictionary with arrays."""
        data = {
            "name": "John",
            "hobbies": ["reading", "swimming"],
            "empty_list": []
        }
        result = flatten_json(data)
        assert result["name"] == "John"
        assert result["hobbies"] == '["reading", "swimming"]'
        assert result["empty_list"] is None


class TestGetS3Key:
    """Test cases for get_s3_key function."""
    
    def test_raw_data_key(self):
        """Test generating S3 key for raw data."""
        key = get_s3_key("tenant1", "raw", "connectwise", "tickets", "2024-01-01T00:00:00Z")
        expected = "tenant1/raw/connectwise/tickets/2024-01-01T00:00:00Z.parquet"
        assert key == expected
    
    def test_canonical_data_key(self):
        """Test generating S3 key for canonical data."""
        key = get_s3_key("tenant2", "canonical", "connectwise", "companies", "2024-01-01T12:30:45Z")
        expected = "tenant2/canonical/connectwise/companies/2024-01-01T12:30:45Z.parquet"
        assert key == expected


class TestChunkList:
    """Test cases for chunk_list function."""
    
    def test_even_chunks(self):
        """Test chunking list with even division."""
        data = [1, 2, 3, 4, 5, 6]
        chunks = chunk_list(data, 2)
        expected = [[1, 2], [3, 4], [5, 6]]
        assert chunks == expected
    
    def test_uneven_chunks(self):
        """Test chunking list with uneven division."""
        data = [1, 2, 3, 4, 5]
        chunks = chunk_list(data, 2)
        expected = [[1, 2], [3, 4], [5]]
        assert chunks == expected
    
    def test_empty_list(self):
        """Test chunking empty list."""
        chunks = chunk_list([], 3)
        assert chunks == []


class TestSafeGet:
    """Test cases for safe_get function."""
    
    def test_simple_key(self):
        """Test getting value with simple key."""
        data = {"name": "John", "age": 30}
        assert safe_get(data, "name") == "John"
        assert safe_get(data, "missing") is None
        assert safe_get(data, "missing", "default") == "default"
    
    def test_dot_notation(self):
        """Test getting value with dot notation."""
        data = {
            "user": {
                "profile": {
                    "name": "John"
                }
            }
        }
        assert safe_get(data, "user.profile.name") == "John"
        assert safe_get(data, "user.profile.missing") is None
        assert safe_get(data, "user.missing.name") is None


class TestNormalizeDatetime:
    """Test cases for normalize_datetime function."""
    
    def test_iso_format(self):
        """Test normalizing ISO format datetime."""
        dt_str = "2024-01-01T12:30:45Z"
        result = normalize_datetime(dt_str)
        assert result == "2024-01-01T12:30:45Z"
    
    def test_iso_with_microseconds(self):
        """Test normalizing ISO format with microseconds."""
        dt_str = "2024-01-01T12:30:45.123456Z"
        result = normalize_datetime(dt_str)
        assert result == "2024-01-01T12:30:45.123456Z"
    
    def test_simple_date(self):
        """Test normalizing simple date."""
        dt_str = "2024-01-01"
        result = normalize_datetime(dt_str)
        assert result == "2024-01-01T00:00:00Z"
    
    def test_none_input(self):
        """Test normalizing None input."""
        result = normalize_datetime(None)
        assert result is None
    
    def test_empty_string(self):
        """Test normalizing empty string."""
        result = normalize_datetime("")
        assert result is None


class TestCalculateDataFreshness:
    """Test cases for calculate_data_freshness function."""
    
    def test_recent_timestamp(self):
        """Test calculating freshness for recent timestamp."""
        # Create a timestamp 1 hour ago
        one_hour_ago = datetime.now(timezone.utc).replace(microsecond=0) - \
                      timedelta(hours=1)
        timestamp_str = one_hour_ago.isoformat().replace('+00:00', 'Z')
        
        freshness = calculate_data_freshness(timestamp_str)
        # Should be approximately 3600 seconds (1 hour)
        assert 3590 <= freshness <= 3610
    
    def test_none_input(self):
        """Test calculating freshness for None input."""
        result = calculate_data_freshness(None)
        assert result is None
    
    def test_invalid_timestamp(self):
        """Test calculating freshness for invalid timestamp."""
        result = calculate_data_freshness("invalid-timestamp")
        assert result is None


class TestGetTimestamp:
    """Test cases for get_timestamp function."""
    
    def test_timestamp_format(self):
        """Test that timestamp is in correct format."""
        timestamp = get_timestamp()
        
        # Should be in ISO format ending with Z
        assert timestamp.endswith('Z')
        
        # Should be parseable as datetime
        dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
        assert dt.tzinfo == timezone.utc
        
        # Should be recent (within last minute)
        now = datetime.now(timezone.utc)
        diff = (now - dt).total_seconds()
        assert diff < 60