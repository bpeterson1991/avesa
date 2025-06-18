"""
Logging utilities for the ConnectWise data pipeline.
"""

import logging
import json
import sys
from typing import Any, Dict, Optional
from datetime import datetime


class JSONFormatter(logging.Formatter):
    """Custom JSON formatter for structured logging."""
    
    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON."""
        log_entry = {
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
            'module': record.module,
            'function': record.funcName,
            'line': record.lineno
        }
        
        # Add exception info if present
        if record.exc_info:
            log_entry['exception'] = self.formatException(record.exc_info)
        
        # Add extra fields if present
        if hasattr(record, 'tenant_id'):
            log_entry['tenant_id'] = record.tenant_id
        if hasattr(record, 'table_name'):
            log_entry['table_name'] = record.table_name
        if hasattr(record, 'record_count'):
            log_entry['record_count'] = record.record_count
        if hasattr(record, 'execution_time'):
            log_entry['execution_time'] = record.execution_time
        
        return json.dumps(log_entry)


def get_logger(name: str, level: str = "INFO") -> logging.Logger:
    """Get configured logger instance."""
    logger = logging.getLogger(name)
    
    # Avoid adding multiple handlers
    if logger.handlers:
        return logger
    
    # Set log level
    log_level = getattr(logging, level.upper(), logging.INFO)
    logger.setLevel(log_level)
    
    # Create console handler
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(log_level)
    
    # Set JSON formatter
    formatter = JSONFormatter()
    handler.setFormatter(formatter)
    
    logger.addHandler(handler)
    
    # Prevent propagation to root logger
    logger.propagate = False
    
    return logger


class PipelineLogger:
    """Enhanced logger with pipeline-specific methods."""
    
    def __init__(self, name: str, tenant_id: Optional[str] = None, table_name: Optional[str] = None):
        self.logger = get_logger(name)
        self.tenant_id = tenant_id
        self.table_name = table_name
    
    def _add_context(self, extra: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Add pipeline context to log entry."""
        context = {}
        if self.tenant_id:
            context['tenant_id'] = self.tenant_id
        if self.table_name:
            context['table_name'] = self.table_name
        if extra:
            context.update(extra)
        return context
    
    def info(self, message: str, **kwargs):
        """Log info message with context."""
        self.logger.info(message, extra=self._add_context(kwargs))
    
    def warning(self, message: str, **kwargs):
        """Log warning message with context."""
        self.logger.warning(message, extra=self._add_context(kwargs))
    
    def error(self, message: str, **kwargs):
        """Log error message with context."""
        self.logger.error(message, extra=self._add_context(kwargs))
    
    def debug(self, message: str, **kwargs):
        """Log debug message with context."""
        self.logger.debug(message, extra=self._add_context(kwargs))
    
    def log_api_call(self, endpoint: str, status_code: int, response_time: float, record_count: int = 0):
        """Log API call details."""
        self.info(
            f"API call completed: {endpoint}",
            endpoint=endpoint,
            status_code=status_code,
            response_time=response_time,
            record_count=record_count
        )
    
    def log_data_processing(self, operation: str, record_count: int, execution_time: float):
        """Log data processing details."""
        self.info(
            f"Data processing completed: {operation}",
            operation=operation,
            record_count=record_count,
            execution_time=execution_time
        )
    
    def log_s3_operation(self, operation: str, bucket: str, key: str, size_bytes: int = 0):
        """Log S3 operation details."""
        self.info(
            f"S3 operation completed: {operation}",
            operation=operation,
            bucket=bucket,
            key=key,
            size_bytes=size_bytes
        )