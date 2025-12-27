"""
Logging utilities for consistent logging across the library.

Provides a configured logger that works in both local and
Databricks environments.
"""

from __future__ import annotations

import logging
import sys
from typing import Optional


# Default format
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def get_logger(
    name: str,
    level: int = logging.INFO,
    format_string: Optional[str] = None
) -> logging.Logger:
    """
    Get a configured logger instance.
    
    Args:
        name: Logger name (typically __name__)
        level: Logging level
        format_string: Custom format string
        
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    
    # Only configure if no handlers exist
    if not logger.handlers:
        logger.setLevel(level)
        
        # Create console handler
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(level)
        
        # Create formatter
        formatter = logging.Formatter(
            format_string or LOG_FORMAT,
            datefmt=DATE_FORMAT
        )
        handler.setFormatter(formatter)
        
        logger.addHandler(handler)
    
    return logger


class PipelineLogger:
    """
    Context-aware logger for pipeline execution.
    
    Adds pipeline context to log messages and supports
    structured logging for better observability.
    """
    
    def __init__(
        self,
        pipeline_name: str,
        layer: str,
        run_id: Optional[str] = None
    ):
        """
        Initialize pipeline logger.
        
        Args:
            pipeline_name: Name of the pipeline
            layer: Data layer (bronze, silver, gold)
            run_id: Unique run identifier
        """
        self.pipeline_name = pipeline_name
        self.layer = layer
        self.run_id = run_id or self._generate_run_id()
        self._logger = get_logger(f"datalib.pipeline.{pipeline_name}")
    
    def _generate_run_id(self) -> str:
        """Generate a unique run ID."""
        from datetime import datetime
        import uuid
        return f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
    
    def _format_message(self, message: str) -> str:
        """Format message with pipeline context."""
        return f"[{self.layer}][{self.pipeline_name}][{self.run_id}] {message}"
    
    def info(self, message: str, **kwargs) -> None:
        """Log info message."""
        self._logger.info(self._format_message(message), **kwargs)
    
    def warning(self, message: str, **kwargs) -> None:
        """Log warning message."""
        self._logger.warning(self._format_message(message), **kwargs)
    
    def error(self, message: str, **kwargs) -> None:
        """Log error message."""
        self._logger.error(self._format_message(message), **kwargs)
    
    def debug(self, message: str, **kwargs) -> None:
        """Log debug message."""
        self._logger.debug(self._format_message(message), **kwargs)
    
    def log_metrics(self, metrics: dict) -> None:
        """Log pipeline metrics."""
        metrics_str = ", ".join(f"{k}={v}" for k, v in metrics.items())
        self.info(f"Metrics: {metrics_str}")
