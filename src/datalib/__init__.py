"""
Datalib - Databricks Data Engineering Library

A reusable, configuration-driven library for data processing on Databricks.
Supports bronze/silver/gold medallion architecture patterns.
"""

from datalib._version import __version__, __version_info__, __environment__

from datalib.core.config import ConfigLoader, PipelineConfig
from datalib.core.pipeline import Pipeline, PipelineRunner
from datalib.core.context import SparkContext, DatabricksContext

from datalib.io.reader import DataReader
from datalib.io.writer import DataWriter

from datalib.transformations.base import Transformation
from datalib.transformations.common import (
    AddTimestampColumn,
    CastColumns,
    RenameColumns,
    FilterRows,
    DeduplicateRows,
)

from datalib.utils.logging import get_logger
from datalib.utils.validators import validate_config, validate_schema

__all__ = [
    # Version
    "__version__",
    "__version_info__",
    "__environment__",
    # Core
    "ConfigLoader",
    "PipelineConfig",
    "Pipeline",
    "PipelineRunner",
    "SparkContext",
    "DatabricksContext",
    # I/O
    "DataReader",
    "DataWriter",
    # Transformations
    "Transformation",
    "AddTimestampColumn",
    "CastColumns",
    "RenameColumns",
    "FilterRows",
    "DeduplicateRows",
    # Utils
    "get_logger",
    "validate_config",
    "validate_schema",
]
