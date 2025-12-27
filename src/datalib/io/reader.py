"""
Data reading operations for various source types.

Supports reading from Delta tables, files (parquet, csv, json),
JDBC sources, and streaming sources.
"""

from __future__ import annotations

from typing import Any, Dict, Optional, TYPE_CHECKING

from datalib.core.config import SourceConfig
from datalib.utils.logging import get_logger

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from datalib.core.context import DatabricksContext


logger = get_logger(__name__)


class DataReaderError(Exception):
    """Raised when data reading fails."""
    pass


class DataReader:
    """
    Universal data reader for various source types.
    
    Supports:
    - Delta tables (catalog.schema.table or delta path)
    - Parquet files
    - CSV files
    - JSON files
    - JDBC sources
    - Custom formats via options
    
    Example:
        >>> reader = DataReader(context)
        >>> df = reader.read(source_config)
    """
    
    def __init__(self, context: "DatabricksContext"):
        """
        Initialize data reader.
        
        Args:
            context: Databricks context for Spark access
        """
        self.context = context
        self.spark = context.spark
    
    def read(self, source: SourceConfig) -> "DataFrame":
        """
        Read data from source.
        
        Args:
            source: Source configuration
            
        Returns:
            DataFrame with source data
            
        Raises:
            DataReaderError: If reading fails
        """
        source_type = source.type.lower()
        
        try:
            if source_type == "delta":
                return self._read_delta(source)
            elif source_type == "table":
                return self._read_table(source)
            elif source_type == "parquet":
                return self._read_parquet(source)
            elif source_type == "csv":
                return self._read_csv(source)
            elif source_type == "json":
                return self._read_json(source)
            elif source_type == "jdbc":
                return self._read_jdbc(source)
            elif source_type == "sql":
                return self._read_sql(source)
            else:
                # Try reading as generic format
                return self._read_generic(source)
                
        except Exception as e:
            raise DataReaderError(f"Failed to read from {source_type} source: {e}") from e
    
    def _read_delta(self, source: SourceConfig) -> "DataFrame":
        """Read from Delta table or path."""
        if source.path:
            logger.info(f"Reading Delta from path: {source.path}")
            reader = self.spark.read.format("delta")
            for key, value in source.options.items():
                reader = reader.option(key, value)
            return reader.load(source.path)
        elif source.get_full_table_name():
            return self._read_table(source)
        else:
            raise DataReaderError("Delta source requires path or table name")
    
    def _read_table(self, source: SourceConfig) -> "DataFrame":
        """Read from Unity Catalog table."""
        table_name = source.get_full_table_name()
        if not table_name:
            raise DataReaderError("Table source requires catalog, schema, and table")
        
        logger.info(f"Reading table: {table_name}")
        return self.spark.table(table_name)
    
    def _read_parquet(self, source: SourceConfig) -> "DataFrame":
        """Read from Parquet files."""
        if not source.path:
            raise DataReaderError("Parquet source requires path")
        
        logger.info(f"Reading Parquet from: {source.path}")
        reader = self.spark.read.format("parquet")
        for key, value in source.options.items():
            reader = reader.option(key, value)
        return reader.load(source.path)
    
    def _read_csv(self, source: SourceConfig) -> "DataFrame":
        """Read from CSV files."""
        if not source.path:
            raise DataReaderError("CSV source requires path")
        
        logger.info(f"Reading CSV from: {source.path}")
        
        # Default CSV options
        default_options = {
            "header": "true",
            "inferSchema": "true",
            "mode": "PERMISSIVE",
        }
        options = {**default_options, **source.options}
        
        reader = self.spark.read.format("csv")
        for key, value in options.items():
            reader = reader.option(key, value)
        return reader.load(source.path)
    
    def _read_json(self, source: SourceConfig) -> "DataFrame":
        """Read from JSON files."""
        if not source.path:
            raise DataReaderError("JSON source requires path")
        
        logger.info(f"Reading JSON from: {source.path}")
        
        # Default JSON options
        default_options = {
            "multiLine": "true",
            "mode": "PERMISSIVE",
        }
        options = {**default_options, **source.options}
        
        reader = self.spark.read.format("json")
        for key, value in options.items():
            reader = reader.option(key, value)
        return reader.load(source.path)
    
    def _read_jdbc(self, source: SourceConfig) -> "DataFrame":
        """Read from JDBC source."""
        if not source.connection_string:
            raise DataReaderError("JDBC source requires connection_string")
        
        if not (source.table or source.query):
            raise DataReaderError("JDBC source requires table or query")
        
        logger.info(f"Reading from JDBC source")
        
        reader = self.spark.read.format("jdbc")
        reader = reader.option("url", source.connection_string)
        
        if source.table:
            reader = reader.option("dbtable", source.table)
        elif source.query:
            reader = reader.option("query", source.query)
        
        for key, value in source.options.items():
            reader = reader.option(key, value)
        
        return reader.load()
    
    def _read_sql(self, source: SourceConfig) -> "DataFrame":
        """Read from SQL query."""
        if not source.query:
            raise DataReaderError("SQL source requires query")
        
        logger.info(f"Executing SQL query")
        return self.spark.sql(source.query)
    
    def _read_generic(self, source: SourceConfig) -> "DataFrame":
        """Read using generic format reader."""
        if not source.path:
            raise DataReaderError(f"Generic source requires path")
        
        logger.info(f"Reading {source.type} from: {source.path}")
        
        reader = self.spark.read.format(source.type)
        for key, value in source.options.items():
            reader = reader.option(key, value)
        return reader.load(source.path)
    
    def read_stream(self, source: SourceConfig) -> "DataFrame":
        """
        Read data as a streaming DataFrame.
        
        Args:
            source: Source configuration
            
        Returns:
            Streaming DataFrame
        """
        source_type = source.type.lower()
        
        if source_type == "delta":
            if source.path:
                reader = self.spark.readStream.format("delta")
            else:
                table_name = source.get_full_table_name()
                reader = self.spark.readStream.table(table_name)
                return reader
        else:
            reader = self.spark.readStream.format(source_type)
        
        for key, value in source.options.items():
            reader = reader.option(key, value)
        
        if source.path:
            return reader.load(source.path)
        
        raise DataReaderError(f"Streaming not supported for source type: {source_type}")
