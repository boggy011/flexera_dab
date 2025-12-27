"""
Data writing operations for various target types.

Supports writing to Delta tables with various modes including
overwrite, append, merge, and SCD Type 2.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, TYPE_CHECKING

from pyspark.sql import functions as F

from datalib.core.config import TargetConfig
from datalib.utils.logging import get_logger

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from datalib.core.context import DatabricksContext


logger = get_logger(__name__)


class DataWriterError(Exception):
    """Raised when data writing fails."""
    pass


class DataWriter:
    """
    Universal data writer for various target types.
    
    Supports:
    - Delta tables with overwrite/append
    - Delta merge (upsert)
    - SCD Type 2
    - Partitioned writes
    - Z-ordering optimization
    
    Example:
        >>> writer = DataWriter(context)
        >>> records_written = writer.write(df, target_config)
    """
    
    def __init__(self, context: "DatabricksContext"):
        """
        Initialize data writer.
        
        Args:
            context: Databricks context for Spark access
        """
        self.context = context
        self.spark = context.spark
    
    def write(self, df: "DataFrame", target: TargetConfig) -> int:
        """
        Write DataFrame to target.
        
        Args:
            df: DataFrame to write
            target: Target configuration
            
        Returns:
            Number of records written
            
        Raises:
            DataWriterError: If writing fails
        """
        mode = target.mode.lower()
        
        try:
            # Ensure schema exists
            self._ensure_schema_exists(target)
            
            if mode == "overwrite":
                return self._write_overwrite(df, target)
            elif mode == "append":
                return self._write_append(df, target)
            elif mode == "merge":
                return self._write_merge(df, target)
            elif mode == "scd2":
                return self._write_scd2(df, target)
            else:
                raise DataWriterError(f"Unknown write mode: {mode}")
                
        except Exception as e:
            raise DataWriterError(f"Failed to write to {target.get_full_table_name()}: {e}") from e
    
    def _ensure_schema_exists(self, target: TargetConfig) -> None:
        """Ensure target schema exists."""
        self.context.create_schema_if_not_exists(target.catalog, target.schema)
    
    def _write_overwrite(self, df: "DataFrame", target: TargetConfig) -> int:
        """Write with overwrite mode."""
        table_name = target.get_full_table_name()
        logger.info(f"Writing to {table_name} with overwrite mode")
        
        record_count = df.count()
        
        writer = df.write.format(target.format).mode("overwrite")
        
        # Apply options
        for key, value in target.options.items():
            writer = writer.option(key, value)
        
        # Apply partitioning
        if target.partition_by:
            writer = writer.partitionBy(*target.partition_by)
        
        writer.saveAsTable(table_name)
        
        # Optimize table if configured
        self._optimize_table(target)
        
        return record_count
    
    def _write_append(self, df: "DataFrame", target: TargetConfig) -> int:
        """Write with append mode."""
        table_name = target.get_full_table_name()
        logger.info(f"Appending to {table_name}")
        
        record_count = df.count()
        
        writer = df.write.format(target.format).mode("append")
        
        for key, value in target.options.items():
            writer = writer.option(key, value)
        
        if target.partition_by:
            writer = writer.partitionBy(*target.partition_by)
        
        writer.saveAsTable(table_name)
        
        return record_count
    
    def _write_merge(self, df: "DataFrame", target: TargetConfig) -> int:
        """Write with merge (upsert) mode."""
        from delta.tables import DeltaTable
        
        table_name = target.get_full_table_name()
        logger.info(f"Merging into {table_name}")
        
        if not target.merge_keys:
            raise DataWriterError("Merge mode requires merge_keys in configuration")
        
        # Check if target table exists
        if not self.context.table_exists(target.catalog, target.schema, target.table):
            logger.info(f"Target table {table_name} doesn't exist, creating with initial data")
            return self._write_overwrite(df, target)
        
        # Build merge condition
        merge_condition = " AND ".join([
            f"target.{key} = source.{key}" for key in target.merge_keys
        ])
        
        # Get column list for update/insert
        columns = [c for c in df.columns if c not in target.merge_keys]
        update_set = {col: f"source.{col}" for col in columns}
        insert_values = {col: f"source.{col}" for col in df.columns}
        
        delta_table = DeltaTable.forName(self.spark, table_name)
        
        merge_builder = (
            delta_table.alias("target")
            .merge(df.alias("source"), merge_condition)
            .whenMatchedUpdate(set=update_set)
            .whenNotMatchedInsert(values=insert_values)
        )
        
        merge_builder.execute()
        
        return df.count()
    
    def _write_scd2(self, df: "DataFrame", target: TargetConfig) -> int:
        """Write with SCD Type 2 mode."""
        from delta.tables import DeltaTable
        
        table_name = target.get_full_table_name()
        logger.info(f"Writing SCD2 to {table_name}")
        
        if not target.merge_keys:
            raise DataWriterError("SCD2 mode requires merge_keys in configuration")
        
        # SCD2 columns
        effective_date_col = target.options.get("effective_date_column", "effective_date")
        end_date_col = target.options.get("end_date_column", "end_date")
        current_flag_col = target.options.get("current_flag_column", "is_current")
        
        # Add SCD2 columns to source
        source_df = (
            df.withColumn(effective_date_col, F.current_timestamp())
            .withColumn(end_date_col, F.lit(None).cast("timestamp"))
            .withColumn(current_flag_col, F.lit(True))
        )
        
        # Check if target table exists
        if not self.context.table_exists(target.catalog, target.schema, target.table):
            logger.info(f"Target table {table_name} doesn't exist, creating with initial data")
            target.mode = "overwrite"
            return self._write_overwrite(source_df, target)
        
        # Build merge condition (only match current records)
        merge_condition = " AND ".join([
            f"target.{key} = source.{key}" for key in target.merge_keys
        ]) + f" AND target.{current_flag_col} = true"
        
        # Determine which columns to compare for changes
        compare_cols = target.scd_columns if target.scd_columns else [
            c for c in df.columns if c not in target.merge_keys
        ]
        
        # Build change condition
        change_conditions = " OR ".join([
            f"target.{col} <> source.{col}" for col in compare_cols
        ])
        
        delta_table = DeltaTable.forName(self.spark, table_name)
        
        # First: Close expired records
        (
            delta_table.alias("target")
            .merge(source_df.alias("source"), merge_condition)
            .whenMatchedUpdate(
                condition=change_conditions,
                set={
                    end_date_col: F.current_timestamp(),
                    current_flag_col: F.lit(False)
                }
            )
            .execute()
        )
        
        # Second: Insert new versions
        # Filter to only records that have changes
        existing_df = self.spark.table(table_name).filter(F.col(current_flag_col) == True)
        
        new_records = (
            source_df.alias("source")
            .join(
                existing_df.alias("existing"),
                on=[F.col(f"source.{k}") == F.col(f"existing.{k}") for k in target.merge_keys],
                how="left_anti"
            )
        )
        
        # Insert new records
        if new_records.count() > 0:
            new_records.write.format("delta").mode("append").saveAsTable(table_name)
        
        return df.count()
    
    def _optimize_table(self, target: TargetConfig) -> None:
        """Optimize Delta table after write."""
        optimize = target.options.get("optimize", False)
        z_order_by = target.options.get("z_order_by", [])
        
        if optimize:
            table_name = target.get_full_table_name()
            logger.info(f"Optimizing table {table_name}")
            
            if z_order_by:
                z_order_cols = ", ".join(z_order_by)
                self.spark.sql(f"OPTIMIZE {table_name} ZORDER BY ({z_order_cols})")
            else:
                self.spark.sql(f"OPTIMIZE {table_name}")
    
    def write_stream(
        self,
        df: "DataFrame",
        target: TargetConfig,
        checkpoint_location: str,
        trigger_interval: Optional[str] = None
    ):
        """
        Write DataFrame as a streaming query.
        
        Args:
            df: Streaming DataFrame
            target: Target configuration
            checkpoint_location: Checkpoint location path
            trigger_interval: Trigger interval (e.g., "10 seconds")
            
        Returns:
            StreamingQuery object
        """
        table_name = target.get_full_table_name()
        logger.info(f"Starting streaming write to {table_name}")
        
        writer = (
            df.writeStream
            .format(target.format)
            .outputMode("append")
            .option("checkpointLocation", checkpoint_location)
        )
        
        if trigger_interval:
            from pyspark.sql.streaming import Trigger
            writer = writer.trigger(processingTime=trigger_interval)
        
        for key, value in target.options.items():
            writer = writer.option(key, value)
        
        if target.partition_by:
            writer = writer.partitionBy(*target.partition_by)
        
        return writer.toTable(table_name)
