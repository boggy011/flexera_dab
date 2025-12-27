"""
Pipeline orchestration and execution.

Provides the core Pipeline class that coordinates reading data,
applying transformations, running quality checks, and writing output.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, TYPE_CHECKING

from datalib.core.config import PipelineConfig, SourceConfig, TargetConfig, TransformationConfig
from datalib.core.context import DatabricksContext
from datalib.utils.logging import get_logger

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


logger = get_logger(__name__)


@dataclass
class PipelineMetrics:
    """Metrics collected during pipeline execution."""
    
    pipeline_name: str
    start_time: datetime = field(default_factory=datetime.now)
    end_time: Optional[datetime] = None
    status: str = "running"
    records_read: int = 0
    records_written: int = 0
    records_failed: int = 0
    transformations_applied: int = 0
    quality_checks_passed: int = 0
    quality_checks_failed: int = 0
    error_message: Optional[str] = None
    duration_seconds: float = 0.0
    
    def complete(self, status: str = "success", error: Optional[str] = None) -> None:
        """Mark pipeline as complete."""
        self.end_time = datetime.now()
        self.status = status
        self.error_message = error
        self.duration_seconds = (self.end_time - self.start_time).total_seconds()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert metrics to dictionary."""
        return {
            "pipeline_name": self.pipeline_name,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "status": self.status,
            "records_read": self.records_read,
            "records_written": self.records_written,
            "records_failed": self.records_failed,
            "transformations_applied": self.transformations_applied,
            "quality_checks_passed": self.quality_checks_passed,
            "quality_checks_failed": self.quality_checks_failed,
            "error_message": self.error_message,
            "duration_seconds": self.duration_seconds,
        }


class PipelineExecutionError(Exception):
    """Raised when pipeline execution fails."""
    
    def __init__(self, message: str, metrics: Optional[PipelineMetrics] = None):
        super().__init__(message)
        self.metrics = metrics


class Pipeline:
    """
    Core pipeline class for data processing.
    
    Coordinates the full ETL lifecycle:
    1. Read data from source(s)
    2. Apply transformations
    3. Run quality checks
    4. Write to target
    
    Example:
        >>> config = ConfigLoader().load_pipeline_config("configs/bronze/customers.yaml")
        >>> pipeline = Pipeline(config, context)
        >>> metrics = pipeline.run()
    """
    
    def __init__(
        self,
        config: PipelineConfig,
        context: Optional[DatabricksContext] = None
    ):
        """
        Initialize pipeline.
        
        Args:
            config: Pipeline configuration
            context: Databricks context (created if not provided)
        """
        self.config = config
        self.context = context or DatabricksContext()
        self._transformations: Dict[str, Callable] = {}
        self._quality_checks: List[Callable] = []
        self._metrics = PipelineMetrics(pipeline_name=config.name)
        
        # Register default transformations
        self._register_default_transformations()
    
    def _register_default_transformations(self) -> None:
        """Register built-in transformation functions."""
        from datalib.transformations.common import (
            AddTimestampColumn,
            CastColumns,
            RenameColumns,
            FilterRows,
            DeduplicateRows,
            SelectColumns,
            DropColumns,
            FillNulls,
            StandardizeStrings,
        )
        
        self._transformations = {
            "add_timestamp": AddTimestampColumn,
            "cast_columns": CastColumns,
            "rename_columns": RenameColumns,
            "filter_rows": FilterRows,
            "deduplicate": DeduplicateRows,
            "select_columns": SelectColumns,
            "drop_columns": DropColumns,
            "fill_nulls": FillNulls,
            "standardize_strings": StandardizeStrings,
        }
    
    def register_transformation(
        self,
        name: str,
        transformation: Callable[["DataFrame", Dict[str, Any]], "DataFrame"]
    ) -> None:
        """
        Register a custom transformation function.
        
        Args:
            name: Transformation name (used in config)
            transformation: Transformation function or class
        """
        self._transformations[name] = transformation
    
    def _read_source(self, source: SourceConfig) -> "DataFrame":
        """
        Read data from a source.
        
        Args:
            source: Source configuration
            
        Returns:
            DataFrame with source data
        """
        from datalib.io.reader import DataReader
        
        reader = DataReader(self.context)
        return reader.read(source)
    
    def _apply_transformations(self, df: "DataFrame") -> "DataFrame":
        """
        Apply all configured transformations.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Transformed DataFrame
        """
        for trans_config in self.config.transformations:
            if not trans_config.enabled:
                logger.info(f"Skipping disabled transformation: {trans_config.type}")
                continue
                
            trans_class = self._transformations.get(trans_config.type)
            if trans_class is None:
                raise PipelineExecutionError(
                    f"Unknown transformation type: {trans_config.type}",
                    self._metrics
                )
            
            logger.info(f"Applying transformation: {trans_config.type}")
            transformation = trans_class(**trans_config.params)
            df = transformation.transform(df)
            self._metrics.transformations_applied += 1
        
        return df
    
    def _run_quality_checks(self, df: "DataFrame") -> "DataFrame":
        """
        Run data quality checks.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Original or filtered DataFrame
        """
        if not self.config.quality.enabled:
            return df
        
        from datalib.utils.validators import run_quality_checks
        
        results = run_quality_checks(
            df,
            self.config.quality.checks,
            fail_on_error=self.config.quality.fail_on_error
        )
        
        self._metrics.quality_checks_passed = results["passed"]
        self._metrics.quality_checks_failed = results["failed"]
        
        if results["failed"] > 0 and self.config.quality.fail_on_error:
            raise PipelineExecutionError(
                f"Quality checks failed: {results['failed']} checks failed",
                self._metrics
            )
        
        return results.get("validated_df", df)
    
    def _write_target(self, df: "DataFrame", target: TargetConfig) -> int:
        """
        Write data to target.
        
        Args:
            df: DataFrame to write
            target: Target configuration
            
        Returns:
            Number of records written
        """
        from datalib.io.writer import DataWriter
        
        writer = DataWriter(self.context)
        return writer.write(df, target)
    
    def run(self) -> PipelineMetrics:
        """
        Execute the pipeline.
        
        Returns:
            Pipeline execution metrics
        """
        logger.info(f"Starting pipeline: {self.config.name}")
        self._metrics = PipelineMetrics(pipeline_name=self.config.name)
        
        try:
            # Read from sources
            if len(self.config.sources) == 1:
                df = self._read_source(self.config.sources[0])
            elif len(self.config.sources) > 1:
                # Join multiple sources (implement based on config)
                dfs = [self._read_source(source) for source in self.config.sources]
                df = self._join_sources(dfs)
            else:
                raise PipelineExecutionError(
                    "No source configured for pipeline",
                    self._metrics
                )
            
            self._metrics.records_read = df.count()
            logger.info(f"Read {self._metrics.records_read} records from source")
            
            # Apply transformations
            df = self._apply_transformations(df)
            
            # Run quality checks
            df = self._run_quality_checks(df)
            
            # Write to target
            if self.config.target:
                self._metrics.records_written = self._write_target(df, self.config.target)
                logger.info(f"Wrote {self._metrics.records_written} records to target")
            
            self._metrics.complete("success")
            logger.info(f"Pipeline completed successfully in {self._metrics.duration_seconds:.2f}s")
            
        except PipelineExecutionError:
            raise
        except Exception as e:
            self._metrics.complete("failed", str(e))
            logger.error(f"Pipeline failed: {e}")
            raise PipelineExecutionError(str(e), self._metrics) from e
        
        return self._metrics
    
    def _join_sources(self, dfs: List["DataFrame"]) -> "DataFrame":
        """
        Join multiple source DataFrames.
        
        This is a placeholder - implement based on your join requirements.
        
        Args:
            dfs: List of DataFrames to join
            
        Returns:
            Joined DataFrame
        """
        if len(dfs) < 2:
            return dfs[0] if dfs else None
        
        # Get join configuration from parameters
        join_config = self.config.parameters.get("join", {})
        join_type = join_config.get("type", "inner")
        join_keys = join_config.get("keys", [])
        
        result = dfs[0]
        for i, df in enumerate(dfs[1:], 1):
            if join_keys:
                result = result.join(df, on=join_keys, how=join_type)
            else:
                raise PipelineExecutionError(
                    "Join keys must be specified for multi-source pipelines",
                    self._metrics
                )
        
        return result
    
    def dry_run(self) -> Dict[str, Any]:
        """
        Perform a dry run without writing data.
        
        Returns:
            Dictionary with execution plan and sample data
        """
        logger.info(f"Dry run for pipeline: {self.config.name}")
        
        result = {
            "pipeline": self.config.name,
            "layer": self.config.layer,
            "sources": [],
            "transformations": [],
            "target": None,
            "sample_data": None,
        }
        
        # Read sources
        if self.config.sources:
            df = self._read_source(self.config.sources[0])
            result["sources"] = [
                {"type": s.type, "table": s.get_full_table_name() or s.path}
                for s in self.config.sources
            ]
            
            # Apply transformations
            for trans_config in self.config.transformations:
                if trans_config.enabled:
                    result["transformations"].append({
                        "type": trans_config.type,
                        "params": trans_config.params,
                    })
                    trans_class = self._transformations.get(trans_config.type)
                    if trans_class:
                        transformation = trans_class(**trans_config.params)
                        df = transformation.transform(df)
            
            # Sample data
            result["sample_data"] = df.limit(10).toPandas().to_dict()
        
        if self.config.target:
            result["target"] = {
                "table": self.config.target.get_full_table_name(),
                "mode": self.config.target.mode,
                "format": self.config.target.format,
            }
        
        return result


class PipelineRunner:
    """
    Utility class for running pipelines from configuration files.
    
    Provides convenience methods for running single pipelines or
    batches of pipelines from a configuration directory.
    """
    
    def __init__(
        self,
        config_base_path: str,
        context: Optional[DatabricksContext] = None,
        environment: Optional[str] = None
    ):
        """
        Initialize pipeline runner.
        
        Args:
            config_base_path: Base path to configuration files
            context: Databricks context
            environment: Environment name (dev, qa, prd)
        """
        from datalib.core.config import ConfigLoader
        
        self.config_base_path = config_base_path
        self.context = context or DatabricksContext()
        self.environment = environment or self.context.environment
        self.config_loader = ConfigLoader(
            base_path=config_base_path,
            environment=self.environment
        )
    
    def run_pipeline(
        self,
        config_path: str,
        widget_params: Optional[Dict[str, str]] = None
    ) -> PipelineMetrics:
        """
        Run a single pipeline from configuration.
        
        Args:
            config_path: Path to configuration file (relative to base path)
            widget_params: Optional widget parameters
            
        Returns:
            Pipeline execution metrics
        """
        self.config_loader.widget_params = widget_params or {}
        config = self.config_loader.load_pipeline_config(config_path)
        
        pipeline = Pipeline(config, self.context)
        return pipeline.run()
    
    def run_layer(
        self,
        layer: str,
        parallel: bool = False
    ) -> List[PipelineMetrics]:
        """
        Run all pipelines in a layer.
        
        Args:
            layer: Layer name (bronze, silver, gold)
            parallel: Run pipelines in parallel
            
        Returns:
            List of pipeline metrics
        """
        configs = self.config_loader.list_configs(layer=layer)
        results = []
        
        for config_path in configs:
            try:
                metrics = self.run_pipeline(str(config_path.relative_to(self.config_base_path)))
                results.append(metrics)
            except PipelineExecutionError as e:
                logger.error(f"Pipeline failed: {config_path}")
                if e.metrics:
                    results.append(e.metrics)
        
        return results
