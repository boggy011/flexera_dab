"""
Context management for Spark and Databricks environments.

Provides unified access to Spark session, Databricks utilities,
and environment-specific configurations.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


@dataclass
class SparkContext:
    """
    Wrapper for Spark session with convenience methods.
    
    Provides a unified interface for Spark operations that works
    in both local and Databricks environments.
    """
    
    _spark: Optional["SparkSession"] = field(default=None, repr=False)
    app_name: str = "DataLib"
    config: Dict[str, str] = field(default_factory=dict)
    
    @property
    def spark(self) -> "SparkSession":
        """Get or create Spark session."""
        if self._spark is None:
            self._spark = self._create_spark_session()
        return self._spark
    
    def _create_spark_session(self) -> "SparkSession":
        """Create a new Spark session."""
        from pyspark.sql import SparkSession
        
        builder = SparkSession.builder.appName(self.app_name)
        
        # Apply configuration
        for key, value in self.config.items():
            builder = builder.config(key, value)
        
        # Try to get existing session first
        try:
            return SparkSession.builder.getOrCreate()
        except Exception:
            return builder.getOrCreate()
    
    def read_table(self, table_name: str) -> Any:
        """Read a Delta table."""
        return self.spark.table(table_name)
    
    def sql(self, query: str) -> Any:
        """Execute SQL query."""
        return self.spark.sql(query)
    
    def catalog_exists(self, catalog_name: str) -> bool:
        """Check if a catalog exists."""
        try:
            catalogs = [row.catalog for row in self.spark.sql("SHOW CATALOGS").collect()]
            return catalog_name in catalogs
        except Exception:
            return False
    
    def schema_exists(self, catalog: str, schema: str) -> bool:
        """Check if a schema exists in a catalog."""
        try:
            schemas = [
                row.databaseName 
                for row in self.spark.sql(f"SHOW SCHEMAS IN {catalog}").collect()
            ]
            return schema in schemas
        except Exception:
            return False
    
    def table_exists(self, catalog: str, schema: str, table: str) -> bool:
        """Check if a table exists."""
        try:
            return self.spark.catalog.tableExists(f"{catalog}.{schema}.{table}")
        except Exception:
            return False
    
    def create_schema_if_not_exists(self, catalog: str, schema: str) -> None:
        """Create schema if it doesn't exist."""
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    
    def get_table_properties(self, table_name: str) -> Dict[str, str]:
        """Get properties of a Delta table."""
        try:
            props = self.spark.sql(f"SHOW TBLPROPERTIES {table_name}").collect()
            return {row.key: row.value for row in props}
        except Exception:
            return {}


@dataclass
class DatabricksContext(SparkContext):
    """
    Extended context for Databricks-specific operations.
    
    Provides access to Databricks utilities (dbutils), secrets,
    widgets, and environment configuration.
    """
    
    _dbutils: Any = field(default=None, repr=False)
    environment: str = field(default_factory=lambda: os.getenv("ENVIRONMENT", "dev"))
    
    @property
    def dbutils(self) -> Any:
        """Get Databricks utilities."""
        if self._dbutils is None:
            self._dbutils = self._get_dbutils()
        return self._dbutils
    
    def _get_dbutils(self) -> Any:
        """Get dbutils from Databricks runtime."""
        try:
            from pyspark.dbutils import DBUtils
            return DBUtils(self.spark)
        except ImportError:
            try:
                # Alternative method for some Databricks runtimes
                import IPython
                return IPython.get_ipython().user_ns.get("dbutils")
            except Exception:
                return None
    
    @property
    def is_databricks(self) -> bool:
        """Check if running in Databricks environment."""
        return self.dbutils is not None
    
    def get_widget(self, name: str, default: str = "") -> str:
        """
        Get notebook widget value.
        
        Args:
            name: Widget name
            default: Default value if widget doesn't exist
            
        Returns:
            Widget value
        """
        if not self.is_databricks:
            return default
        try:
            return self.dbutils.widgets.get(name)
        except Exception:
            return default
    
    def get_all_widgets(self) -> Dict[str, str]:
        """
        Get all notebook widget values.
        
        Returns:
            Dictionary of widget names to values
        """
        if not self.is_databricks:
            return {}
        try:
            # This is a workaround as there's no direct API to list all widgets
            return {}
        except Exception:
            return {}
    
    def get_secret(self, scope: str, key: str) -> str:
        """
        Get secret from Databricks secret scope.
        
        Args:
            scope: Secret scope name
            key: Secret key name
            
        Returns:
            Secret value
        """
        if not self.is_databricks:
            raise RuntimeError("Secrets are only available in Databricks")
        return self.dbutils.secrets.get(scope=scope, key=key)
    
    def get_current_user(self) -> Optional[str]:
        """Get current user email."""
        if not self.is_databricks:
            return None
        try:
            return self.spark.sql("SELECT current_user()").collect()[0][0]
        except Exception:
            return None
    
    def get_workspace_url(self) -> Optional[str]:
        """Get Databricks workspace URL."""
        if not self.is_databricks:
            return None
        try:
            context = self.dbutils.notebook.entry_point.getDbutils().notebook().getContext()
            return context.browserHostName().get()
        except Exception:
            return None
    
    def get_notebook_path(self) -> Optional[str]:
        """Get current notebook path."""
        if not self.is_databricks:
            return None
        try:
            context = self.dbutils.notebook.entry_point.getDbutils().notebook().getContext()
            return context.notebookPath().get()
        except Exception:
            return None
    
    def get_cluster_id(self) -> Optional[str]:
        """Get current cluster ID."""
        if not self.is_databricks:
            return None
        try:
            return self.spark.conf.get("spark.databricks.clusterUsageTags.clusterId")
        except Exception:
            return None
    
    def run_notebook(
        self,
        notebook_path: str,
        timeout_seconds: int = 3600,
        arguments: Optional[Dict[str, str]] = None
    ) -> str:
        """
        Run another notebook.
        
        Args:
            notebook_path: Path to notebook
            timeout_seconds: Timeout in seconds
            arguments: Arguments to pass to notebook
            
        Returns:
            Notebook result
        """
        if not self.is_databricks:
            raise RuntimeError("Notebook runs are only available in Databricks")
        return self.dbutils.notebook.run(
            notebook_path,
            timeout_seconds,
            arguments or {}
        )
    
    def list_files(self, path: str) -> list:
        """
        List files in a path.
        
        Args:
            path: DBFS or workspace path
            
        Returns:
            List of file info objects
        """
        if not self.is_databricks:
            raise RuntimeError("File listing is only available in Databricks")
        return self.dbutils.fs.ls(path)
    
    def copy_file(self, source: str, destination: str, recurse: bool = False) -> bool:
        """
        Copy file or directory.
        
        Args:
            source: Source path
            destination: Destination path
            recurse: Recurse into directories
            
        Returns:
            True if successful
        """
        if not self.is_databricks:
            raise RuntimeError("File operations are only available in Databricks")
        return self.dbutils.fs.cp(source, destination, recurse)
    
    @classmethod
    def from_widgets(cls, widget_names: list) -> "DatabricksContext":
        """
        Create context and extract widget values.
        
        Args:
            widget_names: List of widget names to extract
            
        Returns:
            DatabricksContext instance
        """
        ctx = cls()
        widgets = {name: ctx.get_widget(name) for name in widget_names}
        return ctx
