"""
Configuration management for data pipelines.

Supports YAML-based configuration with environment variable substitution,
validation, and hierarchical merging.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import yaml


class ConfigurationError(Exception):
    """Raised when configuration is invalid or cannot be loaded."""
    pass


@dataclass
class SourceConfig:
    """Configuration for data source."""
    type: str  # jdbc, delta, parquet, csv, json, etc.
    path: Optional[str] = None
    catalog: Optional[str] = None
    schema: Optional[str] = None
    table: Optional[str] = None
    connection_string: Optional[str] = None
    query: Optional[str] = None
    options: Dict[str, Any] = field(default_factory=dict)
    
    def get_full_table_name(self) -> Optional[str]:
        """Get fully qualified table name."""
        if self.catalog and self.schema and self.table:
            return f"{self.catalog}.{self.schema}.{self.table}"
        elif self.schema and self.table:
            return f"{self.schema}.{self.table}"
        elif self.table:
            return self.table
        return None


@dataclass
class TargetConfig:
    """Configuration for data target."""
    catalog: str
    schema: str
    table: str
    mode: str = "overwrite"  # overwrite, append, merge, scd2
    format: str = "delta"
    partition_by: List[str] = field(default_factory=list)
    options: Dict[str, Any] = field(default_factory=dict)
    merge_keys: List[str] = field(default_factory=list)
    scd_columns: List[str] = field(default_factory=list)
    
    def get_full_table_name(self) -> str:
        """Get fully qualified table name."""
        return f"{self.catalog}.{self.schema}.{self.table}"


@dataclass
class ProcessingConfig:
    """Configuration for processing behavior."""
    mode: str = "full"  # full, incremental
    watermark_column: Optional[str] = None
    watermark_delay: str = "1 hour"
    batch_size: Optional[int] = None
    parallelism: Optional[int] = None
    checkpoint_location: Optional[str] = None
    trigger_interval: Optional[str] = None
    options: Dict[str, Any] = field(default_factory=dict)


@dataclass
class TransformationConfig:
    """Configuration for a transformation step."""
    type: str
    params: Dict[str, Any] = field(default_factory=dict)
    enabled: bool = True


@dataclass
class QualityConfig:
    """Configuration for data quality checks."""
    enabled: bool = True
    fail_on_error: bool = False
    checks: List[Dict[str, Any]] = field(default_factory=list)
    
    
@dataclass
class PipelineConfig:
    """Complete pipeline configuration."""
    name: str
    layer: str  # bronze, silver, gold
    description: str = ""
    version: str = "1.0.0"
    owner: str = ""
    tags: Dict[str, str] = field(default_factory=dict)
    source: Optional[SourceConfig] = None
    sources: List[SourceConfig] = field(default_factory=list)
    target: Optional[TargetConfig] = None
    processing: ProcessingConfig = field(default_factory=ProcessingConfig)
    transformations: List[TransformationConfig] = field(default_factory=list)
    quality: QualityConfig = field(default_factory=QualityConfig)
    parameters: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        """Validate configuration after initialization."""
        if not self.name:
            raise ConfigurationError("Pipeline name is required")
        if self.layer not in ("bronze", "silver", "gold"):
            raise ConfigurationError(f"Invalid layer: {self.layer}. Must be bronze, silver, or gold")
        
        # Handle single source vs multiple sources
        if self.source and not self.sources:
            self.sources = [self.source]
        elif self.sources and not self.source:
            self.source = self.sources[0] if self.sources else None


class ConfigLoader:
    """
    Load and parse YAML configuration files with environment variable substitution.
    
    Supports:
    - ${ENV_VAR} syntax for environment variables
    - ${ENV_VAR:default} syntax for defaults
    - Hierarchical configuration merging
    - Widget parameter substitution from Databricks notebooks
    """
    
    ENV_PATTERN = re.compile(r'\$\{([^}:]+)(?::([^}]*))?\}')
    
    def __init__(
        self,
        base_path: Optional[Union[str, Path]] = None,
        environment: Optional[str] = None,
        widget_params: Optional[Dict[str, str]] = None
    ):
        """
        Initialize configuration loader.
        
        Args:
            base_path: Base path for configuration files
            environment: Environment name (dev, qa, prd)
            widget_params: Parameters from Databricks notebook widgets
        """
        self.base_path = Path(base_path) if base_path else Path.cwd()
        self.environment = environment or os.getenv("ENVIRONMENT", "dev")
        self.widget_params = widget_params or {}
        
    def _substitute_variables(self, value: Any) -> Any:
        """
        Substitute environment variables and widget parameters in value.
        
        Args:
            value: Value to process (string, dict, or list)
            
        Returns:
            Value with variables substituted
        """
        if isinstance(value, str):
            def replace_match(match):
                var_name = match.group(1)
                default_value = match.group(2)
                
                # Check widget params first, then environment variables
                if var_name in self.widget_params:
                    return self.widget_params[var_name]
                    
                env_value = os.getenv(var_name)
                if env_value is not None:
                    return env_value
                elif default_value is not None:
                    return default_value
                else:
                    raise ConfigurationError(
                        f"Environment variable '{var_name}' not found and no default provided"
                    )
            
            return self.ENV_PATTERN.sub(replace_match, value)
            
        elif isinstance(value, dict):
            return {k: self._substitute_variables(v) for k, v in value.items()}
            
        elif isinstance(value, list):
            return [self._substitute_variables(item) for item in value]
            
        return value
    
    def _merge_configs(self, base: Dict, override: Dict) -> Dict:
        """
        Deep merge two configuration dictionaries.
        
        Args:
            base: Base configuration
            override: Override configuration
            
        Returns:
            Merged configuration
        """
        result = base.copy()
        
        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._merge_configs(result[key], value)
            else:
                result[key] = value
                
        return result
    
    def load_yaml(self, file_path: Union[str, Path]) -> Dict[str, Any]:
        """
        Load a YAML file with variable substitution.
        
        Args:
            file_path: Path to YAML file (relative or absolute)
            
        Returns:
            Parsed and substituted configuration dictionary
        """
        path = Path(file_path)
        if not path.is_absolute():
            path = self.base_path / path
            
        if not path.exists():
            raise ConfigurationError(f"Configuration file not found: {path}")
            
        try:
            with open(path, 'r') as f:
                config = yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise ConfigurationError(f"Invalid YAML in {path}: {e}")
            
        if config is None:
            config = {}
            
        return self._substitute_variables(config)
    
    def load_pipeline_config(
        self,
        config_path: Union[str, Path],
        env_override_path: Optional[Union[str, Path]] = None
    ) -> PipelineConfig:
        """
        Load pipeline configuration from YAML file.
        
        Args:
            config_path: Path to main configuration file
            env_override_path: Optional path to environment-specific overrides
            
        Returns:
            Validated PipelineConfig object
        """
        config = self.load_yaml(config_path)
        
        # Apply environment-specific overrides if provided
        if env_override_path:
            env_config = self.load_yaml(env_override_path)
            config = self._merge_configs(config, env_config)
            
        # Look for default environment overrides
        config_path = Path(config_path)
        env_file = config_path.parent / f"{config_path.stem}.{self.environment}.yaml"
        if env_file.exists():
            env_config = self.load_yaml(env_file)
            config = self._merge_configs(config, env_config)
        
        return self._dict_to_pipeline_config(config)
    
    def _dict_to_pipeline_config(self, config: Dict[str, Any]) -> PipelineConfig:
        """
        Convert dictionary to PipelineConfig dataclass.
        
        Args:
            config: Configuration dictionary
            
        Returns:
            PipelineConfig instance
        """
        pipeline_data = config.get("pipeline", {})
        
        # Parse source config
        source_data = config.get("source", {})
        source = SourceConfig(**source_data) if source_data else None
        
        # Parse multiple sources
        sources_data = config.get("sources", [])
        sources = [SourceConfig(**s) for s in sources_data]
        
        # Parse target config
        target_data = config.get("target", {})
        target = TargetConfig(**target_data) if target_data else None
        
        # Parse processing config
        processing_data = config.get("processing", {})
        processing = ProcessingConfig(**processing_data)
        
        # Parse transformations
        transformations_data = config.get("transformations", [])
        transformations = [TransformationConfig(**t) for t in transformations_data]
        
        # Parse quality config
        quality_data = config.get("quality", {})
        quality = QualityConfig(**quality_data)
        
        return PipelineConfig(
            name=pipeline_data.get("name", ""),
            layer=pipeline_data.get("layer", "bronze"),
            description=pipeline_data.get("description", ""),
            version=pipeline_data.get("version", "1.0.0"),
            owner=pipeline_data.get("owner", ""),
            tags=pipeline_data.get("tags", {}),
            source=source,
            sources=sources,
            target=target,
            processing=processing,
            transformations=transformations,
            quality=quality,
            parameters=config.get("parameters", {}),
        )
    
    def list_configs(
        self,
        layer: Optional[str] = None,
        pattern: str = "*.yaml"
    ) -> List[Path]:
        """
        List available configuration files.
        
        Args:
            layer: Filter by layer (bronze, silver, gold)
            pattern: Glob pattern for files
            
        Returns:
            List of configuration file paths
        """
        search_path = self.base_path
        if layer:
            search_path = search_path / layer
            
        configs = list(search_path.glob(f"**/{pattern}"))
        
        # Exclude environment-specific overrides from main list
        configs = [c for c in configs if not any(
            c.stem.endswith(f".{env}") for env in ("dev", "qa", "prd")
        )]
        
        return sorted(configs)
