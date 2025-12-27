"""
Unit tests for the configuration module.
"""

import os
import pytest
import yaml
from pathlib import Path

from datalib.core.config import (
    ConfigLoader,
    PipelineConfig,
    SourceConfig,
    TargetConfig,
    ConfigurationError,
)


class TestSourceConfig:
    """Tests for SourceConfig dataclass."""
    
    def test_get_full_table_name_with_all_parts(self):
        """Test full table name with catalog, schema, and table."""
        source = SourceConfig(
            type="table",
            catalog="main",
            schema="bronze",
            table="customers"
        )
        assert source.get_full_table_name() == "main.bronze.customers"
    
    def test_get_full_table_name_without_catalog(self):
        """Test full table name without catalog."""
        source = SourceConfig(
            type="table",
            schema="bronze",
            table="customers"
        )
        assert source.get_full_table_name() == "bronze.customers"
    
    def test_get_full_table_name_table_only(self):
        """Test full table name with table only."""
        source = SourceConfig(type="table", table="customers")
        assert source.get_full_table_name() == "customers"
    
    def test_get_full_table_name_none(self):
        """Test full table name when no table specified."""
        source = SourceConfig(type="parquet", path="/data/file.parquet")
        assert source.get_full_table_name() is None


class TestTargetConfig:
    """Tests for TargetConfig dataclass."""
    
    def test_get_full_table_name(self):
        """Test full table name generation."""
        target = TargetConfig(
            catalog="main",
            schema="silver",
            table="customers_cleansed"
        )
        assert target.get_full_table_name() == "main.silver.customers_cleansed"
    
    def test_default_values(self):
        """Test default values for optional fields."""
        target = TargetConfig(
            catalog="main",
            schema="silver",
            table="test"
        )
        assert target.mode == "overwrite"
        assert target.format == "delta"
        assert target.partition_by == []


class TestPipelineConfig:
    """Tests for PipelineConfig dataclass."""
    
    def test_valid_config(self, sample_config_dict):
        """Test creating valid pipeline config."""
        config = PipelineConfig(
            name="test_pipeline",
            layer="bronze",
            source=SourceConfig(type="table", table="source"),
            target=TargetConfig(catalog="main", schema="bronze", table="target"),
        )
        assert config.name == "test_pipeline"
        assert config.layer == "bronze"
    
    def test_invalid_layer(self):
        """Test that invalid layer raises error."""
        with pytest.raises(ConfigurationError) as exc_info:
            PipelineConfig(name="test", layer="invalid")
        assert "Invalid layer" in str(exc_info.value)
    
    def test_missing_name(self):
        """Test that missing name raises error."""
        with pytest.raises(ConfigurationError) as exc_info:
            PipelineConfig(name="", layer="bronze")
        assert "name is required" in str(exc_info.value)
    
    def test_source_to_sources_conversion(self):
        """Test that single source is added to sources list."""
        source = SourceConfig(type="table", table="test")
        config = PipelineConfig(name="test", layer="bronze", source=source)
        assert len(config.sources) == 1
        assert config.sources[0] == source


class TestConfigLoader:
    """Tests for ConfigLoader class."""
    
    def test_load_yaml(self, temp_config_file):
        """Test loading YAML file."""
        loader = ConfigLoader(base_path=temp_config_file.parent)
        config = loader.load_yaml(temp_config_file.name)
        
        assert config["pipeline"]["name"] == "test_pipeline"
        assert config["pipeline"]["layer"] == "bronze"
    
    def test_load_yaml_file_not_found(self, tmp_path):
        """Test error when file not found."""
        loader = ConfigLoader(base_path=tmp_path)
        
        with pytest.raises(ConfigurationError) as exc_info:
            loader.load_yaml("nonexistent.yaml")
        assert "not found" in str(exc_info.value)
    
    def test_environment_variable_substitution(self, tmp_path):
        """Test environment variable substitution."""
        # Set environment variable
        os.environ["TEST_VAR"] = "test_value"
        
        # Create config with variable
        config_dict = {
            "pipeline": {
                "name": "test",
                "layer": "bronze",
            },
            "source": {
                "type": "jdbc",
                "connection_string": "${TEST_VAR}",
            },
        }
        
        config_file = tmp_path / "test.yaml"
        with open(config_file, 'w') as f:
            yaml.dump(config_dict, f)
        
        loader = ConfigLoader(base_path=tmp_path)
        config = loader.load_yaml("test.yaml")
        
        assert config["source"]["connection_string"] == "test_value"
        
        # Cleanup
        del os.environ["TEST_VAR"]
    
    def test_default_value_substitution(self, tmp_path):
        """Test default value when env var not set."""
        config_dict = {
            "pipeline": {
                "name": "test",
                "layer": "bronze",
            },
            "source": {
                "type": "table",
                "catalog": "${UNDEFINED_VAR:default_catalog}",
            },
        }
        
        config_file = tmp_path / "test.yaml"
        with open(config_file, 'w') as f:
            yaml.dump(config_dict, f)
        
        loader = ConfigLoader(base_path=tmp_path)
        config = loader.load_yaml("test.yaml")
        
        assert config["source"]["catalog"] == "default_catalog"
    
    def test_missing_env_var_no_default(self, tmp_path):
        """Test error when env var missing and no default."""
        config_dict = {
            "value": "${UNDEFINED_VAR_NO_DEFAULT}",
        }
        
        config_file = tmp_path / "test.yaml"
        with open(config_file, 'w') as f:
            yaml.dump(config_dict, f)
        
        loader = ConfigLoader(base_path=tmp_path)
        
        with pytest.raises(ConfigurationError) as exc_info:
            loader.load_yaml("test.yaml")
        assert "not found" in str(exc_info.value)
    
    def test_load_pipeline_config(self, temp_config_file):
        """Test loading full pipeline config."""
        loader = ConfigLoader(base_path=temp_config_file.parent)
        config = loader.load_pipeline_config(temp_config_file.name)
        
        assert isinstance(config, PipelineConfig)
        assert config.name == "test_pipeline"
        assert config.layer == "bronze"
        assert config.source is not None
        assert config.target is not None
        assert len(config.transformations) == 2
    
    def test_widget_params_substitution(self, tmp_path):
        """Test widget parameter substitution."""
        config_dict = {
            "pipeline": {
                "name": "test",
                "layer": "bronze",
            },
            "source": {
                "type": "table",
                "catalog": "${WIDGET_CATALOG}",
            },
        }
        
        config_file = tmp_path / "test.yaml"
        with open(config_file, 'w') as f:
            yaml.dump(config_dict, f)
        
        loader = ConfigLoader(
            base_path=tmp_path,
            widget_params={"WIDGET_CATALOG": "widget_value"}
        )
        config = loader.load_yaml("test.yaml")
        
        assert config["source"]["catalog"] == "widget_value"
    
    def test_list_configs(self, tmp_path):
        """Test listing configuration files."""
        # Create directory structure
        bronze_dir = tmp_path / "bronze"
        bronze_dir.mkdir()
        
        (bronze_dir / "config1.yaml").write_text("test: 1")
        (bronze_dir / "config2.yaml").write_text("test: 2")
        (bronze_dir / "config1.dev.yaml").write_text("test: dev")  # Should be excluded
        
        loader = ConfigLoader(base_path=tmp_path)
        configs = loader.list_configs(layer="bronze")
        
        assert len(configs) == 2
        assert all(c.suffix == ".yaml" for c in configs)
        assert not any("dev" in c.stem for c in configs)
