"""
Pytest configuration and fixtures for datalib tests.
"""

import os
import pytest
from unittest.mock import MagicMock, patch


# Set test environment
os.environ["ENVIRONMENT"] = "test"
os.environ["CATALOG_NAME"] = "test_catalog"


@pytest.fixture
def mock_spark():
    """Create a mock Spark session for unit tests."""
    mock = MagicMock()
    mock.sql.return_value = MagicMock()
    mock.read.return_value = MagicMock()
    mock.table.return_value = MagicMock()
    return mock


@pytest.fixture
def mock_dbutils():
    """Create a mock dbutils for unit tests."""
    mock = MagicMock()
    mock.widgets.get.return_value = "test_value"
    mock.secrets.get.return_value = "secret_value"
    return mock


@pytest.fixture
def mock_context(mock_spark, mock_dbutils):
    """Create a mock Databricks context."""
    from datalib.core.context import DatabricksContext
    
    with patch.object(DatabricksContext, '_create_spark_session', return_value=mock_spark):
        with patch.object(DatabricksContext, '_get_dbutils', return_value=mock_dbutils):
            context = DatabricksContext(environment="test")
            context._spark = mock_spark
            context._dbutils = mock_dbutils
            yield context


@pytest.fixture
def sample_config_dict():
    """Sample pipeline configuration dictionary."""
    return {
        "pipeline": {
            "name": "test_pipeline",
            "layer": "bronze",
            "description": "Test pipeline",
            "version": "1.0.0",
            "owner": "test-team",
        },
        "source": {
            "type": "table",
            "catalog": "test_catalog",
            "schema": "source_schema",
            "table": "source_table",
        },
        "target": {
            "catalog": "test_catalog",
            "schema": "target_schema",
            "table": "target_table",
            "mode": "overwrite",
            "format": "delta",
        },
        "transformations": [
            {
                "type": "add_timestamp",
                "params": {"column_name": "ingestion_ts"},
            },
            {
                "type": "cast_columns",
                "params": {"column_types": {"id": "string"}},
            },
        ],
        "quality": {
            "enabled": True,
            "fail_on_error": False,
            "checks": [
                {"type": "not_null", "column": "id"},
            ],
        },
    }


@pytest.fixture
def temp_config_file(tmp_path, sample_config_dict):
    """Create a temporary YAML config file."""
    import yaml
    
    config_file = tmp_path / "test_config.yaml"
    with open(config_file, 'w') as f:
        yaml.dump(sample_config_dict, f)
    
    return config_file
