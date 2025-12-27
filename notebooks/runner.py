# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline Runner Notebook
# MAGIC 
# MAGIC Reusable notebook for executing data pipelines based on YAML configuration.
# MAGIC 
# MAGIC ## Parameters
# MAGIC - `config_path`: Path to the YAML configuration file (relative to configs folder)
# MAGIC - `environment`: Environment name (dev, qa, prd)
# MAGIC - `dry_run`: If "true", only validate without writing data

# COMMAND ----------

# DBTITLE 1,Setup Widgets
dbutils.widgets.text("config_path", "", "Config Path")
dbutils.widgets.dropdown("environment", "dev", ["dev", "qa", "prd"], "Environment")
dbutils.widgets.dropdown("dry_run", "false", ["true", "false"], "Dry Run")
dbutils.widgets.text("catalog_override", "", "Catalog Override (optional)")

# COMMAND ----------

# DBTITLE 1,Get Widget Values
config_path = dbutils.widgets.get("config_path")
environment = dbutils.widgets.get("environment")
dry_run = dbutils.widgets.get("dry_run").lower() == "true"
catalog_override = dbutils.widgets.get("catalog_override")

print(f"Configuration Path: {config_path}")
print(f"Environment: {environment}")
print(f"Dry Run: {dry_run}")
print(f"Catalog Override: {catalog_override or 'None'}")

# Validate required parameters
if not config_path:
    dbutils.notebook.exit("ERROR: config_path parameter is required")

# COMMAND ----------

# DBTITLE 1,Set Environment Variables
import os

# Set environment
os.environ["ENVIRONMENT"] = environment

# Set catalog based on environment if not overridden
if catalog_override:
    os.environ["CATALOG_NAME"] = catalog_override
else:
    catalog_mapping = {
        "dev": "dev_catalog",
        "qa": "qa_catalog", 
        "prd": "prd_catalog"
    }
    os.environ["CATALOG_NAME"] = catalog_mapping.get(environment, "main")

# Set common paths based on environment
env_paths = {
    "dev": {
        "LANDING_ZONE_PATH": "/mnt/landing/dev",
        "CHECKPOINT_PATH": "/mnt/checkpoints/dev",
    },
    "qa": {
        "LANDING_ZONE_PATH": "/mnt/landing/qa",
        "CHECKPOINT_PATH": "/mnt/checkpoints/qa",
    },
    "prd": {
        "LANDING_ZONE_PATH": "/mnt/landing/prd",
        "CHECKPOINT_PATH": "/mnt/checkpoints/prd",
    },
}

for key, value in env_paths.get(environment, {}).items():
    os.environ[key] = value

print(f"Catalog: {os.environ.get('CATALOG_NAME')}")
print(f"Landing Zone: {os.environ.get('LANDING_ZONE_PATH')}")

# COMMAND ----------

# DBTITLE 1,Import Library
from datalib import __version__, ConfigLoader, Pipeline, DatabricksContext, PipelineRunner
from datalib.utils.logging import get_logger, PipelineLogger

print(f"DataLib Version: {__version__}")

# COMMAND ----------

# DBTITLE 1,Initialize Context and Logger
# Create Databricks context
context = DatabricksContext(environment=environment)

# Get notebook path for logging
notebook_path = context.get_notebook_path() or "unknown"
cluster_id = context.get_cluster_id() or "unknown"
user = context.get_current_user() or "unknown"

print(f"Notebook: {notebook_path}")
print(f"Cluster: {cluster_id}")
print(f"User: {user}")

# COMMAND ----------

# DBTITLE 1,Determine Config Base Path
# The configs are relative to the notebook location
# Adjust based on your workspace structure

# Get the workspace path where configs are located
# This assumes configs are deployed alongside notebooks
import os

# Try to find configs in standard locations
possible_paths = [
    "/Workspace/Repos/main/databricks-project/configs",
    f"/Workspace/{environment}/configs",
    "/Workspace/Shared/configs",
    # For local testing
    "./configs",
]

# Or use the path relative to notebook
notebook_dir = os.path.dirname(notebook_path) if notebook_path != "unknown" else ""
repo_root = notebook_dir.rsplit("/notebooks", 1)[0] if "/notebooks" in notebook_dir else notebook_dir
config_base = f"{repo_root}/configs"

print(f"Config base path: {config_base}")

# COMMAND ----------

# DBTITLE 1,Load Configuration
# Collect widget parameters for variable substitution
widget_params = {
    "ENVIRONMENT": environment,
    "CATALOG_NAME": os.environ.get("CATALOG_NAME", "main"),
}

# Add catalog override if provided
if catalog_override:
    widget_params["CATALOG_NAME"] = catalog_override

# Initialize config loader
config_loader = ConfigLoader(
    base_path=config_base,
    environment=environment,
    widget_params=widget_params
)

# Load pipeline configuration
try:
    config = config_loader.load_pipeline_config(config_path)
    print(f"Loaded configuration for pipeline: {config.name}")
    print(f"Layer: {config.layer}")
    print(f"Description: {config.description}")
except Exception as e:
    print(f"ERROR loading configuration: {e}")
    dbutils.notebook.exit(f"ERROR: Failed to load configuration - {e}")

# COMMAND ----------

# DBTITLE 1,Display Configuration Summary
# Show configuration summary
print("=" * 60)
print("PIPELINE CONFIGURATION SUMMARY")
print("=" * 60)
print(f"Name: {config.name}")
print(f"Layer: {config.layer}")
print(f"Version: {config.version}")
print(f"Owner: {config.owner}")
print()

print("SOURCE:")
if config.source:
    print(f"  Type: {config.source.type}")
    print(f"  Table/Path: {config.source.get_full_table_name() or config.source.path}")
print()

print("TARGET:")
if config.target:
    print(f"  Table: {config.target.get_full_table_name()}")
    print(f"  Mode: {config.target.mode}")
    print(f"  Format: {config.target.format}")
    print(f"  Partitions: {config.target.partition_by}")
print()

print("TRANSFORMATIONS:")
for i, t in enumerate(config.transformations, 1):
    status = "✓" if t.enabled else "✗ (disabled)"
    print(f"  {i}. {t.type} {status}")
print()

print("QUALITY CHECKS:")
print(f"  Enabled: {config.quality.enabled}")
print(f"  Fail on Error: {config.quality.fail_on_error}")
print(f"  Number of Checks: {len(config.quality.checks)}")
print("=" * 60)

# COMMAND ----------

# DBTITLE 1,Initialize Pipeline
# Create pipeline instance
pipeline = Pipeline(config, context)

# Initialize pipeline logger
pipeline_logger = PipelineLogger(
    pipeline_name=config.name,
    layer=config.layer
)

pipeline_logger.info(f"Pipeline initialized in {environment} environment")

# COMMAND ----------

# DBTITLE 1,Execute Pipeline (Dry Run Check)
if dry_run:
    print("=" * 60)
    print("DRY RUN MODE - No data will be written")
    print("=" * 60)
    
    # Perform dry run
    dry_run_result = pipeline.dry_run()
    
    print("\nExecution Plan:")
    print(f"  Sources: {len(dry_run_result.get('sources', []))}")
    print(f"  Transformations: {len(dry_run_result.get('transformations', []))}")
    print(f"  Target: {dry_run_result.get('target', {}).get('table', 'None')}")
    
    if dry_run_result.get("sample_data"):
        print("\nSample Data Preview:")
        sample_df = spark.createDataFrame(
            [dry_run_result["sample_data"]]
        ) if dry_run_result["sample_data"] else None
        if sample_df:
            display(sample_df)
    
    # Exit with success for dry run
    dbutils.notebook.exit("DRY_RUN_COMPLETE")

# COMMAND ----------

# DBTITLE 1,Execute Pipeline
try:
    pipeline_logger.info("Starting pipeline execution")
    
    # Run the pipeline
    metrics = pipeline.run()
    
    pipeline_logger.info(f"Pipeline completed successfully")
    pipeline_logger.log_metrics(metrics.to_dict())
    
except Exception as e:
    pipeline_logger.error(f"Pipeline failed: {str(e)}")
    raise

# COMMAND ----------

# DBTITLE 1,Display Execution Metrics
print("=" * 60)
print("EXECUTION METRICS")
print("=" * 60)
print(f"Status: {metrics.status}")
print(f"Duration: {metrics.duration_seconds:.2f} seconds")
print(f"Records Read: {metrics.records_read:,}")
print(f"Records Written: {metrics.records_written:,}")
print(f"Transformations Applied: {metrics.transformations_applied}")
print(f"Quality Checks Passed: {metrics.quality_checks_passed}")
print(f"Quality Checks Failed: {metrics.quality_checks_failed}")

if metrics.error_message:
    print(f"Error: {metrics.error_message}")

print("=" * 60)

# COMMAND ----------

# DBTITLE 1,Validate Target Table
if config.target and metrics.status == "success":
    target_table = config.target.get_full_table_name()
    
    print(f"\nValidating target table: {target_table}")
    
    # Get row count
    count = spark.table(target_table).count()
    print(f"Total rows in target: {count:,}")
    
    # Show sample
    print("\nSample data from target:")
    display(spark.table(target_table).limit(10))
    
    # Show table properties
    print("\nTable properties:")
    spark.sql(f"DESCRIBE EXTENDED {target_table}").show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Return Result
import json

result = {
    "pipeline_name": config.name,
    "environment": environment,
    "status": metrics.status,
    "records_written": metrics.records_written,
    "duration_seconds": metrics.duration_seconds,
    "run_id": pipeline_logger.run_id,
}

# Exit with JSON result
dbutils.notebook.exit(json.dumps(result))
