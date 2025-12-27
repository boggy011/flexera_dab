# DataLib Project - Detailed Setup Guide

This document provides step-by-step instructions for setting up and running the DataLib project in your Databricks environment.

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Repository Setup](#repository-setup)
3. [Databricks Workspace Setup](#databricks-workspace-setup)
4. [Environment Configuration](#environment-configuration)
5. [Building and Deploying](#building-and-deploying)
6. [Running Pipelines](#running-pipelines)
7. [Customization Guide](#customization-guide)

---

## 1. Prerequisites

### Required Tools

```bash
# Python 3.9 or higher
python --version  # Should be 3.9+

# Databricks CLI v0.200 or higher
pip install databricks-cli --upgrade
databricks --version  # Should be 0.200+

# Git
git --version
```

### Required Access

- Databricks workspace access for each environment (dev, qa, prd)
- Unity Catalog permissions (CREATE SCHEMA, CREATE TABLE)
- Personal access token or service principal credentials

---

## 2. Repository Setup

### Clone and Initialize

```bash
# Clone the repository
git clone <your-repo-url>
cd databricks-project

# Install development dependencies
pip install -e ".[dev]"

# Verify installation
python -c "from datalib import __version__; print(__version__)"
```

### Project Structure Overview

```
databricks-project/
├── databricks.yaml              # DAB main config
├── resources/
│   ├── jobs.yml                 # Job definitions
│   └── clusters.yml             # Cluster templates
├── src/datalib/                 # Python library
│   ├── core/                    # Core components
│   ├── transformations/         # Data transformations
│   ├── io/                      # Readers/Writers
│   └── utils/                   # Utilities
├── configs/                     # YAML pipeline configs
│   ├── bronze/                  # Bronze layer
│   ├── silver/                  # Silver layer
│   └── gold/                    # Gold layer
├── notebooks/
│   └── runner.py                # Reusable runner notebook
├── scripts/                     # Build/deploy scripts
└── tests/                       # Unit tests
```

---

## 3. Databricks Workspace Setup

### 3.1 Configure Databricks CLI Profiles

Create profiles for each environment using Service Principal authentication:

```bash
# Option 1: Using environment variables (recommended for CI/CD)
export DATABRICKS_HOST="https://adb-xxxx.azuredatabricks.net"
export DATABRICKS_CLIENT_ID="your-client-id"
export DATABRICKS_CLIENT_SECRET="your-client-secret"

# Option 2: Using CLI profiles
# Create ~/.databrickscfg file with the following content:

cat > ~/.databrickscfg << 'EOF'
[dev]
host = https://adb-xxxx.azuredatabricks.net
client_id = your-dev-client-id
client_secret = your-dev-client-secret

[qa]
host = https://adb-yyyy.azuredatabricks.net
client_id = your-qa-client-id
client_secret = your-qa-client-secret

[prd]
host = https://adb-zzzz.azuredatabricks.net
client_id = your-prd-client-id
client_secret = your-prd-client-secret
EOF

# Verify authentication works
databricks auth token --profile dev
databricks workspace list / --profile dev
```

**Note:** Service Principal authentication requires:
1. An Azure AD App Registration with a client secret
2. The service principal added to the Databricks workspace with appropriate permissions

### 3.2 Create Unity Catalog Resources

For each environment, create the required catalogs and schemas:

```sql
-- Development
CREATE CATALOG IF NOT EXISTS dev_catalog;
CREATE SCHEMA IF NOT EXISTS dev_catalog.bronze;
CREATE SCHEMA IF NOT EXISTS dev_catalog.silver;
CREATE SCHEMA IF NOT EXISTS dev_catalog.gold;

-- QA
CREATE CATALOG IF NOT EXISTS qa_catalog;
CREATE SCHEMA IF NOT EXISTS qa_catalog.bronze;
CREATE SCHEMA IF NOT EXISTS qa_catalog.silver;
CREATE SCHEMA IF NOT EXISTS qa_catalog.gold;

-- Production
CREATE CATALOG IF NOT EXISTS prd_catalog;
CREATE SCHEMA IF NOT EXISTS prd_catalog.bronze;
CREATE SCHEMA IF NOT EXISTS prd_catalog.silver;
CREATE SCHEMA IF NOT EXISTS prd_catalog.gold;
```

### 3.3 Set Up Secret Scopes (Optional)

For JDBC connections and other secrets:

```bash
# Create secret scope
databricks secrets create-scope --scope datalib --profile dev

# Add secrets
databricks secrets put --scope datalib --key jdbc-connection --profile dev
databricks secrets put --scope datalib --key api-key --profile dev
```

---

## 4. Environment Configuration

### 4.1 Edit Environment Config Files

Update the configuration files in `configs/environments/`:

```bash
# Edit development config
vi configs/environments/dev.env
```

Key settings to update:

```bash
# configs/environments/dev.env
export DATABRICKS_HOST="https://adb-xxxx.azuredatabricks.net"
export CATALOG_NAME="dev_catalog"
export LANDING_ZONE_PATH="/mnt/landing/dev"
export JOB_CLUSTER_NODE_TYPE="Standard_DS3_v2"
export JOB_CLUSTER_NUM_WORKERS="1"
```

### 4.2 Update databricks.yaml

Modify the workspace hosts in `databricks.yaml`:

```yaml
targets:
  dev:
    workspace:
      host: https://adb-xxxx.azuredatabricks.net
  qa:
    workspace:
      host: https://adb-yyyy.azuredatabricks.net
  prd:
    workspace:
      host: https://adb-zzzz.azuredatabricks.net
```

### 4.3 Configure GitHub Secrets (for CI/CD)

Add these secrets to your GitHub repository (Settings > Secrets and variables > Actions):

| Secret Name | Description |
|-------------|-------------|
| `DATABRICKS_HOST_DEV` | Dev workspace URL (e.g., `https://adb-xxxx.azuredatabricks.net`) |
| `DATABRICKS_CLIENT_ID_DEV` | Dev service principal client ID |
| `DATABRICKS_CLIENT_SECRET_DEV` | Dev service principal client secret |
| `DATABRICKS_HOST_QA` | QA workspace URL |
| `DATABRICKS_CLIENT_ID_QA` | QA service principal client ID |
| `DATABRICKS_CLIENT_SECRET_QA` | QA service principal client secret |
| `DATABRICKS_HOST_PRD` | Production workspace URL |
| `DATABRICKS_CLIENT_ID_PRD` | Production service principal client ID |
| `DATABRICKS_CLIENT_SECRET_PRD` | Production service principal client secret |

**Note:** All environments use Service Principal (OAuth M2M) authentication. The Databricks CLI automatically picks up `DATABRICKS_HOST`, `DATABRICKS_CLIENT_ID`, and `DATABRICKS_CLIENT_SECRET` environment variables.

---

## 5. Building and Deploying

### 5.1 Build Wheel Package

```bash
# For development (includes branch name in version)
./scripts/build_wheel.sh dev feature-my-branch 1

# For QA (increments minor version)
./scripts/build_wheel.sh qa

# Check the output
ls -la dist/
cat dist/version_info.txt
```

### 5.2 Validate Bundle

```bash
# Validate for specific environment
databricks bundle validate -t dev
databricks bundle validate -t qa
databricks bundle validate -t prd
```

### 5.3 Deploy to Environment

```bash
# Deploy to development
./scripts/deploy.sh dev --build

# Deploy to QA
./scripts/deploy.sh qa --build

# Promote to production (uses QA wheel)
./scripts/promote_wheel.sh qa prd
./scripts/deploy.sh prd
```

### 5.4 View Deployed Resources

```bash
# List deployed jobs
databricks bundle summary -t dev

# Check job status
databricks jobs list --profile dev
```

---

## 6. Running Pipelines

### 6.1 Run Job via CLI

```bash
# Run a specific job
databricks bundle run -t dev bronze_customer_ingestion

# Run with parameters
databricks bundle run -t dev bronze_customer_ingestion \
  --params '{"config_path": "bronze/customer_ingestion.yaml"}'
```

### 6.2 Run via Notebook UI

1. Open Databricks workspace
2. Navigate to the deployed notebook (`runner.py`)
3. Set widget parameters:
   - `config_path`: `bronze/customer_ingestion.yaml`
   - `environment`: `dev`
   - `dry_run`: `false`
4. Run all cells

### 6.3 Dry Run Mode

Test configuration without writing data:

```bash
# Via CLI
databricks bundle run -t dev bronze_customer_ingestion \
  --params '{"dry_run": "true"}'
```

---

## 7. Customization Guide

### 7.1 Adding a New Pipeline

1. **Create YAML config** in appropriate layer folder:

```yaml
# configs/bronze/new_pipeline.yaml
pipeline:
  name: new_pipeline
  layer: bronze
  description: "My new pipeline"

source:
  type: table
  catalog: "${CATALOG_NAME}"
  schema: "source_schema"
  table: "source_table"

target:
  catalog: "${CATALOG_NAME}"
  schema: "bronze"
  table: "new_table"
  mode: "overwrite"

transformations:
  - type: add_timestamp
    params:
      column_name: "ingestion_timestamp"
```

2. **Add job definition** in `resources/jobs.yml`:

```yaml
new_pipeline_job:
  name: "[${bundle.target}] Bronze - New Pipeline"
  tasks:
    - task_key: run_pipeline
      job_cluster_key: pipeline_cluster
      libraries:
        - whl: "{{WHEEL_PATH}}"
      notebook_task:
        notebook_path: ../notebooks/runner.py
        base_parameters:
          config_path: "bronze/new_pipeline.yaml"
          environment: "${bundle.target}"
```

3. **Deploy**:

```bash
./scripts/deploy.sh dev
```

### 7.2 Adding Custom Transformations

1. **Create transformation class**:

```python
# src/datalib/transformations/custom.py
from datalib.transformations.base import Transformation

class MyCustomTransform(Transformation):
    def __init__(self, param1: str):
        self.param1 = param1
    
    def transform(self, df):
        # Your transformation logic
        return df.withColumn("new_col", ...)
```

2. **Register in pipeline**:

```python
# In your pipeline or runner
pipeline.register_transformation("my_custom", MyCustomTransform)
```

3. **Use in YAML**:

```yaml
transformations:
  - type: my_custom
    params:
      param1: "value"
```

### 7.3 Using Interactive Clusters

To use an existing interactive cluster instead of job clusters:

1. **Get cluster ID** from Databricks UI

2. **Update job definition** in `resources/jobs.yml`:

```yaml
tasks:
  - task_key: run_pipeline
    # Comment out job_cluster_key
    # job_cluster_key: pipeline_cluster
    
    # Add existing cluster ID
    existing_cluster_id: "1234-567890-abcdefgh"
```

3. **Or use placeholder**:

```yaml
existing_cluster_id: "{{INTERACTIVE_CLUSTER_ID}}"
```

Then set `INTERACTIVE_CLUSTER_ID` in your environment config.

---

## Troubleshooting

### Common Issues

1. **Bundle validation fails**
   - Check workspace connectivity: `databricks workspace list / --profile dev`
   - Verify authentication: `databricks auth token --profile dev`

2. **Job fails to find wheel**
   - Verify wheel was uploaded: `databricks workspace ls /Workspace/.bundle/`
   - Check WHEEL_PATH variable in deployment

3. **Pipeline can't find config**
   - Verify config files are in correct location
   - Check notebook path references in job definition

4. **Permission denied on catalogs**
   - Grant required permissions in Unity Catalog
   - Verify service principal has necessary access

### Getting Help

- Check logs in Databricks job run details
- Review `dist/version_info.txt` for build information
- Run tests locally: `pytest tests/ -v`

---

## Version History

| Version | Date | Changes |
|---------|------|---------|
| 1.0.0 | Initial | Initial release |
