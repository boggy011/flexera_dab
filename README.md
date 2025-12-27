# Databricks Data Engineering Project

A production-ready, modular Databricks project with reusable code library, configuration-driven notebooks, and Databricks Asset Bundles (DAB) deployment across environments.

## 📁 Project Structure

```
databricks-project/
├── databricks.yaml                 # DAB main configuration
├── resources/                      # DAB resource definitions
│   ├── jobs.yml                    # Job definitions with placeholders
│   └── clusters.yml                # Cluster configurations
├── src/
│   └── datalib/                    # Python library (wheel)
│       ├── __init__.py
│       ├── core/                   # Core processing logic
│       ├── transformations/        # Data transformations
│       ├── utils/                  # Utilities
│       └── io/                     # I/O operations
├── jobs/                           # Job JSON configurations
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── notebooks/                      # Reusable notebooks
│   ├── runner.py                   # Main reusable notebook
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── configs/                        # YAML configurations by business area
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── tests/                          # Unit tests
├── scripts/                        # Build & deployment scripts
├── setup.py                        # Package setup
├── pyproject.toml                  # Modern Python packaging
└── .github/workflows/              # CI/CD pipelines
```

## 🏗️ Architecture

### Environments
| Environment | Code | Databricks Workspace | Purpose |
|-------------|------|---------------------|---------|
| Sandbox | dev | /Workspace/dev | Development & testing |
| Stage | qa | /Workspace/qa | QA & integration testing |
| Production | prd | /Workspace/prd | Production workloads |

### Versioning Strategy
- **Sandbox (dev)**: `{major}.{minor}.{patch}.dev{build}+{branch_name}`
  - Example: `1.0.0.dev42+feature-new-transform`
- **Stage (qa)**: `{major}.{minor+1}.0rc{build}`
  - Example: `1.1.0rc1`
- **Production (prd)**: Same wheel as QA, promoted
  - Example: `1.1.0` (release candidate promoted)

## 🚀 Quick Start

### Prerequisites
- Python 3.9+
- Databricks CLI v0.200+
- Access to Databricks workspace

### Setup

```bash
# Clone the repository
git clone <repository-url>
cd databricks-project

# Install development dependencies
pip install -e ".[dev]"

# Configure Databricks CLI
databricks configure --profile dev
databricks configure --profile qa
databricks configure --profile prd

# Validate DAB configuration
databricks bundle validate -t dev
```

### Build Wheel

```bash
# For development (includes branch name)
./scripts/build_wheel.sh dev feature-my-branch 1

# For QA (increments minor version)
./scripts/build_wheel.sh qa

# Wheel is automatically used in production from QA
```

### Deploy

```bash
# Deploy to sandbox
databricks bundle deploy -t dev

# Deploy to stage
databricks bundle deploy -t qa

# Deploy to production
databricks bundle deploy -t prd
```

## 📖 Configuration Guide

### YAML Config Structure

Each business area (bronze/silver/gold) has YAML configs:

```yaml
# configs/bronze/customer_ingestion.yaml
pipeline:
  name: customer_ingestion
  layer: bronze
  
source:
  type: jdbc
  connection_string: "${JDBC_CONNECTION}"
  table: customers
  
target:
  catalog: main
  schema: bronze
  table: customers_raw
  
processing:
  mode: incremental
  watermark_column: updated_at
  partition_by: [ingestion_date]
```

### Job Configuration

Jobs reference YAML configs and use placeholders:

```json
{
  "job_cluster_key": "{{JOB_CLUSTER}}",
  "existing_cluster_id": "{{INTERACTIVE_CLUSTER_ID}}",
  "libraries": [
    {"whl": "{{WHEEL_PATH}}"}
  ]
}
```

## 🔧 Development Workflow

1. **Create feature branch** from `main`
2. **Develop & test** locally
3. **Build wheel** for sandbox: `./scripts/build_wheel.sh dev <branch> <build>`
4. **Deploy to sandbox**: `databricks bundle deploy -t dev`
5. **Test in Databricks** sandbox workspace
6. **Create PR** to `main`
7. **After merge**, CI/CD builds QA wheel and deploys
8. **After QA approval**, promote to production

## 📊 Business Areas

### Bronze Layer
Raw data ingestion from source systems.

### Silver Layer
Cleansed, conformed data with business logic.

### Gold Layer
Aggregated, business-ready datasets.

## 🧪 Testing

```bash
# Run unit tests
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=datalib --cov-report=html
```

## 📚 Additional Documentation

- [Configuration Reference](docs/configuration.md)
- [Deployment Guide](docs/deployment.md)
- [Development Guide](docs/development.md)
