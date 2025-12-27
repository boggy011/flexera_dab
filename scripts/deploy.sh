#!/bin/bash
# =============================================================================
# Deploy Script
# =============================================================================
# Deploys the datalib project to Databricks using Asset Bundles.
#
# Usage:
#   ./scripts/deploy.sh <environment> [options]
#
# Environments:
#   dev  - Sandbox/Development
#   qa   - Stage/QA
#   prd  - Production
#
# Options:
#   --build        Build wheel before deploying
#   --dry-run      Validate without deploying
#   --force        Force deployment (skip confirmations)
#   --destroy      Destroy deployed resources
#
# Examples:
#   ./scripts/deploy.sh dev --build
#   ./scripts/deploy.sh qa --dry-run
#   ./scripts/deploy.sh prd --force
# =============================================================================

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Arguments
ENVIRONMENT="${1:-}"
shift || true

# Parse options
BUILD_WHEEL=false
DRY_RUN=false
FORCE=false
DESTROY=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --build)
            BUILD_WHEEL=true
            shift
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        --force)
            FORCE=true
            shift
            ;;
        --destroy)
            DESTROY=true
            shift
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            exit 1
            ;;
    esac
done

# Validate environment
validate_environment() {
    if [[ -z "$ENVIRONMENT" ]]; then
        echo -e "${RED}ERROR: Environment is required${NC}"
        echo ""
        echo "Usage: $0 <environment> [options]"
        echo "Environments: dev, qa, prd"
        exit 1
    fi
    
    if [[ ! "$ENVIRONMENT" =~ ^(dev|qa|prd)$ ]]; then
        echo -e "${RED}ERROR: Invalid environment: $ENVIRONMENT${NC}"
        echo "Valid environments: dev, qa, prd"
        exit 1
    fi
}

# Check prerequisites
check_prerequisites() {
    echo -e "${YELLOW}Checking prerequisites...${NC}"
    
    # Check Databricks CLI
    if ! command -v databricks &> /dev/null; then
        echo -e "${RED}ERROR: Databricks CLI not found${NC}"
        echo "Install: pip install databricks-cli"
        exit 1
    fi
    
    # Check CLI version (needs v0.200+)
    CLI_VERSION=$(databricks --version 2>/dev/null | grep -oP '\d+\.\d+' | head -1)
    echo -e "  Databricks CLI: ${GREEN}v$CLI_VERSION${NC}"
    
    # Check if bundle is available
    if ! databricks bundle --help &> /dev/null; then
        echo -e "${RED}ERROR: Databricks Asset Bundles not available${NC}"
        echo "Update CLI: pip install databricks-cli --upgrade"
        exit 1
    fi
    
    echo -e "  Asset Bundles: ${GREEN}Available${NC}"
}

# Load environment configuration
load_environment_config() {
    echo -e "${YELLOW}Loading environment configuration...${NC}"
    
    # Environment-specific config file
    ENV_CONFIG="$PROJECT_ROOT/configs/environments/${ENVIRONMENT}.env"
    
    if [[ -f "$ENV_CONFIG" ]]; then
        echo -e "  Loading: ${GREEN}$ENV_CONFIG${NC}"
        source "$ENV_CONFIG"
    else
        echo -e "  ${YELLOW}No environment config file found: $ENV_CONFIG${NC}"
        echo -e "  ${YELLOW}Using defaults and environment variables${NC}"
    fi
    
    # Validate required Service Principal variables
    if [[ -z "$DATABRICKS_HOST" ]]; then
        echo -e "${RED}ERROR: DATABRICKS_HOST is not set${NC}"
        exit 1
    fi
    
    if [[ -z "$DATABRICKS_CLIENT_ID" ]] || [[ -z "$DATABRICKS_CLIENT_SECRET" ]]; then
        echo -e "${RED}ERROR: DATABRICKS_CLIENT_ID and DATABRICKS_CLIENT_SECRET must be set${NC}"
        echo -e "${YELLOW}Service Principal authentication is required for all environments${NC}"
        exit 1
    fi
    
    echo -e "  DATABRICKS_HOST: ${GREEN}$DATABRICKS_HOST${NC}"
    echo -e "  DATABRICKS_CLIENT_ID: ${GREEN}${DATABRICKS_CLIENT_ID:0:8}...${NC}"
    echo -e "  Authentication: ${GREEN}Service Principal (OAuth M2M)${NC}"
    
    # Set defaults for other variables
    export WHEEL_PATH="${WHEEL_PATH:-}"
    export INTERACTIVE_CLUSTER_ID="${INTERACTIVE_CLUSTER_ID:-}"
}

# Replace placeholders in resource files
replace_placeholders() {
    echo -e "${YELLOW}Replacing placeholders...${NC}"
    
    # Determine wheel path
    if [[ -z "$WHEEL_PATH" ]]; then
        # Get wheel from dist directory
        WHEEL_FILE=$(ls "$PROJECT_ROOT/dist"/*.whl 2>/dev/null | head -1)
        if [[ -n "$WHEEL_FILE" ]]; then
            WHEEL_NAME=$(basename "$WHEEL_FILE")
            # Workspace path where wheel will be uploaded
            WHEEL_PATH="/Workspace/.bundle/datalib-pipelines/${ENVIRONMENT}/artifacts/${WHEEL_NAME}"
        else
            echo -e "${RED}ERROR: No wheel found in dist directory${NC}"
            echo "Run: ./scripts/build_wheel.sh $ENVIRONMENT"
            exit 1
        fi
    fi
    
    echo -e "  WHEEL_PATH: ${GREEN}$WHEEL_PATH${NC}"
    
    # Cluster configuration based on environment
    case "$ENVIRONMENT" in
        dev)
            JOB_CLUSTER_NODE_TYPE="${JOB_CLUSTER_NODE_TYPE:-Standard_DS3_v2}"
            JOB_CLUSTER_NUM_WORKERS="${JOB_CLUSTER_NUM_WORKERS:-1}"
            ;;
        qa)
            JOB_CLUSTER_NODE_TYPE="${JOB_CLUSTER_NODE_TYPE:-Standard_DS4_v2}"
            JOB_CLUSTER_NUM_WORKERS="${JOB_CLUSTER_NUM_WORKERS:-2}"
            ;;
        prd)
            JOB_CLUSTER_NODE_TYPE="${JOB_CLUSTER_NODE_TYPE:-Standard_DS5_v2}"
            JOB_CLUSTER_NUM_WORKERS="${JOB_CLUSTER_NUM_WORKERS:-4}"
            ;;
    esac
    
    echo -e "  JOB_CLUSTER_NODE_TYPE: ${GREEN}$JOB_CLUSTER_NODE_TYPE${NC}"
    echo -e "  JOB_CLUSTER_NUM_WORKERS: ${GREEN}$JOB_CLUSTER_NUM_WORKERS${NC}"
    
    # Export for bundle
    export WHEEL_PATH
    export JOB_CLUSTER_NODE_TYPE
    export JOB_CLUSTER_NUM_WORKERS
    export INTERACTIVE_CLUSTER_ID
}

# Build wheel if requested
build_wheel() {
    if [[ "$BUILD_WHEEL" == "true" ]]; then
        echo -e "${YELLOW}Building wheel...${NC}"
        
        # Get branch name
        BRANCH_NAME=$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "local")
        BUILD_NUMBER=$(git rev-list --count HEAD 2>/dev/null || echo "0")
        
        "$SCRIPT_DIR/build_wheel.sh" "$ENVIRONMENT" "$BRANCH_NAME" "$BUILD_NUMBER"
    fi
}

# Validate bundle
validate_bundle() {
    echo -e "${YELLOW}Validating bundle...${NC}"
    
    cd "$PROJECT_ROOT"
    databricks bundle validate -t "$ENVIRONMENT"
    
    echo -e "${GREEN}Bundle validation successful!${NC}"
}

# Deploy bundle
deploy_bundle() {
    echo -e "${YELLOW}Deploying bundle to $ENVIRONMENT...${NC}"
    
    cd "$PROJECT_ROOT"
    
    if [[ "$DRY_RUN" == "true" ]]; then
        echo -e "${BLUE}DRY RUN - No changes will be made${NC}"
        databricks bundle validate -t "$ENVIRONMENT"
    elif [[ "$DESTROY" == "true" ]]; then
        if [[ "$FORCE" != "true" ]]; then
            echo -e "${RED}WARNING: This will destroy all deployed resources!${NC}"
            read -p "Are you sure? (yes/no): " confirm
            if [[ "$confirm" != "yes" ]]; then
                echo "Aborted."
                exit 0
            fi
        fi
        databricks bundle destroy -t "$ENVIRONMENT" --auto-approve
    else
        if [[ "$FORCE" == "true" ]]; then
            databricks bundle deploy -t "$ENVIRONMENT" --force
        else
            databricks bundle deploy -t "$ENVIRONMENT"
        fi
    fi
}

# Show deployment summary
show_summary() {
    echo ""
    echo -e "${GREEN}============================================${NC}"
    echo -e "${GREEN}Deployment Complete!${NC}"
    echo -e "${GREEN}============================================${NC}"
    echo ""
    echo -e "Environment: ${BLUE}$ENVIRONMENT${NC}"
    echo -e "Wheel Path:  ${BLUE}$WHEEL_PATH${NC}"
    echo ""
    echo -e "Next steps:"
    echo -e "  1. Check jobs in Databricks workspace"
    echo -e "  2. Enable job schedules as needed"
    echo -e "  3. Run a test job: databricks bundle run -t $ENVIRONMENT <job_name>"
    echo ""
}

# Main execution
main() {
    echo -e "${BLUE}============================================${NC}"
    echo -e "${BLUE}DataLib Deployment${NC}"
    echo -e "${BLUE}============================================${NC}"
    echo ""
    
    validate_environment
    check_prerequisites
    load_environment_config
    build_wheel
    replace_placeholders
    validate_bundle
    deploy_bundle
    show_summary
}

# Run main
main "$@"
