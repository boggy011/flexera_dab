#!/bin/bash
# =============================================================================
# Replace Placeholders Script
# =============================================================================
# Replaces placeholders in job configuration files with actual values.
#
# This script is typically called by the deploy script, but can be run
# manually for debugging purposes.
#
# Usage:
#   ./scripts/replace_placeholders.sh <environment>
#
# Placeholders:
#   {{WHEEL_PATH}}              - Path to wheel in workspace
#   {{INTERACTIVE_CLUSTER_ID}}  - Interactive cluster ID
#   {{JOB_CLUSTER_NODE_TYPE}}   - Node type for job clusters
#   {{JOB_CLUSTER_NUM_WORKERS}} - Number of workers
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

ENVIRONMENT="${1:-dev}"

# Load environment-specific configuration
ENV_CONFIG_DIR="$PROJECT_ROOT/configs/environments"
mkdir -p "$ENV_CONFIG_DIR"

# Default configurations per environment
case "$ENVIRONMENT" in
    dev)
        : "${WHEEL_PATH:=/Workspace/.bundle/datalib-pipelines/dev/artifacts/datalib-*.whl}"
        : "${INTERACTIVE_CLUSTER_ID:=}"
        : "${JOB_CLUSTER_NODE_TYPE:=Standard_DS3_v2}"
        : "${JOB_CLUSTER_NUM_WORKERS:=1}"
        ;;
    qa)
        : "${WHEEL_PATH:=/Workspace/.bundle/datalib-pipelines/qa/artifacts/datalib-*.whl}"
        : "${INTERACTIVE_CLUSTER_ID:=}"
        : "${JOB_CLUSTER_NODE_TYPE:=Standard_DS4_v2}"
        : "${JOB_CLUSTER_NUM_WORKERS:=2}"
        ;;
    prd)
        : "${WHEEL_PATH:=/Workspace/.bundle/datalib-pipelines/prd/artifacts/datalib-*.whl}"
        : "${INTERACTIVE_CLUSTER_ID:=}"
        : "${JOB_CLUSTER_NODE_TYPE:=Standard_DS5_v2}"
        : "${JOB_CLUSTER_NUM_WORKERS:=4}"
        ;;
esac

echo "Replacing placeholders for environment: $ENVIRONMENT"
echo "  WHEEL_PATH: $WHEEL_PATH"
echo "  JOB_CLUSTER_NODE_TYPE: $JOB_CLUSTER_NODE_TYPE"
echo "  JOB_CLUSTER_NUM_WORKERS: $JOB_CLUSTER_NUM_WORKERS"
echo "  INTERACTIVE_CLUSTER_ID: ${INTERACTIVE_CLUSTER_ID:-<not set>}"

# Export for DAB
export WHEEL_PATH
export JOB_CLUSTER_NODE_TYPE
export JOB_CLUSTER_NUM_WORKERS
export INTERACTIVE_CLUSTER_ID

echo "Placeholders exported as environment variables"
