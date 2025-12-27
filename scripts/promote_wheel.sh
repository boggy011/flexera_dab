#!/bin/bash
# =============================================================================
# Promote Wheel Script
# =============================================================================
# Promotes a wheel from one environment to another (typically qa -> prd).
#
# Usage:
#   ./scripts/promote_wheel.sh <source_env> <target_env>
#
# Examples:
#   ./scripts/promote_wheel.sh qa prd
#
# This script:
#   1. Copies the wheel from source environment workspace to target
#   2. Updates the version to remove rc suffix (for prd)
#   3. Deploys to target environment using the promoted wheel
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
SOURCE_ENV="${1:-}"
TARGET_ENV="${2:-}"

# Validate arguments
if [[ -z "$SOURCE_ENV" ]] || [[ -z "$TARGET_ENV" ]]; then
    echo -e "${RED}ERROR: Source and target environments are required${NC}"
    echo ""
    echo "Usage: $0 <source_env> <target_env>"
    echo "Example: $0 qa prd"
    exit 1
fi

# Validate environments
if [[ ! "$SOURCE_ENV" =~ ^(dev|qa)$ ]]; then
    echo -e "${RED}ERROR: Invalid source environment: $SOURCE_ENV${NC}"
    echo "Valid source environments: dev, qa"
    exit 1
fi

if [[ ! "$TARGET_ENV" =~ ^(qa|prd)$ ]]; then
    echo -e "${RED}ERROR: Invalid target environment: $TARGET_ENV${NC}"
    echo "Valid target environments: qa, prd"
    exit 1
fi

# Main promotion logic
main() {
    echo -e "${BLUE}============================================${NC}"
    echo -e "${BLUE}Wheel Promotion${NC}"
    echo -e "${BLUE}============================================${NC}"
    echo ""
    echo -e "Source: ${GREEN}$SOURCE_ENV${NC}"
    echo -e "Target: ${GREEN}$TARGET_ENV${NC}"
    echo ""
    
    # Get version info from source
    VERSION_INFO="$PROJECT_ROOT/dist/version_info.txt"
    
    if [[ ! -f "$VERSION_INFO" ]]; then
        echo -e "${RED}ERROR: Version info not found: $VERSION_INFO${NC}"
        echo "Build the wheel first: ./scripts/build_wheel.sh $SOURCE_ENV"
        exit 1
    fi
    
    # Read version info
    source "$VERSION_INFO"
    
    echo -e "Source Version: ${BLUE}$version${NC}"
    echo -e "Wheel File: ${BLUE}$wheel_file${NC}"
    echo ""
    
    # For production, remove rc suffix from version
    if [[ "$TARGET_ENV" == "prd" ]]; then
        # Convert 1.1.0rc1 to 1.1.0
        NEW_VERSION=$(echo "$version" | sed 's/rc[0-9]*//')
        echo -e "Production Version: ${GREEN}$NEW_VERSION${NC}"
        
        # Update version file for production
        cat > "$PROJECT_ROOT/src/datalib/_version.py" << EOF
"""
Version management for datalib.

This module is updated by the build scripts based on environment:
- dev: {major}.{minor}.{patch}.dev{build}+{branch}
- qa:  {major}.{minor+1}.0rc{build}
- prd: Same as qa (promoted)
"""

__version__ = "$NEW_VERSION"
__version_info__ = tuple("$NEW_VERSION".split('.'))

# Environment metadata
__environment__ = "prd"
__build_number__ = "$build_number"
__branch__ = "main"
__promoted_from__ = "$SOURCE_ENV"
__source_version__ = "$version"
EOF
        
        # Rebuild wheel with production version
        echo -e "${YELLOW}Rebuilding wheel with production version...${NC}"
        cd "$PROJECT_ROOT"
        rm -rf "$PROJECT_ROOT/dist"
        mkdir -p "$PROJECT_ROOT/dist"
        python -m pip wheel . --no-deps --wheel-dir "$PROJECT_ROOT/dist"
        
        # Get new wheel name
        WHEEL_FILE=$(ls "$PROJECT_ROOT/dist"/*.whl | head -1)
        WHEEL_NAME=$(basename "$WHEEL_FILE")
        
        echo -e "Production Wheel: ${GREEN}$WHEEL_NAME${NC}"
        
        # Update version info
        cat > "$PROJECT_ROOT/dist/version_info.txt" << EOF
version=$NEW_VERSION
environment=prd
branch=main
build_number=$build_number
wheel_file=$WHEEL_NAME
promoted_from=$SOURCE_ENV
source_version=$version
promotion_timestamp=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
EOF
    fi
    
    echo ""
    echo -e "${GREEN}============================================${NC}"
    echo -e "${GREEN}Promotion Complete!${NC}"
    echo -e "${GREEN}============================================${NC}"
    echo ""
    echo -e "Wheel ready for deployment to ${BLUE}$TARGET_ENV${NC}"
    echo ""
    echo -e "Next steps:"
    echo -e "  1. Deploy: ./scripts/deploy.sh $TARGET_ENV"
    echo -e "  2. Verify deployment in Databricks workspace"
    echo ""
}

# Run main
main "$@"
