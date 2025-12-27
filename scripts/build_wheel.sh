#!/bin/bash
# =============================================================================
# Build Wheel Script
# =============================================================================
# Builds the datalib wheel package with proper versioning based on environment.
#
# Usage:
#   ./scripts/build_wheel.sh <environment> [branch_name] [build_number]
#
# Versioning Strategy:
#   - dev: {major}.{minor}.{patch}.dev{build}+{branch}
#   - qa:  {major}.{minor+1}.0rc{build}
#   - prd: Same wheel as qa (promoted, no rebuild)
#
# Examples:
#   ./scripts/build_wheel.sh dev feature-new-transform 42
#   ./scripts/build_wheel.sh qa
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
ENVIRONMENT="${1:-dev}"
BRANCH_NAME="${2:-local}"
BUILD_NUMBER="${3:-0}"

# Version file
VERSION_FILE="$PROJECT_ROOT/src/datalib/_version.py"
DIST_DIR="$PROJECT_ROOT/dist"

# Read current version from version file
get_current_version() {
    grep "__version__ = " "$VERSION_FILE" | cut -d'"' -f2 | cut -d'.' -f1-3
}

# Clean branch name for version string
clean_branch_name() {
    echo "$1" | tr '/' '-' | tr '_' '-' | tr '[:upper:]' '[:lower:]' | head -c 40
}

# Generate version based on environment
generate_version() {
    local env="$1"
    local branch="$2"
    local build="$3"
    
    # Get base version (major.minor.patch)
    local base_version=$(get_current_version)
    local major=$(echo "$base_version" | cut -d'.' -f1)
    local minor=$(echo "$base_version" | cut -d'.' -f2)
    local patch=$(echo "$base_version" | cut -d'.' -f3)
    
    case "$env" in
        dev)
            # Development version: 1.0.0.dev42+feature-branch
            local clean_branch=$(clean_branch_name "$branch")
            echo "${major}.${minor}.${patch}.dev${build}+${clean_branch}"
            ;;
        qa)
            # Release candidate: 1.1.0rc1 (minor version incremented)
            local new_minor=$((minor + 1))
            echo "${major}.${new_minor}.0rc${build}"
            ;;
        prd)
            # Production: Same as qa but without rc suffix
            # This should use the promoted qa wheel, not rebuild
            echo "ERROR: Production should use promoted qa wheel"
            exit 1
            ;;
        *)
            echo "ERROR: Unknown environment: $env"
            exit 1
            ;;
    esac
}

# Update version file
update_version_file() {
    local version="$1"
    local env="$2"
    local branch="$3"
    local build="$4"
    
    cat > "$VERSION_FILE" << EOF
"""
Version management for datalib.

This module is updated by the build scripts based on environment:
- dev: {major}.{minor}.{patch}.dev{build}+{branch}
- qa:  {major}.{minor+1}.0rc{build}
- prd: Same as qa (promoted)
"""

__version__ = "$version"
__version_info__ = tuple("$version".replace('+', '.').replace('rc', '.').split('.'))

# Environment metadata
__environment__ = "$env"
__build_number__ = "$build"
__branch__ = "$branch"
EOF
}

# Main build process
main() {
    echo -e "${BLUE}============================================${NC}"
    echo -e "${BLUE}Building DataLib Wheel${NC}"
    echo -e "${BLUE}============================================${NC}"
    echo ""
    echo -e "Environment:  ${GREEN}$ENVIRONMENT${NC}"
    echo -e "Branch:       ${GREEN}$BRANCH_NAME${NC}"
    echo -e "Build Number: ${GREEN}$BUILD_NUMBER${NC}"
    echo ""
    
    # Validate environment
    if [[ "$ENVIRONMENT" == "prd" ]]; then
        echo -e "${RED}ERROR: Production builds should promote qa wheel, not rebuild.${NC}"
        echo -e "${YELLOW}Use: ./scripts/promote_wheel.sh qa prd${NC}"
        exit 1
    fi
    
    # Generate version
    VERSION=$(generate_version "$ENVIRONMENT" "$BRANCH_NAME" "$BUILD_NUMBER")
    echo -e "Generated Version: ${GREEN}$VERSION${NC}"
    echo ""
    
    # Update version file
    echo -e "${YELLOW}Updating version file...${NC}"
    update_version_file "$VERSION" "$ENVIRONMENT" "$BRANCH_NAME" "$BUILD_NUMBER"
    
    # Clean dist directory
    echo -e "${YELLOW}Cleaning dist directory...${NC}"
    rm -rf "$DIST_DIR"
    mkdir -p "$DIST_DIR"
    
    # Build wheel
    echo -e "${YELLOW}Building wheel...${NC}"
    cd "$PROJECT_ROOT"
    python -m pip wheel . --no-deps --wheel-dir "$DIST_DIR"
    
    # List built wheel
    echo ""
    echo -e "${GREEN}Built wheel:${NC}"
    ls -la "$DIST_DIR"/*.whl
    
    # Get wheel filename
    WHEEL_FILE=$(ls "$DIST_DIR"/*.whl | head -1)
    WHEEL_NAME=$(basename "$WHEEL_FILE")
    
    echo ""
    echo -e "${GREEN}============================================${NC}"
    echo -e "${GREEN}Build Complete!${NC}"
    echo -e "${GREEN}============================================${NC}"
    echo ""
    echo -e "Wheel:   ${BLUE}$WHEEL_NAME${NC}"
    echo -e "Path:    ${BLUE}$WHEEL_FILE${NC}"
    echo -e "Version: ${BLUE}$VERSION${NC}"
    echo ""
    
    # Create version info file for deployment
    cat > "$DIST_DIR/version_info.txt" << EOF
version=$VERSION
environment=$ENVIRONMENT
branch=$BRANCH_NAME
build_number=$BUILD_NUMBER
wheel_file=$WHEEL_NAME
build_timestamp=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
EOF
    
    echo -e "${GREEN}Version info saved to: $DIST_DIR/version_info.txt${NC}"
}

# Run main
main "$@"
