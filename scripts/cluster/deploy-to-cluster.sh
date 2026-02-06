#!/bin/bash
# =============================================================================
# Deploy Primcast to all cluster nodes from your local machine
# Run this from your development machine
# =============================================================================

set -e

# =============================================================================
# Parse command line arguments
# =============================================================================
DEPLOY_BINARIES=false
DEPLOY_CONFIG=true
DEPLOY_SCRIPTS=true
BUILD_BEFORE=false

usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  -b, --binaries     Deploy binary files (open_loop, server, client)"
    echo "  -B, --build        Build release before deploying (implies --binaries)"
    echo "  -c, --config-only  Deploy only config file"
    echo "  -s, --scripts-only Deploy only scripts"
    echo "  -a, --all          Deploy everything (binaries + config + scripts)"
    echo "  -h, --help         Show this help message"
    echo ""
    echo "Default: Deploy config and scripts only (no binaries)"
    echo ""
    echo "Examples:"
    echo "  $0                  # Deploy config and scripts"
    echo "  $0 -b               # Deploy binaries + config + scripts"
    echo "  $0 -B               # Build, then deploy everything"
    echo "  $0 -c               # Deploy only config"
    exit 0
}

while [[ $# -gt 0 ]]; do
    case $1 in
        -b|--binaries)
            DEPLOY_BINARIES=true
            shift
            ;;
        -B|--build)
            BUILD_BEFORE=true
            DEPLOY_BINARIES=true
            shift
            ;;
        -c|--config-only)
            DEPLOY_CONFIG=true
            DEPLOY_SCRIPTS=false
            DEPLOY_BINARIES=false
            shift
            ;;
        -s|--scripts-only)
            DEPLOY_CONFIG=false
            DEPLOY_SCRIPTS=true
            DEPLOY_BINARIES=false
            shift
            ;;
        -a|--all)
            DEPLOY_BINARIES=true
            DEPLOY_CONFIG=true
            DEPLOY_SCRIPTS=true
            shift
            ;;
        -h|--help)
            usage
            ;;
        *)
            echo "Unknown option: $1"
            usage
            ;;
    esac
done

# =============================================================================
# Configuration
# =============================================================================
SERVERS=(
    "ronistone@192.168.2.18"
    "ronistone@192.168.2.17"
    "ronistone@192.168.2.19"
)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

echo "=========================================="
echo "Deploying Primcast to Cluster"
echo "=========================================="
echo "  Binaries: $DEPLOY_BINARIES"
echo "  Config:   $DEPLOY_CONFIG"
echo "  Scripts:  $DEPLOY_SCRIPTS"
echo "=========================================="

# Build release if requested
if [ "$BUILD_BEFORE" = true ]; then
    echo ""
    echo "Step 1: Building release binary locally..."
    cd "$PROJECT_ROOT"
    cargo build --release --examples
fi

# Deploy to each server
echo ""
echo "Deploying files to all servers..."

for SERVER in "${SERVERS[@]}"; do
    echo ""
    echo "Deploying to $SERVER..."
    
    # Create remote directory structure
    ssh "$SERVER" "mkdir -p ~/primcast/db ~/primcast/logs ~/primcast/scripts/cluster ~/primcast/target/release/examples"
    
    # Copy binary files
    if [ "$DEPLOY_BINARIES" = true ]; then
        echo "  Copying binaries..."
        scp "$PROJECT_ROOT/target/release/examples/open_loop" "$SERVER:~/primcast/target/release/examples/"
        
        # Try to copy optional binaries (ignore errors if they don't exist)
        scp "$PROJECT_ROOT/target/release/examples/server" "$SERVER:~/primcast/target/release/examples/" 2>/dev/null || true
        scp "$PROJECT_ROOT/target/release/examples/client" "$SERVER:~/primcast/target/release/examples/" 2>/dev/null || true
    fi
    
    # Copy config file
    if [ "$DEPLOY_CONFIG" = true ]; then
        echo "  Copying config..."
        scp "$PROJECT_ROOT/cluster_config.yaml" "$SERVER:~/primcast/"
    fi
    
    # Copy all cluster scripts
    if [ "$DEPLOY_SCRIPTS" = true ]; then
        echo "  Copying scripts..."
        scp "$PROJECT_ROOT/scripts/cluster/"*.sh "$SERVER:~/primcast/scripts/cluster/"
        
        # Make scripts executable
        ssh "$SERVER" "chmod +x ~/primcast/scripts/cluster/*.sh"
    fi
    
    echo "Done: $SERVER"
done

echo ""
echo "=========================================="
echo "Deployment complete!"
echo "=========================================="
