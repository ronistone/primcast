#!/bin/bash
# =============================================================================
# Start Primcast on this server
# Usage: ./start-primcast.sh [PRIMCAST_PID]
# If PRIMCAST_PID is not provided, will auto-detect based on server IP
# =============================================================================

set -e

# Get PRIMCAST_PID from parameter or auto-detect
if [ -n "$1" ]; then
    PRIMCAST_PID=$1
elif [ -n "$PRIMCAST_PID" ]; then
    # Use environment variable if set
    PRIMCAST_PID=$PRIMCAST_PID
else
    # Auto-detect based on IP
    SERVER_IP=$(hostname -I | awk '{print $1}')
    case $SERVER_IP in
        192.168.2.18) PRIMCAST_PID=0 ;;
        192.168.2.17) PRIMCAST_PID=1 ;;
        192.168.2.19) PRIMCAST_PID=2 ;;
        *)
            echo "ERROR: Could not auto-detect server. Unknown IP: $SERVER_IP"
            echo "Usage: $0 [PRIMCAST_PID]"
            exit 1
            ;;
    esac
fi

# Default parameters (can be overridden)
GLOBAL_DESTS=${GLOBAL_DESTS:-1}
GLOBALS=${GLOBALS:-1}
MSG_SIZE=${MSG_SIZE:-2}
THREADS=${THREADS:-4}
DEBUG=${DEBUG:-1}

# Find project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

cd "$PROJECT_ROOT"

echo "=========================================="
echo "Starting Primcast"
echo "=========================================="
echo "  PID: $PRIMCAST_PID"
echo "  Config: cluster_config.yaml"
echo "  Threads: $THREADS"
echo "=========================================="

# Check if binary exists
if [ ! -f "./target/release/examples/open_loop" ]; then
    echo "Binary not found! Building..."
    cargo build --release --examples
fi

# Create db directory
mkdir -p db

# Run primcast
export RUST_BACKTRACE=1
exec ./target/release/examples/open_loop \
    --gid 0 \
    --pid $PRIMCAST_PID \
    --cfg cluster_config.yaml \
    --global-dests $GLOBAL_DESTS \
    --globals $GLOBALS \
    -m $MSG_SIZE \
    --threads $THREADS \
    --debug $DEBUG
