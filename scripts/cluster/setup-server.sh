#!/bin/bash
# =============================================================================
# Primcast Cluster - Server Setup Script
# Run this script on each server to prepare the environment
# =============================================================================

set -e

echo "=========================================="
echo "Primcast Cluster - Server Setup"
echo "=========================================="

# Detect which server this is based on IP
SERVER_IP=$(hostname -I | awk '{print $1}')
echo "Detected IP: $SERVER_IP"

case $SERVER_IP in
    192.168.2.18)
        ZOO_MY_ID=1
        PRIMCAST_PID=0
        echo "This is Server 1 (pid=0, zoo_id=1)"
        ;;
    192.168.2.17)
        ZOO_MY_ID=2
        PRIMCAST_PID=1
        echo "This is Server 2 (pid=1, zoo_id=2)"
        ;;
    192.168.2.19)
        ZOO_MY_ID=3
        PRIMCAST_PID=2
        echo "This is Server 3 (pid=2, zoo_id=3)"
        ;;
    *)
        echo "ERROR: Unknown server IP: $SERVER_IP"
        echo "Expected one of: 192.168.2.17, 192.168.2.18, 192.168.2.19"
        exit 1
        ;;
esac

echo "Server configuration detected: ZOO_MY_ID=$ZOO_MY_ID, PRIMCAST_PID=$PRIMCAST_PID"

# =============================================================================
# Step 1: Install Dependencies
# =============================================================================
echo ""
echo "Step 1: Installing system dependencies..."

# Check if running as root or with sudo
if [ "$EUID" -ne 0 ]; then
    SUDO="sudo"
else
    SUDO=""
fi

# Update package list
$SUDO apt-get update

# Install required packages
$SUDO apt-get install -y \
    build-essential \
    curl \
    git \
    docker.io \
    docker-compose \
    pkg-config \
    libssl-dev \
    liblmdb-dev \
    clang \
    libclang-dev

# Add current user to docker group (if not root)
if [ "$EUID" -ne 0 ]; then
    $SUDO usermod -aG docker $USER
    echo "NOTE: You may need to log out and back in for docker group membership to take effect"
fi

# =============================================================================
# Step 2: Install Rust
# =============================================================================
echo ""
echo "Step 2: Installing Rust..."

if command -v rustc &> /dev/null; then
    echo "Rust is already installed: $(rustc --version)"
else
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
    source $HOME/.cargo/env
fi

# =============================================================================
# Step 3: Create directories
# =============================================================================
echo ""
echo "Step 3: Creating directories..."

mkdir -p ~/primcast/db
mkdir -p ~/primcast/logs

echo ""
echo "=========================================="
echo "Setup complete!"
echo "=========================================="
echo ""
echo "Next steps:"
echo "  1. Copy the primcast project to ~/primcast/"
echo "  2. Run: cd ~/primcast && cargo build --release"
echo "  3. Start ZooKeeper: ./scripts/cluster/start-zookeeper.sh"
echo "  4. Start Primcast: ./scripts/cluster/start-primcast.sh"
echo ""
