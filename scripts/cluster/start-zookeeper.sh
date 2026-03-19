#!/bin/bash
# =============================================================================
# Start ZooKeeper on this server
# Usage: ./start-zookeeper.sh [ZOO_MY_ID]
# If ZOO_MY_ID is not provided, will auto-detect based on server IP
# =============================================================================

set -e

# Get ZOO_MY_ID from parameter or auto-detect
if [ -n "$1" ]; then
    ZOO_MY_ID=$1
elif [ -n "$ZOO_MY_ID" ]; then
    # Use environment variable if set
    ZOO_MY_ID=$ZOO_MY_ID
else
    # Auto-detect based on IP
    SERVER_IP=$(hostname -I | awk '{print $1}')
    case $SERVER_IP in
        192.168.2.18) ZOO_MY_ID=1 ;;
        192.168.2.17) ZOO_MY_ID=2 ;;
        192.168.2.19) ZOO_MY_ID=3 ;;
        *)
            echo "ERROR: Could not auto-detect server. Unknown IP: $SERVER_IP"
            echo "Usage: $0 [ZOO_MY_ID]"
            exit 1
            ;;
    esac
fi

echo "Starting ZooKeeper with ID: $ZOO_MY_ID"

# Build ZOO_SERVERS string with 0.0.0.0 for local server
case $ZOO_MY_ID in
    1)
        ZOO_SERVERS="server.1=192.168.2.18:2888:3888;2181 server.2=192.168.2.17:2888:3888;2181 server.3=192.168.2.19:2888:3888;2181"
        ;;
    2)
        ZOO_SERVERS="server.1=192.168.2.18:2888:3888;2181 server.2=192.168.2.17:2888:3888;2181 server.3=192.168.2.19:2888:3888;2181"
        ;;
    3)
        ZOO_SERVERS="server.1=192.168.2.18:2888:3888;2181 server.2=192.168.2.17:2888:3888;2181 server.3=192.168.2.19:2888:3888;2181"
        ;;
    *)
        echo "ERROR: Invalid ZOO_MY_ID: $ZOO_MY_ID. Must be 1, 2, or 3."
        exit 1
        ;;
esac

# Stop existing container if running
docker rm -f zookeeper 2>/dev/null || true

# Remove old volumes to ensure clean start
echo "Cleaning old ZooKeeper data..."
docker volume rm zookeeper_data 2>/dev/null || true
docker volume rm zookeeper_datalog 2>/dev/null || true

# Run ZooKeeper container
echo "Starting ZooKeeper container..."
docker run -d \
    --name zookeeper \
    --restart unless-stopped \
    --network host \
    -e ZOO_MY_ID=$ZOO_MY_ID \
    -e ZOO_SERVERS="$ZOO_SERVERS" \
    -e ZOO_TICK_TIME=2000 \
    -e ZOO_INIT_LIMIT=10 \
    -e ZOO_SYNC_LIMIT=5 \
    -p 2181:2181 \
    -p 2888:2888 \
    -p 3888:3888 \
    -v zookeeper_data:/data \
    -v zookeeper_datalog:/datalog \
    zookeeper:3.9

sleep 2
echo ""
echo "ZooKeeper started!"
echo "Configuration: $ZOO_SERVERS"
echo ""
echo "Check status: docker logs -f zookeeper"
