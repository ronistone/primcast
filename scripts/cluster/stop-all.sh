#!/bin/bash
# =============================================================================
# Stop all services on all cluster nodes (run from local machine)
# =============================================================================

SERVERS=(
    "ronistone@192.168.2.18"
    "ronistone@192.168.2.17"
    "ronistone@192.168.2.19"
)

echo "Stopping all services on all nodes..."

for SERVER in "${SERVERS[@]}"; do
    echo ""
    echo "Stopping on $SERVER..."
    
    # Kill primcast process
    ssh "$SERVER" "pkill -f open_loop" 2>/dev/null || true
    
    # Stop ZooKeeper
    ssh "$SERVER" "cd ~/primcast && ./scripts/cluster/stop-zookeeper.sh" 2>/dev/null || true
done

echo ""
echo "All services stopped!"
