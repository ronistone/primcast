#!/bin/bash
# =============================================================================
# Start ZooKeeper on all cluster nodes (run from local machine)
# =============================================================================

SERVERS=(
    "ronistone@192.168.2.18:1"
    "ronistone@192.168.2.17:2"
    "ronistone@192.168.2.19:3"
)

echo "Starting ZooKeeper on all nodes..."

for ENTRY in "${SERVERS[@]}"; do
    IFS=':' read -r SERVER ZOO_MY_ID <<< "$ENTRY"
    echo "Starting ZooKeeper on $SERVER (ID: $ZOO_MY_ID)..."
    ssh "$SERVER" "cd ~/primcast && export ZOO_MY_ID=$ZOO_MY_ID && ./scripts/cluster/start-zookeeper.sh" &
done

wait

echo ""
echo "ZooKeeper started on all nodes!"
echo "Wait ~30 seconds for cluster to elect leader, then run start-all-primcast.sh"
