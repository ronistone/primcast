#!/bin/bash
# =============================================================================
# Check status of all cluster nodes
# Run this from your local machine
# =============================================================================

SERVERS=(
    "ronistone@192.168.2.18:Server1:pid0"
    "ronistone@192.168.2.17:Server2:pid1"
    "ronistone@192.168.2.19:Server3:pid2"
)

echo "=========================================="
echo "Cluster Status Check"
echo "=========================================="

for ENTRY in "${SERVERS[@]}"; do
    IFS=':' read -r SERVER NAME PID <<< "$ENTRY"
    
    echo ""
    echo "--- $NAME ($SERVER) - $PID ---"
    
    # Check SSH connectivity
    if ! ssh -o ConnectTimeout=5 "$SERVER" "echo 'SSH: OK'" 2>/dev/null; then
        echo "  SSH: UNREACHABLE"
        continue
    fi
    
    # Check ZooKeeper
    ZK_STATUS=$(ssh "$SERVER" "docker ps --filter name=zookeeper --format '{{.Status}}'" 2>/dev/null)
    if [ -n "$ZK_STATUS" ]; then
        echo "  ZooKeeper: $ZK_STATUS"
    else
        echo "  ZooKeeper: NOT RUNNING"
    fi
    
    # Check Primcast process (use ps to avoid pgrep matching itself)
    PC_PID=$(ssh "$SERVER" "ps aux | grep '[o]pen_loop' | grep -v grep | awk '{print \$2}'" 2>/dev/null)
    if [ -z "$PC_PID" ]; then
        echo "  Primcast: NOT RUNNING"
    else
        PC_COUNT=$(echo "$PC_PID" | wc -l)
        echo "  Primcast: RUNNING (PID: $PC_PID, Count: $PC_COUNT)"
    fi
done

echo ""
echo "=========================================="
