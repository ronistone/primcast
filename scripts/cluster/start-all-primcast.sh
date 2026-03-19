#!/bin/bash
# =============================================================================
# Start Primcast on all cluster nodes (run from local machine)
# Uses nohup for background execution
# =============================================================================

SERVERS=(
    "ronistone@192.168.2.18:0"
    "ronistone@192.168.2.17:1"
    "ronistone@192.168.2.19:2"
)

echo "Starting Primcast on all nodes..."

for ENTRY in "${SERVERS[@]}"; do
    IFS=':' read -r SERVER PRIMCAST_PID <<< "$ENTRY"
    echo "Starting Primcast on $SERVER (PID: $PRIMCAST_PID)..."
    # Use nohup to run in background, redirect output to log file
    ssh "$SERVER" "cd ~/primcast && export PRIMCAST_PID=$PRIMCAST_PID && nohup ./scripts/cluster/start-primcast.sh > logs/primcast.log 2>&1 &"
done

echo ""
echo "Primcast started on all nodes!"
echo ""
echo "To view logs on a server:"
echo "  ssh ronistone@<IP>"
echo "  tail -f ~/primcast/logs/primcast.log"
echo ""
echo "To stop primcast:"
echo "  ssh ronistone@<IP>"
echo "  pkill -f open_loop"
