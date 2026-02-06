#!/bin/bash
# =============================================================================
# Stop ZooKeeper on this server
# =============================================================================

docker rm -f zookeeper 2>/dev/null || true

echo "ZooKeeper stopped!"
