# Primcast Cluster Deployment Guide

## Cluster Architecture

| Server | IP Address | ZooKeeper ID | Primcast PID | SSH Command |
|--------|------------|--------------|--------------|-------------|
| Server 1 | 192.168.2.18 | 1 | 0 | `ssh ronistone@192.168.2.18` |
| Server 2 | 192.168.2.17 | 2 | 1 | `ssh ronistone@192.168.2.17` |
| Server 3 | 192.168.2.19 | 3 | 2 | `ssh ronistone@192.168.2.19` |

## Quick Start

### From Your Local Development Machine

```bash
# 1. Deploy code to all servers
./scripts/cluster/deploy-to-cluster.sh

# 2. Start ZooKeeper on all nodes
./scripts/cluster/start-all-zookeeper.sh

# 3. Wait 30 seconds for ZooKeeper cluster to stabilize

# 4. Start Primcast on all nodes
./scripts/cluster/start-all-primcast.sh

# 5. Check cluster status
./scripts/cluster/check-cluster-status.sh
```

### First-Time Setup (on each server)

SSH into each server and run:

```bash
cd ~/primcast
./scripts/cluster/setup-server.sh
```

This installs:
- Docker and docker-compose
- Rust toolchain
- Build dependencies (libssl, liblmdb, clang)
- Creates required directories

## Detailed Steps

### Step 1: Initial Deployment

From your local machine, run:

```bash
./scripts/cluster/deploy-to-cluster.sh
```

This will:
- Build the release binary locally
- Copy binaries to all servers
- Copy configuration and scripts

### Step 2: Setup Each Server (First Time Only)

SSH into each server:

```bash
ssh ronistone@192.168.2.18  # Server 1
# Then run:
cd ~/primcast
./scripts/cluster/setup-server.sh
```

Repeat for all three servers.

### Step 3: Start ZooKeeper Cluster

From your local machine:

```bash
./scripts/cluster/start-all-zookeeper.sh
```

Or manually on each server:

```bash
./scripts/cluster/start-zookeeper.sh
```

**Important:** Wait ~30 seconds for ZooKeeper cluster to elect a leader.

Verify ZooKeeper status:

```bash
docker logs zookeeper
```

### Step 4: Start Primcast

From your local machine:

```bash
./scripts/cluster/start-all-primcast.sh
```

Or manually on each server:

```bash
./scripts/cluster/start-primcast.sh
```

### Step 5: Monitor

Check cluster status:

```bash
./scripts/cluster/check-cluster-status.sh
```

View logs on a specific server:

```bash
ssh ronistone@192.168.2.18
tmux attach -t primcast
# Ctrl+B, D to detach
```

## Stopping the Cluster

From your local machine:

```bash
./scripts/cluster/stop-all.sh
```

## Configuration Files

- `cluster_config.yaml` - Main cluster configuration with all server IPs
- `scripts/cluster/docker-compose-zk.yaml` - ZooKeeper Docker configuration

## Troubleshooting

### ZooKeeper Won't Start

1. Check if ports are in use:
   ```bash
   sudo netstat -tlnp | grep -E '2181|2888|3888'
   ```

2. Check Docker logs:
   ```bash
   docker logs zookeeper
   ```

3. Ensure all servers can reach each other on ports 2181, 2888, 3888

### Primcast Won't Connect

1. Verify ZooKeeper is running on all nodes
2. Check firewall rules for ports 10000-10002 and 20000-20002
3. Verify `cluster_config.yaml` has correct IPs

### Network Connectivity

Test connectivity between servers:

```bash
# From Server 1
ping 192.168.2.17
ping 192.168.2.19

# Test ZooKeeper port
nc -zv 192.168.2.17 2181
```

## Customizing Primcast Options

Edit `~/.primcast_env` on each server or pass environment variables:

```bash
THREADS=8 GLOBALS=2 ./scripts/cluster/start-primcast.sh
```

Available options:
- `THREADS` - Number of threads (default: 4)
- `GLOBALS` - Number of global operations (default: 1)
- `GLOBAL_DESTS` - Global destinations (default: 1)
- `MSG_SIZE` - Message size (default: 2)
- `DEBUG` - Debug level (default: 1)
