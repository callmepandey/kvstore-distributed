# KeyValue Store

A distributed key-value store with quorum-based replication, WAL, snapshots, and anti-entropy.

---

## Quick Start

### 1 — Create the Python 3.12 virtual environment (once)

```bash
/opt/homebrew/opt/python@3.12/bin/python3.12 -m venv /tmp/kv_venv
/tmp/kv_venv/bin/pip install pytest pytest-asyncio msgpack
```

### 2 — Run the cluster

```bash
# 3-node cluster (default)
/tmp/kv_venv/bin/python3 scripts/start_cluster.py --nodes 3

# 5-node cluster
/tmp/kv_venv/bin/python3 scripts/start_cluster.py --nodes 5

# Custom ports
/tmp/kv_venv/bin/python3 scripts/start_cluster.py --nodes 3 \
  --base-tcp-port 7100 --base-http-port 8100
```

Port layout (defaults):

| Node | TCP  | HTTP / Dashboard         |
|------|------|--------------------------|
| 0    | 7100 | http://localhost:8100    |
| 1    | 7101 | http://localhost:8101    |
| 2    | 7102 | http://localhost:8102    |
| N    | 7100+N | http://localhost:8100+N |

### 3 — Open the dashboard

```
http://localhost:8100
```

The UI lets you browse keys, add/edit/delete values, watch live stats, and see an animated write/read flow diagram.

---

## Run Tests

```bash
/tmp/kv_venv/bin/python3 -m pytest tests/ -v
```

---

## Run a Single Node

```bash
/tmp/kv_venv/bin/python3 -m src \
  --node-id 0 --port 7100 --http-port 8100 --data-dir /tmp/kvdata
```

---

## HTTP API

```bash
# Write
curl -X PUT http://localhost:8100/kv \
  -H 'Content-Type: application/json' \
  -d '{"key":"foo","value":"bar"}'

# Read
curl "http://localhost:8100/kv?key=foo"

# Delete
curl -X DELETE "http://localhost:8100/kv?key=foo"

# Health check
curl http://localhost:8100/health

# Node stats
curl http://localhost:8100/stats

# List all keys
curl http://localhost:8100/keys

# Cluster status
curl http://localhost:8100/cluster
```

---

## Cluster Options

| Flag | Default | Description |
|------|---------|-------------|
| `--nodes` | 3 | Number of nodes |
| `--base-tcp-port` | 7100 | TCP port for node 0 |
| `--base-http-port` | 8100 | HTTP port for node 0 |
| `--data-dir` | /tmp/kvcluster | Data directory |
| `--log-level` | WARNING | DEBUG / INFO / WARNING / ERROR |
| `--snapshot-interval` | 3600 | Snapshot interval in seconds |
| `--anti-entropy-interval` | 30 | Anti-entropy sync interval in seconds |

---

## Architecture

| Component | Role |
|-----------|------|
| **WAL** | Write-Ahead Log — every write is fsynced to disk first for crash safety |
| **MemTable** | In-memory key-value store with HLC timestamps for LWW conflict resolution |
| **HLC** | Hybrid Logical Clock — globally ordered timestamps without clock sync |
| **Quorum** | R + W > N guarantees: reads always see the latest write |
| **Anti-entropy** | Merkle-tree-based background repair — eventually convergent |
| **TCP transport** | Selector-based node-to-node messaging |

Quorum values for N nodes: **W = N/2 + 1, R = N/2 + 1**

| N nodes | W (write) | R (read) | Tolerates |
|---------|-----------|----------|-----------|
| 1 | 1 | 1 | 0 failures |
| 3 | 2 | 2 | 1 failure  |
| 5 | 3 | 3 | 2 failures |
