# How to Start

## Prerequisites

- Python 3.12 (via Homebrew on macOS)

---

## Step 1 — Create the virtual environment

Run this once. It creates a venv at `/tmp/kv_venv` and installs dependencies.

```bash
/opt/homebrew/opt/python@3.12/bin/python3.12 -m venv /tmp/kv_venv
/tmp/kv_venv/bin/pip install pytest pytest-asyncio msgpack
```

---

## Step 2 — Start the cluster

```bash
# 3-node cluster (default)
/tmp/kv_venv/bin/python3 scripts/start_cluster.py --nodes 3

# 5-node cluster
/tmp/kv_venv/bin/python3 scripts/start_cluster.py --nodes 5
```

Press `Ctrl+C` to stop all nodes.

Port layout:

| Node | TCP  | HTTP / Dashboard      |
|------|------|-----------------------|
| 0    | 7100 | http://localhost:8100 |
| 1    | 7101 | http://localhost:8101 |
| 2    | 7102 | http://localhost:8102 |

---

## Step 3 — Open the dashboard

```
http://localhost:8100
```

### Dashboard pages

| Page        | URL hash   | What it does                                          |
|-------------|------------|-------------------------------------------------------|
| Dashboard   | `#`        | Stats, cluster status, quorum settings, stress test   |
| Write Flow  | `#write`   | Animated walkthrough of a PUT operation               |
| Read Flow   | `#read`    | Animated walkthrough of a GET operation               |
| Get Key     | `#get`     | Browse, search, and inspect keys                      |
| Add Key     | `#add`     | Write a new key or edit an existing one               |
| Control     | `#control` | Kill/restart nodes, unified GET/PUT, activity log     |

---

## Step 4 — Run the tests (optional)

```bash
/tmp/kv_venv/bin/python3 -m pytest tests/ -v
```

---

## Cluster Control page (`#control`)

The Control page lets you interact with the cluster and prove fault tolerance live.

### Kill a node

Click **✕ Kill Node** on any node card. The process exits immediately. The card turns red and the cluster banner updates to show how many nodes are still online.

Writes and reads keep working as long as the surviving nodes meet quorum (need 2 of 3 for a default 3-node cluster).

### Restart a node

Click **▶ Restart Node** on a dead node card. Any alive node spawns the process for you — no terminal needed. The card polls every second and flips back to green once the node is up (usually within 2–3 seconds).

Data written while the node was down is recovered automatically:
- The node reloads its last snapshot from disk
- It replays any WAL entries written after that snapshot
- Anti-entropy syncs any remaining delta from peers within 30 seconds

### Run operations

Use the **Operation** panel to execute GET / PUT / DELETE against any node you choose. The **Node** dropdown only shows online nodes and updates automatically as nodes go up or down.

Every operation is recorded in the **Activity Log** with timestamp, operation type, key, target node, and result.

### Quorum settings

The cluster banner shows current N / W / R values and a health alert:

| State                    | Banner colour |
|--------------------------|---------------|
| All nodes online         | Green         |
| Some nodes down, quorum met | Orange     |
| Below quorum             | Red           |

You can also change W and R live from the **Dashboard → Quorum Settings** panel.

---

## Docker (backend + frontend)

**Tech stack:**
- **Backend** — Python 3.12 nodes running in Docker containers, one container per node, persisted via named volumes
- **Frontend** — nginx Alpine container serving the static `ui/index.html` dashboard

### Start with Docker Compose

```bash
cd docker
docker compose up --build
```

| Service  | Container          | Exposed at            | Role                        |
|----------|--------------------|-----------------------|-----------------------------|
| node0    | kvstore-node0      | TCP 7000 / HTTP 8000  | KV node 0                   |
| node1    | kvstore-node1      | TCP 7001 / HTTP 8001  | KV node 1                   |
| node2    | kvstore-node2      | TCP 7002 / HTTP 8002  | KV node 2                   |
| frontend | kvstore-frontend   | http://localhost      | nginx serving the dashboard |

### Open the dashboard

```
http://localhost
```

Set the **Node URL** in the header to `http://localhost:8000` (or 8001 / 8002) to connect to a specific node.

You can also access each node's built-in dashboard directly:
- http://localhost:8000
- http://localhost:8001
- http://localhost:8002

### Stop the cluster

```bash
docker compose down
```

To also delete persisted data:

```bash
docker compose down -v
```

### Node environment variables

Each node container is configured via environment variables (set in `docker-compose.yml`):

| Variable                 | Default | Description                        |
|--------------------------|---------|------------------------------------|
| `NODE_ID`                | 0       | Unique node ID                     |
| `PEERS`                  | —       | Comma-separated `host:port` peers  |
| `LISTEN_PORT`            | 7000    | TCP port for node-to-node comms    |
| `HTTP_PORT`              | 8000    | HTTP API port                      |
| `DATA_DIR`               | /data   | Data directory (mounted as volume) |
| `SNAPSHOT_INTERVAL`      | 21600   | Seconds between auto-snapshots     |
| `ANTI_ENTROPY_INTERVAL`  | 10      | Seconds between Merkle-tree syncs  |
| `LOG_LEVEL`              | INFO    | DEBUG / INFO / WARNING / ERROR     |

### Docker file layout

```
docker/
  Dockerfile          # Python 3.12-slim image, copies src/ and ui/
  docker-compose.yml  # 3 KV nodes + nginx frontend
  entrypoint.sh       # Maps env vars to CLI flags, starts the node
  nginx.conf          # Serves ui/index.html at /
```

---

## Single-node mode (no cluster)

```bash
/tmp/kv_venv/bin/python3 -m src \
  --node-id 0 --port 7100 --http-port 8100 --data-dir /tmp/kvdata
```

---

## Quick API reference

```bash
# Write a key
curl -X PUT http://localhost:8100/kv \
  -H 'Content-Type: application/json' \
  -d '{"key":"hello","value":"world"}'

# Read a key
curl "http://localhost:8100/kv?key=hello"

# Delete a key
curl -X DELETE "http://localhost:8100/kv?key=hello"

# List all keys
curl http://localhost:8100/keys

# Node health
curl http://localhost:8100/health

# Node stats (keys, WAL LSN, data directory)
curl http://localhost:8100/stats

# Cluster status + quorum info
curl http://localhost:8100/cluster

# Get current quorum settings
curl http://localhost:8100/quorum

# Change quorum settings live
curl -X PUT http://localhost:8100/quorum \
  -H 'Content-Type: application/json' \
  -d '{"write_quorum":2,"read_quorum":2}'

# Trigger snapshot + compact WAL
curl -X POST http://localhost:8100/snapshot

# Kill a node (process exits after response)
curl -X POST http://localhost:8100/admin/kill

# Spawn a dead node (relay through any alive node)
curl -X POST http://localhost:8100/admin/spawn \
  -H 'Content-Type: application/json' \
  -d '{"node_id":2,"tcp_port":7102,"http_port":8102,"data_dir":"/tmp/kvcluster","peers":"localhost:7100,localhost:7101"}'
```

---

## Cluster startup flags

| Flag                      | Default          | Description                        |
|---------------------------|------------------|------------------------------------|
| `--nodes`                 | 3                | Number of nodes                    |
| `--base-tcp-port`         | 7100             | TCP port for node 0                |
| `--base-http-port`        | 8100             | HTTP port for node 0               |
| `--data-dir`              | /tmp/kvcluster   | Data directory                     |
| `--log-level`             | WARNING          | DEBUG / INFO / WARNING / ERROR     |
| `--snapshot-interval`     | 3600             | Seconds between auto-snapshots     |
| `--anti-entropy-interval` | 30               | Seconds between Merkle-tree syncs  |
