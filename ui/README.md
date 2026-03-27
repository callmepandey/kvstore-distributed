# KV Store — Web UI

A single-page dashboard for interacting with a running KV Store node.

## How It Works

The UI is served directly by the KV Store HTTP server at `GET /`.
It talks to the same HTTP API endpoints the CLI uses — no separate server needed.

## Access

Start a node, then open the dashboard in your browser:

```
http://localhost:<http-port>/
```

For example, with the default setup:

```bash
/tmp/kv_venv/bin/python -m src --node-id 0 --port 7100 --http-port 7101 --data-dir /tmp/kvdata
```

Then visit: **http://localhost:7101**

## Features

| Feature | Description |
|---|---|
| Live stats | Node status, key count, WAL LSN — auto-refreshes every 5s |
| Key browser | Lists all live keys with a value preview |
| Search / filter | Filter keys in real time |
| View value | Click any key to see its full value |
| Add key | `+ Add Key` button opens a modal |
| Edit key | Click **Edit** on any row to update the value |
| Delete key | Click **Delete** on any row (with confirmation) |
| Keyboard shortcuts | `Esc` closes modals · `Cmd/Ctrl + Enter` saves |

## API Endpoints Used

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/health` | Node status and ID |
| `GET` | `/stats` | Key count and WAL LSN |
| `GET` | `/keys` | List all live keys |
| `GET` | `/kv?key=<k>` | Read a value |
| `PUT` | `/kv` | Write a key-value pair |
| `DELETE` | `/kv?key=<k>` | Delete a key |

## Files

```
ui/
└── index.html    ← single-file dashboard (no build step, no dependencies)
```
