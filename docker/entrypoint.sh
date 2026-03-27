#!/bin/bash
set -e

exec python -m src.server \
  --node-id "${NODE_ID:-0}" \
  --peers "${PEERS:-}" \
  --port "${LISTEN_PORT:-7000}" \
  --http-port "${HTTP_PORT:-8000}" \
  --data-dir "${DATA_DIR:-/data}" \
  --snapshot-interval "${SNAPSHOT_INTERVAL:-21600}" \
  --anti-entropy-interval "${ANTI_ENTROPY_INTERVAL:-30}" \
  --log-level "${LOG_LEVEL:-INFO}"
