"""
Server entry point for running a KVStore node.

Runs a KVStore node with:
- TCP transport for node-to-node communication
- HTTP API for client access (GET/PUT/DELETE)
- Periodic snapshot scheduler

Usage:
  python -m src.server --node-id 0 --peers host1:7001,host2:7002 --port 7000 --data-dir /data
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import socket
import sys
import threading
import time
from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn
from urllib.parse import urlparse, parse_qs

from .config import Config
from .kvstore import KVStore
from .quorum import QuorumError

logger = logging.getLogger(__name__)


_UI_DIR = os.path.join(os.path.dirname(__file__), "..", "ui")


def _check_peer_alive(host: str, port: int, timeout: float = 0.8) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


class KVHTTPHandler(BaseHTTPRequestHandler):
    """
    Simple HTTP API for client access.

    Endpoints:
      GET  /                    → serve UI dashboard
      GET  /kv?key=<key>        → get value
      PUT  /kv                  → put key-value (JSON body: {"key": "...", "value": "..."})
      DELETE /kv?key=<key>      → delete key
      POST /snapshot            → trigger snapshot
      GET  /health              → health check
      GET  /stats               → node stats
      GET  /keys                → list all non-tombstoned keys
    """

    server: KVHTTPServer  # type hint for the server reference

    def do_OPTIONS(self) -> None:
        self.send_response(204)
        self._cors_headers()
        self.end_headers()

    def do_GET(self) -> None:
        parsed = urlparse(self.path)

        if parsed.path in ("/", "/ui"):
            index = os.path.join(_UI_DIR, "index.html")
            try:
                with open(index, "rb") as f:
                    body = f.read()
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self._cors_headers()
                self.end_headers()
                self.wfile.write(body)
            except FileNotFoundError:
                self._json_response(404, {"error": "UI not found"})
            return

        if parsed.path == "/keys":
            keys = [k.decode(errors="replace") for k in self.server.store.memtable.keys()]
            self._json_response(200, {"keys": sorted(keys)})
            return

        if parsed.path == "/cluster":
            config = self.server.store.config
            peers_status = []
            for peer in config.peers:
                parts = peer.rsplit(":", 1)
                host = parts[0]
                tcp_port = int(parts[1]) if len(parts) > 1 else config.listen_port
                # HTTP port is tcp_port + 1000 by server convention
                inferred_http_port = tcp_port + 1000
                peers_status.append({
                    "address": peer,
                    "host": host,
                    "port": tcp_port,
                    "http_port": inferred_http_port,
                    "alive": _check_peer_alive(host, tcp_port),
                })
            self._json_response(200, {
                "node_id": config.node_id,
                "tcp_port": config.listen_port,
                "http_port": self.server.http_port,
                "peers": peers_status,
                "quorum": {
                    "replication_factor": config.replication_factor,
                    "write_quorum": config.write_quorum,
                    "read_quorum": config.read_quorum,
                },
            })
            return

        if parsed.path == "/quorum":
            config = self.server.store.config
            self._json_response(200, {
                "replication_factor": config.replication_factor,
                "write_quorum": config.write_quorum,
                "read_quorum": config.read_quorum,
            })
            return

        if parsed.path == "/health":
            self._json_response(200, {"status": "ok", "node_id": self.server.store.node_id})
            return

        if parsed.path == "/stats":
            store = self.server.store
            from pathlib import Path
            data_dir = str(Path(store.config.wal_dir).parent.parent)
            self._json_response(200, {
                "node_id": store.node_id,
                "keys": len(store.memtable.keys()),
                "total_entries": len(store.memtable),
                "wal_lsn": store.wal.current_lsn(),
                "data_dir": data_dir,
            })
            return

        if parsed.path == "/kv":
            params = parse_qs(parsed.query)
            key = params.get("key", [None])[0]
            if not key:
                self._json_response(400, {"error": "missing 'key' parameter"})
                return
            try:
                value = self.server.store.get(key.encode())
                if value is None:
                    self._json_response(404, {"key": key, "value": None})
                else:
                    self._json_response(200, {"key": key, "value": value.decode(errors="replace")})
            except QuorumError as e:
                self._json_response(503, {"error": str(e)})
            return

        self._json_response(404, {"error": "not found"})

    def do_PUT(self) -> None:
        if self.path == "/quorum":
            try:
                content_len = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(content_len)
                data = json.loads(body)
                config = self.server.store.config
                w = int(data.get("write_quorum", config.write_quorum))
                r = int(data.get("read_quorum", config.read_quorum))
                if w < 1 or r < 1:
                    self._json_response(400, {"error": "Quorum values must be >= 1"})
                    return
                if r + w <= config.replication_factor:
                    self._json_response(400, {"error": f"R({r}) + W({w}) must be > N({config.replication_factor})"})
                    return
                config.write_quorum = w
                config.read_quorum = r
                self._json_response(200, {"status": "ok", "write_quorum": w, "read_quorum": r})
            except (json.JSONDecodeError, ValueError) as e:
                self._json_response(400, {"error": f"bad request: {e}"})
            return

        if self.path != "/kv":
            self._json_response(404, {"error": "not found"})
            return

        try:
            content_len = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(content_len)
            data = json.loads(body)
            key = data.get("key", "")
            value = data.get("value", "")
            if not key:
                self._json_response(400, {"error": "missing 'key'"})
                return
            self.server.store.put(key.encode(), value.encode())
            self._json_response(200, {"status": "ok", "key": key})
        except QuorumError as e:
            self._json_response(503, {"error": str(e)})
        except (json.JSONDecodeError, KeyError) as e:
            self._json_response(400, {"error": f"bad request: {e}"})

    def do_DELETE(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path != "/kv":
            self._json_response(404, {"error": "not found"})
            return

        params = parse_qs(parsed.query)
        key = params.get("key", [None])[0]
        if not key:
            self._json_response(400, {"error": "missing 'key' parameter"})
            return
        try:
            self.server.store.delete(key.encode())
            self._json_response(200, {"status": "ok", "key": key})
        except QuorumError as e:
            self._json_response(503, {"error": str(e)})

    def do_POST(self) -> None:
        if self.path == "/snapshot":
            self.server.store.create_snapshot()
            self._json_response(200, {"status": "snapshot created"})
            return

        if self.path == "/admin/spawn":
            try:
                content_len = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(content_len)
                data = json.loads(body)
                node_id  = int(data["node_id"])
                tcp_port = int(data["tcp_port"])
                http_port = int(data["http_port"])
                data_dir  = str(data.get("data_dir", "/tmp/kvcluster"))
                peers     = str(data.get("peers", ""))
                log_level = str(data.get("log_level", "WARNING"))
                import subprocess
                repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
                cmd = [
                    sys.executable, "-m", "src",
                    "--node-id",   str(node_id),
                    "--port",      str(tcp_port),
                    "--http-port", str(http_port),
                    "--data-dir",  data_dir,
                    "--log-level", log_level,
                ]
                if peers:
                    cmd += ["--peers", peers]
                subprocess.Popen(cmd, cwd=repo_root,
                                 stdout=subprocess.DEVNULL,
                                 stderr=subprocess.DEVNULL)
                self._json_response(200, {"status": "spawned", "node_id": node_id})
            except (json.JSONDecodeError, KeyError, ValueError) as e:
                self._json_response(400, {"error": f"bad request: {e}"})
            return

        if self.path == "/admin/kill":
            node_id = self.server.store.node_id
            self._json_response(200, {"status": "shutting down", "node_id": node_id})
            # Give the response time to flush, then exit
            def _die():
                import time
                time.sleep(0.15)
                os._exit(0)
            threading.Thread(target=_die, daemon=True).start()
            return

        self._json_response(404, {"error": "not found"})

    def _cors_headers(self) -> None:
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, PUT, DELETE, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")

    def _json_response(self, status: int, data: dict) -> None:
        body = json.dumps(data).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self._cors_headers()
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args) -> None:
        logger.debug("HTTP %s", format % args)


class KVHTTPServer(ThreadingMixIn, HTTPServer):
    """Thread-per-request HTTP server holding a reference to the KVStore."""

    daemon_threads = True  # background threads die with the main thread

    def __init__(self, addr: tuple[str, int], store: KVStore, http_port: int) -> None:
        self.store = store
        self.http_port = http_port
        super().__init__(addr, KVHTTPHandler)


class SnapshotScheduler:
    """Periodic snapshot creation in a background thread."""

    def __init__(self, store: KVStore, interval_seconds: int) -> None:
        self._store = store
        self._interval = interval_seconds
        self._running = False
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self._running = True
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._running = False
        if self._thread:
            self._thread.join(timeout=self._interval + 2)

    def _loop(self) -> None:
        while self._running:
            deadline = time.monotonic() + self._interval
            while self._running and time.monotonic() < deadline:
                time.sleep(1)
            if self._running:
                try:
                    self._store.create_snapshot()
                except Exception:
                    logger.exception("Snapshot failed")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Distributed KV Store Node")
    parser.add_argument("--node-id", type=int, required=True, help="Unique node ID")
    parser.add_argument("--peers", type=str, default="", help="Comma-separated host:port of peer nodes")
    parser.add_argument("--port", type=int, default=7000, help="TCP port for node-to-node communication")
    parser.add_argument("--http-port", type=int, default=0, help="HTTP API port (default: tcp_port + 1000)")
    parser.add_argument("--data-dir", type=str, default="data", help="Data directory")
    parser.add_argument("--snapshot-interval", type=int, default=21600, help="Snapshot interval in seconds")
    parser.add_argument("--anti-entropy-interval", type=int, default=30, help="Anti-entropy interval in seconds")
    parser.add_argument("--log-level", type=str, default="INFO", help="Log level")
    return parser.parse_args(argv)


def run(argv: list[str] | None = None) -> None:
    args = parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(asctime)s [node%(name)s] %(levelname)s %(message)s",
    )

    peers = [p.strip() for p in args.peers.split(",") if p.strip()]
    http_port = args.http_port if args.http_port else args.port + 1000

    config = Config(
        node_id=args.node_id,
        peers=peers,
        listen_port=args.port,
        wal_dir=f"{args.data_dir}/node{args.node_id}/wal",
        snapshot_dir=f"{args.data_dir}/node{args.node_id}/snapshots",
        snapshot_interval_seconds=args.snapshot_interval,
        anti_entropy_interval_seconds=args.anti_entropy_interval,
        replication_factor=len(peers) + 1,
        write_quorum=(len(peers) + 1) // 2 + 1,
        read_quorum=(len(peers) + 1) // 2 + 1,
    )

    store = KVStore(config)
    store.start()

    # HTTP API
    http_server = KVHTTPServer(("0.0.0.0", http_port), store, http_port)
    http_thread = threading.Thread(target=http_server.serve_forever, daemon=True)
    http_thread.start()
    logger.info("HTTP API on port %d", http_port)

    # Snapshot scheduler
    scheduler = SnapshotScheduler(store, args.snapshot_interval)
    scheduler.start()

    # Graceful shutdown
    shutdown_event = threading.Event()

    def signal_handler(sig, frame):
        logger.info("Shutting down...")
        shutdown_event.set()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    print(f"Node {args.node_id} running (tcp={args.port}, http={http_port})")
    print("Press Ctrl+C to stop")

    shutdown_event.wait()

    scheduler.stop()
    http_server.shutdown()
    store.stop()
    print("Stopped.")


if __name__ == "__main__":
    run()
