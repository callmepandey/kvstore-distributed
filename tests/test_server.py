"""Tests for the HTTP server and CLI entry point."""

import json
import socket
import tempfile
import threading
import time
import urllib.request
import urllib.error

from src.config import Config
from src.kvstore import KVStore
from src.server import KVHTTPServer, SnapshotScheduler, parse_args


def get_free_port() -> int:
    with socket.socket() as s:
        s.bind(("", 0))
        return s.getsockname()[1]


def make_single_node(tmpdir: str) -> tuple[KVStore, int, int]:
    """Create a single-node KVStore + HTTP server. Returns (store, tcp_port, http_port)."""
    tcp_port = get_free_port()
    http_port = get_free_port()
    cfg = Config(
        node_id=0,
        peers=[],
        listen_port=tcp_port,
        wal_dir=f"{tmpdir}/wal",
        snapshot_dir=f"{tmpdir}/snapshots",
        wal_fsync=False,
        replication_factor=1,
        write_quorum=1,
        read_quorum=1,
    )
    store = KVStore(cfg)
    store.start()
    return store, tcp_port, http_port


def http_get(port: int, path: str) -> tuple[int, dict]:
    url = f"http://127.0.0.1:{port}{path}"
    try:
        with urllib.request.urlopen(url, timeout=5) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())


def http_put(port: int, path: str, data: dict) -> tuple[int, dict]:
    url = f"http://127.0.0.1:{port}{path}"
    body = json.dumps(data).encode()
    req = urllib.request.Request(url, data=body, method="PUT")
    req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())


def http_delete(port: int, path: str) -> tuple[int, dict]:
    url = f"http://127.0.0.1:{port}{path}"
    req = urllib.request.Request(url, method="DELETE")
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())


def http_post(port: int, path: str) -> tuple[int, dict]:
    url = f"http://127.0.0.1:{port}{path}"
    req = urllib.request.Request(url, data=b"", method="POST")
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())


class TestHTTPAPI:
    def _setup(self, tmpdir):
        store, tcp_port, http_port = make_single_node(tmpdir)
        server = KVHTTPServer(("127.0.0.1", http_port), store, http_port)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        time.sleep(0.2)
        return store, server, http_port

    def test_health(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store, server, port = self._setup(tmpdir)
            try:
                status, data = http_get(port, "/health")
                assert status == 200
                assert data["status"] == "ok"
                assert data["node_id"] == 0
            finally:
                server.shutdown()
                store.stop()

    def test_put_and_get(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store, server, port = self._setup(tmpdir)
            try:
                status, data = http_put(port, "/kv", {"key": "hello", "value": "world"})
                assert status == 200
                assert data["status"] == "ok"

                status, data = http_get(port, "/kv?key=hello")
                assert status == 200
                assert data["value"] == "world"
            finally:
                server.shutdown()
                store.stop()

    def test_get_missing_key(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store, server, port = self._setup(tmpdir)
            try:
                status, data = http_get(port, "/kv?key=nonexistent")
                assert status == 404
                assert data["value"] is None
            finally:
                server.shutdown()
                store.stop()

    def test_delete(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store, server, port = self._setup(tmpdir)
            try:
                http_put(port, "/kv", {"key": "todelete", "value": "val"})
                status, data = http_delete(port, "/kv?key=todelete")
                assert status == 200

                status, data = http_get(port, "/kv?key=todelete")
                assert status == 404
            finally:
                server.shutdown()
                store.stop()

    def test_get_missing_param(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store, server, port = self._setup(tmpdir)
            try:
                status, data = http_get(port, "/kv")
                assert status == 400
                assert "missing" in data["error"]
            finally:
                server.shutdown()
                store.stop()

    def test_put_bad_json(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store, server, port = self._setup(tmpdir)
            try:
                url = f"http://127.0.0.1:{port}/kv"
                req = urllib.request.Request(url, data=b"not json", method="PUT")
                req.add_header("Content-Type", "application/json")
                try:
                    urllib.request.urlopen(req, timeout=5)
                except urllib.error.HTTPError as e:
                    assert e.code == 400
            finally:
                server.shutdown()
                store.stop()

    def test_snapshot_endpoint(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store, server, port = self._setup(tmpdir)
            try:
                http_put(port, "/kv", {"key": "snap_key", "value": "snap_val"})
                status, data = http_post(port, "/snapshot")
                assert status == 200
                assert "snapshot" in data["status"]
            finally:
                server.shutdown()
                store.stop()

    def test_stats(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store, server, port = self._setup(tmpdir)
            try:
                http_put(port, "/kv", {"key": "a", "value": "1"})
                http_put(port, "/kv", {"key": "b", "value": "2"})

                status, data = http_get(port, "/stats")
                assert status == 200
                assert data["keys"] == 2
                assert data["node_id"] == 0
                assert data["wal_lsn"] >= 2
            finally:
                server.shutdown()
                store.stop()

    def test_404_on_unknown_path(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store, server, port = self._setup(tmpdir)
            try:
                status, data = http_get(port, "/unknown")
                assert status == 404
            finally:
                server.shutdown()
                store.stop()


class TestParseArgs:
    def test_required_args(self):
        args = parse_args(["--node-id", "0"])
        assert args.node_id == 0
        assert args.port == 7000
        assert args.peers == ""

    def test_all_args(self):
        args = parse_args([
            "--node-id", "2",
            "--peers", "host1:7000,host2:7001",
            "--port", "7002",
            "--http-port", "8002",
            "--data-dir", "/tmp/kv",
            "--snapshot-interval", "3600",
            "--anti-entropy-interval", "10",
            "--log-level", "DEBUG",
        ])
        assert args.node_id == 2
        assert args.peers == "host1:7000,host2:7001"
        assert args.port == 7002
        assert args.http_port == 8002
        assert args.data_dir == "/tmp/kv"
        assert args.snapshot_interval == 3600
        assert args.anti_entropy_interval == 10
        assert args.log_level == "DEBUG"


class TestSnapshotScheduler:
    def test_scheduler_creates_snapshot(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cfg = Config(
                node_id=0, peers=[],
                listen_port=get_free_port(),
                wal_dir=f"{tmpdir}/wal",
                snapshot_dir=f"{tmpdir}/snapshots",
                wal_fsync=False,
                replication_factor=1, write_quorum=1, read_quorum=1,
            )
            store = KVStore(cfg)
            store.start()
            try:
                store.put(b"sched", b"test")

                # Very short interval
                scheduler = SnapshotScheduler(store, interval_seconds=1)
                scheduler.start()
                time.sleep(2.5)
                scheduler.stop()

                # Check that a snapshot was created
                meta, entries = store.snapshot_mgr.load_latest()
                assert meta is not None
                assert meta.entry_count >= 1
            finally:
                store.stop()
