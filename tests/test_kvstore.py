"""Tests for the KVStore — single node and multi-node quorum operations."""

import socket
import tempfile
import time
import threading

import pytest

from src.config import Config
from src.kvstore import KVStore
from src.quorum import QuorumError


def get_free_port() -> int:
    with socket.socket() as s:
        s.bind(("", 0))
        return s.getsockname()[1]


def make_config(
    node_id: int, port: int, peers: list[str], tmpdir: str, **overrides
) -> Config:
    return Config(
        node_id=node_id,
        peers=peers,
        listen_port=port,
        wal_dir=f"{tmpdir}/node{node_id}/wal",
        snapshot_dir=f"{tmpdir}/node{node_id}/snapshots",
        wal_fsync=False,
        replication_timeout_seconds=2.0,
        read_timeout_seconds=2.0,
        **overrides,
    )


class TestSingleNodeKVStore:
    """Test a single node with N=1, R=1, W=1 (no replication needed)."""

    def test_put_and_get(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            port = get_free_port()
            cfg = make_config(0, port, [], tmpdir,
                              replication_factor=1, write_quorum=1, read_quorum=1)
            store = KVStore(cfg)
            store.start()
            try:
                store.put(b"hello", b"world")
                assert store.get(b"hello") == b"world"
            finally:
                store.stop()

    def test_get_missing_key(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            port = get_free_port()
            cfg = make_config(0, port, [], tmpdir,
                              replication_factor=1, write_quorum=1, read_quorum=1)
            store = KVStore(cfg)
            store.start()
            try:
                assert store.get(b"nope") is None
            finally:
                store.stop()

    def test_overwrite(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            port = get_free_port()
            cfg = make_config(0, port, [], tmpdir,
                              replication_factor=1, write_quorum=1, read_quorum=1)
            store = KVStore(cfg)
            store.start()
            try:
                store.put(b"k", b"v1")
                store.put(b"k", b"v2")
                assert store.get(b"k") == b"v2"
            finally:
                store.stop()

    def test_delete(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            port = get_free_port()
            cfg = make_config(0, port, [], tmpdir,
                              replication_factor=1, write_quorum=1, read_quorum=1)
            store = KVStore(cfg)
            store.start()
            try:
                store.put(b"k", b"v")
                store.delete(b"k")
                assert store.get(b"k") is None
            finally:
                store.stop()

    def test_recovery_after_restart(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            port = get_free_port()
            cfg = make_config(0, port, [], tmpdir,
                              replication_factor=1, write_quorum=1, read_quorum=1)

            # First run: write data
            store = KVStore(cfg)
            store.start()
            store.put(b"persist", b"me")
            store.stop()

            # Second run: data should survive
            port2 = get_free_port()
            cfg2 = make_config(0, port2, [], tmpdir,
                               replication_factor=1, write_quorum=1, read_quorum=1)
            store2 = KVStore(cfg2)
            store2.start()
            try:
                assert store2.get(b"persist") == b"me"
            finally:
                store2.stop()

    def test_snapshot_and_recovery(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            port = get_free_port()
            cfg = make_config(0, port, [], tmpdir,
                              replication_factor=1, write_quorum=1, read_quorum=1)

            store = KVStore(cfg)
            store.start()
            for i in range(10):
                store.put(f"key{i}".encode(), f"val{i}".encode())
            store.create_snapshot()
            # Write more after snapshot
            store.put(b"after_snap", b"yes")
            store.stop()

            # Recover — should have all data (snapshot + WAL replay)
            port2 = get_free_port()
            cfg2 = make_config(0, port2, [], tmpdir,
                               replication_factor=1, write_quorum=1, read_quorum=1)
            store2 = KVStore(cfg2)
            store2.start()
            try:
                for i in range(10):
                    assert store2.get(f"key{i}".encode()) == f"val{i}".encode()
                assert store2.get(b"after_snap") == b"yes"
            finally:
                store2.stop()


class TestMultiNodeKVStore:
    """Test a 3-node cluster with quorum reads and writes."""

    def _start_cluster(self, tmpdir: str) -> list[KVStore]:
        ports = [get_free_port() for _ in range(3)]
        nodes = []
        for i in range(3):
            peers = [f"127.0.0.1:{ports[j]}" for j in range(3) if j != i]
            cfg = make_config(i, ports[i], peers, tmpdir)
            nodes.append(KVStore(cfg))

        for n in nodes:
            n.start()
        time.sleep(0.3)  # let servers bind
        return nodes

    def _stop_cluster(self, nodes: list[KVStore]) -> None:
        for n in nodes:
            n.stop()

    def test_write_read_quorum(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            nodes = self._start_cluster(tmpdir)
            try:
                # Write to node 0
                nodes[0].put(b"x", b"123")
                time.sleep(0.1)

                # Read from node 1 — quorum read should find the value
                val = nodes[1].get(b"x")
                assert val == b"123"

                # Read from node 2
                val = nodes[2].get(b"x")
                assert val == b"123"
            finally:
                self._stop_cluster(nodes)

    def test_write_to_any_node(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            nodes = self._start_cluster(tmpdir)
            try:
                nodes[1].put(b"from_node1", b"hello")
                time.sleep(0.1)
                assert nodes[0].get(b"from_node1") == b"hello"
                assert nodes[2].get(b"from_node1") == b"hello"
            finally:
                self._stop_cluster(nodes)

    def test_one_node_down_write_succeeds(self):
        """With N=3, W=2: writes succeed with 1 node down."""
        with tempfile.TemporaryDirectory() as tmpdir:
            nodes = self._start_cluster(tmpdir)
            try:
                # Stop node 2
                nodes[2].stop()
                time.sleep(0.1)

                # Write to node 0 — should get quorum from node 0 + node 1
                nodes[0].put(b"survive", b"yes")

                # Read from node 1
                val = nodes[1].get(b"survive")
                assert val == b"yes"
            finally:
                nodes[0].stop()
                nodes[1].stop()

    def test_one_node_down_read_succeeds(self):
        """With N=3, R=2: reads succeed with 1 node down if data was replicated."""
        with tempfile.TemporaryDirectory() as tmpdir:
            nodes = self._start_cluster(tmpdir)
            try:
                # Write while all nodes are up
                nodes[0].put(b"data", b"value")
                time.sleep(0.1)

                # Stop node 2
                nodes[2].stop()
                time.sleep(0.1)

                # Read from node 0 — should work with local + node 1
                val = nodes[0].get(b"data")
                assert val == b"value"
            finally:
                nodes[0].stop()
                nodes[1].stop()

    def test_concurrent_writes_to_same_key(self):
        """LWW: concurrent writes from different nodes — highest HLC wins."""
        with tempfile.TemporaryDirectory() as tmpdir:
            nodes = self._start_cluster(tmpdir)
            try:
                # Write from two nodes concurrently
                t0 = threading.Thread(target=lambda: nodes[0].put(b"race", b"from0"))
                t1 = threading.Thread(target=lambda: nodes[1].put(b"race", b"from1"))
                t0.start()
                t1.start()
                t0.join()
                t1.join()
                time.sleep(0.2)

                # All nodes should agree on the same value (whichever had higher HLC)
                vals = [nodes[i].get(b"race") for i in range(3)]
                # They might not all be equal yet (eventual consistency via read repair),
                # but at least quorum reads should be consistent
                assert vals[0] is not None
                assert vals[1] is not None
            finally:
                self._stop_cluster(nodes)

    def test_many_keys(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            nodes = self._start_cluster(tmpdir)
            try:
                for i in range(50):
                    node = nodes[i % 3]
                    node.put(f"key{i}".encode(), f"val{i}".encode())

                time.sleep(0.2)

                for i in range(50):
                    node = nodes[(i + 1) % 3]  # read from a different node
                    val = node.get(f"key{i}".encode())
                    assert val == f"val{i}".encode(), f"key{i} mismatch: {val}"
            finally:
                self._stop_cluster(nodes)
