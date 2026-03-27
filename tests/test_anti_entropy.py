"""Tests for the anti-entropy Merkle tree and sync mechanism."""

import socket
import tempfile
import time

from src.anti_entropy import AntiEntropyManager, MerkleTree
from src.config import Config
from src.hlc import HLCTimestamp
from src.kvstore import KVStore
from src.memtable import MemEntry, MemTable


def ts(physical: int, node_id: int = 0) -> HLCTimestamp:
    return HLCTimestamp(physical, 0, node_id)


def get_free_port() -> int:
    with socket.socket() as s:
        s.bind(("", 0))
        return s.getsockname()[1]


def make_config(node_id, port, peers, tmpdir, **kw):
    return Config(
        node_id=node_id,
        peers=peers,
        listen_port=port,
        wal_dir=f"{tmpdir}/node{node_id}/wal",
        snapshot_dir=f"{tmpdir}/node{node_id}/snapshots",
        wal_fsync=False,
        replication_timeout_seconds=2.0,
        read_timeout_seconds=2.0,
        anti_entropy_interval_seconds=60,  # won't auto-run in tests
        **kw,
    )


# ── Merkle Tree unit tests ──


class TestMerkleTree:
    def test_identical_trees_same_hash(self):
        items = [
            (b"a", MemEntry(b"1", ts(100))),
            (b"b", MemEntry(b"2", ts(200))),
        ]
        t1 = MerkleTree(items)
        t2 = MerkleTree(list(items))
        assert t1.root_hash == t2.root_hash

    def test_different_values_different_hash(self):
        items1 = [(b"a", MemEntry(b"1", ts(100)))]
        items2 = [(b"a", MemEntry(b"2", ts(100)))]
        t1 = MerkleTree(items1)
        t2 = MerkleTree(items2)
        assert t1.root_hash != t2.root_hash

    def test_different_hlc_different_hash(self):
        items1 = [(b"a", MemEntry(b"1", ts(100)))]
        items2 = [(b"a", MemEntry(b"1", ts(200)))]
        t1 = MerkleTree(items1)
        t2 = MerkleTree(items2)
        assert t1.root_hash != t2.root_hash

    def test_empty_tree(self):
        t = MerkleTree([])
        assert t.root_hash == b"\x00" * 32

    def test_single_item(self):
        t = MerkleTree([(b"key", MemEntry(b"val", ts(1)))])
        assert len(t.root_hash) == 32

    def test_order_independent(self):
        """Merkle tree sorts by key, so insertion order doesn't matter."""
        items1 = [
            (b"b", MemEntry(b"2", ts(200))),
            (b"a", MemEntry(b"1", ts(100))),
        ]
        items2 = [
            (b"a", MemEntry(b"1", ts(100))),
            (b"b", MemEntry(b"2", ts(200))),
        ]
        assert MerkleTree(items1).root_hash == MerkleTree(items2).root_hash

    def test_tombstone_vs_value_different_hash(self):
        items1 = [(b"a", MemEntry(b"val", ts(100)))]
        items2 = [(b"a", MemEntry(None, ts(100)))]  # tombstone
        assert MerkleTree(items1).root_hash != MerkleTree(items2).root_hash

    def test_many_items(self):
        items = [(f"key{i}".encode(), MemEntry(f"val{i}".encode(), ts(i))) for i in range(100)]
        t = MerkleTree(items)
        assert len(t.root_hash) == 32
        # Same items → same hash
        t2 = MerkleTree(list(items))
        assert t.root_hash == t2.root_hash


# ── Anti-Entropy integration tests ──


class TestAntiEntropy:
    def _start_cluster(self, tmpdir, n=3):
        ports = [get_free_port() for _ in range(n)]
        nodes = []
        for i in range(n):
            peers = [f"127.0.0.1:{ports[j]}" for j in range(n) if j != i]
            cfg = make_config(i, ports[i], peers, tmpdir)
            nodes.append(KVStore(cfg))
        for node in nodes:
            node.start()
        time.sleep(0.3)
        return nodes

    def _stop_cluster(self, nodes):
        for n in nodes:
            n.stop()

    def test_anti_entropy_repairs_missing_key(self):
        """
        Write to node0 with only local (no quorum peers),
        then run anti-entropy to propagate to node1.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            nodes = self._start_cluster(tmpdir)
            try:
                # Directly insert into node0's memtable (bypass quorum)
                # simulating a key that node0 has but node1 doesn't
                hlc = nodes[0].hlc.tick()
                nodes[0].memtable.put(b"orphan", b"value", hlc)

                # node1 shouldn't have it
                val, _ = nodes[1].memtable.get(b"orphan")
                assert val is None

                # Run anti-entropy from node0
                repaired = nodes[0].anti_entropy.run_once()

                # Now node1 should have it
                time.sleep(0.2)
                val, _ = nodes[1].memtable.get(b"orphan")
                assert val == b"value"
            finally:
                self._stop_cluster(nodes)

    def test_anti_entropy_resolves_conflict_lww(self):
        """Two nodes have different values for the same key — higher HLC wins."""
        with tempfile.TemporaryDirectory() as tmpdir:
            nodes = self._start_cluster(tmpdir)
            try:
                # Give node0 an older value
                old_hlc = HLCTimestamp(1000, 0, 0)
                nodes[0].memtable.put(b"conflict", b"old", old_hlc)

                # Give node1 a newer value
                new_hlc = HLCTimestamp(2000, 0, 1)
                nodes[1].memtable.put(b"conflict", b"new", new_hlc)

                # Run anti-entropy from node0
                nodes[0].anti_entropy.run_once()
                time.sleep(0.2)

                # Both should have "new" (higher HLC)
                val0, hlc0 = nodes[0].memtable.get(b"conflict")
                val1, hlc1 = nodes[1].memtable.get(b"conflict")
                assert val0 == b"new"
                assert val1 == b"new"
            finally:
                self._stop_cluster(nodes)

    def test_anti_entropy_bidirectional(self):
        """Both nodes have unique keys — after sync, both have everything."""
        with tempfile.TemporaryDirectory() as tmpdir:
            nodes = self._start_cluster(tmpdir)
            try:
                hlc0 = nodes[0].hlc.tick()
                nodes[0].memtable.put(b"only_on_0", b"val0", hlc0)

                hlc1 = nodes[1].hlc.tick()
                nodes[1].memtable.put(b"only_on_1", b"val1", hlc1)

                # Sync from node0 → pushes its data, pulls node1's data
                nodes[0].anti_entropy.run_once()
                time.sleep(0.2)

                # node0 should now have both
                val, _ = nodes[0].memtable.get(b"only_on_1")
                assert val == b"val1"

                # node1 should now have both
                val, _ = nodes[1].memtable.get(b"only_on_0")
                assert val == b"val0"
            finally:
                self._stop_cluster(nodes)

    def test_anti_entropy_after_node_rejoin(self):
        """
        Node2 is down during writes, then comes back.
        Anti-entropy should repair node2.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            nodes = self._start_cluster(tmpdir)
            try:
                # Stop node2
                nodes[2].stop()
                time.sleep(0.1)

                # Write to node0 (quorum of node0+node1)
                nodes[0].put(b"missed_by_2", b"data")
                time.sleep(0.1)

                # Restart node2
                port2 = nodes[2].config.listen_port
                cfg2 = make_config(
                    2, port2,
                    [f"127.0.0.1:{nodes[j].config.listen_port}" for j in [0, 1]],
                    tmpdir,
                )
                nodes[2] = KVStore(cfg2)
                nodes[2].start()
                time.sleep(0.3)

                # node2 shouldn't have the key yet (it was down)
                val, _ = nodes[2].memtable.get(b"missed_by_2")
                # It might or might not have it from WAL depending on timing
                # But let's ensure anti-entropy fixes it
                nodes[0].anti_entropy.run_once()
                time.sleep(0.2)

                val, _ = nodes[2].memtable.get(b"missed_by_2")
                assert val == b"data"
            finally:
                self._stop_cluster(nodes)

    def test_anti_entropy_tombstone_propagation(self):
        """Tombstones should be propagated via anti-entropy."""
        with tempfile.TemporaryDirectory() as tmpdir:
            nodes = self._start_cluster(tmpdir)
            try:
                # Both nodes have the key
                hlc = nodes[0].hlc.tick()
                nodes[0].memtable.put(b"to_delete", b"val", hlc)
                nodes[1].memtable.put(b"to_delete", b"val", hlc)

                # Delete on node0 only (directly, bypass quorum)
                del_hlc = nodes[0].hlc.tick()
                nodes[0].memtable.delete(b"to_delete", del_hlc)

                # Run anti-entropy
                nodes[0].anti_entropy.run_once()
                time.sleep(0.2)

                # node1 should have the tombstone
                val, _ = nodes[1].memtable.get(b"to_delete")
                assert val is None  # tombstoned
            finally:
                self._stop_cluster(nodes)

    def test_anti_entropy_no_op_when_in_sync(self):
        """Anti-entropy should be a no-op when nodes are already in sync."""
        with tempfile.TemporaryDirectory() as tmpdir:
            nodes = self._start_cluster(tmpdir)
            try:
                # Write through quorum — all nodes in sync
                nodes[0].put(b"synced", b"yes")
                time.sleep(0.2)

                repaired = nodes[0].anti_entropy.run_once()
                assert repaired == 0
            finally:
                self._stop_cluster(nodes)
