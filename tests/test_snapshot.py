"""Tests for the Snapshot manager."""

import tempfile

from src.hlc import HLCTimestamp
from src.memtable import MemTable
from src.snapshot import SnapshotManager


def ts(physical: int) -> HLCTimestamp:
    return HLCTimestamp(physical, 0, 0)


def populated_memtable() -> MemTable:
    mt = MemTable()
    mt.put(b"alpha", b"one", ts(100))
    mt.put(b"beta", b"two", ts(200))
    mt.put(b"gamma", b"three", ts(300))
    return mt


class TestSnapshot:
    def test_create_and_load(self):
        mt = populated_memtable()
        with tempfile.TemporaryDirectory() as tmpdir:
            mgr = SnapshotManager(tmpdir)
            meta = mgr.create(mt, lsn=10, hlc=ts(300))
            assert meta.lsn == 10
            assert meta.entry_count == 3

            loaded_meta, entries = mgr.load_latest()
            assert loaded_meta.lsn == 10
            assert len(entries) == 3

            # Entries are sorted by key
            keys = [e[0] for e in entries]
            assert keys == [b"alpha", b"beta", b"gamma"]
            assert entries[0][1] == b"one"
            assert entries[1][1] == b"two"

    def test_tombstones_preserved(self):
        mt = MemTable()
        mt.put(b"alive", b"yes", ts(100))
        mt.put(b"dead", b"was_here", ts(100))
        mt.delete(b"dead", ts(200))

        with tempfile.TemporaryDirectory() as tmpdir:
            mgr = SnapshotManager(tmpdir)
            mgr.create(mt, lsn=5, hlc=ts(200))
            _, entries = mgr.load_latest()

            entry_dict = {k: v for k, v, _ in entries}
            assert entry_dict[b"alive"] == b"yes"
            assert entry_dict[b"dead"] is None  # tombstone

    def test_load_latest_returns_most_recent(self):
        mt = MemTable()
        mt.put(b"k", b"v1", ts(100))

        with tempfile.TemporaryDirectory() as tmpdir:
            mgr = SnapshotManager(tmpdir)
            mgr.create(mt, lsn=1, hlc=ts(100))

            mt.put(b"k", b"v2", ts(200))
            mgr.create(mt, lsn=2, hlc=ts(200))

            meta, entries = mgr.load_latest()
            assert meta.lsn == 2
            assert entries[0][1] == b"v2"

    def test_no_snapshots_returns_none(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            mgr = SnapshotManager(tmpdir)
            meta, entries = mgr.load_latest()
            assert meta is None
            assert entries == []

    def test_retention_enforced(self):
        mt = populated_memtable()
        with tempfile.TemporaryDirectory() as tmpdir:
            mgr = SnapshotManager(tmpdir, retention_count=3)
            for i in range(1, 7):
                mgr.create(mt, lsn=i, hlc=ts(i * 100))

            snapshots = mgr._sorted_snapshots()
            assert len(snapshots) == 3  # only last 3 retained

            # Latest should be lsn=6
            meta, _ = mgr.load_latest()
            assert meta.lsn == 6

    def test_roundtrip_to_memtable(self):
        """Create snapshot → load → populate new MemTable."""
        mt = populated_memtable()
        with tempfile.TemporaryDirectory() as tmpdir:
            mgr = SnapshotManager(tmpdir)
            mgr.create(mt, lsn=10, hlc=ts(300))

            _, entries = mgr.load_latest()
            mt2 = MemTable()
            mt2.load_from_entries(entries)

            assert mt2.get(b"alpha") == (b"one", ts(100))
            assert mt2.get(b"beta") == (b"two", ts(200))
            assert mt2.get(b"gamma") == (b"three", ts(300))

    def test_large_values(self):
        mt = MemTable()
        big_val = b"x" * 500_000
        mt.put(b"big", big_val, ts(1))

        with tempfile.TemporaryDirectory() as tmpdir:
            mgr = SnapshotManager(tmpdir)
            mgr.create(mt, lsn=1, hlc=ts(1))
            _, entries = mgr.load_latest()
            assert entries[0][1] == big_val

    def test_checksum_detects_corruption(self):
        mt = populated_memtable()
        with tempfile.TemporaryDirectory() as tmpdir:
            mgr = SnapshotManager(tmpdir)
            meta = mgr.create(mt, lsn=1, hlc=ts(100))

            # Corrupt the file
            data = bytearray(meta.path.read_bytes())
            data[50] ^= 0xFF  # corrupt an entry byte
            meta.path.write_bytes(bytes(data))

            try:
                mgr.load_latest()
                assert False, "Should have raised ValueError"
            except ValueError as e:
                assert "checksum" in str(e).lower()
