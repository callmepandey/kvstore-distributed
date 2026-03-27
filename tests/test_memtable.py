"""Tests for the MemTable."""

import threading

from src.hlc import HLCTimestamp
from src.memtable import MemTable


def ts(physical: int, logical: int = 0, node_id: int = 0) -> HLCTimestamp:
    return HLCTimestamp(physical, logical, node_id)


class TestMemTable:
    def test_put_and_get(self):
        mt = MemTable()
        mt.put(b"key1", b"val1", ts(100))
        value, hlc = mt.get(b"key1")
        assert value == b"val1"
        assert hlc == ts(100)

    def test_get_missing_key(self):
        mt = MemTable()
        value, hlc = mt.get(b"nope")
        assert value is None
        assert hlc is None

    def test_put_overwrites_with_newer_hlc(self):
        mt = MemTable()
        mt.put(b"k", b"old", ts(100))
        mt.put(b"k", b"new", ts(200))
        value, hlc = mt.get(b"k")
        assert value == b"new"
        assert hlc == ts(200)

    def test_put_rejects_stale_write(self):
        mt = MemTable()
        mt.put(b"k", b"new", ts(200))
        result = mt.put(b"k", b"old", ts(100))
        assert result is False
        value, _ = mt.get(b"k")
        assert value == b"new"

    def test_put_rejects_equal_hlc(self):
        mt = MemTable()
        mt.put(b"k", b"first", ts(100))
        result = mt.put(b"k", b"second", ts(100))
        assert result is False
        value, _ = mt.get(b"k")
        assert value == b"first"

    def test_delete_creates_tombstone(self):
        mt = MemTable()
        mt.put(b"k", b"val", ts(100))
        mt.delete(b"k", ts(200))
        value, hlc = mt.get(b"k")
        assert value is None
        assert hlc == ts(200)  # tombstone has the HLC

    def test_contains(self):
        mt = MemTable()
        assert not mt.contains(b"k")
        mt.put(b"k", b"v", ts(100))
        assert mt.contains(b"k")
        mt.delete(b"k", ts(200))
        assert not mt.contains(b"k")

    def test_keys_excludes_tombstones(self):
        mt = MemTable()
        mt.put(b"a", b"1", ts(100))
        mt.put(b"b", b"2", ts(100))
        mt.delete(b"a", ts(200))
        keys = mt.keys()
        assert b"a" not in keys
        assert b"b" in keys

    def test_items_includes_tombstones(self):
        mt = MemTable()
        mt.put(b"a", b"1", ts(100))
        mt.delete(b"a", ts(200))
        items = mt.items()
        assert len(items) == 1
        assert items[0][1].value is None

    def test_len(self):
        mt = MemTable()
        assert len(mt) == 0
        mt.put(b"a", b"1", ts(100))
        mt.put(b"b", b"2", ts(100))
        assert len(mt) == 2
        mt.delete(b"a", ts(200))
        assert len(mt) == 2  # tombstone still counted

    def test_clear(self):
        mt = MemTable()
        mt.put(b"a", b"1", ts(100))
        mt.clear()
        assert len(mt) == 0
        assert mt.get(b"a") == (None, None)

    def test_load_from_entries(self):
        mt = MemTable()
        entries = [
            (b"a", b"1", ts(100)),
            (b"b", b"2", ts(200)),
            (b"a", b"3", ts(300)),  # overwrites first "a"
        ]
        mt.load_from_entries(entries)
        assert len(mt) == 2
        val, hlc = mt.get(b"a")
        assert val == b"3"
        assert hlc == ts(300)

    def test_load_from_entries_respects_hlc(self):
        mt = MemTable()
        # Pre-populate
        mt.put(b"x", b"existing", ts(500))
        # Load entries — stale one should be rejected
        mt.load_from_entries([(b"x", b"stale", ts(100))])
        val, _ = mt.get(b"x")
        assert val == b"existing"

    def test_concurrent_reads_and_writes(self):
        mt = MemTable()
        errors = []

        def writer():
            for i in range(1000):
                mt.put(f"key{i}".encode(), f"val{i}".encode(), ts(i))

        def reader():
            for i in range(1000):
                val, hlc = mt.get(f"key{i}".encode())
                # val could be None (not yet written) or the correct value
                if val is not None and val != f"val{i}".encode():
                    errors.append(f"key{i}: expected val{i}, got {val}")

        threads = [threading.Thread(target=writer)]
        threads += [threading.Thread(target=reader) for _ in range(3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert errors == []

    def test_concurrent_writes_lww(self):
        """Multiple writers to same key — highest HLC wins."""
        mt = MemTable()

        def writer(node_id: int):
            for i in range(100):
                hlc = HLCTimestamp(physical=i, logical=0, node_id=node_id)
                mt.put(b"contended", f"node{node_id}_{i}".encode(), hlc)

        threads = [threading.Thread(target=writer, args=(n,)) for n in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # The winner should be physical=99, highest node_id as tiebreaker
        val, hlc = mt.get(b"contended")
        assert hlc.physical == 99
        assert hlc.node_id == 3  # highest node_id wins the tie
