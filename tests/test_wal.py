"""Tests for the Write-Ahead Log."""

import os
import struct
import tempfile
from pathlib import Path

from src.hlc import HLCTimestamp
from src.protocol import OpType
from src.wal import WAL, WALEntry


def make_entry(lsn: int, key: bytes = b"k", value: bytes = b"v") -> WALEntry:
    return WALEntry(
        lsn=lsn,
        hlc=HLCTimestamp(lsn * 1000, 0, 0),
        op=OpType.PUT,
        key=key,
        value=value,
    )


class TestWALEntry:
    def test_serialize_deserialize_roundtrip(self):
        entry = make_entry(1, b"hello", b"world")
        data = entry.serialize()
        restored, end = WALEntry.deserialize(data)
        assert restored.lsn == 1
        assert restored.key == b"hello"
        assert restored.value == b"world"
        assert restored.hlc == entry.hlc
        assert restored.op == OpType.PUT
        assert end == len(data)

    def test_delete_entry_none_value(self):
        entry = WALEntry(
            lsn=5,
            hlc=HLCTimestamp(5000, 0, 0),
            op=OpType.DELETE,
            key=b"gone",
            value=None,
        )
        data = entry.serialize()
        restored, _ = WALEntry.deserialize(data)
        assert restored.op == OpType.DELETE
        assert restored.value is None
        assert restored.key == b"gone"

    def test_crc_detects_corruption(self):
        entry = make_entry(1)
        data = bytearray(entry.serialize())
        # Corrupt a byte in the body
        data[12] ^= 0xFF
        try:
            WALEntry.deserialize(bytes(data))
            assert False, "Should have raised ValueError"
        except ValueError as e:
            assert "CRC mismatch" in str(e)

    def test_truncated_data(self):
        entry = make_entry(1)
        data = entry.serialize()
        try:
            WALEntry.deserialize(data[:5])
            assert False, "Should have raised ValueError"
        except ValueError:
            pass

    def test_large_key_value(self):
        key = b"k" * 10000
        value = b"v" * 100000
        entry = WALEntry(
            lsn=1,
            hlc=HLCTimestamp(1000, 0, 0),
            op=OpType.PUT,
            key=key,
            value=value,
        )
        data = entry.serialize()
        restored, _ = WALEntry.deserialize(data)
        assert restored.key == key
        assert restored.value == value

    def test_multiple_entries_back_to_back(self):
        entries = [make_entry(i, f"key{i}".encode(), f"val{i}".encode()) for i in range(5)]
        data = b"".join(e.serialize() for e in entries)
        offset = 0
        restored = []
        while offset < len(data):
            entry, offset = WALEntry.deserialize(data, offset)
            restored.append(entry)
        assert len(restored) == 5
        assert [e.lsn for e in restored] == [0, 1, 2, 3, 4]


class TestWAL:
    def _make_wal(self, tmpdir: str, **kwargs) -> WAL:
        return WAL(data_dir=tmpdir, fsync=False, **kwargs)

    def test_append_and_replay(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            wal = self._make_wal(tmpdir)
            for i in range(1, 6):
                wal.append(make_entry(i))
            wal.close()

            entries = WAL(tmpdir, fsync=False).replay()
            assert len(entries) == 5
            assert [e.lsn for e in entries] == [1, 2, 3, 4, 5]

    def test_next_lsn(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            wal = self._make_wal(tmpdir)
            assert wal.next_lsn() == 1
            wal.append(make_entry(1))
            assert wal.next_lsn() == 2
            wal.append(make_entry(2))
            assert wal.next_lsn() == 3
            wal.close()

    def test_replay_after_lsn(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            wal = self._make_wal(tmpdir)
            for i in range(1, 11):
                wal.append(make_entry(i))
            wal.close()

            entries = WAL(tmpdir, fsync=False).replay(after_lsn=7)
            assert [e.lsn for e in entries] == [8, 9, 10]

    def test_segment_rotation(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            # Tiny segment size to force rotation
            wal = self._make_wal(tmpdir, segment_size_bytes=100)
            for i in range(1, 21):
                wal.append(make_entry(i, f"key{i}".encode(), b"x" * 50))
            wal.close()

            # Should have multiple segment files
            segments = sorted(Path(tmpdir).glob("segment_*.wal"))
            assert len(segments) > 1

            # All entries should still be recoverable
            wal2 = WAL(tmpdir, fsync=False)
            entries = wal2.replay()
            assert len(entries) == 20
            assert [e.lsn for e in entries] == list(range(1, 21))
            wal2.close()

    def test_crash_recovery(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            wal = self._make_wal(tmpdir)
            for i in range(1, 6):
                wal.append(make_entry(i))
            wal.close()

            # Simulate crash: reopen from disk
            wal2 = WAL(tmpdir, fsync=False)
            assert wal2.current_lsn() == 5
            assert wal2.next_lsn() == 6

            # Append more after recovery
            wal2.append(make_entry(6))
            entries = wal2.replay()
            assert len(entries) == 6
            wal2.close()

    def test_recovery_with_corrupted_tail(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            wal = self._make_wal(tmpdir)
            for i in range(1, 4):
                wal.append(make_entry(i))
            wal.close()

            # Corrupt the tail of the segment file
            seg_files = sorted(Path(tmpdir).glob("segment_*.wal"))
            with open(seg_files[-1], "ab") as f:
                f.write(b"\x00\xff\xfe\xfd")  # garbage at the end

            wal2 = WAL(tmpdir, fsync=False)
            entries = wal2.replay()
            assert len(entries) == 3  # 3 valid entries, garbage ignored
            wal2.close()

    def test_purge_segments(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            wal = self._make_wal(tmpdir, segment_size_bytes=100)
            for i in range(1, 21):
                wal.append(make_entry(i, f"key{i}".encode(), b"x" * 50))
            wal.close()

            segments_before = sorted(Path(tmpdir).glob("segment_*.wal"))
            count_before = len(segments_before)

            wal2 = WAL(tmpdir, fsync=False)
            deleted = wal2.purge_segments_before(lsn=10)
            wal2.close()

            assert deleted > 0
            segments_after = sorted(Path(tmpdir).glob("segment_*.wal"))
            assert len(segments_after) < count_before

            # Remaining entries should all have LSN > 10 or be in the active segment
            wal3 = WAL(tmpdir, fsync=False)
            entries = wal3.replay(after_lsn=10)
            assert all(e.lsn > 10 for e in entries)
            wal3.close()

    def test_empty_wal(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            wal = self._make_wal(tmpdir)
            assert wal.current_lsn() == 0
            assert wal.next_lsn() == 1
            entries = wal.replay()
            assert entries == []
            wal.close()
