"""
Write-Ahead Log with segment rotation, CRC integrity, and crash recovery.

WAL entry on disk:
  [CRC32: 4B][length: 4B][LSN: 8B][HLC: 16B][op_type: 1B][key_len: 4B][key][val_len: 4B][value]

Segment files: segment_NNNNNN.wal (zero-padded 6-digit sequence number).
"""

from __future__ import annotations

import os
import struct
import threading
import zlib
from dataclasses import dataclass
from pathlib import Path

from .hlc import HLC_SIZE, HLCTimestamp
from .protocol import OpType

# CRC(4) + length(4) + LSN(8) + HLC(16) + op(1) + key_len(4) + val_len(4)
ENTRY_HEADER_STRUCT = struct.Struct("!II Q 16s B I I")
ENTRY_HEADER_SIZE = ENTRY_HEADER_STRUCT.size  # 41 bytes


@dataclass(slots=True)
class WALEntry:
    lsn: int
    hlc: HLCTimestamp
    op: OpType
    key: bytes
    value: bytes | None  # None for DELETE

    def serialize(self) -> bytes:
        """Serialize entry with CRC. Returns the full on-disk bytes."""
        val = self.value or b""
        key_len = len(self.key)
        val_len = len(val)
        # Everything after CRC and length fields
        body = struct.pack(
            f"!Q16sBI{key_len}sI{val_len}s",
            self.lsn,
            self.hlc.pack(),
            self.op,
            key_len,
            self.key,
            val_len,
            val,
        )
        length = len(body)
        crc = zlib.crc32(body) & 0xFFFFFFFF
        return struct.pack("!II", crc, length) + body

    @classmethod
    def deserialize(cls, data: bytes, offset: int = 0) -> tuple[WALEntry, int]:
        """
        Deserialize one entry starting at offset.
        Returns (entry, next_offset).
        Raises ValueError on CRC mismatch or truncated data.
        """
        if offset + 8 > len(data):
            raise ValueError("Truncated entry header")

        crc_stored, length = struct.unpack_from("!II", data, offset)
        body_start = offset + 8
        body_end = body_start + length

        if body_end > len(data):
            raise ValueError("Truncated entry body")

        body = data[body_start:body_end]
        crc_computed = zlib.crc32(body) & 0xFFFFFFFF
        if crc_stored != crc_computed:
            raise ValueError(
                f"CRC mismatch: stored={crc_stored:#x}, computed={crc_computed:#x}"
            )

        pos = 0
        (lsn,) = struct.unpack_from("!Q", body, pos)
        pos += 8
        hlc = HLCTimestamp.unpack(body[pos : pos + HLC_SIZE])
        pos += HLC_SIZE
        (op_val,) = struct.unpack_from("!B", body, pos)
        pos += 1
        (key_len,) = struct.unpack_from("!I", body, pos)
        pos += 4
        key = body[pos : pos + key_len]
        pos += key_len
        (val_len,) = struct.unpack_from("!I", body, pos)
        pos += 4
        value = body[pos : pos + val_len] if val_len > 0 else None
        pos += val_len

        entry = cls(
            lsn=lsn,
            hlc=hlc,
            op=OpType(op_val),
            key=key,
            value=value,
        )
        return entry, body_end


class WAL:
    """
    Append-only write-ahead log with segment rotation.

    Thread-safe: all public methods acquire _lock.
    """

    SEGMENT_NAME_FMT = "segment_{:06d}.wal"

    def __init__(
        self,
        data_dir: str,
        segment_size_bytes: int = 64 * 1024 * 1024,
        fsync: bool = True,
    ) -> None:
        self._dir = Path(data_dir)
        self._dir.mkdir(parents=True, exist_ok=True)
        self._segment_size = segment_size_bytes
        self._fsync = fsync
        self._lock = threading.Lock()

        self._current_lsn: int = 0
        self._segment_seq: int = 0
        self._segment_file: open | None = None
        self._segment_bytes: int = 0

        # Recover existing state
        self._recover_state()

    # ── Public API ──

    def append(self, entry: WALEntry) -> int:
        """
        Append an entry to the WAL. The entry's LSN should already be assigned.
        Returns the LSN. Calls fsync if configured.
        """
        with self._lock:
            data = entry.serialize()
            self._ensure_segment()

            self._segment_file.write(data)
            self._segment_file.flush()
            if self._fsync:
                os.fsync(self._segment_file.fileno())

            self._segment_bytes += len(data)
            self._current_lsn = entry.lsn

            # Rotate if segment is full
            if self._segment_bytes >= self._segment_size:
                self._rotate_segment()

            return entry.lsn

    def next_lsn(self) -> int:
        """Return the next LSN to assign."""
        with self._lock:
            return self._current_lsn + 1

    def current_lsn(self) -> int:
        with self._lock:
            return self._current_lsn

    def replay(self, after_lsn: int = 0) -> list[WALEntry]:
        """
        Replay all entries with LSN > after_lsn, in order.
        Used for crash recovery and replication catch-up.
        """
        with self._lock:
            entries = []
            for seg_path in self._sorted_segments():
                seg_entries = self._read_segment(seg_path)
                for e in seg_entries:
                    if e.lsn > after_lsn:
                        entries.append(e)
            return entries

    def purge_segments_before(self, lsn: int) -> int:
        """
        Delete segment files where ALL entries have LSN <= lsn.
        Returns the number of segments deleted.
        """
        with self._lock:
            deleted = 0
            for seg_path in self._sorted_segments():
                # Don't delete the active segment
                if self._segment_file and seg_path == self._current_segment_path():
                    continue
                entries = self._read_segment(seg_path)
                if entries and all(e.lsn <= lsn for e in entries):
                    seg_path.unlink()
                    deleted += 1
                elif entries and entries[0].lsn > lsn:
                    break  # segments are ordered, no more to delete
            return deleted

    def close(self) -> None:
        with self._lock:
            if self._segment_file:
                self._segment_file.close()
                self._segment_file = None

    # ── Internals ──

    def _recover_state(self) -> None:
        """Scan existing segments to find the highest LSN and segment seq."""
        segments = self._sorted_segments()
        if not segments:
            self._segment_seq = 1
            return

        # Parse highest segment number
        last_seg = segments[-1]
        self._segment_seq = int(last_seg.stem.split("_")[1])

        # Find highest LSN across all segments
        for seg_path in segments:
            entries = self._read_segment(seg_path)
            if entries:
                self._current_lsn = max(self._current_lsn, entries[-1].lsn)

        # Set segment_bytes for the active (last) segment
        self._segment_bytes = last_seg.stat().st_size

    def _sorted_segments(self) -> list[Path]:
        segments = sorted(self._dir.glob("segment_*.wal"))
        return segments

    def _current_segment_path(self) -> Path:
        return self._dir / self.SEGMENT_NAME_FMT.format(self._segment_seq)

    def _ensure_segment(self) -> None:
        if self._segment_file is None:
            path = self._current_segment_path()
            self._segment_file = open(path, "ab")
            self._segment_bytes = path.stat().st_size if path.exists() else 0

    def _rotate_segment(self) -> None:
        if self._segment_file:
            self._segment_file.close()
        self._segment_seq += 1
        self._segment_file = None
        self._segment_bytes = 0

    @staticmethod
    def _read_segment(path: Path) -> list[WALEntry]:
        """Read all valid entries from a segment file."""
        data = path.read_bytes()
        entries = []
        offset = 0
        while offset < len(data):
            try:
                entry, offset = WALEntry.deserialize(data, offset)
                entries.append(entry)
            except ValueError:
                # Truncated or corrupted tail — stop here
                break
        return entries
