"""
Point-in-time snapshot creation and loading.

Snapshot file format:
  Header: magic(6) + version(2) + snapshot_lsn(8) + snapshot_hlc(16) + entry_count(8) + checksum_algo(1) = 41 bytes
  Entries: [key_len(4) + key + val_len(4) + val + hlc(16)] ...
  Footer: checksum(4)
"""

from __future__ import annotations

import struct
import zlib
from dataclasses import dataclass
from pathlib import Path

from .hlc import HLC_SIZE, HLCTimestamp
from .memtable import MemTable

SNAPSHOT_MAGIC = b"KVSNAP"
SNAPSHOT_VERSION = 1
HEADER_STRUCT = struct.Struct("!6sH Q 16s Q B")  # 41 bytes
FOOTER_STRUCT = struct.Struct("!I")  # 4 bytes


@dataclass(slots=True)
class SnapshotMeta:
    lsn: int
    hlc: HLCTimestamp
    entry_count: int
    path: Path


class SnapshotManager:
    """Creates and loads point-in-time snapshots of the MemTable."""

    SNAPSHOT_NAME_FMT = "snapshot_{:06d}.dat"

    def __init__(self, data_dir: str, retention_count: int = 5) -> None:
        self._dir = Path(data_dir)
        self._dir.mkdir(parents=True, exist_ok=True)
        self._retention = retention_count

    def create(self, memtable: MemTable, lsn: int, hlc: HLCTimestamp) -> SnapshotMeta:
        """
        Create a snapshot from the current MemTable state.

        This captures all entries (including tombstones) at the given LSN/HLC
        freeze point. The caller is responsible for ensuring the freeze point
        is consistent (e.g., holding the WAL lock when recording lsn/hlc).
        """
        items = memtable.items()  # (key, MemEntry) pairs
        seq = self._next_seq()
        path = self._dir / self.SNAPSHOT_NAME_FMT.format(seq)

        # Serialize entries
        entry_data = bytearray()
        for key, mem_entry in sorted(items, key=lambda x: x[0]):
            val = mem_entry.value or b""
            entry_data.extend(struct.pack("!I", len(key)))
            entry_data.extend(key)
            # val_len = 0 and a flag for tombstone: use -1 (0xFFFFFFFF) for None
            if mem_entry.value is None:
                entry_data.extend(struct.pack("!I", 0xFFFFFFFF))
            else:
                entry_data.extend(struct.pack("!I", len(val)))
                entry_data.extend(val)
            entry_data.extend(mem_entry.hlc.pack())

        entry_bytes = bytes(entry_data)
        checksum = zlib.crc32(entry_bytes) & 0xFFFFFFFF

        # Write file
        header = HEADER_STRUCT.pack(
            SNAPSHOT_MAGIC,
            SNAPSHOT_VERSION,
            lsn,
            hlc.pack(),
            len(items),
            1,  # checksum_algo = CRC32
        )
        footer = FOOTER_STRUCT.pack(checksum)

        path.write_bytes(header + entry_bytes + footer)

        self._enforce_retention()

        return SnapshotMeta(lsn=lsn, hlc=hlc, entry_count=len(items), path=path)

    def load_latest(self) -> tuple[SnapshotMeta | None, list[tuple[bytes, bytes | None, HLCTimestamp]]]:
        """
        Load the most recent snapshot.
        Returns (meta, entries) or (None, []) if no snapshots exist.
        Entries are (key, value_or_None, hlc) tuples.
        """
        snapshots = self._sorted_snapshots()
        if not snapshots:
            return None, []

        path = snapshots[-1]
        return self._load(path)

    def _load(self, path: Path) -> tuple[SnapshotMeta, list[tuple[bytes, bytes | None, HLCTimestamp]]]:
        data = path.read_bytes()
        header_size = HEADER_STRUCT.size
        footer_size = FOOTER_STRUCT.size

        if len(data) < header_size + footer_size:
            raise ValueError("Snapshot file too small")

        # Parse header
        magic, version, lsn, hlc_bytes, entry_count, checksum_algo = (
            HEADER_STRUCT.unpack_from(data, 0)
        )
        if magic != SNAPSHOT_MAGIC:
            raise ValueError(f"Bad magic: {magic}")
        if version != SNAPSHOT_VERSION:
            raise ValueError(f"Unsupported version: {version}")

        hlc = HLCTimestamp.unpack(hlc_bytes)

        # Parse footer
        (stored_checksum,) = FOOTER_STRUCT.unpack_from(data, len(data) - footer_size)

        # Verify checksum over entry data
        entry_data = data[header_size : len(data) - footer_size]
        computed_checksum = zlib.crc32(entry_data) & 0xFFFFFFFF
        if stored_checksum != computed_checksum:
            raise ValueError("Snapshot checksum mismatch")

        # Parse entries
        entries = []
        offset = 0
        while offset < len(entry_data):
            (key_len,) = struct.unpack_from("!I", entry_data, offset)
            offset += 4
            key = entry_data[offset : offset + key_len]
            offset += key_len

            (val_len_raw,) = struct.unpack_from("!I", entry_data, offset)
            offset += 4

            if val_len_raw == 0xFFFFFFFF:
                value = None
            else:
                value = entry_data[offset : offset + val_len_raw]
                offset += val_len_raw

            entry_hlc = HLCTimestamp.unpack(entry_data[offset : offset + HLC_SIZE])
            offset += HLC_SIZE

            entries.append((key, value, entry_hlc))

        meta = SnapshotMeta(lsn=lsn, hlc=hlc, entry_count=entry_count, path=path)
        return meta, entries

    def _sorted_snapshots(self) -> list[Path]:
        return sorted(self._dir.glob("snapshot_*.dat"))

    def _next_seq(self) -> int:
        snapshots = self._sorted_snapshots()
        if not snapshots:
            return 1
        last = snapshots[-1]
        return int(last.stem.split("_")[1]) + 1

    def _enforce_retention(self) -> None:
        snapshots = self._sorted_snapshots()
        while len(snapshots) > self._retention:
            snapshots[0].unlink()
            snapshots = snapshots[1:]
