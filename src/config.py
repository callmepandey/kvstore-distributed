"""
Configuration dataclasses for the distributed KV store.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class Config:
    # Cluster
    node_id: int = 0
    peers: list[str] = field(default_factory=list)

    # Quorum
    replication_factor: int = 3
    write_quorum: int = 2
    read_quorum: int = 2

    # WAL
    wal_dir: str = "data/wal"
    wal_segment_size_bytes: int = 64 * 1024 * 1024  # 64 MB
    wal_fsync: bool = True

    # Snapshots
    snapshot_dir: str = "data/snapshots"
    snapshot_interval_seconds: int = 21600  # 6 hours
    snapshot_retention_count: int = 5

    # Anti-entropy
    anti_entropy_interval_seconds: int = 30

    # Heartbeat / Failure detection
    heartbeat_interval_seconds: float = 1.0
    failure_timeout_seconds: float = 10.0

    # Tombstone GC
    tombstone_gc_grace_seconds: int = 86400  # 24 hours

    # Transport
    listen_host: str = "0.0.0.0"
    listen_port: int = 7000
    replication_timeout_seconds: float = 5.0
    read_timeout_seconds: float = 5.0

    def validate(self) -> None:
        """Raise ValueError if the config is invalid."""
        if self.read_quorum + self.write_quorum <= self.replication_factor:
            raise ValueError(
                f"R({self.read_quorum}) + W({self.write_quorum}) must be > "
                f"N({self.replication_factor}) for consistency"
            )
        if self.write_quorum < 1 or self.read_quorum < 1:
            raise ValueError("Quorum values must be >= 1")
        if self.replication_factor < 1:
            raise ValueError("Replication factor must be >= 1")
