"""
KVStore — the main distributed key-value store node.

Each instance represents one node in the cluster. Provides:
  - get(key): quorum read
  - put(key, value): quorum write
  - delete(key): quorum delete (tombstone)

Also handles incoming replication and read requests from peers.
"""

from __future__ import annotations

import logging
import socket
import threading

from .anti_entropy import AntiEntropyManager
from .config import Config
from .hlc import HLC, HLCTimestamp
from .memtable import MemTable
from .protocol import (
    Message,
    MessageType,
    OpType,
    make_read_response,
    make_replicate_ack,
    make_replicate_nack,
)
from .quorum import QuorumCoordinator, QuorumError, ReadResult
from .snapshot import SnapshotManager
from .transport import TCPTransport
from .wal import WAL, WALEntry

logger = logging.getLogger(__name__)


class KVStore:
    """A single node in the distributed key-value store."""

    def __init__(self, config: Config) -> None:
        config.validate()
        self.config = config
        self.node_id = config.node_id

        self.hlc = HLC(config.node_id)
        self.wal = WAL(
            data_dir=config.wal_dir,
            segment_size_bytes=config.wal_segment_size_bytes,
            fsync=config.wal_fsync,
        )
        self.memtable = MemTable()
        self.snapshot_mgr = SnapshotManager(
            data_dir=config.snapshot_dir,
            retention_count=config.snapshot_retention_count,
        )
        self.transport = TCPTransport(
            listen_host=config.listen_host,
            listen_port=config.listen_port,
            handler=self._handle_message,
        )
        self.quorum = QuorumCoordinator(config, self.transport)
        self.anti_entropy = AntiEntropyManager(
            config=config,
            memtable=self.memtable,
            transport=self.transport,
            apply_entry_fn=self._apply_remote_entry,
        )

        self._running = False

    # ── Lifecycle ──

    def start(self) -> None:
        """Start the node: recover state, start transport and anti-entropy."""
        self._recover()
        self.transport.start()
        self._running = True
        if self.config.peers:
            self.anti_entropy.start()
        logger.info("Node %d started on port %d", self.node_id, self.config.listen_port)

    def stop(self) -> None:
        """Graceful shutdown."""
        self._running = False
        self.anti_entropy.stop()
        self.transport.stop()
        self.wal.close()
        logger.info("Node %d stopped", self.node_id)

    # ── Public API ──

    def put(self, key: bytes, value: bytes) -> bool:
        """
        Quorum write.
        Writes to local WAL + MemTable, replicates to peers.
        Raises QuorumError if W nodes are not reachable.
        """
        ts = self.hlc.tick()
        lsn = self.wal.next_lsn()

        # 1. Write to local WAL
        entry = WALEntry(lsn=lsn, hlc=ts, op=OpType.PUT, key=key, value=value)
        self.wal.append(entry)

        # 2. Update local MemTable
        self.memtable.put(key, value, ts)

        # 3. Replicate to peers
        remote_acks = self.quorum.replicate_to_peers(lsn, ts, OpType.PUT, key, value)
        total_acks = 1 + remote_acks  # local + remote

        if total_acks < self.config.write_quorum:
            raise QuorumError(
                f"Write quorum not met: got {total_acks}, need {self.config.write_quorum}"
            )
        return True

    def get(self, key: bytes) -> bytes | None:
        """
        Quorum read.
        Reads from local MemTable and peers, returns value with highest HLC.
        Raises QuorumError if R nodes are not reachable.
        """
        # 1. Local read
        local_val, local_hlc = self.memtable.get(key)

        # 2. Read from peers
        peer_results = self.quorum.read_from_peers(key)
        total_responses = 1 + len(peer_results)  # local + remote

        if total_responses < self.config.read_quorum:
            raise QuorumError(
                f"Read quorum not met: got {total_responses}, need {self.config.read_quorum}"
            )

        # 3. Pick the value with the highest HLC
        best_val = local_val
        best_hlc = local_hlc

        for r in peer_results:
            if r.hlc is not None and (best_hlc is None or r.hlc > best_hlc):
                best_val = r.value
                best_hlc = r.hlc

        # 4. Read repair: if local was stale, update
        if best_hlc is not None and (local_hlc is None or best_hlc > local_hlc):
            self.memtable.put(key, best_val, best_hlc)

        return best_val

    def delete(self, key: bytes) -> bool:
        """Quorum delete. Writes a tombstone."""
        ts = self.hlc.tick()
        lsn = self.wal.next_lsn()

        entry = WALEntry(lsn=lsn, hlc=ts, op=OpType.DELETE, key=key, value=None)
        self.wal.append(entry)
        self.memtable.delete(key, ts)

        remote_acks = self.quorum.replicate_to_peers(lsn, ts, OpType.DELETE, key, None)
        total_acks = 1 + remote_acks

        if total_acks < self.config.write_quorum:
            raise QuorumError(
                f"Delete quorum not met: got {total_acks}, need {self.config.write_quorum}"
            )
        return True

    def create_snapshot(self) -> None:
        """Create a point-in-time snapshot for backup."""
        lsn = self.wal.current_lsn()
        hlc = self.hlc.current()
        meta = self.snapshot_mgr.create(self.memtable, lsn, hlc)
        # GC old WAL segments covered by the snapshot
        self.wal.purge_segments_before(meta.lsn)
        logger.info("Snapshot created at LSN=%d", meta.lsn)

    # ── Internal: message handling ──

    def _handle_message(self, msg: Message, conn: socket.socket) -> None:
        """Dispatch incoming messages from peers."""
        if msg.msg_type == MessageType.REPLICATE:
            self._handle_replicate(msg, conn)
        elif msg.msg_type == MessageType.READ_REQUEST:
            self._handle_read_request(msg, conn)
        elif msg.msg_type == MessageType.ANTI_ENTROPY:
            self._handle_anti_entropy(msg, conn)
        elif msg.msg_type == MessageType.HEARTBEAT:
            pass

    def _handle_replicate(self, msg: Message, conn: socket.socket) -> None:
        """Handle an incoming replication request from a peer."""
        hlc = HLCTimestamp.unpack(msg.payload["hlc"])
        op = OpType(msg.payload["op"])
        key = msg.payload["key"]
        value = msg.payload["value"]
        lsn = msg.payload["lsn"]
        origin = msg.payload["origin"]

        # Update our HLC from the remote timestamp
        self.hlc.update(hlc)

        # Write to local WAL with a new local LSN
        local_lsn = self.wal.next_lsn()
        entry = WALEntry(lsn=local_lsn, hlc=hlc, op=op, key=key, value=value)
        self.wal.append(entry)

        # Update MemTable (LWW — only applies if this HLC > existing)
        if op == OpType.DELETE:
            self.memtable.delete(key, hlc)
        else:
            self.memtable.put(key, value, hlc)

        # ACK back
        resp = make_replicate_ack(lsn, self.config.node_id)
        try:
            conn.sendall(resp.serialize())
        except OSError:
            pass

    def _handle_read_request(self, msg: Message, conn: socket.socket) -> None:
        """Handle an incoming read request from a peer."""
        key = msg.payload["key"]
        req_id = msg.payload["req_id"]

        value, hlc = self.memtable.get(key)
        resp = make_read_response(key, value, hlc, req_id, self.config.node_id)
        try:
            conn.sendall(resp.serialize())
        except OSError:
            pass

    def _handle_anti_entropy(self, msg: Message, conn: socket.socket) -> None:
        """Handle incoming anti-entropy digest from a peer."""
        resp = self.anti_entropy.handle_anti_entropy(msg)
        try:
            conn.sendall(resp.serialize())
        except OSError:
            pass

    def _apply_remote_entry(
        self, key: bytes, value: bytes | None, hlc: HLCTimestamp
    ) -> None:
        """Apply a remotely-sourced entry to local WAL + MemTable."""
        self.hlc.update(hlc)
        local_lsn = self.wal.next_lsn()
        op = OpType.DELETE if value is None else OpType.PUT
        entry = WALEntry(lsn=local_lsn, hlc=hlc, op=op, key=key, value=value)
        self.wal.append(entry)
        if op == OpType.DELETE:
            self.memtable.delete(key, hlc)
        else:
            self.memtable.put(key, value, hlc)

    # ── Recovery ──

    def _recover(self) -> None:
        """Recover state from snapshot + WAL on startup."""
        # 1. Load latest snapshot
        meta, entries = self.snapshot_mgr.load_latest()
        after_lsn = 0
        if meta:
            self.memtable.load_from_entries(entries)
            after_lsn = meta.lsn
            logger.info("Loaded snapshot at LSN=%d (%d entries)", meta.lsn, len(entries))

        # 2. Replay WAL entries after the snapshot
        wal_entries = self.wal.replay(after_lsn=after_lsn)
        for entry in wal_entries:
            if entry.op == OpType.DELETE:
                self.memtable.delete(entry.key, entry.hlc)
            else:
                self.memtable.put(entry.key, entry.value, entry.hlc)
        if wal_entries:
            logger.info("Replayed %d WAL entries", len(wal_entries))
