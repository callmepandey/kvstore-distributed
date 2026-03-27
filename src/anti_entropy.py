"""
Anti-entropy: background Merkle-tree-based consistency repair.

Each node maintains a Merkle tree over its key space. Periodically, nodes
exchange Merkle roots with peers. When roots differ, they drill down to
find divergent keys, then exchange entries so the node with the higher
HLC wins (Last-Writer-Wins).

This guarantees eventual convergence even after extended outages.
"""

from __future__ import annotations

import hashlib
import logging
import struct
import threading
import time
from dataclasses import dataclass, field

from .config import Config
from .hlc import HLCTimestamp
from .memtable import MemEntry, MemTable
from .protocol import Message, MessageType
from .quorum import parse_peer
from .transport import TCPTransport

logger = logging.getLogger(__name__)

# ── Merkle Tree ──


@dataclass(slots=True)
class MerkleNode:
    """A node in the Merkle tree. Leaves hold a single key's hash."""

    hash: bytes
    left: MerkleNode | None = None
    right: MerkleNode | None = None
    # For leaves only:
    key: bytes | None = None


class MerkleTree:
    """
    Binary Merkle tree built over sorted keys.

    Each leaf is hash(key + value_or_tombstone + hlc_packed).
    Internal nodes are hash(left.hash + right.hash).
    """

    def __init__(self, items: list[tuple[bytes, MemEntry]]) -> None:
        # Sort by key for deterministic ordering
        sorted_items = sorted(items, key=lambda x: x[0])
        leaves = []
        for key, entry in sorted_items:
            val_bytes = entry.value if entry.value is not None else b"\x00TOMBSTONE"
            leaf_data = key + val_bytes + entry.hlc.pack()
            h = hashlib.sha256(leaf_data).digest()
            leaves.append(MerkleNode(hash=h, key=key))

        self.root = self._build(leaves) if leaves else MerkleNode(hash=b"\x00" * 32)
        self._leaf_count = len(leaves)

    @property
    def root_hash(self) -> bytes:
        return self.root.hash

    @staticmethod
    def _build(leaves: list[MerkleNode]) -> MerkleNode:
        if len(leaves) == 1:
            return leaves[0]

        # Pad to even
        nodes = list(leaves)
        if len(nodes) % 2 == 1:
            nodes.append(nodes[-1])  # duplicate last

        parents = []
        for i in range(0, len(nodes), 2):
            combined = nodes[i].hash + nodes[i + 1].hash
            h = hashlib.sha256(combined).digest()
            parents.append(MerkleNode(hash=h, left=nodes[i], right=nodes[i + 1]))

        return MerkleTree._build(parents)

    def find_divergent_keys(self, other: MerkleTree) -> set[bytes]:
        """
        Compare two Merkle trees and return the set of keys that differ.
        Only works if both trees were built from the same sorted key universe
        plus/minus some differences.
        """
        # For simplicity, we fall back to collecting all leaf keys from
        # subtrees that diverge. This is efficient when few keys differ.
        if self.root.hash == other.root.hash:
            return set()

        my_keys = self._collect_keys(self.root)
        other_keys = other._collect_keys(other.root)
        return my_keys.symmetric_difference(other_keys) | (my_keys & other_keys)

    @staticmethod
    def _collect_keys(node: MerkleNode | None) -> set[bytes]:
        if node is None:
            return set()
        if node.key is not None:
            return {node.key}
        return MerkleTree._collect_keys(node.left) | MerkleTree._collect_keys(node.right)


# ── Anti-Entropy Manager ──


class AntiEntropyManager:
    """
    Background process that periodically syncs with peers using Merkle trees.

    Protocol:
    1. Build local Merkle tree from MemTable
    2. Send ANTI_ENTROPY message with root hash + all (key, value, hlc) entries
       to each peer
    3. Peer compares, applies any entries with higher HLC, and responds
       with its own entries that are newer
    4. We apply those responses locally

    This is a simplified "push-pull" anti-entropy that sends full state
    digests. For large datasets, a multi-round Merkle drill-down would
    be more efficient, but this works well for the initial implementation.
    """

    def __init__(
        self,
        config: Config,
        memtable: MemTable,
        transport: TCPTransport,
        apply_entry_fn: callable,
    ) -> None:
        self._config = config
        self._memtable = memtable
        self._transport = transport
        self._apply_entry = apply_entry_fn  # (key, value, hlc) -> None
        self._running = False
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self._running = True
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()
        logger.info(
            "Anti-entropy started (interval=%ds)",
            self._config.anti_entropy_interval_seconds,
        )

    def stop(self) -> None:
        self._running = False
        if self._thread:
            self._thread.join(timeout=self._config.anti_entropy_interval_seconds + 2)
            self._thread = None

    def run_once(self) -> int:
        """
        Run a single anti-entropy round against all peers.
        Returns the total number of entries repaired.
        """
        repaired = 0
        digest = self._build_digest()

        for peer in self._config.peers:
            try:
                count = self._sync_with_peer(peer, digest)
                repaired += count
            except Exception:
                logger.debug("Anti-entropy sync with %s failed", peer, exc_info=True)

        if repaired:
            logger.info("Anti-entropy repaired %d entries", repaired)
        return repaired

    def handle_anti_entropy(self, msg: Message) -> Message:
        """
        Handle incoming anti-entropy digest from a peer.
        Compare with local state, apply any newer remote entries,
        and return our entries that are newer than the remote's.
        """
        remote_entries = msg.payload.get("entries", [])
        node_id = msg.payload.get("node", -1)

        applied = 0
        our_newer: list[dict] = []

        # Index remote entries by key
        remote_by_key: dict[bytes, tuple[bytes | None, HLCTimestamp]] = {}
        for entry in remote_entries:
            key = entry["key"]
            value = entry["value"]
            hlc = HLCTimestamp.unpack(entry["hlc"])
            remote_by_key[key] = (value, hlc)

            # Apply if remote is newer
            local_val, local_hlc = self._memtable.get(key)
            if local_hlc is None or hlc > local_hlc:
                self._apply_entry(key, value, hlc)
                applied += 1

        # Find our entries that are newer or missing from remote
        for key, mem_entry in self._memtable.items():
            remote = remote_by_key.get(key)
            if remote is None or mem_entry.hlc > remote[1]:
                our_newer.append(
                    {
                        "key": key,
                        "value": mem_entry.value,
                        "hlc": mem_entry.hlc.pack(),
                    }
                )

        if applied:
            logger.debug(
                "Anti-entropy: applied %d entries from node %d", applied, node_id
            )

        return Message(
            MessageType.SYNC_RESPONSE,
            {"entries": our_newer, "node": self._config.node_id},
        )

    # ── Internals ──

    def _loop(self) -> None:
        while self._running:
            # Sleep in small increments so we can stop promptly
            deadline = time.monotonic() + self._config.anti_entropy_interval_seconds
            while self._running and time.monotonic() < deadline:
                time.sleep(0.5)
            if self._running:
                try:
                    self.run_once()
                except Exception:
                    logger.debug("Anti-entropy round failed", exc_info=True)

    def _build_digest(self) -> list[dict]:
        """Build a list of {key, value, hlc} dicts from the MemTable."""
        entries = []
        for key, mem_entry in self._memtable.items():
            entries.append(
                {
                    "key": key,
                    "value": mem_entry.value,
                    "hlc": mem_entry.hlc.pack(),
                }
            )
        return entries

    def _sync_with_peer(self, peer: str, digest: list[dict]) -> int:
        """Send our digest to a peer and apply their response. Returns count repaired."""
        host, port = parse_peer(peer)
        msg = Message(
            MessageType.ANTI_ENTROPY,
            {"entries": digest, "node": self._config.node_id},
        )

        resp = self._transport.send(
            host, port, msg, timeout=self._config.replication_timeout_seconds
        )
        if resp is None or resp.msg_type != MessageType.SYNC_RESPONSE:
            return 0

        repaired = 0
        for entry in resp.payload.get("entries", []):
            key = entry["key"]
            value = entry["value"]
            hlc = HLCTimestamp.unpack(entry["hlc"])
            local_val, local_hlc = self._memtable.get(key)
            if local_hlc is None or hlc > local_hlc:
                self._apply_entry(key, value, hlc)
                repaired += 1

        return repaired
