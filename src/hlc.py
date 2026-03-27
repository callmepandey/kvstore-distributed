"""
Hybrid Logical Clock (HLC) per Kulkarni et al. 2014.

Provides a totally-ordered timestamp that combines wall-clock time with a
logical counter and a node_id tie-breaker. Used for Last-Writer-Wins conflict
resolution across nodes.
"""

from __future__ import annotations

import struct
import threading
import time
from dataclasses import dataclass


# Packed format: physical (uint64) + logical (uint32) + node_id (uint32) = 16 bytes
HLC_STRUCT = struct.Struct("!QII")
HLC_SIZE = HLC_STRUCT.size  # 16


@dataclass(slots=True, order=True, frozen=True)
class HLCTimestamp:
    """Immutable HLC timestamp. Ordered by (physical, logical, node_id)."""

    physical: int
    logical: int
    node_id: int

    def pack(self) -> bytes:
        return HLC_STRUCT.pack(self.physical, self.logical, self.node_id)

    @classmethod
    def unpack(cls, data: bytes) -> HLCTimestamp:
        physical, logical, node_id = HLC_STRUCT.unpack(data)
        return cls(physical, logical, node_id)

    @classmethod
    def zero(cls) -> HLCTimestamp:
        return cls(0, 0, 0)


class HLC:
    """
    Thread-safe Hybrid Logical Clock.

    Call tick() for local events (puts/deletes).
    Call update(remote_ts) when receiving a message from another node.
    """

    def __init__(self, node_id: int) -> None:
        self.node_id = node_id
        self._physical = 0
        self._logical = 0
        self._lock = threading.Lock()

    @staticmethod
    def _now_ms() -> int:
        return int(time.time() * 1000)

    def tick(self) -> HLCTimestamp:
        """Generate a new timestamp for a local event."""
        with self._lock:
            now = self._now_ms()
            if now > self._physical:
                self._physical = now
                self._logical = 0
            else:
                self._logical += 1
            return HLCTimestamp(self._physical, self._logical, self.node_id)

    def update(self, remote: HLCTimestamp) -> HLCTimestamp:
        """Merge with a remote timestamp (on message receive)."""
        with self._lock:
            now = self._now_ms()
            if now > self._physical and now > remote.physical:
                self._physical = now
                self._logical = 0
            elif remote.physical > self._physical:
                self._physical = remote.physical
                self._logical = remote.logical + 1
            elif self._physical > remote.physical:
                self._logical += 1
            else:
                # self._physical == remote.physical
                self._logical = max(self._logical, remote.logical) + 1
            return HLCTimestamp(self._physical, self._logical, self.node_id)

    def current(self) -> HLCTimestamp:
        """Return the current timestamp without advancing."""
        with self._lock:
            return HLCTimestamp(self._physical, self._logical, self.node_id)
