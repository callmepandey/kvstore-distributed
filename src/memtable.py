"""
Thread-safe in-memory key-value store with HLC timestamps.

Supports tombstones (value=None) for deletes.
Uses a read-write lock for concurrent reads with exclusive writes.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass

from .hlc import HLCTimestamp


@dataclass(slots=True)
class MemEntry:
    value: bytes | None  # None = tombstone
    hlc: HLCTimestamp


class RWLock:
    """Simple read-write lock using threading primitives."""

    def __init__(self) -> None:
        self._read_ready = threading.Condition(threading.Lock())
        self._readers = 0

    def acquire_read(self) -> None:
        with self._read_ready:
            self._readers += 1

    def release_read(self) -> None:
        with self._read_ready:
            self._readers -= 1
            if self._readers == 0:
                self._read_ready.notify_all()

    def acquire_write(self) -> None:
        self._read_ready.acquire()
        while self._readers > 0:
            self._read_ready.wait()

    def release_write(self) -> None:
        self._read_ready.release()


class MemTable:
    """
    Thread-safe in-memory key-value store.

    Each key maps to (value, HLC timestamp). Tombstones are stored as
    entries with value=None.

    Writes only succeed if the incoming HLC >= the existing HLC for that key
    (Last-Writer-Wins).
    """

    def __init__(self) -> None:
        self._data: dict[bytes, MemEntry] = {}
        self._lock = RWLock()

    def get(self, key: bytes) -> tuple[bytes | None, HLCTimestamp | None]:
        """
        Return (value, hlc) for key, or (None, None) if not found.
        Tombstoned keys return (None, hlc).
        """
        self._lock.acquire_read()
        try:
            entry = self._data.get(key)
            if entry is None:
                return None, None
            return entry.value, entry.hlc
        finally:
            self._lock.release_read()

    def put(self, key: bytes, value: bytes | None, hlc: HLCTimestamp) -> bool:
        """
        Store a key-value pair with its HLC timestamp.
        Returns True if the write was applied, False if a newer entry exists.
        Pass value=None for a tombstone (delete).
        """
        self._lock.acquire_write()
        try:
            existing = self._data.get(key)
            if existing is not None and existing.hlc >= hlc:
                return False  # stale write
            self._data[key] = MemEntry(value=value, hlc=hlc)
            return True
        finally:
            self._lock.release_write()

    def delete(self, key: bytes, hlc: HLCTimestamp) -> bool:
        """Write a tombstone for key. Returns True if applied."""
        return self.put(key, None, hlc)

    def contains(self, key: bytes) -> bool:
        """Check if a key exists (non-tombstoned)."""
        value, hlc = self.get(key)
        return hlc is not None and value is not None

    def keys(self) -> list[bytes]:
        """Return all non-tombstoned keys."""
        self._lock.acquire_read()
        try:
            return [k for k, e in self._data.items() if e.value is not None]
        finally:
            self._lock.release_read()

    def items(self) -> list[tuple[bytes, MemEntry]]:
        """Return all entries (including tombstones) as (key, MemEntry) pairs."""
        self._lock.acquire_read()
        try:
            return list(self._data.items())
        finally:
            self._lock.release_read()

    def __len__(self) -> int:
        """Number of entries including tombstones."""
        self._lock.acquire_read()
        try:
            return len(self._data)
        finally:
            self._lock.release_read()

    def clear(self) -> None:
        self._lock.acquire_write()
        try:
            self._data.clear()
        finally:
            self._lock.release_write()

    def load_from_entries(
        self, entries: list[tuple[bytes, bytes | None, HLCTimestamp]]
    ) -> None:
        """Bulk-load entries (key, value, hlc). Used during recovery."""
        self._lock.acquire_write()
        try:
            for key, value, hlc in entries:
                existing = self._data.get(key)
                if existing is None or hlc > existing.hlc:
                    self._data[key] = MemEntry(value=value, hlc=hlc)
        finally:
            self._lock.release_write()
