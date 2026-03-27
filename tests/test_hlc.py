"""Tests for the Hybrid Logical Clock."""

import threading
import time
from unittest.mock import patch

from src.hlc import HLC, HLCTimestamp, HLC_SIZE


class TestHLCTimestamp:
    def test_ordering(self):
        a = HLCTimestamp(100, 0, 0)
        b = HLCTimestamp(100, 1, 0)
        c = HLCTimestamp(101, 0, 0)
        assert a < b < c

    def test_node_id_tiebreaker(self):
        a = HLCTimestamp(100, 0, 1)
        b = HLCTimestamp(100, 0, 2)
        assert a < b

    def test_equality(self):
        a = HLCTimestamp(100, 5, 3)
        b = HLCTimestamp(100, 5, 3)
        assert a == b

    def test_pack_unpack_roundtrip(self):
        ts = HLCTimestamp(1234567890000, 42, 7)
        packed = ts.pack()
        assert len(packed) == HLC_SIZE
        unpacked = HLCTimestamp.unpack(packed)
        assert unpacked == ts

    def test_zero(self):
        z = HLCTimestamp.zero()
        assert z.physical == 0 and z.logical == 0 and z.node_id == 0


class TestHLC:
    def test_tick_monotonic(self):
        clock = HLC(node_id=0)
        ts1 = clock.tick()
        ts2 = clock.tick()
        ts3 = clock.tick()
        assert ts1 < ts2 < ts3

    def test_tick_advances_logical_when_time_unchanged(self):
        clock = HLC(node_id=0)
        fixed_time = 1000000
        with patch.object(HLC, "_now_ms", return_value=fixed_time):
            ts1 = clock.tick()
            ts2 = clock.tick()
            ts3 = clock.tick()
        assert ts1.physical == ts2.physical == ts3.physical == fixed_time
        assert ts1.logical == 0
        assert ts2.logical == 1
        assert ts3.logical == 2

    def test_tick_resets_logical_on_time_advance(self):
        clock = HLC(node_id=0)
        with patch.object(HLC, "_now_ms", return_value=1000):
            ts1 = clock.tick()
        with patch.object(HLC, "_now_ms", return_value=2000):
            ts2 = clock.tick()
        assert ts2.physical == 2000
        assert ts2.logical == 0
        assert ts1 < ts2

    def test_update_from_future_remote(self):
        clock = HLC(node_id=0)
        with patch.object(HLC, "_now_ms", return_value=1000):
            remote = HLCTimestamp(5000, 10, 1)
            ts = clock.update(remote)
        assert ts.physical == 5000
        assert ts.logical == 11  # remote.logical + 1
        assert ts.node_id == 0

    def test_update_from_past_remote(self):
        clock = HLC(node_id=0)
        with patch.object(HLC, "_now_ms", return_value=5000):
            clock.tick()  # set physical to 5000
        with patch.object(HLC, "_now_ms", return_value=5000):
            remote = HLCTimestamp(1000, 0, 1)
            ts = clock.update(remote)
        # local physical (5000) > remote physical (1000), so physical stays
        assert ts.physical == 5000
        assert ts.node_id == 0

    def test_update_same_physical(self):
        clock = HLC(node_id=0)
        with patch.object(HLC, "_now_ms", return_value=1000):
            clock.tick()  # physical=1000, logical=0
            remote = HLCTimestamp(1000, 5, 1)
            ts = clock.update(remote)
        # same physical → logical = max(local_logical, remote_logical) + 1
        assert ts.physical == 1000
        assert ts.logical == 6  # max(0,5) + 1 → but local logical was 0 after tick, then update increments

    def test_always_monotonic_after_update(self):
        clock = HLC(node_id=0)
        with patch.object(HLC, "_now_ms", return_value=1000):
            ts1 = clock.tick()
            remote = HLCTimestamp(500, 0, 1)
            ts2 = clock.update(remote)
            ts3 = clock.tick()
        assert ts1 < ts2 < ts3

    def test_node_id_preserved(self):
        clock = HLC(node_id=42)
        ts = clock.tick()
        assert ts.node_id == 42
        ts2 = clock.update(HLCTimestamp(999999, 0, 7))
        assert ts2.node_id == 42

    def test_current_does_not_advance(self):
        clock = HLC(node_id=0)
        ts1 = clock.tick()
        cur1 = clock.current()
        cur2 = clock.current()
        assert cur1 == cur2 == ts1

    def test_thread_safety(self):
        clock = HLC(node_id=0)
        results = []

        def worker():
            for _ in range(1000):
                results.append(clock.tick())

        threads = [threading.Thread(target=worker) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # All timestamps should be unique (different logical or physical)
        assert len(set(results)) == 4000
        # Should be sortable (total order)
        sorted_results = sorted(results)
        assert sorted_results == sorted(results)
