# Design: Distributed Key-Value Store

How this system answers each core requirement.

---

## 1. Basic Operations — `get(key)` and `put(key, value)`

**File:** `src/kvstore.py`

### put(key, value)

```
put("foo", "bar")
  │
  ├─ 1. HLC tick — assigns a globally ordered timestamp to this write
  ├─ 2. WAL append — serializes entry to disk with CRC, fsynced before anything else
  ├─ 3. MemTable update — writes key → value into in-memory hashmap
  └─ 4. Replicate to peers — sends to all nodes in parallel, waits for W ACKs
```

The write is only confirmed to the caller once at least W nodes have durably stored it. If quorum is not met, a `QuorumError` is raised — the caller is never told "ok" unless the data is safe.

### get(key)

```
get("foo")
  │
  ├─ 1. Local MemTable read — O(1) hashmap lookup, counts as 1 response
  ├─ 2. Read from peers in parallel — sends READ_REQUEST to all peers
  ├─ 3. Wait for R total responses (local counts as one)
  ├─ 4. Pick the response with the highest HLC timestamp (Last-Writer-Wins)
  └─ 5. Read repair — if local was stale, silently update it in background
```

Because R + W > N, the read quorum always overlaps the write quorum. This means at least one node in every read set saw the most recent write — `get` always returns the latest value.

---

## 2. Multi-Node — 3 servers, each running the same class

**Files:** `src/kvstore.py`, `src/quorum.py`, `src/transport.py`

Every node runs an identical `KVStore` instance. There is no primary/secondary distinction — any node can accept reads and writes.

### How nodes communicate

```python
# From quorum.py — each node uses send_to_node() style TCP calls:
self._transport.send(host, port, message)
```

`TCPTransport` opens a connection to a peer, sends a serialized `Message` (msgpack), and waits for a response. Each message type has a defined protocol:

| Message | Sender → Receiver | Purpose |
|---------|------------------|---------|
| `REPLICATE` | coordinator → peers | "Please store this key-value" |
| `REPLICATE_ACK` | peer → coordinator | "Stored, here's my ACK" |
| `READ_REQUEST` | coordinator → peers | "What is your value for this key?" |
| `READ_RESPONSE` | peer → coordinator | "Here's my value + HLC timestamp" |
| `ANTI_ENTROPY` | any → any | Merkle root exchange for repair |

### How data is distributed

There is no sharding. **Every node stores every key.** This is full replication:

```
Node 0: { foo → bar, baz → qux, ... }
Node 1: { foo → bar, baz → qux, ... }   ← identical copy
Node 2: { foo → bar, baz → qux, ... }   ← identical copy
```

Tradeoff: read/write capacity does not scale with more nodes, but fault tolerance is maximized — any node can answer any query independently.

### Quorum math (N=3 default)

```
W = N/2 + 1 = 2    (write must reach 2 of 3 nodes)
R = N/2 + 1 = 2    (read must collect from 2 of 3 nodes)
R + W = 4 > N = 3  ✓ — reads always overlap writes
```

---

## 3. Fault Tolerance

### If a node crashes — data is not lost

**File:** `src/wal.py`

Every write goes to the **Write-Ahead Log (WAL)** on disk *before* the MemTable is updated. The WAL entry is fsynced:

```
put("foo", "bar")
 └─ WAL.append(entry)   ← disk write with fsync
     └─ MemTable.put()  ← RAM update (fast, can be lost on crash)
```

On restart, `KVStore._recover()` replays the WAL from disk back into memory:

```python
def _recover(self):
    meta, entries = self.snapshot_mgr.load_latest()   # load last snapshot
    wal_entries = self.wal.replay(after_lsn=meta.lsn) # replay WAL on top
    for entry in wal_entries:
        self.memtable.put(entry.key, entry.value, entry.hlc)
```

The node comes back with the exact state it had before the crash.

Each WAL entry has a CRC32 checksum. If the process crashes mid-write (torn write), the corrupted tail entry is detected and discarded on replay — the last clean entry survives.

### If a node is temporarily unreachable — the system still works

With N=3, W=2, R=2:
- **Writes** succeed if 2 of 3 nodes are up (coordinator + 1 peer ACK).
- **Reads** succeed if 2 of 3 nodes are up.
- The cluster keeps serving normally while 1 node is down.

When the node comes back, **anti-entropy** heals it automatically:

```
Node 2 comes back online after outage
  │
  └─ AntiEntropyManager wakes up (every 30s by default)
      ├─ Builds a Merkle tree over its own key space
      ├─ Exchanges Merkle root with each peer
      ├─ Finds divergent subtrees (keys that differ)
      └─ Pulls missing/stale keys from peers, applies LWW via HLC
```

**File:** `src/anti_entropy.py`

### Data survives node restarts — persistence

Recovery sequence on every startup (`src/kvstore.py:_recover`):

```
1. Load latest snapshot file → bulk restore of all keys at that point-in-time
2. Replay WAL entries written after snapshot LSN → brings node to exact pre-crash state
3. Bind TCP port and rejoin cluster
4. Anti-entropy catches any keys missed during downtime
```

---

## 4. Backup — consistent snapshots while writes continue

**Files:** `src/snapshot.py`, `src/kvstore.py`, `src/wal.py`

### The challenge

Writes are continuously arriving. A naive backup that reads the MemTable key by key while writes happen would produce an inconsistent mix — some keys at time T, others at time T+1.

### How this is solved: WAL LSN as a freeze point

```python
def create_snapshot(self):
    lsn = self.wal.current_lsn()   # 1. Record the LSN right now
    hlc = self.hlc.current()        # 2. Record HLC right now
    meta = self.snapshot_mgr.create(self.memtable, lsn, hlc)
    self.wal.purge_segments_before(meta.lsn)
```

The LSN is captured *before* the MemTable is serialized. This freeze point means:

- The snapshot records every write with LSN ≤ that value
- Any writes that arrive during the snapshot serialize to a higher LSN
- Those later writes are still safely in the WAL

When the snapshot file is fully written to disk, it is a **consistent point-in-time image**: all keys reflect state at exactly LSN `n`, not a mix of states across time.

### No writes are lost

```
Timeline:
  LSN 100 ← snapshot starts here (freeze point captured)
  LSN 101 ← write arrives, goes to WAL and MemTable normally
  LSN 102 ← another write
  ...
  (snapshot file finishes writing to disk)

Recovery from this snapshot:
  Load snapshot (everything through LSN 100) →
  Replay WAL from LSN 101 onward →
  Full state restored, nothing missing
```

### Snapshot file format

```
Header  [42 bytes]: magic + version + snapshot_lsn + snapshot_hlc + entry_count
Entries [variable]: key_len + key + val_len + value + hlc   (per key)
Footer  [4 bytes]:  CRC32 checksum over all entries
```

The CRC footer detects any corruption of the backup file before it is trusted.

### WAL retention after snapshot

After the snapshot is verified, WAL segments whose highest LSN is below the snapshot LSN are deleted — they are redundant. The system only keeps WAL segments that contain writes newer than the latest snapshot.

### Automatic backups

A `SnapshotScheduler` runs in a background thread and triggers `create_snapshot()` every N seconds (default: 1 hour, configurable with `--snapshot-interval`). The last 5 snapshots are kept on disk; older ones are deleted automatically.

---

## Summary

| Requirement | How it is met |
|-------------|---------------|
| `get(key)` | Quorum read across R nodes, picks highest HLC (Last-Writer-Wins) |
| `put(key, value)` | WAL fsync → MemTable → replicate to W nodes |
| Multi-node | Full replication to all N nodes; any node handles any request |
| Node communication | TCP transport with msgpack serialization |
| Data distribution | Full copy on every node; quorum ensures consistency |
| Crash safety | WAL is source of truth; MemTable rebuilt from WAL on restart |
| Partial outage | Quorum tolerates N/2 failures; anti-entropy heals recovered nodes |
| Persistence | Snapshot + WAL replay; CRC on every WAL entry and snapshot file |
| Live backup | LSN freeze point makes snapshot consistent; WAL covers writes during backup |
| No write loss | WAL fsynced before ACK sent; snapshot LSN + WAL replay covers everything |
