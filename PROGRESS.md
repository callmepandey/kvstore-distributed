# Distributed KV Store — Progress Tracker

**Total tests: 110 passing**

## Phase 1: HLC + Config + Protocol — DONE
- [x] `src/hlc.py` — Hybrid Logical Clock (15 tests)
- [x] `src/config.py` — Configuration dataclasses
- [x] `src/protocol.py` — Message types and serialization (14 tests)

## Phase 2: Write-Ahead Log — DONE
- [x] `src/wal.py` — WAL with segmentation, CRC, fsync (14 tests)

## Phase 3: MemTable — DONE
- [x] `src/memtable.py` — Thread-safe in-memory KV store (15 tests)

## Phase 4: Snapshot — DONE
- [x] `src/snapshot.py` — Snapshot creation and loading (8 tests)

## Phase 5: Transport — DONE
- [x] `src/transport.py` — TCP transport (send_to_node) (6 tests)

## Phase 6: Quorum + Replication — DONE
- [x] `src/quorum.py` — Quorum coordination (tested via KVStore)

## Phase 7: KVStore — DONE
- [x] `src/kvstore.py` — Main KVStore class (12 tests: 6 single-node, 6 multi-node)

## Phase 8: Anti-Entropy — DONE
- [x] `src/anti_entropy.py` — Merkle tree + push-pull background sync (14 tests)
  - Merkle tree: 8 unit tests (hash identity, divergence, ordering, tombstones)
  - Integration: 6 tests (missing key repair, LWW conflict resolution, bidirectional sync, node rejoin, tombstone propagation, no-op when in sync)

## Phase 9: Server — DONE
- [x] `src/server.py` — HTTP API + CLI entry point + snapshot scheduler (12 tests)
- [x] `src/.__main__.py` — `python -m src` runner
  - HTTP API: health, PUT /kv, GET /kv, DELETE /kv, POST /snapshot, GET /stats
  - CLI: --node-id, --peers, --port, --http-port, --data-dir, --snapshot-interval, --anti-entropy-interval
  - Snapshot scheduler: periodic background snapshots

## Phase 10: Docker
- [ ] `docker/Dockerfile`
- [ ] `docker/docker-compose.yml`
- [ ] `docker/entrypoint.sh`

## Phase 11: Integration Tests
- [ ] `tests/test_integration.py` — Multi-node Docker-based tests
