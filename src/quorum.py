"""
Quorum coordination for reads and writes.

Enforces R + W > N invariant and manages parallel requests to peer nodes.
"""

from __future__ import annotations

import concurrent.futures
import logging
from dataclasses import dataclass

from .config import Config
from .hlc import HLCTimestamp
from .protocol import (
    Message,
    MessageType,
    OpType,
    make_read_request,
    make_read_response,
    make_replicate,
    make_replicate_ack,
)
from .transport import TCPTransport

logger = logging.getLogger(__name__)


class QuorumError(Exception):
    """Raised when quorum cannot be achieved."""
    pass


@dataclass(slots=True)
class ReadResult:
    value: bytes | None
    hlc: HLCTimestamp | None
    node_id: int


def parse_peer(peer: str) -> tuple[str, int]:
    """Parse 'host:port' into (host, port)."""
    host, port_str = peer.rsplit(":", 1)
    return host, int(port_str)


class QuorumCoordinator:
    """
    Coordinates quorum reads and writes across the cluster.

    Used by KVStore to achieve consistency guarantees.
    """

    def __init__(
        self,
        config: Config,
        transport: TCPTransport,
    ) -> None:
        self._config = config
        self._transport = transport
        self._request_counter = 0

    def replicate_to_peers(
        self,
        lsn: int,
        hlc: HLCTimestamp,
        op: OpType,
        key: bytes,
        value: bytes | None,
    ) -> int:
        """
        Send a replication request to all peers in parallel.
        Returns the number of remote ACKs received.
        The caller counts its own local write as 1 toward the quorum.
        """
        msg = make_replicate(lsn, hlc, op, key, value, self._config.node_id)
        timeout = self._config.replication_timeout_seconds

        ack_count = 0
        if not self._config.peers:
            return ack_count

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=len(self._config.peers)
        ) as pool:
            futures = {}
            for peer in self._config.peers:
                host, port = parse_peer(peer)
                fut = pool.submit(self._transport.send, host, port, msg, timeout)
                futures[fut] = peer

            needed = self._config.write_quorum - 1  # -1 for local write
            for fut in concurrent.futures.as_completed(futures):
                resp = fut.result()
                if resp and resp.msg_type == MessageType.REPLICATE_ACK:
                    ack_count += 1
                    if ack_count >= needed:
                        break  # quorum met, don't wait for stragglers

        return ack_count

    def read_from_peers(
        self, key: bytes
    ) -> list[ReadResult]:
        """
        Send read requests to all peers in parallel.
        Returns a list of ReadResults from responsive peers.
        """
        self._request_counter += 1
        req_id = self._request_counter
        msg = make_read_request(key, req_id)
        timeout = self._config.read_timeout_seconds

        results = []
        if not self._config.peers:
            return results

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=len(self._config.peers)
        ) as pool:
            futures = {}
            for peer in self._config.peers:
                host, port = parse_peer(peer)
                fut = pool.submit(self._transport.send, host, port, msg, timeout)
                futures[fut] = peer

            needed = self._config.read_quorum - 1  # -1 for local read
            for fut in concurrent.futures.as_completed(futures):
                resp = fut.result()
                if resp and resp.msg_type == MessageType.READ_RESPONSE:
                    hlc_bytes = resp.payload.get("hlc")
                    hlc = HLCTimestamp.unpack(hlc_bytes) if hlc_bytes else None
                    results.append(
                        ReadResult(
                            value=resp.payload.get("value"),
                            hlc=hlc,
                            node_id=resp.payload.get("node", -1),
                        )
                    )
                    if len(results) >= needed:
                        break

        return results
