"""
Message types and serialization for node-to-node communication.

Wire format:
  [msg_type: 1 byte][payload_len: 4 bytes][payload: msgpack bytes]
"""

from __future__ import annotations

import struct
from enum import IntEnum
from typing import Any

import msgpack

from .hlc import HLCTimestamp

HEADER_STRUCT = struct.Struct("!BI")  # msg_type (uint8) + payload_len (uint32)
HEADER_SIZE = HEADER_STRUCT.size  # 5 bytes


class MessageType(IntEnum):
    REPLICATE = 1
    REPLICATE_ACK = 2
    REPLICATE_NACK = 3
    READ_REQUEST = 4
    READ_RESPONSE = 5
    ANTI_ENTROPY = 6
    SYNC_REQUEST = 7
    SYNC_RESPONSE = 8
    HEARTBEAT = 9
    SNAPSHOT_META = 10


class OpType(IntEnum):
    PUT = 1
    DELETE = 2


class Message:
    """A message exchanged between nodes."""

    __slots__ = ("msg_type", "payload")

    def __init__(self, msg_type: MessageType, payload: dict[str, Any]) -> None:
        self.msg_type = msg_type
        self.payload = payload

    def serialize(self) -> bytes:
        """Serialize to wire format: header + msgpack payload."""
        body = msgpack.packb(self.payload, use_bin_type=True)
        header = HEADER_STRUCT.pack(self.msg_type, len(body))
        return header + body

    @classmethod
    def deserialize(cls, data: bytes) -> Message:
        """Deserialize from wire format."""
        msg_type_val, payload_len = HEADER_STRUCT.unpack_from(data, 0)
        body = data[HEADER_SIZE : HEADER_SIZE + payload_len]
        payload = msgpack.unpackb(body, raw=False)
        return cls(MessageType(msg_type_val), payload)

    @classmethod
    def read_from_buffer(cls, buf: bytes) -> tuple[Message | None, bytes]:
        """
        Try to read one complete message from a buffer.
        Returns (message, remaining_buffer) or (None, original_buffer)
        if not enough data yet.
        """
        if len(buf) < HEADER_SIZE:
            return None, buf
        msg_type_val, payload_len = HEADER_STRUCT.unpack_from(buf, 0)
        total = HEADER_SIZE + payload_len
        if len(buf) < total:
            return None, buf
        body = buf[HEADER_SIZE:total]
        payload = msgpack.unpackb(body, raw=False)
        return cls(MessageType(msg_type_val), payload), buf[total:]

    def __repr__(self) -> str:
        return f"Message({self.msg_type.name}, {self.payload})"


# ── Convenience builders ──


def make_replicate(
    lsn: int,
    hlc: HLCTimestamp,
    op: OpType,
    key: bytes,
    value: bytes | None,
    origin_node: int,
) -> Message:
    return Message(
        MessageType.REPLICATE,
        {
            "lsn": lsn,
            "hlc": hlc.pack(),
            "op": int(op),
            "key": key,
            "value": value,
            "origin": origin_node,
        },
    )


def make_replicate_ack(lsn: int, node_id: int) -> Message:
    return Message(MessageType.REPLICATE_ACK, {"lsn": lsn, "node": node_id})


def make_replicate_nack(lsn: int, node_id: int, reason: str = "") -> Message:
    return Message(
        MessageType.REPLICATE_NACK, {"lsn": lsn, "node": node_id, "reason": reason}
    )


def make_read_request(key: bytes, request_id: int) -> Message:
    return Message(MessageType.READ_REQUEST, {"key": key, "req_id": request_id})


def make_read_response(
    key: bytes,
    value: bytes | None,
    hlc: HLCTimestamp | None,
    request_id: int,
    node_id: int,
) -> Message:
    return Message(
        MessageType.READ_RESPONSE,
        {
            "key": key,
            "value": value,
            "hlc": hlc.pack() if hlc else None,
            "req_id": request_id,
            "node": node_id,
        },
    )


def make_heartbeat(node_id: int, lsn: int) -> Message:
    return Message(MessageType.HEARTBEAT, {"node": node_id, "lsn": lsn})
