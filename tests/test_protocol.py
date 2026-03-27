"""Tests for the protocol message serialization."""

from src.hlc import HLCTimestamp
from src.protocol import (
    HEADER_SIZE,
    Message,
    MessageType,
    OpType,
    make_heartbeat,
    make_read_request,
    make_read_response,
    make_replicate,
    make_replicate_ack,
    make_replicate_nack,
)


class TestMessage:
    def test_serialize_deserialize_roundtrip(self):
        msg = Message(MessageType.HEARTBEAT, {"node": 1, "lsn": 42})
        data = msg.serialize()
        restored = Message.deserialize(data)
        assert restored.msg_type == MessageType.HEARTBEAT
        assert restored.payload["node"] == 1
        assert restored.payload["lsn"] == 42

    def test_read_from_buffer_complete(self):
        msg = Message(MessageType.HEARTBEAT, {"node": 0, "lsn": 10})
        data = msg.serialize()
        result, remaining = Message.read_from_buffer(data)
        assert result is not None
        assert result.msg_type == MessageType.HEARTBEAT
        assert remaining == b""

    def test_read_from_buffer_incomplete_header(self):
        result, remaining = Message.read_from_buffer(b"\x01\x00")
        assert result is None
        assert remaining == b"\x01\x00"

    def test_read_from_buffer_incomplete_payload(self):
        msg = Message(MessageType.HEARTBEAT, {"node": 0, "lsn": 10})
        data = msg.serialize()
        partial = data[: len(data) - 2]
        result, remaining = Message.read_from_buffer(partial)
        assert result is None
        assert remaining == partial

    def test_read_from_buffer_multiple_messages(self):
        msg1 = Message(MessageType.HEARTBEAT, {"node": 0, "lsn": 1})
        msg2 = Message(MessageType.HEARTBEAT, {"node": 1, "lsn": 2})
        data = msg1.serialize() + msg2.serialize()

        result1, remaining = Message.read_from_buffer(data)
        assert result1 is not None
        assert result1.payload["lsn"] == 1

        result2, remaining = Message.read_from_buffer(remaining)
        assert result2 is not None
        assert result2.payload["lsn"] == 2
        assert remaining == b""

    def test_binary_values_preserved(self):
        payload = {"key": b"\x00\xff\x80", "value": b"\xde\xad\xbe\xef"}
        msg = Message(MessageType.READ_RESPONSE, payload)
        data = msg.serialize()
        restored = Message.deserialize(data)
        assert restored.payload["key"] == b"\x00\xff\x80"
        assert restored.payload["value"] == b"\xde\xad\xbe\xef"


class TestConvenienceBuilders:
    def test_make_replicate(self):
        hlc = HLCTimestamp(1000, 1, 0)
        msg = make_replicate(
            lsn=5, hlc=hlc, op=OpType.PUT, key=b"foo", value=b"bar", origin_node=0
        )
        assert msg.msg_type == MessageType.REPLICATE
        assert msg.payload["lsn"] == 5
        assert msg.payload["op"] == OpType.PUT
        assert msg.payload["key"] == b"foo"
        assert msg.payload["value"] == b"bar"
        assert msg.payload["origin"] == 0
        # HLC is packed bytes
        restored_hlc = HLCTimestamp.unpack(msg.payload["hlc"])
        assert restored_hlc == hlc

    def test_make_replicate_roundtrip(self):
        hlc = HLCTimestamp(9999, 0, 2)
        msg = make_replicate(
            lsn=1, hlc=hlc, op=OpType.DELETE, key=b"k", value=None, origin_node=2
        )
        data = msg.serialize()
        restored = Message.deserialize(data)
        assert restored.payload["value"] is None
        assert restored.payload["op"] == OpType.DELETE

    def test_make_replicate_ack(self):
        msg = make_replicate_ack(lsn=10, node_id=1)
        assert msg.msg_type == MessageType.REPLICATE_ACK
        assert msg.payload["lsn"] == 10

    def test_make_replicate_nack(self):
        msg = make_replicate_nack(lsn=10, node_id=1, reason="stale")
        assert msg.msg_type == MessageType.REPLICATE_NACK
        assert msg.payload["reason"] == "stale"

    def test_make_read_request(self):
        msg = make_read_request(key=b"mykey", request_id=42)
        assert msg.msg_type == MessageType.READ_REQUEST
        assert msg.payload["key"] == b"mykey"
        assert msg.payload["req_id"] == 42

    def test_make_read_response_with_value(self):
        hlc = HLCTimestamp(500, 0, 1)
        msg = make_read_response(
            key=b"k", value=b"v", hlc=hlc, request_id=42, node_id=1
        )
        assert msg.msg_type == MessageType.READ_RESPONSE
        assert msg.payload["value"] == b"v"
        restored_hlc = HLCTimestamp.unpack(msg.payload["hlc"])
        assert restored_hlc == hlc

    def test_make_read_response_key_not_found(self):
        msg = make_read_response(
            key=b"k", value=None, hlc=None, request_id=1, node_id=0
        )
        assert msg.payload["value"] is None
        assert msg.payload["hlc"] is None

    def test_make_heartbeat(self):
        msg = make_heartbeat(node_id=2, lsn=100)
        assert msg.msg_type == MessageType.HEARTBEAT
        assert msg.payload["node"] == 2
        assert msg.payload["lsn"] == 100
