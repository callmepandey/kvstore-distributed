"""Tests for the TCP transport layer."""

import socket
import time
import threading

from src.protocol import Message, MessageType
from src.transport import TCPTransport


def get_free_port() -> int:
    with socket.socket() as s:
        s.bind(("", 0))
        return s.getsockname()[1]


class TestTCPTransport:
    def test_send_and_receive(self):
        port = get_free_port()
        received = []

        def handler(msg: Message, conn: socket.socket):
            received.append(msg)
            # Send a response
            resp = Message(MessageType.REPLICATE_ACK, {"lsn": msg.payload["lsn"]})
            conn.sendall(resp.serialize())

        server = TCPTransport(listen_port=port, handler=handler)
        server.start()
        time.sleep(0.1)

        try:
            client = TCPTransport()
            msg = Message(MessageType.HEARTBEAT, {"node": 0, "lsn": 42})
            resp = client.send("127.0.0.1", port, msg, timeout=2)

            assert resp is not None
            assert resp.msg_type == MessageType.REPLICATE_ACK
            assert resp.payload["lsn"] == 42
            assert len(received) == 1
            assert received[0].msg_type == MessageType.HEARTBEAT
        finally:
            server.stop()

    def test_send_no_response(self):
        port = get_free_port()
        received = []

        def handler(msg: Message, conn: socket.socket):
            received.append(msg)

        server = TCPTransport(listen_port=port, handler=handler)
        server.start()
        time.sleep(0.1)

        try:
            client = TCPTransport()
            msg = Message(MessageType.HEARTBEAT, {"node": 1, "lsn": 99})
            ok = client.send_no_response("127.0.0.1", port, msg)
            assert ok is True
            time.sleep(0.2)
            assert len(received) == 1
        finally:
            server.stop()

    def test_send_to_unreachable_returns_none(self):
        port = get_free_port()
        client = TCPTransport()
        msg = Message(MessageType.HEARTBEAT, {"node": 0, "lsn": 1})
        resp = client.send("127.0.0.1", port, msg, timeout=0.5)
        assert resp is None

    def test_multiple_messages(self):
        port = get_free_port()
        received = []

        def handler(msg: Message, conn: socket.socket):
            received.append(msg)
            resp = Message(MessageType.REPLICATE_ACK, {"lsn": msg.payload.get("lsn", 0)})
            conn.sendall(resp.serialize())

        server = TCPTransport(listen_port=port, handler=handler)
        server.start()
        time.sleep(0.1)

        try:
            client = TCPTransport()
            for i in range(5):
                msg = Message(MessageType.HEARTBEAT, {"node": 0, "lsn": i})
                resp = client.send("127.0.0.1", port, msg, timeout=2)
                assert resp is not None
                assert resp.payload["lsn"] == i

            assert len(received) == 5
        finally:
            server.stop()

    def test_concurrent_clients(self):
        port = get_free_port()
        received = []
        lock = threading.Lock()

        def handler(msg: Message, conn: socket.socket):
            with lock:
                received.append(msg.payload["node"])
            resp = Message(MessageType.REPLICATE_ACK, {"lsn": 0})
            conn.sendall(resp.serialize())

        server = TCPTransport(listen_port=port, handler=handler)
        server.start()
        time.sleep(0.1)

        try:
            def client_worker(node_id: int):
                client = TCPTransport()
                for _ in range(10):
                    msg = Message(MessageType.HEARTBEAT, {"node": node_id, "lsn": 0})
                    client.send("127.0.0.1", port, msg, timeout=2)

            threads = [threading.Thread(target=client_worker, args=(i,)) for i in range(3)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()

            assert len(received) == 30
        finally:
            server.stop()

    def test_start_stop(self):
        port = get_free_port()
        server = TCPTransport(listen_port=port)
        server.start()
        time.sleep(0.1)
        server.stop()
        # Should be able to restart
        server2 = TCPTransport(listen_port=port)
        server2.start()
        time.sleep(0.1)
        server2.stop()
