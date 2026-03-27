"""
TCP transport layer for node-to-node communication.

Each node runs a TCP server. Messages are framed with the protocol header
(msg_type + payload_len) so the receiver can extract complete messages from
the byte stream.
"""

from __future__ import annotations

import logging
import socket
import selectors
import threading
from typing import Callable

from .protocol import HEADER_SIZE, HEADER_STRUCT, Message

logger = logging.getLogger(__name__)

# Max message payload: 2 MB (generous for large values)
MAX_PAYLOAD = 2 * 1024 * 1024


class TCPTransport:
    """
    Async TCP server + synchronous client for node-to-node messaging.

    The server runs in a background thread, dispatching incoming messages
    to a handler callback. The client (send_to_node) is synchronous and
    waits for a response.
    """

    def __init__(
        self,
        listen_host: str = "0.0.0.0",
        listen_port: int = 7000,
        handler: Callable[[Message, socket.socket], None] | None = None,
    ) -> None:
        self._host = listen_host
        self._port = listen_port
        self._handler = handler
        self._server_sock: socket.socket | None = None
        self._selector = selectors.DefaultSelector()
        self._running = False
        self._thread: threading.Thread | None = None
        self._buffers: dict[int, bytearray] = {}  # fd -> buffer

    def start(self) -> None:
        """Start the TCP server in a background thread."""
        self._server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_sock.bind((self._host, self._port))
        self._server_sock.listen(64)
        self._server_sock.setblocking(False)
        self._selector.register(self._server_sock, selectors.EVENT_READ)
        self._running = True
        self._thread = threading.Thread(target=self._serve_loop, daemon=True)
        self._thread.start()
        logger.info("Transport listening on %s:%d", self._host, self._port)

    def stop(self) -> None:
        """Stop the server."""
        self._running = False
        if self._server_sock:
            try:
                self._selector.unregister(self._server_sock)
            except Exception:
                pass
            self._server_sock.close()
            self._server_sock = None
        if self._thread:
            self._thread.join(timeout=2)
            self._thread = None
        self._selector.close()

    @property
    def port(self) -> int:
        return self._port

    def send(self, host: str, port: int, msg: Message, timeout: float = 5.0) -> Message | None:
        """
        Send a message to a remote node and wait for a single response.
        Returns the response Message, or None on timeout/error.
        """
        try:
            sock = socket.create_connection((host, port), timeout=timeout)
            sock.settimeout(timeout)
            sock.sendall(msg.serialize())

            # Read response
            buf = bytearray()
            while True:
                chunk = sock.recv(4096)
                if not chunk:
                    break
                buf.extend(chunk)
                result, remaining = Message.read_from_buffer(bytes(buf))
                if result is not None:
                    sock.close()
                    return result
            sock.close()
            return None
        except (OSError, TimeoutError) as e:
            logger.debug("send to %s:%d failed: %s", host, port, e)
            return None

    def send_no_response(self, host: str, port: int, msg: Message, timeout: float = 5.0) -> bool:
        """Send a message without waiting for a response. Returns True on success."""
        try:
            sock = socket.create_connection((host, port), timeout=timeout)
            sock.sendall(msg.serialize())
            sock.close()
            return True
        except (OSError, TimeoutError) as e:
            logger.debug("send_no_response to %s:%d failed: %s", host, port, e)
            return False

    def _serve_loop(self) -> None:
        while self._running:
            try:
                events = self._selector.select(timeout=0.5)
            except (OSError, ValueError):
                break
            for key, mask in events:
                if key.fileobj is self._server_sock:
                    self._accept(key.fileobj)
                else:
                    self._read(key.fileobj)

    def _accept(self, sock: socket.socket) -> None:
        try:
            conn, addr = sock.accept()
            conn.setblocking(False)
            self._selector.register(conn, selectors.EVENT_READ)
            self._buffers[conn.fileno()] = bytearray()
        except OSError:
            pass

    def _read(self, conn: socket.socket) -> None:
        fd = conn.fileno()
        try:
            data = conn.recv(65536)
        except (OSError, ConnectionError):
            data = b""

        if not data:
            self._cleanup_conn(conn)
            return

        buf = self._buffers.get(fd, bytearray())
        buf.extend(data)

        # Process all complete messages in the buffer
        while True:
            result, remaining = Message.read_from_buffer(bytes(buf))
            if result is None:
                break
            buf = bytearray(remaining)
            if self._handler:
                try:
                    self._handler(result, conn)
                except Exception:
                    logger.exception("Handler error")

        self._buffers[fd] = buf

    def _cleanup_conn(self, conn: socket.socket) -> None:
        fd = conn.fileno()
        try:
            self._selector.unregister(conn)
        except (KeyError, ValueError):
            pass
        self._buffers.pop(fd, None)
        try:
            conn.close()
        except OSError:
            pass
