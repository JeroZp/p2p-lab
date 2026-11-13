"""A p2p-lab node: connections, handshake, heartbeats and message routing.

Python counterpart of go/internal/core. Each connection runs three tasks
(read, write, heartbeat) in a TaskGroup: if one fails, the group cancels
the others, which replaces Go's manual context/close/cleanup wiring.
"""
from __future__ import annotations

import asyncio
import logging
import random
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path

from prometheus_client import Gauge

from .identity import load_or_create
from .wire import ZERO_ID, Message, encode_frame, read_message

log = logging.getLogger("p2plab.node")

# Module-level so tests can shorten them with monkeypatch
HEARTBEAT_INTERVAL = 2.0
HEARTBEAT_TIMEOUT = 6.0
RECONCILE_INTERVAL = 1.0
BACKOFF_BASE = 0.5
BACKOFF_MAX = 5.0

CONNECTIONS = Gauge("p2p_connections_active", "Current number of active connections.")

Handler = Callable[[Message], Awaitable[None]]


class PeerClosed(Exception):
    """Raised inside a connection's task group to end it."""


class Peer:
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, dialed: bool):
        self.id = ZERO_ID                # known after the handshake
        self.reader = reader
        self.writer = writer
        self.dialed = dialed
        self.outbox: asyncio.Queue[Message] = asyncio.Queue(maxsize=64)
        self.last_seen = time.monotonic()
        self.closed = asyncio.Event()

    @property
    def alive(self) -> bool:
        return not self.closed.is_set()


@dataclass
class _Target:
    """Reconnection state for one address (Go: maintainLoop's target)."""
    peer: Peer | None = None
    backoff: float = 0.0
    next_try: float = 0.0


class Node:
    def __init__(self, key_path: str | Path):
        self.key, self.id = load_or_create(key_path)
        self.peers: dict[bytes, Peer] = {}
        self.handlers: dict[str, Handler] = {}
        self.on_connect: list[Callable[[bytes], None]] = []
        self._tasks: set[asyncio.Task] = set()
        self._server: asyncio.Server | None = None

    # ---------- public API ----------

    def on(self, msg_type: str, handler: Handler) -> None:
        """Route every message of this type to handler (Go: Subscribe)."""
        self.handlers[msg_type] = handler

    def message(self, msg_type: str, msg_id: str = "", payload: dict | None = None) -> Message:
        return Message(type=msg_type, from_id=self.id, id=msg_id, payload=payload)

    async def send(self, peer_id: bytes, msg: Message) -> None:
        peer = self.peers.get(peer_id)
        if peer is None or not peer.alive:
            raise ConnectionError(f"no connection to peer {peer_id.hex()[:16]}")
        await peer.outbox.put(msg)

    async def listen(self, host: str, port: int) -> None:
        self._server = await asyncio.start_server(self._accept, host, port)

    @property
    def port(self) -> int:
        return self._server.sockets[0].getsockname()[1]

    async def dial(self, host: str, port: int) -> Peer:
        async with asyncio.timeout(2):
            reader, writer = await asyncio.open_connection(host, port)
        peer = Peer(reader, writer, dialed=True)
        peer.outbox.put_nowait(self.message("handshake", "handshake"))  # dialer goes first
        self._spawn(self._run_peer(peer))
        return peer

    def maintain(self, addrs: list[str]) -> None:
        """Keep a connection to every address, redialing with backoff."""
        self._spawn(self._maintain_loop(addrs))

    async def close(self) -> None:
        if self._server:
            self._server.close()
        for task in list(self._tasks):
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)

    # ---------- connections ----------

    def _spawn(self, coro) -> asyncio.Task:
        # asyncio holds only weak references to tasks: keep a strong one,
        # or a running task can be garbage-collected mid-flight.
        task = asyncio.create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task

    def _accept(self, reader, writer) -> None:
        self._spawn(self._run_peer(Peer(reader, writer, dialed=False)))

    async def _run_peer(self, peer: Peer) -> None:
        try:
            async with asyncio.TaskGroup() as tg:
                tg.create_task(self._read_loop(peer))
                tg.create_task(self._write_loop(peer))
                tg.create_task(self._heartbeat_loop(peer))
        except* (PeerClosed, OSError, asyncio.IncompleteReadError, ValueError, KeyError) as eg:
            log.debug("connection ended: %r", eg.exceptions[0])
        finally:
            peer.closed.set()
            peer.writer.close()
            self._unregister(peer)

    async def _read_loop(self, peer: Peer) -> None:
        while True:
            msg = await read_message(peer.reader)   # EOF → IncompleteReadError
            peer.last_seen = time.monotonic()       # any message proves liveness
            if msg.type == "heartbeat":
                continue
            if msg.type == "handshake":
                await self._handshake(peer, msg)
                continue
            handler = self.handlers.get(msg.type)
            if handler is None:
                continue                            # unknown type: ignore, like Go
            try:
                await handler(msg)
            except Exception:
                log.exception("handler for %s failed", msg.type)  # one bad message ≠ dead connection

    async def _write_loop(self, peer: Peer) -> None:
        while True:
            msg = await peer.outbox.get()
            peer.writer.write(encode_frame(msg))
            await peer.writer.drain()   # backpressure: waits while the socket buffer is full

    async def _heartbeat_loop(self, peer: Peer) -> None:
        while True:
            await asyncio.sleep(HEARTBEAT_INTERVAL)
            if time.monotonic() - peer.last_seen > HEARTBEAT_TIMEOUT:
                raise PeerClosed("heartbeat timeout")
            try:
                peer.outbox.put_nowait(Message("heartbeat"))
            except asyncio.QueueFull:
                pass   # we are already sending data, which counts

    # ---------- peer pool ----------

    async def _handshake(self, peer: Peer, msg: Message) -> None:
        if peer.id != ZERO_ID:
            return                                  # duplicate handshake: ignore
        if msg.from_id == self.id:
            raise PeerClosed("connected to ourselves")

        peer.id = msg.from_id
        old = self.peers.get(peer.id)
        if old is not None and old is not peer:
            old.writer.close()                      # newest connection wins (Go: addPeer)
        else:
            CONNECTIONS.inc()
        self.peers[peer.id] = peer
        log.info("peer connected: %s", peer.id.hex())

        for callback in self.on_connect:
            callback(peer.id)
        if not peer.dialed:                         # accepting side replies
            await peer.outbox.put(self.message("handshake", "handshake"))

    def _unregister(self, peer: Peer) -> None:
        if self.peers.get(peer.id) is peer:         # only if still the registered one
            del self.peers[peer.id]
            CONNECTIONS.dec()
            log.info("peer disconnected: %s", peer.id.hex()[:16])

    # ---------- reconnection ----------

    async def _maintain_loop(self, addrs: list[str]) -> None:
        targets = {addr: _Target() for addr in addrs}
        while True:
            for addr, t in targets.items():
                if t.peer is not None and t.peer.alive:
                    t.backoff = 0.0
                    continue
                if time.monotonic() < t.next_try:
                    continue
                host, port = addr.rsplit(":", 1)
                try:
                    t.peer = await self.dial(host, int(port))
                    log.info("connected to %s", addr)
                except (OSError, TimeoutError) as e:
                    t.backoff = BACKOFF_BASE if t.backoff == 0 else min(t.backoff * 2, BACKOFF_MAX)
                    wait = t.backoff * random.uniform(0.8, 1.2)   # ±20% jitter
                    t.next_try = time.monotonic() + wait
                    log.info("reconnect to %s failed: %s (retry in %.1fs)", addr, e, wait)
            await asyncio.sleep(RECONCILE_INTERVAL)