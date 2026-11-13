"""Epidemic broadcast, compatible with go/internal/gossip."""
from __future__ import annotations

import asyncio
import base64
import random
import time

from prometheus_client import Counter, Histogram

from .node import Node
from .wire import Message

FAN_OUT = 3
DEFAULT_TTL = 5
SEEN_TTL = 600.0       # seconds to remember a message ID
EVICT_EVERY = 60.0     # at most one cleanup per minute

FORWARDED = Counter("p2p_gossip_forwarded_total", "Gossip messages forwarded to peers.")
DUPLICATES = Counter("p2p_gossip_duplicates_total", "Gossip messages dropped as duplicates.")
DELIVERY = Histogram(
    "p2p_gossip_delivery_seconds",
    "Time from broadcast at the originator to first delivery on this node.",
    buckets=[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1],
)


class Gossip:
    def __init__(self, node: Node):
        self.node = node
        self.seen: dict[str, float] = {}       # message ID → when we saw it
        self.received: asyncio.Queue[bytes] = asyncio.Queue(maxsize=64)
        self._last_evict = time.monotonic()
        node.on("gossip", self.handle)

    async def broadcast(self, data: bytes) -> None:
        msg_id = f"{self.node.id.hex()}-{time.time_ns()}"
        payload = {
            "ttl": DEFAULT_TTL,
            "data": base64.b64encode(data).decode(),
            "sent_at": time.time_ns(),
        }
        self._mark_seen(msg_id)
        await self._forward(self.node.message("gossip", msg_id, payload))

    async def handle(self, msg: Message) -> None:
        if msg.id in self.seen:
            DUPLICATES.inc()
            return
        self._mark_seen(msg.id)

        payload = msg.payload or {}
        if sent_at := payload.get("sent_at"):
            DELIVERY.observe(max(0.0, (time.time_ns() - sent_at) / 1e9))

        data = base64.b64decode(payload.get("data") or "")
        try:
            self.received.put_nowait(data)
        except asyncio.QueueFull:
            pass                       # nobody reading: don't block forwarding

        ttl = payload.get("ttl", 0)
        if ttl > 1:
            # Same id and original sender; only the TTL changes
            fwd = Message(msg.type, msg.from_id, msg.id, {**payload, "ttl": ttl - 1})
            await self._forward(fwd)

    async def _forward(self, msg: Message) -> None:
        peers = list(self.node.peers)
        for peer_id in random.sample(peers, min(FAN_OUT, len(peers))):
            try:
                await self.node.send(peer_id, msg)
                FORWARDED.inc()
            except ConnectionError:
                pass                   # peer just went away

    def _mark_seen(self, msg_id: str) -> None:
        now = time.monotonic()
        self.seen[msg_id] = now
        if now - self._last_evict > EVICT_EVERY:
            cutoff = now - SEEN_TTL
            self.seen = {k: t for k, t in self.seen.items() if t > cutoff}
            self._last_evict = now