"""Kademlia-style DHT, compatible with go/internal/dht.

the routing table is a plain dict sorted by XOR
distance on demand, instead of 256 k-buckets. Equivalent for small
clusters; real Kademlia needs buckets to bound the table's size.

Handlers run on the connection's read loop, so they must never await an
RPC reply themselves: the reply would arrive on that same, blocked loop.
"""
from __future__ import annotations

import asyncio
import base64
import hashlib
import itertools

from prometheus_client import Histogram

from .node import Node
from .wire import Message

K = 8
ALPHA = 3
RPC_TIMEOUT = 5.0
STRAGGLER_WAIT = 0.05
MAX_VALUES = 20

LOOKUP = Histogram("p2p_dht_lookup_duration_seconds", "End-to-end DHT lookup latency in seconds.")

Entry = tuple[bytes, str]   # (NodeID, address)


def distance(a: bytes, b: bytes) -> int:
    """XOR distance. Python ints have no size limit: 256 bits in one line."""
    return int.from_bytes(a, "big") ^ int.from_bytes(b, "big")


def hash_key(key: str) -> bytes:
    return hashlib.sha256(key.encode()).digest()


def encode_entries(entries: list[Entry]) -> list[dict]:
    # Go's Entry struct has no json tags, so its keys are "ID" and "Addr"
    return [{"ID": list(peer_id), "Addr": addr} for peer_id, addr in entries]


def decode_entries(payload: dict | None) -> list[Entry]:
    return [(bytes(e["ID"]), e.get("Addr", "")) for e in (payload or {}).get("nodes") or []]


class DHT:
    def __init__(self, node: Node, addr: str = ""):
        self.node = node
        self.addr = addr
        self.table: dict[bytes, str] = {}           # NodeID → address
        self.values: dict[str, list[bytes]] = {}    # key → set of values
        self.pending: dict[str, asyncio.Future] = {}
        self._seq = itertools.count()

        node.on_connect.append(lambda peer_id: self.add(peer_id, ""))
        node.on("dht_find_node", self._on_find_node)
        node.on("dht_find_value", self._on_find_value)
        node.on("dht_store", self._on_store)
        node.on("dht_find_reply", self._on_reply)
        node.on("dht_value_reply", self._on_reply)

    # ---------- routing table and storage ----------

    def add(self, peer_id: bytes, addr: str) -> None:
        if peer_id == self.node.id:
            return
        if addr or peer_id not in self.table:
            self.table[peer_id] = addr

    def closest(self, target: bytes, n: int = K) -> list[Entry]:
        return sorted(self.table.items(), key=lambda e: distance(e[0], target))[:n]

    def _store_local(self, key: str, value: bytes) -> None:
        values = self.values.setdefault(key, [])
        if value not in values:
            values.append(value)
            del values[:-MAX_VALUES]                # keep only the newest MAX_VALUES

    # ---------- public operations ----------

    async def find_node(self, target: bytes) -> list[Entry]:
        """Iterative lookup for the K nodes closest to target."""
        with LOOKUP.time():
            candidates = self.closest(target)
            queried = {self.node.id}
            seen = {self.node.id, *(peer_id for peer_id, _ in candidates)}

            while batch := [c for c in candidates if c[0] not in queried][:ALPHA]:
                queried.update(peer_id for peer_id, _ in batch)
                replies = await self._gather_fast(
                    [self._ask_find_node(peer_id, target) for peer_id, _ in batch]
                )
                added = False
                for entry in (e for reply in replies for e in reply):
                    if entry[0] in seen:
                        continue
                    seen.add(entry[0])
                    self.add(*entry)
                    candidates.append(entry)
                    added = True
                candidates = sorted(candidates, key=lambda e: distance(e[0], target))[:K]
                if not added:
                    break                           # no progress: converged
            return candidates

    async def store(self, key: str, value: bytes) -> None:
        """Store locally and at the K nodes closest to the key."""
        self._store_local(key, value)
        payload = {"key": key, "value": base64.b64encode(value).decode()}
        for peer_id, _ in await self.find_node(hash_key(key)):
            try:
                await self.node.send(peer_id, self.node.message("dht_store", self._next_id("st"), payload))
            except ConnectionError:
                pass                                # best-effort replication, like Go

    async def find_value(self, key: str) -> list[bytes] | None:
        if values := self.values.get(key):
            return list(values)

        target = hash_key(key)
        candidates = self.closest(target)
        queried = {self.node.id}
        while batch := [c for c in candidates if c[0] not in queried][:ALPHA]:
            for peer_id, _ in batch:
                queried.add(peer_id)
                msg = self.node.message("dht_find_value", self._next_id("fv"), {"key": key, "value": None})
                try:
                    reply = await self._request(peer_id, msg)
                except (ConnectionError, TimeoutError):
                    continue
                if reply.type == "dht_value_reply" and (vals := (reply.payload or {}).get("values")):
                    return [base64.b64decode(v) for v in vals]
                known = {c[0] for c in candidates}
                for entry in decode_entries(reply.payload):
                    if entry[0] not in known and entry[0] != self.node.id:
                        self.add(*entry)
                        candidates.append(entry)
            candidates.sort(key=lambda e: distance(e[0], target))
        return None

    # ---------- RPC plumbing ----------

    def _next_id(self, prefix: str) -> str:
        return f"{prefix}-{self.node.id.hex()[:8]}-{next(self._seq)}"

    async def _request(self, peer_id: bytes, msg: Message) -> Message:
        """Send a request and wait for the reply with the same id.
        Fails fast if the peer disconnects (Go follow-up 3)."""
        peer = self.node.peers.get(peer_id)
        if peer is None:
            raise ConnectionError("not connected")

        reply = asyncio.get_running_loop().create_future()
        self.pending[msg.id] = reply
        closed = asyncio.create_task(peer.closed.wait())
        try:
            await self.node.send(peer_id, msg)
            done, _ = await asyncio.wait(
                {reply, closed}, timeout=RPC_TIMEOUT, return_when=asyncio.FIRST_COMPLETED
            )
            if reply in done:
                return reply.result()
            if closed in done:
                raise ConnectionError("peer disconnected")
            raise TimeoutError("RPC timed out")
        finally:
            closed.cancel()
            self.pending.pop(msg.id, None)

    async def _ask_find_node(self, peer_id: bytes, target: bytes) -> list[Entry] | None:
        msg = self.node.message("dht_find_node", self._next_id("fn"),
                                {"target": list(target), "addr": self.addr})
        try:
            return decode_entries((await self._request(peer_id, msg)).payload)
        except (ConnectionError, TimeoutError):
            return None

    async def _gather_fast(self, coros) -> list[list[Entry]]:
        """Run RPCs in parallel; after the first good reply, wait at most
        STRAGGLER_WAIT for the rest (Go follow-up 5)."""
        pending = {asyncio.create_task(c) for c in coros}
        results: list[list[Entry]] = []
        deadline = None
        loop = asyncio.get_running_loop()
        while pending:
            timeout = None if deadline is None else max(0.0, deadline - loop.time())
            done, pending = await asyncio.wait(pending, timeout=timeout,
                                               return_when=asyncio.FIRST_COMPLETED)
            if not done:
                break                               # straggler wait is over
            for task in done:
                if (entries := task.result()) is not None:
                    results.append(entries)
                    if deadline is None:
                        deadline = loop.time() + STRAGGLER_WAIT
        for task in pending:
            task.cancel()                           # slow peers: stop waiting
        return results

    # ---------- answering requests (from Go or Python peers) ----------

    async def _on_find_node(self, msg: Message) -> None:
        payload = msg.payload or {}
        self.add(msg.from_id, payload.get("addr", ""))
        await self._reply_nodes(msg, bytes(payload.get("target") or bytes(32)))

    async def _reply_nodes(self, msg: Message, target: bytes) -> None:
        payload = {"nodes": encode_entries(self.closest(target))}
        await self.node.send(msg.from_id, self.node.message("dht_find_reply", msg.id, payload))

    async def _on_store(self, msg: Message) -> None:
        payload = msg.payload or {}
        self._store_local(payload["key"], base64.b64decode(payload.get("value") or ""))

    async def _on_find_value(self, msg: Message) -> None:
        key = (msg.payload or {}).get("key", "")
        if values := self.values.get(key):
            payload = {"values": [base64.b64encode(v).decode() for v in values]}
            await self.node.send(msg.from_id, self.node.message("dht_value_reply", msg.id, payload))
        else:
            await self._reply_nodes(msg, hash_key(key))

    async def _on_reply(self, msg: Message) -> None:
        if (future := self.pending.get(msg.id)) and not future.done():
            future.set_result(msg)