import asyncio

import pytest

from p2plab import node as node_mod
from p2plab.node import Node


async def wait_for(cond, timeout=3.0):
    async with asyncio.timeout(timeout):
        while not cond():
            await asyncio.sleep(0.02)


@pytest.fixture
def fast_heartbeat(monkeypatch):
    monkeypatch.setattr(node_mod, "HEARTBEAT_INTERVAL", 0.05)
    monkeypatch.setattr(node_mod, "HEARTBEAT_TIMEOUT", 0.15)


async def pair(tmp_path):
    a, b = Node(tmp_path / "a.key"), Node(tmp_path / "b.key")
    await b.listen("127.0.0.1", 0)
    await a.dial("127.0.0.1", b.port)
    await wait_for(lambda: b.id in a.peers and a.id in b.peers)
    return a, b


async def test_handshake_registers_both_sides(tmp_path):
    a, b = await pair(tmp_path)
    await a.close()
    await b.close()


async def test_message_reaches_handler(tmp_path):
    a, b = await pair(tmp_path)
    received: asyncio.Queue = asyncio.Queue()

    async def on_ping(msg):
        await received.put(msg)

    b.on("ping", on_ping)
    await a.send(b.id, a.message("ping", "p-1"))
    msg = await asyncio.wait_for(received.get(), 2)
    assert msg.id == "p-1" and msg.from_id == a.id

    await a.close()
    await b.close()


async def test_heartbeat_closes_silent_peer(tmp_path, fast_heartbeat):
    async def silent(reader, writer):   # accepts, never sends anything
        await reader.read()

    server = await asyncio.start_server(silent, "127.0.0.1", 0)
    a = Node(tmp_path / "a.key")
    peer = await a.dial("127.0.0.1", server.sockets[0].getsockname()[1])

    await wait_for(lambda: not peer.alive, timeout=2)
    await a.close()
    server.close()


async def test_heartbeat_keeps_idle_peers_alive(tmp_path, fast_heartbeat):
    a, b = await pair(tmp_path)
    await asyncio.sleep(0.5)            # many timeouts' worth of silence...
    assert b.id in a.peers and a.id in b.peers   # ...but heartbeats kept it alive
    await a.close()
    await b.close()