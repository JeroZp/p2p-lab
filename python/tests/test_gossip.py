import asyncio

from p2plab.gossip import Gossip
from p2plab.node import Node


async def wait_for(cond, timeout=3.0):
    async with asyncio.timeout(timeout):
        while not cond():
            await asyncio.sleep(0.02)


async def line(tmp_path, n):
    """n nodes connected in a line: 0 - 1 - 2 - ..."""
    nodes = [Node(tmp_path / f"n{i}.key") for i in range(n)]
    engines = [Gossip(node) for node in nodes]
    for node in nodes:
        await node.listen("127.0.0.1", 0)
    for a, b in zip(nodes, nodes[1:]):
        await a.dial("127.0.0.1", b.port)
    expected = [1 if i in (0, n - 1) else 2 for i in range(n)]
    await wait_for(lambda: [len(x.peers) for x in nodes] == expected)
    return nodes, engines


async def test_gossip_crosses_multiple_hops(tmp_path):
    nodes, engines = await line(tmp_path, 4)
    await engines[0].broadcast(b"hello")
    for g in engines[1:]:
        assert await asyncio.wait_for(g.received.get(), 2) == b"hello"
    for node in nodes:
        await node.close()


async def test_duplicate_is_delivered_once(tmp_path):
    nodes, engines = await line(tmp_path, 2)
    msg = nodes[0].message("gossip", "dup-1", {"ttl": 1, "data": "aGk=", "sent_at": 0})
    await engines[1].handle(msg)
    await engines[1].handle(msg)
    assert engines[1].received.qsize() == 1
    for node in nodes:
        await node.close()