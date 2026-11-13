import asyncio

from p2plab.dht import DHT, distance
from p2plab.node import Node


async def wait_for(cond, timeout=3.0):
    async with asyncio.timeout(timeout):
        while not cond():
            await asyncio.sleep(0.02)


async def mesh(tmp_path, n):
    nodes = [Node(tmp_path / f"n{i}.key") for i in range(n)]
    dhts = [DHT(node) for node in nodes]
    for node in nodes:
        await node.listen("127.0.0.1", 0)
    for i, a in enumerate(nodes):
        for b in nodes[i + 1:]:
            await a.dial("127.0.0.1", b.port)
    await wait_for(lambda: all(len(x.peers) == n - 1 for x in nodes))
    return nodes, dhts


def test_xor_distance():
    a, b = bytes(32), bytes([1] * 32)
    assert distance(a, a) == 0
    assert distance(a, b) == distance(b, a) > 0


async def test_find_value_from_remote_node(tmp_path):
    nodes, dhts = await mesh(tmp_path, 4)
    await dhts[0].store("hello", b"world")
    await wait_for(lambda: "hello" in dhts[3].values)

    dhts[3].values.clear()                  # force a real network lookup
    assert await dhts[3].find_value("hello") == [b"world"]

    for node in nodes:
        await node.close()


async def test_missing_key(tmp_path):
    nodes, dhts = await mesh(tmp_path, 3)
    assert await dhts[1].find_value("nope") is None
    for node in nodes:
        await node.close()