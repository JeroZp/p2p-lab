"""Run a Python node:  python -m p2plab --addr HOST:PORT --dial a:p,b:p"""
import argparse
import asyncio
import logging

from aiohttp import web
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from .node import Node


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="p2plab")
    p.add_argument("--addr", default="127.0.0.1:7100", help="P2P address to listen on")
    p.add_argument("--dial", default="", help="comma-separated peer addresses")
    p.add_argument("--key", default="node_data/py-node.key", help="Ed25519 key file")
    p.add_argument("--http", default="127.0.0.1:9106", help="HTTP address for /metrics")
    return p.parse_args()


async def metrics(_request: web.Request) -> web.Response:
    return web.Response(body=generate_latest(), headers={"Content-Type": CONTENT_TYPE_LATEST})


async def main() -> None:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

    node = Node(args.key)
    host, port = args.addr.rsplit(":", 1)
    await node.listen(host, int(port))
    logging.info("node %s listening on %s", node.id.hex(), args.addr)

    if args.dial:
        node.maintain([a.strip() for a in args.dial.split(",")])

    app = web.Application()
    app.router.add_get("/metrics", metrics)
    runner = web.AppRunner(app)
    await runner.setup()
    http_host, http_port = args.http.rsplit(":", 1)
    await web.TCPSite(runner, http_host, int(http_port)).start()
    logging.info("HTTP on http://%s", args.http)

    try:
        await asyncio.Event().wait()   # run until Ctrl+C
    finally:
        await node.close()
        await runner.cleanup()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass