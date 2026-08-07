"""A dependency-free local pub/sub server for multi-process development."""

from __future__ import annotations

import argparse
import asyncio
import json
from typing import Any


class LocalBusServer:
    def __init__(self) -> None:
        self._subscriptions: dict[asyncio.StreamWriter, set[str]] = {}

    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self._subscriptions[writer] = set()
        peer = writer.get_extra_info("peername")
        print(f"[local-bus] client connected: {peer}")
        try:
            while line := await reader.readline():
                frame = json.loads(line.decode("utf-8"))
                operation = frame.get("op")
                if operation == "subscribe":
                    self._subscriptions[writer].add(frame["topic"])
                elif operation == "publish":
                    await self.publish(frame["topic"], frame["payload"])
        except (ConnectionError, json.JSONDecodeError) as exc:
            print(f"[local-bus] client error {peer}: {exc}")
        finally:
            self._subscriptions.pop(writer, None)
            writer.close()
            await writer.wait_closed()
            print(f"[local-bus] client disconnected: {peer}")

    async def publish(self, topic: str, payload: dict[str, Any]) -> None:
        frame = (json.dumps({"op": "message", "topic": topic, "payload": payload}, ensure_ascii=False) + "\n").encode("utf-8")
        stale: list[asyncio.StreamWriter] = []
        for writer, topics in list(self._subscriptions.items()):
            if topic not in topics:
                continue
            try:
                writer.write(frame)
                await writer.drain()
            except ConnectionError:
                stale.append(writer)
        for writer in stale:
            self._subscriptions.pop(writer, None)


async def run(host: str, port: int) -> None:
    bus = LocalBusServer()
    server = await asyncio.start_server(bus.handle_client, host, port)
    address = ", ".join(str(sock.getsockname()) for sock in server.sockets or [])
    print(f"[local-bus] listening on {address}")
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="本地多进程消息总线")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args()
    asyncio.run(run(args.host, args.port))
