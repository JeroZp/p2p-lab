"""Wire format shared with the Go implementation"""
from __future__ import annotations

import asyncio
import json
import struct
from dataclasses import dataclass

MAX_MESSAGE_SIZE = 10 * 1024 * 1024
ZERO_ID = bytes(32)


@dataclass
class Message:
    type: str
    from_id: bytes = ZERO_ID   # "from" is a Python keyword
    id: str = ""
    payload: dict | None = None

    def to_json(self) -> bytes:
        body = {"type": self.type, "from": list(self.from_id), "id": self.id}
        if self.payload is not None:      # Go: `json:"payload,omitempty"`
            body["payload"] = self.payload
        return json.dumps(body, separators=(",", ":")).encode()

    @classmethod
    def from_json(cls, data: bytes) -> Message:
        body = json.loads(data)
        return cls(
            type=body["type"],
            from_id=bytes(body.get("from") or ZERO_ID),
            id=body.get("id", ""),
            payload=body.get("payload"),
        )


def encode_frame(msg: Message) -> bytes:
    """Length-prefixed message, ready to write to a stream."""
    body = msg.to_json()
    return struct.pack(">I", len(body)) + body


def decode_frame(data: bytes) -> Message:
    """Parse one complete frame from bytes (used by tests)."""
    (length,) = struct.unpack(">I", data[:4])
    return Message.from_json(data[4 : 4 + length])


async def read_message(reader: asyncio.StreamReader) -> Message:
    """Read exactly one message from a stream."""
    (length,) = struct.unpack(">I", await reader.readexactly(4))
    if length > MAX_MESSAGE_SIZE:
        raise ValueError(f"message too large: {length} bytes")
    return Message.from_json(await reader.readexactly(length))