"""Small, explicit JSON envelopes used by all transports."""

from __future__ import annotations

import time
import uuid
import zlib
from typing import Any


def envelope(message_type: str, src: str, dst: str | None = None, **payload: Any) -> dict[str, Any]:
    """Build a traceable message before low-entropy optimisation is added."""
    message: dict[str, Any] = {
        "id": uuid.uuid4().hex,
        "type": message_type,
        "src": src,
        "timestamp": time.time(),
        "payload": payload,
    }
    if dst is not None:
        message["dst"] = dst
    return message


def payload_of(message: dict[str, Any]) -> dict[str, Any]:
    payload = message.get("payload", {})
    if not isinstance(payload, dict):
        raise ValueError("message payload must be a JSON object")
    return payload


def compact_agent_code(agent_id: str) -> int:
    """Stable 32-bit code used only after the full REGISTER handshake."""
    return zlib.crc32(agent_id.encode("utf-8")) & 0xFFFFFFFF


def compact_heartbeat(agent_code: int, session_epoch: int, sequence: int) -> dict[str, int | str]:
    """Build protocol-v2 liveness-only heartbeat without a JSON envelope."""
    if any(type(value) is not int or value < 0 for value in (agent_code, session_epoch, sequence)):
        raise ValueError("compact heartbeat fields must be non-negative integers")
    return {"v": 2, "t": "HB", "a": agent_code, "e": session_epoch, "n": sequence}


def compact_heartbeat_fields(message: dict[str, Any]) -> tuple[int, int, int] | None:
    """Validate and unpack a protocol-v2 compact heartbeat."""
    if message.get("v") != 2 or message.get("t") != "HB":
        return None
    values = (message.get("a"), message.get("e"), message.get("n"))
    if any(type(value) is not int or value < 0 for value in values):
        raise ValueError("invalid compact heartbeat")
    agent_code, session_epoch, sequence = values
    return agent_code, session_epoch, sequence
