"""Raw message-event observation for communication baseline collection.

This module intentionally records individual events only. Aggregation,
redundancy analysis, and low-entropy optimisation are separate later stages.
"""

from __future__ import annotations

import json
import os
import threading
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Literal, Protocol

Direction = Literal["inbound", "outbound"]


def _encoded_size(value: object) -> int:
    return len(json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))


def _field_paths(value: object, prefix: str = "", paths: list[str] | None = None) -> tuple[str, ...]:
    """Return structure only, never message values, to keep logs lightweight."""
    paths = [] if paths is None else paths
    if isinstance(value, dict):
        for key, child in value.items():
            child_path = f"{prefix}.{key}" if prefix else str(key)
            _field_paths(child, child_path, paths)
    elif isinstance(value, list):
        paths.append(f"{prefix}[]")
    else:
        paths.append(prefix)
    return tuple(paths)


@dataclass(frozen=True)
class MessageEvent:
    """One application-message event at a transport boundary."""

    observed_at: float
    direction: Direction
    transport: str
    client_id: str
    topic: str
    qos: int | None
    delivery_status: str
    duplicate_flag: bool
    message_id: str | None
    message_type: str | None
    src: str | None
    dst: str | None
    session_id: str | None
    task_id: str | None
    payload_bytes: int
    message_bytes: int
    field_paths: tuple[str, ...]

    @classmethod
    def from_message(
        cls,
        *,
        direction: Direction,
        transport: str,
        client_id: str,
        topic: str,
        message: dict[str, Any],
        qos: int | None,
        delivery_status: str,
        duplicate_flag: bool = False,
    ) -> "MessageEvent":
        payload = message.get("payload")
        payload_dict = payload if isinstance(payload, dict) else {
            key: value for key, value in message.items() if key not in {"v", "t"}
        }
        raw_type = message.get("type")
        if not isinstance(raw_type, str):
            # Direct low-entropy TaskMessage frames intentionally have no
            # envelope. Their MQTT topic determines the business category.
            if topic == "control/task/result":
                raw_type = "TASK_RESULT"
            elif topic == "control/task/context/update":
                raw_type = "TASK_CONTEXT_UPDATE"
            elif topic.endswith("/context/snapshot"):
                raw_type = "TASK_CONTEXT_SNAPSHOT"
            elif topic.endswith("/context"):
                raw_type = "TASK_CONTEXT_UPDATE"
        compact_type = "HEARTBEAT_V2" if message.get("v") == 2 and message.get("t") == "HB" else None
        compact_message_id: str | None = None
        if compact_type is not None:
            agent_code, epoch, sequence = message.get("a"), message.get("e"), message.get("n")
            if all(type(value) is int and value >= 0 for value in (agent_code, epoch, sequence)):
                # Compact heartbeats intentionally omit the normal envelope ID.
                # Their registered agent code, epoch, and monotonic sequence
                # still form a stable logical delivery identifier for metrics.
                compact_message_id = f"hb2:{agent_code}:{epoch}:{sequence}"
        direct_task_message_id = message.get("message_id")
        if not isinstance(direct_task_message_id, str):
            direct_task_message_id = None
        return cls(
            observed_at=time.time(),
            direction=direction,
            transport=transport,
            client_id=client_id,
            topic=topic,
            qos=qos,
            delivery_status=delivery_status,
            duplicate_flag=duplicate_flag,
            message_id=(
                message.get("id")
                if isinstance(message.get("id"), str)
                else direct_task_message_id or compact_message_id
            ),
            message_type=raw_type if isinstance(raw_type, str) else compact_type,
            src=message.get("src") if isinstance(message.get("src"), str) else None,
            dst=message.get("dst") if isinstance(message.get("dst"), str) else None,
            session_id=payload_dict.get("session_id") if isinstance(payload_dict.get("session_id"), str) else None,
            task_id=payload_dict.get("task_id") if isinstance(payload_dict.get("task_id"), str) else None,
            payload_bytes=_encoded_size(payload_dict),
            message_bytes=_encoded_size(message),
            field_paths=_field_paths(message),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class MessageObserver(Protocol):
    def record(self, event: MessageEvent) -> None: ...


class NullMessageObserver:
    """Default observer that adds no I/O when baseline capture is disabled."""

    def record(self, event: MessageEvent) -> None:
        return


class JsonlMessageObserver:
    """Append raw events to a per-process JSONL file without message bodies."""

    def __init__(self, directory: str, client_id: str) -> None:
        target = Path(directory)
        target.mkdir(parents=True, exist_ok=True)
        safe_client_id = "".join(char if char.isalnum() or char in "-_" else "_" for char in client_id)
        self.path = target / f"communication-{safe_client_id}-{os.getpid()}.jsonl"
        self._lock = threading.Lock()

    def record(self, event: MessageEvent) -> None:
        # Telemetry must never interrupt task communication. If local logging
        # becomes unavailable, the caller continues and the baseline has a gap.
        try:
            line = json.dumps(event.to_dict(), ensure_ascii=False, separators=(",", ":"))
            with self._lock, self.path.open("a", encoding="utf-8") as stream:
                stream.write(line + "\n")
        except (OSError, TypeError, ValueError) as exc:
            print(f"[observability] unable to record message event: {exc}")
