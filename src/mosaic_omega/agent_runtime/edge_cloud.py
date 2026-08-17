"""Minimal device/edge/cloud resource contract used by the authoritative scheduler."""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any


class ExecutionTier(str, Enum):
    DEVICE = "device"
    EDGE = "edge"
    CLOUD = "cloud"


@dataclass(frozen=True)
class ResourceDescriptor:
    cpu_cores: int = 0
    memory_mb: int = 0
    gpu_count: int = 0
    accelerator_tags: tuple[str, ...] = ()

    @classmethod
    def from_dict(cls, raw: dict[str, Any] | None) -> "ResourceDescriptor":
        raw = raw or {}
        return cls(
            cpu_cores=max(0, int(raw.get("cpu_cores", 0))),
            memory_mb=max(0, int(raw.get("memory_mb", 0))),
            gpu_count=max(0, int(raw.get("gpu_count", 0))),
            accelerator_tags=tuple(str(item) for item in raw.get("accelerator_tags", ())),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "cpu_cores": self.cpu_cores,
            "memory_mb": self.memory_mb,
            "gpu_count": self.gpu_count,
            "accelerator_tags": list(self.accelerator_tags),
        }
