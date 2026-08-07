"""Stable integration contract for future device-edge-cloud scheduling.

This module intentionally contains contracts and local eligibility checks, not
an actual placement algorithm. A scheduling teammate can implement
``EdgeCloudSchedulingPort`` and pass it into ``Coordinator`` without changing
the registry, MQTT, task-state, or recovery code.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any, Protocol, Sequence

if TYPE_CHECKING:
    from .models import AgentProfile, AgentRecord
    from .tasks import TaskSpec


class ExecutionTier(str, Enum):
    DEVICE = "device"
    EDGE = "edge"
    CLOUD = "cloud"


class DataSensitivity(str, Enum):
    PUBLIC = "public"
    INTERNAL = "internal"
    CONFIDENTIAL = "confidential"
    RESTRICTED = "restricted"


@dataclass(frozen=True)
class ResourceDescriptor:
    """Static resource capacity advertised by an Agent execution endpoint."""

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


@dataclass(frozen=True)
class PlacementConstraints:
    """Task-declared constraints consumed by a future placement scheduler."""

    allowed_tiers: frozenset[ExecutionTier] = frozenset(ExecutionTier)
    preferred_tier: ExecutionTier | None = None
    max_latency_ms: float | None = None
    min_memory_mb: int = 0
    min_gpu_count: int = 0
    data_sensitivity: DataSensitivity = DataSensitivity.INTERNAL
    require_local_data: bool = False

    @classmethod
    def from_dict(cls, raw: dict[str, Any] | None) -> "PlacementConstraints":
        raw = raw or {}
        tiers = raw.get("allowed_tiers")
        allowed_tiers = frozenset(ExecutionTier(item) for item in tiers) if tiers is not None else frozenset(ExecutionTier)
        if not allowed_tiers:
            raise ValueError("placement.allowed_tiers cannot be empty")
        preferred = raw.get("preferred_tier")
        preferred_tier = ExecutionTier(preferred) if preferred is not None else None
        if preferred_tier is not None and preferred_tier not in allowed_tiers:
            raise ValueError("placement.preferred_tier must be one of allowed_tiers")
        latency = raw.get("max_latency_ms")
        return cls(
            allowed_tiers=allowed_tiers,
            preferred_tier=preferred_tier,
            max_latency_ms=float(latency) if latency is not None else None,
            min_memory_mb=max(0, int(raw.get("min_memory_mb", 0))),
            min_gpu_count=max(0, int(raw.get("min_gpu_count", 0))),
            data_sensitivity=DataSensitivity(raw.get("data_sensitivity", DataSensitivity.INTERNAL.value)),
            require_local_data=bool(raw.get("require_local_data", False)),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "allowed_tiers": sorted(tier.value for tier in self.allowed_tiers),
            "preferred_tier": self.preferred_tier.value if self.preferred_tier else None,
            "max_latency_ms": self.max_latency_ms,
            "min_memory_mb": self.min_memory_mb,
            "min_gpu_count": self.min_gpu_count,
            "data_sensitivity": self.data_sensitivity.value,
            "require_local_data": self.require_local_data,
        }


@dataclass(frozen=True)
class PlacementRequest:
    task_id: str
    required_skills: frozenset[str]
    constraints: PlacementConstraints
    metadata: dict[str, Any]

    @classmethod
    def from_task(cls, task: "TaskSpec") -> "PlacementRequest":
        return cls(
            task_id=task.task_id,
            required_skills=task.required_skills,
            constraints=task.placement,
            metadata=dict(task.metadata),
        )


@dataclass(frozen=True)
class PlacementDecision:
    """Decision returned by an external device-edge-cloud scheduler."""

    agent_id: str
    tier: ExecutionTier
    scheduler: str
    reason: str = ""
    score: float | None = None
    reservation_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "agent_id": self.agent_id,
            "tier": self.tier.value,
            "scheduler": self.scheduler,
            "reason": self.reason,
            "score": self.score,
            "reservation_id": self.reservation_id,
        }


class EdgeCloudSchedulingPort(Protocol):
    """Injection point owned by the future device-edge-cloud scheduler.

    The coordinator supplies only agents that already satisfy basic skill,
    load, tier, and resource constraints. The scheduler may account for live
    network quality, queueing delay, energy, data locality, and cloud cost.
    Returning ``None`` delegates to the existing local dynamic router.
    """

    async def select(
        self,
        request: PlacementRequest,
        candidates: Sequence["AgentRecord"],
    ) -> PlacementDecision | None: ...


def agent_matches_placement(profile: "AgentProfile", constraints: PlacementConstraints) -> bool:
    """Apply deterministic constraints before an external scheduler runs."""
    if profile.tier not in constraints.allowed_tiers:
        return False
    resources = profile.resources
    return resources.memory_mb >= constraints.min_memory_mb and resources.gpu_count >= constraints.min_gpu_count
