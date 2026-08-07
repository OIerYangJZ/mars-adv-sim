"""Data models for the dynamic-registration simulation."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from .edge_cloud import ExecutionTier, ResourceDescriptor


class AgentStatus(str, Enum):
    JOINING = "JOINING"
    ONLINE = "ONLINE"
    SUSPECT = "SUSPECT"
    OFFLINE = "OFFLINE"


@dataclass(frozen=True)
class AgentProfile:
    agent_id: str
    name: str
    skills: tuple[str, ...]
    endpoint: str
    max_load: int = 1
    reliability: float = 0.95
    tier: ExecutionTier = ExecutionTier.DEVICE
    resources: ResourceDescriptor = field(default_factory=ResourceDescriptor)
    labels: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        # Keep direct Python construction as tolerant as JSON registration.
        object.__setattr__(self, "tier", ExecutionTier(self.tier))
        if not isinstance(self.resources, ResourceDescriptor):
            object.__setattr__(self, "resources", ResourceDescriptor.from_dict(self.resources))
        object.__setattr__(self, "labels", tuple(self.labels))

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "AgentProfile":
        return cls(
            agent_id=str(raw["agent_id"]),
            name=str(raw["name"]),
            skills=tuple(str(item) for item in raw["skills"]),
            endpoint=str(raw.get("endpoint", "")),
            max_load=int(raw.get("max_load", 1)),
            reliability=float(raw.get("reliability", 0.95)),
            tier=ExecutionTier(raw.get("tier", ExecutionTier.DEVICE.value)),
            resources=ResourceDescriptor.from_dict(raw.get("resources")),
            labels=tuple(str(item) for item in raw.get("labels", ())),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "agent_id": self.agent_id,
            "name": self.name,
            "skills": list(self.skills),
            "endpoint": self.endpoint,
            "max_load": self.max_load,
            "reliability": self.reliability,
            "tier": self.tier.value,
            "resources": self.resources.to_dict(),
            "labels": list(self.labels),
        }


@dataclass
class AgentRecord:
    profile: AgentProfile
    session_id: str
    status: AgentStatus
    current_load: int
    joined_at: float
    last_heartbeat: float
    updated_at: float


@dataclass(frozen=True)
class RegistryEvent:
    event_type: str
    agent_id: str
    timestamp: float
    topology_version: int
    detail: dict[str, Any] = field(default_factory=dict)
