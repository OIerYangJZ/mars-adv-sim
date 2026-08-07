"""Adapter from the existing resource scheduler to the runtime placement port."""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from .resource_scheduler import NoSchedulableNodeError, ResourceNode, ResourceScheduler, TaskRequirement
from ..runtime.edge_cloud import (
    EdgeCloudSchedulingPort,
    ExecutionTier,
    PlacementConstraints,
    PlacementDecision,
    PlacementRequest,
    agent_matches_placement,
)
from ..runtime.models import AgentRecord

_DEFAULT_LATENCY_MS = {
    ExecutionTier.DEVICE: 5.0,
    ExecutionTier.EDGE: 30.0,
    ExecutionTier.CLOUD: 120.0,
}


def _metadata_for(record: AgentRecord) -> dict[str, Any]:
    metadata = getattr(record, "metadata", None)
    if isinstance(metadata, dict):
        return metadata
    profile_metadata = getattr(record.profile, "metadata", None)
    return profile_metadata if isinstance(profile_metadata, dict) else {}


def _metric(record: AgentRecord, *names: str) -> Any:
    metadata = _metadata_for(record)
    for name in names:
        value = getattr(record, name, None)
        if value is not None:
            return value
        if name in metadata:
            return metadata[name]
    return None


def _latency_ms(record: AgentRecord) -> float:
    raw = _metric(record, "latency_ms", "network_latency_ms", "rtt_ms")
    if raw is not None:
        try:
            return max(0.0, float(raw))
        except (TypeError, ValueError):
            pass
    return _DEFAULT_LATENCY_MS[record.profile.tier]


def _compute_score(record: AgentRecord) -> float:
    resources = record.profile.resources
    return max(0.1, float(resources.cpu_cores) + float(resources.memory_mb) / 2048.0 + float(resources.gpu_count) * 4.0)


def _local_data_available(request: PlacementRequest, record: AgentRecord) -> bool:
    if not request.constraints.require_local_data:
        return True
    metadata = _metadata_for(record)
    allowed_ids = metadata.get("local_data_agent_ids") or request.metadata.get("local_data_agent_ids")
    if allowed_ids is not None:
        return record.profile.agent_id in {str(item) for item in allowed_ids}
    labels = set(record.profile.labels)
    if "local-data" in labels or "local_data" in labels:
        return True
    return record.profile.tier is not ExecutionTier.CLOUD


class ResourceSchedulerPlacementAdapter(EdgeCloudSchedulingPort):
    scheduler_name = "resource_scheduler_baseline"

    def __init__(self, weights: Mapping[str, float] | None = None) -> None:
        self._weights = dict(weights or {})

    @staticmethod
    def _node(record: AgentRecord, required_skills: set[str]) -> ResourceNode:
        profile = record.profile
        max_load = max(int(profile.max_load), 1)
        current_load = min(1.0, max(0.0, float(record.current_load) / max_load))
        skills = set(required_skills) or {"__any__"}
        skills.update(profile.skills)
        return ResourceNode(
            node_id=profile.agent_id,
            layer=profile.tier.value,
            skills=skills,
            current_load=current_load,
            latency_ms=_latency_ms(record),
            compute_score=_compute_score(record),
            success_rate=max(0.0, min(1.0, float(profile.reliability))),
            online=True,
            metadata={"tier": profile.tier.value, "labels": list(profile.labels)},
        )

    @staticmethod
    def _requirement(request: PlacementRequest, constraints: PlacementConstraints) -> TaskRequirement:
        required_skill = sorted(request.required_skills)[0] if request.required_skills else "__any__"
        return TaskRequirement(
            task_id=request.task_id,
            description=str(request.metadata.get("description", request.task_id)),
            required_skill=required_skill,
            max_latency_ms=constraints.max_latency_ms,
            local_only=False,
            priority=int(request.metadata.get("priority", 5)),
            preferred_layers=tuple(
                tier.value for tier in [constraints.preferred_tier] if tier is not None
            ),
            estimated_load=float(request.metadata.get("estimated_load", 0.1)),
        )

    async def select(self, request: PlacementRequest, candidates: Sequence[AgentRecord]) -> PlacementDecision | None:
        constraints = request.constraints
        required_skills = set(request.required_skills)
        eligible: list[AgentRecord] = []
        for record in candidates:
            profile = record.profile
            if not required_skills.issubset(set(profile.skills)):
                continue
            if not agent_matches_placement(profile, constraints):
                continue
            if not _local_data_available(request, record):
                continue
            if constraints.max_latency_ms is not None and _latency_ms(record) > constraints.max_latency_ms:
                continue
            eligible.append(record)
        if not eligible:
            return None

        preferred = [
            record for record in eligible
            if constraints.preferred_tier is not None and record.profile.tier is constraints.preferred_tier
        ]
        selection_pool = preferred or eligible
        scheduler = ResourceScheduler(weights=self._weights)
        for record in selection_pool:
            scheduler.register_node(self._node(record, required_skills))
        try:
            decision = scheduler.select_node(self._requirement(request, constraints))
        except NoSchedulableNodeError:
            return None
        return PlacementDecision(
            agent_id=decision.node_id,
            tier=ExecutionTier(decision.layer),
            scheduler=self.scheduler_name,
            reason="baseline weighted score: load/latency/compute/reliability",
            score=round(decision.score, 6),
        )
