"""Capability- and load-aware dynamic routing algorithm."""

from __future__ import annotations

from collections.abc import Collection
from dataclasses import dataclass

from .edge_cloud import agent_matches_placement
from .models import AgentRecord
from .registry import Registry
from .tasks import TaskRecord


@dataclass(frozen=True)
class RouteDecision:
    task_id: str
    agent_id: str
    score: float


class DynamicRouter:
    def __init__(self, registry: Registry) -> None:
        self.registry = registry
        self._reserved_load: dict[str, int] = {}

    async def candidates(self, task: TaskRecord, allowed_agent_ids: Collection[str] | None = None) -> list[AgentRecord]:
        candidates = []
        for agent in await self.registry.online_agents():
            if allowed_agent_ids is not None and agent.profile.agent_id not in allowed_agent_ids:
                continue
            if not task.spec.required_skills.issubset(set(agent.profile.skills)):
                continue
            if not agent_matches_placement(agent.profile, task.spec.placement):
                continue
            if agent.current_load >= agent.profile.max_load:
                continue
            candidates.append(agent)
        return candidates

    async def select(self, task: TaskRecord, allowed_agent_ids: Collection[str] | None = None) -> RouteDecision | None:
        candidates = await self.candidates(task, allowed_agent_ids)
        if not candidates:
            return None

        def score(agent: AgentRecord) -> float:
            # All candidates meet every required skill. Extra skills act as a
            # small tie breaker, then reliability and spare capacity dominate.
            profile = agent.profile
            effective_load = agent.current_load + self._reserved_load.get(profile.agent_id, 0)
            if effective_load >= profile.max_load:
                return float("-inf")
            load_ratio = effective_load / profile.max_load
            skill_bonus = min(len(profile.skills) / max(len(task.spec.required_skills), 1), 2.0) / 2.0
            return 0.45 * skill_bonus + 0.35 * profile.reliability + 0.20 * (1 - load_ratio)

        chosen = max(candidates, key=lambda item: (score(item), item.profile.agent_id))
        if score(chosen) == float("-inf"):
            return None
        return RouteDecision(task.spec.task_id, chosen.profile.agent_id, round(score(chosen), 4))

    def reserve(self, agent_id: str) -> None:
        self._reserved_load[agent_id] = self._reserved_load.get(agent_id, 0) + 1

    def release(self, agent_id: str) -> None:
        if agent_id not in self._reserved_load:
            return
        self._reserved_load[agent_id] -= 1
        if self._reserved_load[agent_id] <= 0:
            self._reserved_load.pop(agent_id, None)
