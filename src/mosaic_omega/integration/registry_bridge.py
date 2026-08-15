"""Bridge dynamic-registry AgentProfile/status into scheduler CapabilityProfile.

The dynamic registry owns liveness and advertised edge/device capabilities.
The execution scheduler owns assignment and posterior/cost updates.  This bridge
keeps those responsibilities separate while providing one explicit interface.
"""
from __future__ import annotations

from typing import Any, Sequence

from ..execution_scheduler.models import CapabilityProfile
from ..execution_scheduler.service import ExecutionSchedulerService
from ..runtime.models import AgentProfile, AgentStatus
from .contracts import runtime_agent_to_capability


class DynamicRegistrySchedulerBridge:
    def __init__(self, execution: ExecutionSchedulerService) -> None:
        self.execution = execution

    def register(
        self,
        profile: AgentProfile,
        *,
        status: AgentStatus | str = AgentStatus.ONLINE,
        current_load: int = 0,
        permissions: Sequence[str] = ("*",),
        latency_ms: float = 0.0,
        adapter: Any | None = None,
        fixed_cost: float = 0.0,
    ) -> CapabilityProfile:
        capability = runtime_agent_to_capability(
            profile,
            status=status,
            current_load=current_load,
            permissions=permissions,
            latency_ms=latency_ms,
        )
        capability.fixed_cost = float(fixed_cost)
        return self.execution.register_actor(capability, adapter=adapter)

    def heartbeat(
        self,
        actor_id: str,
        *,
        status: AgentStatus | str = AgentStatus.ONLINE,
        current_load: int | None = None,
        latency_ms: float | None = None,
    ) -> CapabilityProfile:
        state = AgentStatus(status)
        profile = self.execution.capabilities.get(actor_id)
        capacity = max(1, profile.capacity)
        normalized_load = None
        if current_load is not None:
            normalized_load = min(1.0, max(0.0, float(current_load) / capacity))
        return self.execution.capabilities.update_runtime(
            actor_id,
            online=state is AgentStatus.ONLINE,
            current_load=normalized_load,
            latency_ms=latency_ms,
        )

    def offline(self, actor_id: str) -> CapabilityProfile:
        return self.execution.capabilities.update_runtime(actor_id, online=False)
