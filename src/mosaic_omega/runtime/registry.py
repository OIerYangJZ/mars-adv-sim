"""In-memory registration centre, heartbeat state machine, and event log."""

from __future__ import annotations

import asyncio
import time

from .models import AgentProfile, AgentRecord, AgentStatus, RegistryEvent
from .topology import TopologyManager, TopologySnapshot


class Registry:
    def __init__(self, heartbeat_timeout: float = 9.0, suspect_after: float = 6.0) -> None:
        if not 0 < suspect_after < heartbeat_timeout:
            raise ValueError("suspect_after must be positive and smaller than heartbeat_timeout")
        self.heartbeat_timeout = heartbeat_timeout
        self.suspect_after = suspect_after
        self._agents: dict[str, AgentRecord] = {}
        self._events: list[RegistryEvent] = []
        self._topology = TopologyManager()
        self._lock = asyncio.Lock()

    @staticmethod
    def _clock(now: float | None) -> float:
        return time.monotonic() if now is None else now

    def _emit(self, event_type: str, agent_id: str, now: float, **detail: object) -> RegistryEvent:
        event = RegistryEvent(
            event_type=event_type,
            agent_id=agent_id,
            timestamp=now,
            topology_version=self._topology.version,
            detail=dict(detail),
        )
        self._events.append(event)
        return event

    async def register(self, profile: AgentProfile, session_id: str, now: float | None = None) -> AgentRecord:
        timestamp = self._clock(now)
        async with self._lock:
            previous = self._agents.get(profile.agent_id)
            was_offline = previous is not None and previous.status is AgentStatus.OFFLINE
            record = AgentRecord(
                profile=profile,
                session_id=session_id,
                status=AgentStatus.ONLINE,
                current_load=0,
                joined_at=timestamp,
                last_heartbeat=timestamp,
                updated_at=timestamp,
            )
            self._agents[profile.agent_id] = record
            self._topology.add_agent(profile.agent_id)
            event_type = "AGENT_RECOVERED" if was_offline else "AGENT_ONLINE"
            self._emit(event_type, profile.agent_id, timestamp, skills=list(profile.skills), session_id=session_id)
            return record

    async def heartbeat(
        self,
        agent_id: str,
        session_id: str,
        current_load: int | None = None,
        now: float | None = None,
    ) -> bool:
        timestamp = self._clock(now)
        async with self._lock:
            record = self._agents.get(agent_id)
            if record is None or record.session_id != session_id or record.status is AgentStatus.OFFLINE:
                return False
            record.last_heartbeat = timestamp
            record.updated_at = timestamp
            if current_load is not None:
                record.current_load = current_load
            if record.status is AgentStatus.SUSPECT:
                record.status = AgentStatus.ONLINE
                self._emit("AGENT_HEARTBEAT_RECOVERED", agent_id, timestamp)
            return True

    async def unregister(
        self,
        agent_id: str,
        session_id: str,
        reason: str = "graceful_shutdown",
        now: float | None = None,
    ) -> bool:
        timestamp = self._clock(now)
        async with self._lock:
            record = self._agents.get(agent_id)
            if record is None or record.session_id != session_id or record.status is AgentStatus.OFFLINE:
                return False
            record.status = AgentStatus.OFFLINE
            record.updated_at = timestamp
            self._topology.remove_agent(agent_id)
            self._emit("AGENT_OFFLINE", agent_id, timestamp, reason=reason)
            return True

    async def sweep_expired(self, now: float | None = None) -> list[str]:
        """Advance ONLINE -> SUSPECT -> OFFLINE based on missing heartbeats."""
        timestamp = self._clock(now)
        offline: list[str] = []
        async with self._lock:
            for agent_id, record in self._agents.items():
                if record.status is AgentStatus.OFFLINE:
                    continue
                age = timestamp - record.last_heartbeat
                if age >= self.heartbeat_timeout:
                    record.status = AgentStatus.OFFLINE
                    record.updated_at = timestamp
                    self._topology.remove_agent(agent_id)
                    self._emit("AGENT_OFFLINE", agent_id, timestamp, reason="heartbeat_timeout", age_s=round(age, 3))
                    offline.append(agent_id)
                elif age >= self.suspect_after and record.status is AgentStatus.ONLINE:
                    record.status = AgentStatus.SUSPECT
                    record.updated_at = timestamp
                    self._emit("AGENT_SUSPECT", agent_id, timestamp, age_s=round(age, 3))
        return offline

    async def get_record(self, agent_id: str) -> AgentRecord | None:
        async with self._lock:
            return self._agents.get(agent_id)

    async def online_agents(self) -> list[AgentRecord]:
        async with self._lock:
            return [r for r in self._agents.values() if r.status is AgentStatus.ONLINE]

    async def topology_snapshot(self) -> TopologySnapshot:
        async with self._lock:
            return self._topology.snapshot()

    async def replace_task_edges(self, task_id: str, edges: list[tuple[str, str]]) -> int:
        """Atomically replace one task's logical communication edges."""
        async with self._lock:
            self._topology.remove_task_edges(task_id)
            for source, target in edges:
                self._topology.add_task_edge(task_id, source, target)
            return self._topology.version

    async def clear_task_edges(self, task_id: str) -> int:
        async with self._lock:
            self._topology.remove_task_edges(task_id)
            return self._topology.version

    async def events(self) -> list[RegistryEvent]:
        async with self._lock:
            return list(self._events)
