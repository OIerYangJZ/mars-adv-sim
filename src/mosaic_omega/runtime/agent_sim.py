"""Async agent lifecycle simulator."""

from __future__ import annotations

import asyncio
import contextlib
import uuid

from .models import AgentProfile
from .registry import Registry


class AgentSimulator:
    def __init__(self, profile: AgentProfile, registry: Registry, heartbeat_interval: float = 3.0) -> None:
        self.profile = profile
        self.registry = registry
        self.heartbeat_interval = heartbeat_interval
        self.session_id: str | None = None
        self.current_load = 0
        self._heartbeat_task: asyncio.Task[None] | None = None
        self._running = False

    async def start(self) -> None:
        if self._running:
            return
        self.session_id = uuid.uuid4().hex
        self._running = True
        await self.registry.register(self.profile, self.session_id)
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop(), name=f"heartbeat:{self.profile.agent_id}")

    async def _heartbeat_loop(self) -> None:
        try:
            while self._running and self.session_id is not None:
                await asyncio.sleep(self.heartbeat_interval)
                if self._running:
                    await self.registry.heartbeat(self.profile.agent_id, self.session_id, self.current_load)
        except asyncio.CancelledError:
            raise

    async def _stop_heartbeat(self) -> None:
        if self._heartbeat_task is not None:
            self._heartbeat_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._heartbeat_task
            self._heartbeat_task = None

    async def shutdown(self) -> None:
        """Graceful exit: stop heartbeats and explicitly unregister."""
        if not self._running or self.session_id is None:
            return
        self._running = False
        session_id = self.session_id
        await self._stop_heartbeat()
        await self.registry.unregister(self.profile.agent_id, session_id)

    async def crash(self) -> None:
        """Silent crash: stop heartbeats without notifying the registry."""
        self._running = False
        await self._stop_heartbeat()

    async def recover(self) -> None:
        """A restarted process always gets a new session ID."""
        await self.start()
