from __future__ import annotations

import asyncio
import unittest

from mosaic_omega.runtime.agent_sim import AgentSimulator
from mosaic_omega.runtime.models import AgentProfile, AgentStatus
from mosaic_omega.runtime.registry import Registry
from mosaic_omega.runtime.watchdog import Watchdog


class RegistryLifecycleTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.profile = AgentProfile("agent-1", "测试智能体", ("analysis",), "sim://agent-1")

    async def test_timeout_removes_node_from_topology(self) -> None:
        registry = Registry(heartbeat_timeout=9, suspect_after=6)
        await registry.register(self.profile, "session-1", now=100)

        await registry.sweep_expired(now=106.1)
        record = await registry.get_record("agent-1")
        self.assertEqual(record.status, AgentStatus.SUSPECT)

        offline = await registry.sweep_expired(now=109.1)
        snapshot = await registry.topology_snapshot()
        self.assertEqual(offline, ["agent-1"])
        self.assertEqual(record.status, AgentStatus.OFFLINE)
        self.assertNotIn("agent-1", snapshot.nodes)

    async def test_stale_session_cannot_refresh_new_registration(self) -> None:
        registry = Registry(heartbeat_timeout=9, suspect_after=6)
        await registry.register(self.profile, "old", now=100)
        await registry.register(self.profile, "new", now=101)

        accepted = await registry.heartbeat("agent-1", "old", now=108)
        record = await registry.get_record("agent-1")
        self.assertFalse(accepted)
        self.assertEqual(record.session_id, "new")
        self.assertEqual(record.last_heartbeat, 101)

    async def test_silent_crash_is_detected_by_watchdog(self) -> None:
        registry = Registry(heartbeat_timeout=0.15, suspect_after=0.08)
        watchdog = Watchdog(registry, scan_interval=0.01)
        agent = AgentSimulator(self.profile, registry, heartbeat_interval=0.02)
        await watchdog.start()
        await agent.start()
        await asyncio.sleep(0.06)
        await agent.crash()
        await asyncio.sleep(0.22)

        record = await registry.get_record("agent-1")
        snapshot = await registry.topology_snapshot()
        self.assertEqual(record.status, AgentStatus.OFFLINE)
        self.assertNotIn("agent-1", snapshot.nodes)
        await watchdog.stop()
