from __future__ import annotations

import asyncio
import contextlib
import unittest

from mosaic_omega.runtime.agent_runtime import AgentRuntime
from mosaic_omega.runtime.coordinator import Coordinator, TASK_NEW_TOPIC
from mosaic_omega.runtime.local_bus import LocalBusServer
from mosaic_omega.runtime.models import AgentProfile
from mosaic_omega.runtime.protocol import envelope
from mosaic_omega.runtime.registry import Registry
from mosaic_omega.runtime.settings import TransportSettings
from mosaic_omega.runtime.tasks import TaskStatus
from mosaic_omega.runtime.transport import create_transport


class LocalTransportEndToEndTests(unittest.IsolatedAsyncioTestCase):
    async def test_coordinator_routes_dependency_dag_over_local_tcp_bus(self) -> None:
        bus = LocalBusServer()
        server = await asyncio.start_server(bus.handle_client, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        settings = TransportSettings(mode="local", local_port=port)
        coordinator = Coordinator(
            create_transport(settings, "coordinator-test", Coordinator.subscriptions()),
            watchdog_interval=0.02,
        )
        profiles = [
            AgentProfile("planner", "planner", ("plan",), ""),
            AgentProfile("worker", "worker", ("code",), ""),
        ]
        agents = [
            AgentRuntime(profile, create_transport(settings, profile.agent_id, AgentRuntime.subscriptions(profile.agent_id)), 0.02)
            for profile in profiles
        ]
        submitter = create_transport(settings, "submitter-test", ())
        await coordinator.start()
        for agent in agents:
            await agent.start()
        await asyncio.sleep(0.08)
        await submitter.start(lambda topic, message: asyncio.sleep(0))
        tasks = [
            {"task_id": "plan", "title": "plan", "required_skills": ["plan"], "simulated_duration_s": 0.03},
            {"task_id": "code", "title": "code", "required_skills": ["code"], "dependencies": ["plan"], "simulated_duration_s": 0.03},
        ]
        await submitter.publish(TASK_NEW_TOPIC, envelope("TASK_BATCH", "test", tasks=tasks))
        await asyncio.sleep(0.35)

        self.assertEqual(coordinator.tasks.get("plan").status, TaskStatus.COMPLETED)
        self.assertEqual(coordinator.tasks.get("code").status, TaskStatus.COMPLETED)
        snapshot = await coordinator.registry.topology_snapshot()
        self.assertIn(("code", "planner", "worker"), snapshot.task_edges)

        await submitter.stop()
        for agent in agents:
            await agent.stop()
        await coordinator.stop()
        server.close()
        await server.wait_closed()

    async def test_timeout_reassigns_unfinished_task_to_another_agent(self) -> None:
        bus = LocalBusServer()
        server = await asyncio.start_server(bus.handle_client, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        settings = TransportSettings(mode="local", local_port=port)
        registry = Registry(heartbeat_timeout=0.12, suspect_after=0.06)
        coordinator = Coordinator(
            create_transport(settings, "coordinator-failover", Coordinator.subscriptions()),
            registry=registry,
            watchdog_interval=0.01,
        )
        primary = AgentProfile("coder-primary", "primary", ("code",), "", reliability=0.99)
        backup = AgentProfile("coder-backup", "backup", ("code",), "", reliability=0.70)
        primary_agent = AgentRuntime(
            primary, create_transport(settings, primary.agent_id, AgentRuntime.subscriptions(primary.agent_id)), 0.02
        )
        backup_agent = AgentRuntime(
            backup, create_transport(settings, backup.agent_id, AgentRuntime.subscriptions(backup.agent_id)), 0.02
        )
        submitter = create_transport(settings, "submitter-failover", ())
        await coordinator.start()
        await primary_agent.start()
        await backup_agent.start()
        await asyncio.sleep(0.08)
        await submitter.start(lambda topic, message: asyncio.sleep(0))
        task = {
            "task_id": "failover", "title": "will be reassigned", "required_skills": ["code"],
            "simulated_duration_s": 0.28,
        }
        await submitter.publish(TASK_NEW_TOPIC, envelope("TASK_BATCH", "test", tasks=[task]))
        await asyncio.sleep(0.05)
        await primary_agent.stop()  # silent from the registry's point of view: no unregister message
        await asyncio.sleep(0.5)

        record = coordinator.tasks.get("failover")
        self.assertEqual(record.status, TaskStatus.COMPLETED)
        self.assertEqual(record.assignee, "coder-backup")
        self.assertEqual(record.attempts, 2)

        await submitter.stop()
        await backup_agent.stop()
        await coordinator.stop()
        server.close()
        await server.wait_closed()
