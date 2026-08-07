from __future__ import annotations

import asyncio

from mosaic_omega.goal_planner.todag import ToDAGEngine
from mosaic_omega.integration.planner_runtime_bridge import plan_to_task_message
from mosaic_omega.runtime.coordinator import Coordinator, TASK_NEW_TOPIC
from mosaic_omega.runtime.local_bus import LocalBusServer
from mosaic_omega.runtime.settings import TransportSettings
from mosaic_omega.runtime.transport import create_transport


def test_coordinator_bootstraps_todag_constraints_into_task_context() -> None:
    async def scenario() -> None:
        bus = LocalBusServer()
        server = await asyncio.start_server(bus.handle_client, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        settings = TransportSettings(mode="local", local_port=port)
        coordinator = Coordinator(
            create_transport(settings, "coordinator-bootstrap", Coordinator.subscriptions()),
            watchdog_interval=0.05,
        )
        submitter = create_transport(settings, "planner-bootstrap", ())
        await coordinator.start()
        await submitter.start(lambda topic, message: asyncio.sleep(0))
        try:
            goal = {
                "main_goal": "repair ROS package",
                "hard_constraints": ["source code must stay local"],
                "soft_preferences": ["minimal patch"],
                "acceptance_conditions": ["build passes"],
                "budget": {},
                "prohibitions": ["do not upload source code"],
            }
            engine = ToDAGEngine()
            engine.build(goal)
            await submitter.publish(TASK_NEW_TOPIC, plan_to_task_message(engine.coordinator_plan()))
            await asyncio.sleep(0.08)
            first_task = engine.coordinator_plan()[0]["task_id"]
            context = coordinator.task_contexts.get(first_task)
            joined = " ".join(context.constraints.values())
            assert "source code must stay local" in joined
            assert "do not upload source code" in joined
        finally:
            await submitter.stop()
            await coordinator.stop()
            server.close()
            await server.wait_closed()

    asyncio.run(scenario())
