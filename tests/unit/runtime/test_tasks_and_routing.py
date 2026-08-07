from __future__ import annotations

import unittest

from mosaic_omega.runtime.models import AgentProfile
from mosaic_omega.runtime.registry import Registry
from mosaic_omega.runtime.routing import DynamicRouter
from mosaic_omega.runtime.tasks import TaskSpec, TaskStatus, TaskStore


class TaskStoreTests(unittest.IsolatedAsyncioTestCase):
    async def test_dependencies_release_children_only_after_completion(self) -> None:
        store = TaskStore()
        store.add_many([
            TaskSpec("plan", "plan", frozenset({"plan"})),
            TaskSpec("work", "work", frozenset({"code"}), dependencies=("plan",)),
        ])
        self.assertEqual([task.spec.task_id for task in store.ready_tasks()], ["plan"])
        store.assign("plan", "planner")
        store.complete("plan", "planner", {})
        self.assertEqual([task.spec.task_id for task in store.ready_tasks()], ["work"])

    async def test_router_prefers_less_loaded_compatible_agent(self) -> None:
        registry = Registry()
        await registry.register(AgentProfile("coder-a", "A", ("code",), "", max_load=2), "a", now=1)
        await registry.register(AgentProfile("coder-b", "B", ("code",), "", max_load=2), "b", now=1)
        await registry.heartbeat("coder-a", "a", current_load=1, now=2)
        task = TaskSpec("t", "task", frozenset({"code"}))
        store = TaskStore()
        store.add(task)
        decision = await DynamicRouter(registry).select(store.get("t"))
        self.assertEqual(decision.agent_id, "coder-b")

    async def test_release_assigned_task_after_failure(self) -> None:
        store = TaskStore()
        store.add(TaskSpec("t", "task", frozenset({"code"})))
        store.assign("t", "coder-a")
        store.release_agent_tasks("coder-a")
        record = store.get("t")
        self.assertEqual(record.status, TaskStatus.PENDING)
        self.assertIsNone(record.assignee)
