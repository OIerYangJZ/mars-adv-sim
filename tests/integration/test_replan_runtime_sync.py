from __future__ import annotations

from mosaic_omega.goal_planner.todag import ToDAGEngine
from mosaic_omega.integration.planner_runtime_bridge import plan_to_task_specs
from mosaic_omega.runtime.tasks import TaskStatus, TaskStore


def _spec(conditions: list[str]) -> dict:
    return {
        "main_goal": "build a validated report",
        "hard_constraints": ["use evidence"],
        "soft_preferences": ["concise"],
        "acceptance_conditions": conditions,
        "budget": {},
        "prohibitions": ["do not invent sources"],
    }


def test_replan_preserves_unaffected_completed_runtime_nodes() -> None:
    engine = ToDAGEngine()
    before = engine.build(_spec(["method", "risk", "publish"]))
    specs = plan_to_task_specs(engine.coordinator_plan())
    store = TaskStore()
    store.add_many(specs)

    entry = before["entry_task_ids"][0]
    store.assign(entry, "planner")
    store.complete(entry, "planner", {"ok": True})

    after = engine.update_specification(_spec(["method", "risk", "publish", "appendix"]))
    sync = store.apply_planner_update(
        plan_to_task_specs(engine.coordinator_plan()),
        after["change_set"],
    )

    assert entry in sync["preserved_task_ids"]
    assert store.get(entry).status is TaskStatus.COMPLETED
    assert after["change_set"]["added_node_ids"]
    assert set(after["change_set"]["added_node_ids"]).issubset(set(sync["reset_task_ids"]))
