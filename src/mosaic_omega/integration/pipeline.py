"""High-level glue for the three integrated codebases.

This module intentionally stays thin: GoalSpec owns requirement compilation,
ToDAG owns planning, and the runtime owns assignment/communication state.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ..goal_planner.goalspec import compile_goal
from ..goal_planner.todag import ToDAGEngine
from ..runtime.coordinator import Coordinator
from ..runtime.tasks import TaskSpec
from .planner_runtime_bridge import plan_to_task_specs


@dataclass
class CompiledRuntimePlan:
    goalspec: dict[str, Any]
    taskgraph: dict[str, Any]
    task_specs: list[TaskSpec]
    engine: ToDAGEngine


def compile_runtime_plan(
    user_text: str,
    *,
    goalspec_mode: str = "rule",
    planning_horizon: int = 10,
) -> CompiledRuntimePlan:
    goalspec = compile_goal(user_text, mode=goalspec_mode)
    engine = ToDAGEngine(planning_horizon=planning_horizon)
    taskgraph = engine.build(goalspec)
    task_specs = plan_to_task_specs(engine.coordinator_plan())
    return CompiledRuntimePlan(goalspec, taskgraph, task_specs, engine)


async def sync_engine_to_runtime(coordinator: Coordinator, engine: ToDAGEngine) -> dict[str, Any]:
    """Push the current ToDAG plan/change-set into a live Coordinator."""
    snapshot = engine.snapshot()
    specs = plan_to_task_specs(engine.coordinator_plan())
    return await coordinator.apply_planner_update(specs, snapshot.get("change_set", {}))
