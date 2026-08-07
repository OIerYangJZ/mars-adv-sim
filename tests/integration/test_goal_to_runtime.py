from __future__ import annotations

from mosaic_omega.goal_planner.goalspec import compile_goal
from mosaic_omega.goal_planner.goalspec.validator import validate_goalspec_schema
from mosaic_omega.goal_planner.todag import ToDAGEngine
from mosaic_omega.integration.planner_runtime_bridge import plan_to_task_specs
from mosaic_omega.runtime.task_context import TaskContextStore


def test_goal_to_todag_to_runtime_context_keeps_hard_constraints() -> None:
    goalspec = compile_goal(
        "开发一个ROS2系统，不能上传用户数据，必须通过测试，尽量减少改动",
        mode="rule",
    )
    assert set(goalspec) == {
        "main_goal",
        "hard_constraints",
        "soft_preferences",
        "acceptance_conditions",
        "budget",
        "prohibitions",
    }
    valid, errors = validate_goalspec_schema(goalspec)
    assert valid, errors

    engine = ToDAGEngine(planning_horizon=6)
    snapshot = engine.build(goalspec)
    assert snapshot["status"] == "ready"
    specs = plan_to_task_specs(engine.coordinator_plan())
    assert specs

    contexts = TaskContextStore()
    for spec in specs:
        contexts.initialize_from_spec(spec, replace=True)

    first = contexts.get(specs[0].task_id)
    constraint_text = " ".join(first.constraints.values())
    assert "不能上传用户数据" in constraint_text
    assert "必须通过测试" in constraint_text
    fact_text = " ".join(first.facts.values())
    assert "尽量减少改动" in fact_text


def test_rich_todag_metadata_reaches_runtime_task_spec() -> None:
    raw = {
        "main_goal": {
            "goal_text": "repair ROS package",
            "goal_type": "coding",
            "domain": "robotics",
            "sub_goals": [{
                "name": "apply patch",
                "required_skill": "coding",
                "risk": "high",
                "rollback_checkpoint": True,
            }],
        },
        "hard_constraints": [{"constraint": "source code stays local", "type": "privacy"}],
        "soft_preferences": [{"preference": "minimize changes", "priority": 4}],
        "acceptance_conditions": [{"condition": "build passes", "predicate": "build_success"}],
        "budget": {"max_latency_ms": 100},
        "prohibitions": [{"rule": "do not upload source code"}],
    }
    engine = ToDAGEngine()
    engine.build(raw)
    specs = plan_to_task_specs(engine.coordinator_plan())
    work = next(spec for spec in specs if spec.metadata.get("node_type") == "work")
    assert "hard_constraints" in work.metadata
    assert "soft_preferences" in work.metadata
    assert "acceptance_conditions" in work.metadata
    assert "risk" in work.metadata
