"""Convert ToDAG coordinator plans into the runtime's single TaskSpec contract."""
from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from .dag_contract import build_dag_output, validate_plan
from ..runtime.edge_cloud import PlacementConstraints
from ..runtime.protocol import envelope
from ..runtime.tasks import TaskSpec

_SKILL_ALIASES = {"planning": "plan", "coding": "code"}


def _placement_raw(node: Mapping[str, Any]) -> dict[str, Any] | None:
    raw = node.get("placement")
    if raw is not None:
        if not isinstance(raw, Mapping):
            raise ValueError(f"task {node.get('task_id')} placement must be an object")
        return dict(raw)
    keys = (
        "allowed_tiers", "preferred_tier", "max_latency_ms", "min_memory_mb",
        "min_gpu_count", "data_sensitivity", "require_local_data",
    )
    values = {key: node[key] for key in keys if key in node}
    return values or None


def plan_to_task_specs(plan: list[Mapping[str, Any]], requested_by: str = "planner") -> list[TaskSpec]:
    validate_plan(plan)
    specs: list[TaskSpec] = []
    for node in plan:
        required_skills = node.get("required_skills")
        if required_skills is None:
            required_skills = [node["required_skill"]]
        if isinstance(required_skills, str):
            required_skills = [required_skills]
        metadata = dict(node.get("metadata") or {})
        original_skills = [str(skill) for skill in required_skills]
        metadata.setdefault("planner_task_id", node["task_id"])
        metadata.setdefault("description", node["description"])
        metadata.setdefault("priority", node["priority"])
        metadata.setdefault("planner_required_skills", original_skills)
        specs.append(TaskSpec(
            task_id=str(node["task_id"]),
            title=str(node["description"]),
            required_skills=frozenset(
                _SKILL_ALIASES.get(str(skill).strip().lower(), str(skill).strip())
                for skill in required_skills if str(skill).strip()
            ),
            dependencies=tuple(str(dep) for dep in node["depends_on"]),
            requested_by=requested_by,
            priority=int(node["priority"]),
            simulated_duration_s=float(node.get("simulated_duration_s", 1.0)),
            metadata=metadata,
            placement=PlacementConstraints.from_dict(_placement_raw(node)),
        ))
    return specs


def plan_to_task_payload(plan: list[Mapping[str, Any]], requested_by: str = "planner") -> dict[str, Any]:
    specs = plan_to_task_specs(plan, requested_by=requested_by)
    return {"tasks": [spec.to_dict() for spec in specs], "dag": build_dag_output(plan)}


def plan_to_task_message(plan: list[Mapping[str, Any]], requested_by: str = "planner") -> dict[str, Any]:
    return envelope("TASK_BATCH", requested_by, **plan_to_task_payload(plan, requested_by))


def task_specs_to_payload(specs: Iterable[TaskSpec]) -> dict[str, Any]:
    return {"tasks": [spec.to_dict() for spec in specs]}

# Backward-compatible alias used by the upgraded ToDAG tests/docs.
dag_to_task_specs = plan_to_task_specs
