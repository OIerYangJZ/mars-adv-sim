"""Validation helpers for the Coordinator-compatible planner DAG contract."""
from __future__ import annotations

from collections import deque
from collections.abc import Mapping
from typing import Any

DEPENDENCY_TYPES = {"exec", "data", "evidence"}


def _metadata(task: Mapping[str, Any]) -> Mapping[str, Any]:
    value = task.get("metadata")
    return value if isinstance(value, Mapping) else {}


def _dependency_types(task: Mapping[str, Any]) -> Mapping[str, Any]:
    value = task.get("dependency_types")
    if value is None:
        value = _metadata(task).get("dependency_types", {})
    return value if isinstance(value, Mapping) else {}


def _mutex_with(task: Mapping[str, Any]) -> list[str]:
    value = task.get("mutex_with")
    if value is None:
        value = _metadata(task).get("mutex_with", [])
    if value is None:
        return []
    if isinstance(value, str):
        value = [value]
    return [str(item) for item in value] if isinstance(value, list) else []


def validate_plan(plan: list[Mapping[str, Any]]) -> None:
    if not isinstance(plan, list) or not plan:
        raise ValueError("planner DAG must be a non-empty list")
    task_ids: list[str] = []
    for task in plan:
        if not isinstance(task, Mapping):
            raise ValueError("each planner task must be an object")
        task_id = task.get("task_id")
        if not isinstance(task_id, str) or not task_id.strip():
            raise ValueError("each task requires non-empty task_id")
        if not isinstance(task.get("description"), str) or not str(task.get("description")).strip():
            raise ValueError(f"task {task_id} requires description")
        if not isinstance(task.get("required_skill"), str) or not str(task.get("required_skill")).strip():
            raise ValueError(f"task {task_id} requires required_skill")
        depends_on = task.get("depends_on")
        if not isinstance(depends_on, list) or not all(isinstance(item, str) and item for item in depends_on):
            raise ValueError(f"task {task_id}.depends_on must be a string array")
        priority = task.get("priority")
        if isinstance(priority, bool) or not isinstance(priority, int) or not 1 <= priority <= 10:
            raise ValueError(f"task {task_id}.priority must be an integer in [1, 10]")
        task_ids.append(task_id)

    if len(task_ids) != len(set(task_ids)):
        raise ValueError("planner DAG contains duplicate task_id")
    task_set = set(task_ids)
    indegree = {task_id: 0 for task_id in task_ids}
    outgoing = {task_id: [] for task_id in task_ids}
    for task in plan:
        task_id = str(task["task_id"])
        for parent in task["depends_on"]:
            if parent not in task_set:
                raise ValueError(f"task {task_id} depends on missing task {parent}")
            if parent == task_id:
                raise ValueError(f"task {task_id} cannot depend on itself")
            indegree[task_id] += 1
            outgoing[parent].append(task_id)
        dep_types = _dependency_types(task)
        if set(dep_types) - set(task["depends_on"]):
            raise ValueError(f"task {task_id} has dependency type for a non-dependency")
        bad_types = {str(value) for value in dep_types.values()} - DEPENDENCY_TYPES
        if bad_types:
            raise ValueError(f"task {task_id} has invalid dependency type(s): {sorted(bad_types)}")
        for peer in _mutex_with(task):
            if peer not in task_set or peer == task_id:
                raise ValueError(f"task {task_id} has invalid mutex peer {peer}")

    queue = deque(task_id for task_id, degree in indegree.items() if degree == 0)
    visited = 0
    while queue:
        current = queue.popleft()
        visited += 1
        for child in outgoing[current]:
            indegree[child] -= 1
            if indegree[child] == 0:
                queue.append(child)
    if visited != len(task_ids):
        raise ValueError("planner dependencies contain a cycle")


def build_dag_output(plan: list[Mapping[str, Any]]) -> dict[str, Any]:
    validate_plan(plan)
    nodes = [dict(item) for item in plan]
    edges = [
        {"source": parent, "target": task["task_id"]}
        for task in plan
        for parent in task["depends_on"]
    ]
    typed_edges = [
        {
            "source": parent,
            "target": task["task_id"],
            "type": _dependency_types(task).get(parent, "exec"),
        }
        for task in plan
        for parent in task["depends_on"]
    ]
    seen_mutex: set[tuple[str, str]] = set()
    for task in plan:
        for peer in _mutex_with(task):
            key = tuple(sorted((str(task["task_id"]), peer)))
            if key in seen_mutex:
                continue
            seen_mutex.add(key)
            typed_edges.append({"source": key[0], "target": key[1], "type": "mutex"})

    depended = {edge["source"] for edge in edges}
    final_ids = [task_id for task_id in [str(item["task_id"]) for item in plan] if task_id not in depended]
    if len(final_ids) != 1:
        raise ValueError("runtime contract requires exactly one terminal task")
    entries = [str(item["task_id"]) for item in plan if not item["depends_on"]]
    return {
        "schema_version": "1.1",
        "nodes": nodes,
        "edges": edges,
        "typed_edges": typed_edges,
        "entry_task_ids": entries,
        "final_task_id": final_ids[0],
    }
