"""Extensible in-memory task table and dependency DAG."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from .edge_cloud import PlacementConstraints


class TaskStatus(str, Enum):
    PENDING = "PENDING"
    ASSIGNED = "ASSIGNED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


@dataclass(frozen=True)
class TaskSpec:
    task_id: str
    title: str
    required_skills: frozenset[str]
    dependencies: tuple[str, ...] = ()
    requested_by: str = "system"
    priority: int = 5
    simulated_duration_s: float = 1.0
    metadata: dict[str, Any] = field(default_factory=dict)
    placement: PlacementConstraints = field(default_factory=PlacementConstraints)

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "TaskSpec":
        return cls(
            task_id=str(raw["task_id"]),
            title=str(raw.get("title", raw["task_id"])),
            required_skills=frozenset(raw.get("required_skills", [])),
            dependencies=tuple(raw.get("dependencies", [])),
            requested_by=str(raw.get("requested_by", "system")),
            priority=int(raw.get("priority", 5)),
            simulated_duration_s=float(raw.get("simulated_duration_s", 1.0)),
            metadata=dict(raw.get("metadata", {})),
            placement=PlacementConstraints.from_dict(raw.get("placement")),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "title": self.title,
            "required_skills": sorted(self.required_skills),
            "dependencies": list(self.dependencies),
            "requested_by": self.requested_by,
            "priority": self.priority,
            "simulated_duration_s": self.simulated_duration_s,
            "metadata": self.metadata,
            "placement": self.placement.to_dict(),
        }


@dataclass
class TaskRecord:
    spec: TaskSpec
    status: TaskStatus = TaskStatus.PENDING
    assignee: str | None = None
    attempts: int = 0
    result: dict[str, Any] | None = None


class TaskStore:
    """Current in-memory DAG implementation.

    The coordinator depends only on this public API, so a database-backed or
    planner-backed task graph can replace it later.
    """

    def __init__(self) -> None:
        self._records: dict[str, TaskRecord] = {}

    def add_many(self, specs: list[TaskSpec]) -> None:
        incoming = {spec.task_id for spec in specs}
        if len(incoming) != len(specs) or any(task_id in self._records for task_id in incoming):
            raise ValueError("task IDs must be unique")
        known = set(self._records) | incoming
        for spec in specs:
            missing = set(spec.dependencies) - known
            if missing:
                raise ValueError(f"task {spec.task_id} has missing dependencies: {sorted(missing)}")
        self._records.update({spec.task_id: TaskRecord(spec) for spec in specs})

    def add(self, spec: TaskSpec) -> None:
        self.add_many([spec])

    def apply_planner_update(
        self,
        specs: list[TaskSpec],
        change_set: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Replace the planner view while preserving unaffected runtime state.

        ``ToDAGEngine.update_specification`` exports a full current plan plus a
        change-set. Nodes outside the affected set retain completion/result
        state; changed/added/invalidated/recomputed nodes return to PENDING so
        dependency gating can safely schedule them again.
        """
        incoming = {spec.task_id: spec for spec in specs}
        if len(incoming) != len(specs):
            raise ValueError("task IDs must be unique")
        known = set(incoming)
        for spec in specs:
            missing = set(spec.dependencies) - known
            if missing:
                raise ValueError(f"task {spec.task_id} has missing dependencies: {sorted(missing)}")

        change_set = dict(change_set or {})
        affected = set()
        for key in (
            "changed_node_ids",
            "added_node_ids",
            "invalidated_node_ids",
            "recomputed_node_ids",
        ):
            affected.update(str(item) for item in change_set.get(key, ()) or ())

        removed_ids = set(self._records) - set(incoming)
        removed_ids.update(str(item) for item in change_set.get("removed_node_ids", ()) or ())
        released_agents: set[str] = set()
        reset_ids: set[str] = set()
        new_records: dict[str, TaskRecord] = {}

        for task_id, spec in incoming.items():
            previous = self._records.get(task_id)
            if previous is not None and task_id not in affected:
                new_records[task_id] = TaskRecord(
                    spec=spec,
                    status=previous.status,
                    assignee=previous.assignee,
                    attempts=previous.attempts,
                    result=previous.result,
                )
                continue
            if previous is not None and previous.assignee is not None:
                released_agents.add(previous.assignee)
            if previous is not None or task_id in affected:
                reset_ids.add(task_id)
            new_records[task_id] = TaskRecord(spec=spec)

        for task_id in removed_ids:
            previous = self._records.get(task_id)
            if previous is not None and previous.assignee is not None:
                released_agents.add(previous.assignee)

        self._records = new_records
        return {
            "removed_task_ids": sorted(removed_ids),
            "reset_task_ids": sorted(reset_ids),
            "released_agent_ids": sorted(released_agents),
            "preserved_task_ids": sorted(set(incoming) - reset_ids),
        }

    def get(self, task_id: str) -> TaskRecord:
        return self._records[task_id]

    def ready_tasks(self) -> list[TaskRecord]:
        ready = []
        for record in self._records.values():
            if record.status is not TaskStatus.PENDING:
                continue
            if all(self._records[parent].status is TaskStatus.COMPLETED for parent in record.spec.dependencies):
                ready.append(record)
        return sorted(ready, key=lambda item: (-item.spec.priority, item.spec.task_id))

    def assigned_tasks(self, agent_id: str) -> list[TaskRecord]:
        """Return incomplete tasks currently owned by one agent."""
        return [
            record
            for record in self._records.values()
            if record.status is TaskStatus.ASSIGNED and record.assignee == agent_id
        ]

    def assign(self, task_id: str, agent_id: str) -> TaskRecord:
        record = self.get(task_id)
        if record.status is not TaskStatus.PENDING:
            raise ValueError(f"task {task_id} is not pending")
        record.status = TaskStatus.ASSIGNED
        record.assignee = agent_id
        record.attempts += 1
        return record

    def complete(self, task_id: str, agent_id: str, result: dict[str, Any]) -> TaskRecord:
        record = self.get(task_id)
        if record.status is not TaskStatus.ASSIGNED or record.assignee != agent_id:
            raise ValueError(f"agent {agent_id} cannot complete task {task_id}")
        record.status = TaskStatus.COMPLETED
        record.result = result
        return record

    def release_agent_tasks(self, agent_id: str) -> list[TaskRecord]:
        released: list[TaskRecord] = []
        for record in self._records.values():
            if record.status is TaskStatus.ASSIGNED and record.assignee == agent_id:
                record.status = TaskStatus.PENDING
                record.assignee = None
                released.append(record)
        return released

    def snapshot(self) -> list[TaskRecord]:
        return list(self._records.values())
