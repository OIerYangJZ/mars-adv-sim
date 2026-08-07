"""Independent end-edge-cloud resource scheduling primitives.

The Coordinator owns task lifecycle and dispatch.  This module only selects
and reserves a resource node for a single Planner DAG task.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from math import isfinite
from threading import RLock
import time
from typing import Any, Iterable, Mapping


VALID_LAYERS = frozenset({"device", "edge", "cloud"})
DEFAULT_WEIGHTS = {
    "load": 0.35,
    "latency": 0.30,
    "compute": 0.20,
    "success_rate": 0.15,
}


def _number(value: Any, field_name: str) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{field_name}必须是数值")

    try:
        number = float(value)
    except (TypeError, ValueError) as error:
        raise ValueError(f"{field_name}必须是数值") from error

    if not isfinite(number):
        raise ValueError(f"{field_name}必须是有限数值")

    return number


def _normalise_layer(layer: str) -> str:
    if not isinstance(layer, str):
        raise ValueError("layer必须是字符串")

    normalised = layer.strip().lower()

    if normalised not in VALID_LAYERS:
        raise ValueError(
            "layer必须是device、edge或cloud之一"
        )

    return normalised


@dataclass
class ResourceNode:
    """An executable node in the device, edge, or cloud layer."""

    node_id: str
    layer: str
    skills: set[str]
    current_load: float
    latency_ms: float
    compute_score: float
    success_rate: float
    online: bool = True
    last_heartbeat: float = field(default_factory=time.monotonic)
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.node_id, str) or not self.node_id.strip():
            raise ValueError("node_id必须是非空字符串")

        self.node_id = self.node_id.strip()
        self.layer = _normalise_layer(self.layer)
        self.skills = {
            skill.strip()
            for skill in self.skills
            if isinstance(skill, str) and skill.strip()
        }

        if not self.skills:
            raise ValueError("skills必须至少包含一个能力标签")

        self.current_load = _number(self.current_load, "current_load")
        self.latency_ms = _number(self.latency_ms, "latency_ms")
        self.compute_score = _number(self.compute_score, "compute_score")
        self.success_rate = _number(self.success_rate, "success_rate")
        self.last_heartbeat = _number(
            self.last_heartbeat,
            "last_heartbeat",
        )

        if not 0 <= self.current_load <= 1:
            raise ValueError("current_load必须在0到1之间")

        if self.latency_ms < 0:
            raise ValueError("latency_ms不能小于0")

        if self.compute_score < 0:
            raise ValueError("compute_score不能小于0")

        if not 0 <= self.success_rate <= 1:
            raise ValueError("success_rate必须在0到1之间")

        if not isinstance(self.online, bool):
            raise ValueError("online必须是布尔值")

        if not isinstance(self.metadata, dict):
            raise ValueError("metadata必须是对象")

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ResourceNode":
        skills = data["skills"]

        if isinstance(skills, str):
            skills = {skills}

        return cls(
            node_id=data["node_id"],
            layer=data["layer"],
            skills=set(skills),
            current_load=data["current_load"],
            latency_ms=data["latency_ms"],
            compute_score=data["compute_score"],
            success_rate=data["success_rate"],
            online=data.get("online", True),
            last_heartbeat=data.get("last_heartbeat", time.monotonic()),
            metadata=dict(data.get("metadata", {})),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "node_id": self.node_id,
            "layer": self.layer,
            "skills": sorted(self.skills),
            "current_load": self.current_load,
            "latency_ms": self.latency_ms,
            "compute_score": self.compute_score,
            "success_rate": self.success_rate,
            "online": self.online,
            "last_heartbeat": self.last_heartbeat,
            "metadata": self.metadata.copy(),
        }


@dataclass(frozen=True)
class TaskRequirement:
    """Scheduling requirements for one DAG node."""

    task_id: str
    description: str
    required_skill: str
    max_latency_ms: float | None = None
    min_compute_score: float = 0
    local_only: bool = False
    priority: int = 5
    preferred_layers: tuple[str, ...] = ()
    estimated_load: float = 0.1
    allow_degraded: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.task_id, str) or not self.task_id.strip():
            raise ValueError("task_id必须是非空字符串")

        if not isinstance(self.description, str) or not self.description.strip():
            raise ValueError("description必须是非空字符串")

        if (
            not isinstance(self.required_skill, str)
            or not self.required_skill.strip()
        ):
            raise ValueError("required_skill必须是非空字符串")

        object.__setattr__(self, "task_id", self.task_id.strip())
        object.__setattr__(self, "description", self.description.strip())
        object.__setattr__(
            self,
            "required_skill",
            self.required_skill.strip(),
        )

        if self.max_latency_ms is not None:
            max_latency_ms = _number(
                self.max_latency_ms,
                "max_latency_ms",
            )

            if max_latency_ms <= 0:
                raise ValueError("max_latency_ms必须大于0")

            object.__setattr__(self, "max_latency_ms", max_latency_ms)

        min_compute_score = _number(
            self.min_compute_score,
            "min_compute_score",
        )

        if min_compute_score < 0:
            raise ValueError("min_compute_score不能小于0")

        object.__setattr__(self, "min_compute_score", min_compute_score)

        estimated_load = _number(self.estimated_load, "estimated_load")

        if not 0 < estimated_load <= 1:
            raise ValueError("estimated_load必须大于0且不超过1")

        object.__setattr__(self, "estimated_load", estimated_load)

        if (
            not isinstance(self.priority, int)
            or isinstance(self.priority, bool)
            or not 1 <= self.priority <= 10
        ):
            raise ValueError("priority必须是1到10之间的整数")

        if not isinstance(self.local_only, bool):
            raise ValueError("local_only必须是布尔值")

        if not isinstance(self.allow_degraded, bool):
            raise ValueError("allow_degraded必须是布尔值")

        raw_preferred_layers = self.preferred_layers

        if isinstance(raw_preferred_layers, str):
            raw_preferred_layers = (raw_preferred_layers,)

        preferred_layers = tuple(
            _normalise_layer(layer)
            for layer in raw_preferred_layers
        )
        object.__setattr__(self, "preferred_layers", preferred_layers)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "TaskRequirement":
        preferred_layers = data.get("preferred_layers", ())

        if preferred_layers is None:
            preferred_layers = ()

        if isinstance(preferred_layers, str):
            preferred_layers = (preferred_layers,)

        return cls(
            task_id=data["task_id"],
            description=data["description"],
            required_skill=data["required_skill"],
            max_latency_ms=data.get("max_latency_ms"),
            min_compute_score=data.get("min_compute_score", 0),
            local_only=data.get("local_only", False),
            priority=data.get("priority", 5),
            preferred_layers=tuple(preferred_layers),
            estimated_load=data.get("estimated_load", 0.1),
            allow_degraded=data.get("allow_degraded", False),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "description": self.description,
            "required_skill": self.required_skill,
            "max_latency_ms": self.max_latency_ms,
            "min_compute_score": self.min_compute_score,
            "local_only": self.local_only,
            "priority": self.priority,
            "preferred_layers": list(self.preferred_layers),
            "estimated_load": self.estimated_load,
            "allow_degraded": self.allow_degraded,
        }


@dataclass(frozen=True)
class SchedulingDecision:
    """A JSON-serialisable selection result returned to the Coordinator."""

    task_id: str
    node_id: str
    layer: str
    score: float
    score_breakdown: dict[str, float]
    degraded: bool = False
    violations: tuple[str, ...] = ()
    rejected_nodes: dict[str, list[str]] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "node_id": self.node_id,
            "layer": self.layer,
            "score": round(self.score, 6),
            "score_breakdown": {
                key: round(value, 6)
                for key, value in self.score_breakdown.items()
            },
            "degraded": self.degraded,
            "violations": list(self.violations),
            "rejected_nodes": {
                node_id: reasons.copy()
                for node_id, reasons in self.rejected_nodes.items()
            },
        }


class NoSchedulableNodeError(RuntimeError):
    """Raised when no online node can safely execute a task."""

    def __init__(
        self,
        task_id: str,
        rejected_nodes: Mapping[str, list[str]],
    ) -> None:
        self.task_id = task_id
        self.rejected_nodes = {
            node_id: reasons.copy()
            for node_id, reasons in rejected_nodes.items()
        }
        super().__init__(f"任务{task_id}没有可调度的资源节点")

    def to_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "error": "no_schedulable_node",
            "rejected_nodes": self.rejected_nodes,
        }


class ResourceScheduler:
    """Thread-safe resource registry and deterministic node selector."""

    def __init__(
        self,
        nodes: Iterable[ResourceNode] = (),
        *,
        heartbeat_timeout_s: float = 30.0,
        weights: Mapping[str, float] | None = None,
    ) -> None:
        self._lock = RLock()
        self._nodes: dict[str, ResourceNode] = {}
        self._assignments: dict[str, tuple[str, float]] = {}
        self.heartbeat_timeout_s = _number(
            heartbeat_timeout_s,
            "heartbeat_timeout_s",
        )

        if self.heartbeat_timeout_s <= 0:
            raise ValueError("heartbeat_timeout_s必须大于0")

        combined_weights = DEFAULT_WEIGHTS.copy()
        combined_weights.update(weights or {})

        if set(combined_weights) != set(DEFAULT_WEIGHTS):
            raise ValueError("weights必须包含load、latency、compute、success_rate")

        combined_weights = {
            name: _number(value, f"weights.{name}")
            for name, value in combined_weights.items()
        }

        if any(value < 0 for value in combined_weights.values()):
            raise ValueError("weights不能小于0")

        total_weight = sum(combined_weights.values())

        if total_weight <= 0:
            raise ValueError("weights的总和必须大于0")

        self._weights = {
            name: value / total_weight
            for name, value in combined_weights.items()
        }

        for node in nodes:
            self.register_node(node)

    @staticmethod
    def _copy_node(node: ResourceNode) -> ResourceNode:
        return replace(
            node,
            skills=set(node.skills),
            metadata=node.metadata.copy(),
        )

    @staticmethod
    def _as_requirement(
        task: TaskRequirement | Mapping[str, Any],
    ) -> TaskRequirement:
        if isinstance(task, TaskRequirement):
            return task

        return TaskRequirement.from_dict(task)

    def register_node(
        self,
        node: ResourceNode | Mapping[str, Any],
        *,
        replace_existing: bool = False,
    ) -> None:
        if not isinstance(node, ResourceNode):
            node = ResourceNode.from_dict(node)

        with self._lock:
            if node.node_id in self._nodes and not replace_existing:
                raise ValueError(f"节点{node.node_id}已经注册")

            self._nodes[node.node_id] = self._copy_node(node)

    def unregister_node(self, node_id: str) -> ResourceNode | None:
        with self._lock:
            node = self._nodes.pop(node_id, None)

            if node is None:
                return None

            for task_id, (assigned_node_id, _) in list(
                self._assignments.items()
            ):
                if assigned_node_id == node_id:
                    del self._assignments[task_id]

            return self._copy_node(node)

    def get_node(self, node_id: str) -> ResourceNode | None:
        with self._lock:
            node = self._nodes.get(node_id)
            return self._copy_node(node) if node is not None else None

    def list_nodes(self) -> list[ResourceNode]:
        with self._lock:
            return [
                self._copy_node(node)
                for node in sorted(
                    self._nodes.values(),
                    key=lambda item: item.node_id,
                )
            ]

    def heartbeat(
        self,
        node_id: str,
        *,
        current_load: float | None = None,
        latency_ms: float | None = None,
        compute_score: float | None = None,
        success_rate: float | None = None,
        now: float | None = None,
    ) -> None:
        with self._lock:
            node = self._nodes.get(node_id)

            if node is None:
                raise KeyError(f"节点{node_id}未注册")

            updates: dict[str, Any] = {
                "online": True,
                "last_heartbeat": time.monotonic() if now is None else now,
            }

            for field_name, value in {
                "current_load": current_load,
                "latency_ms": latency_ms,
                "compute_score": compute_score,
                "success_rate": success_rate,
            }.items():
                if value is not None:
                    updates[field_name] = value

            self._nodes[node_id] = ResourceNode(
                **{
                    **node.to_dict(),
                    **updates,
                }
            )

    def set_online(self, node_id: str, online: bool) -> None:
        if not isinstance(online, bool):
            raise ValueError("online必须是布尔值")

        with self._lock:
            node = self._nodes.get(node_id)

            if node is None:
                raise KeyError(f"节点{node_id}未注册")

            self._nodes[node_id] = replace(node, online=online)

    def expire_stale_nodes(self, *, now: float | None = None) -> list[str]:
        """Mark nodes without a timely heartbeat as offline."""

        current_time = time.monotonic() if now is None else _number(now, "now")
        expired_node_ids = []

        with self._lock:
            for node in self._nodes.values():
                if (
                    node.online
                    and current_time - node.last_heartbeat
                    > self.heartbeat_timeout_s
                ):
                    node.online = False
                    expired_node_ids.append(node.node_id)

        return sorted(expired_node_ids)

    def _constraint_violations(
        self,
        node: ResourceNode,
        task: TaskRequirement,
        excluded_node_ids: set[str],
        *,
        relax_resource_limits: bool,
    ) -> list[str]:
        violations = []

        if node.node_id in excluded_node_ids:
            violations.append("excluded")

        if not node.online:
            violations.append("offline")

        if task.required_skill not in node.skills:
            violations.append("missing_required_skill")

        if task.local_only and node.layer != "device":
            violations.append("local_only_requires_device")

        if node.current_load + task.estimated_load > 1 + 1e-9:
            violations.append("insufficient_capacity")

        if not relax_resource_limits:
            if (
                task.max_latency_ms is not None
                and node.latency_ms > task.max_latency_ms
            ):
                violations.append("latency_limit_exceeded")

            if node.compute_score < task.min_compute_score:
                violations.append("compute_requirement_not_met")

        return violations

    def _score_node(
        self,
        node: ResourceNode,
        task: TaskRequirement,
        max_compute_score: float,
    ) -> tuple[float, dict[str, float]]:
        load_score = 1 - node.current_load

        if task.max_latency_ms is None:
            latency_score = 1 / (1 + node.latency_ms / 50)
        else:
            latency_score = max(
                0,
                1 - node.latency_ms / task.max_latency_ms,
            )

        compute_score = node.compute_score / max_compute_score
        success_rate_score = node.success_rate
        score_breakdown = {
            "load": load_score,
            "latency": latency_score,
            "compute": compute_score,
            "success_rate": success_rate_score,
        }
        weighted_score = sum(
            self._weights[name] * value
            for name, value in score_breakdown.items()
        )

        if node.layer in task.preferred_layers:
            weighted_score += 0.05
            score_breakdown["preferred_layer_bonus"] = 0.05

        return weighted_score, score_breakdown

    def _select_locked(
        self,
        task: TaskRequirement,
        excluded_node_ids: set[str],
    ) -> SchedulingDecision:
        rejected_nodes: dict[str, list[str]] = {}
        candidates: list[tuple[ResourceNode, tuple[str, ...], bool]] = []

        for node in sorted(
            self._nodes.values(),
            key=lambda item: item.node_id,
        ):
            violations = self._constraint_violations(
                node,
                task,
                excluded_node_ids,
                relax_resource_limits=False,
            )

            if violations:
                rejected_nodes[node.node_id] = violations
                continue

            candidates.append((node, (), False))

        if not candidates and task.allow_degraded:
            for node in sorted(
                self._nodes.values(),
                key=lambda item: item.node_id,
            ):
                violations = self._constraint_violations(
                    node,
                    task,
                    excluded_node_ids,
                    relax_resource_limits=True,
                )

                if violations:
                    continue

                degraded_violations = []

                if (
                    task.max_latency_ms is not None
                    and node.latency_ms > task.max_latency_ms
                ):
                    degraded_violations.append("latency_limit_exceeded")

                if node.compute_score < task.min_compute_score:
                    degraded_violations.append(
                        "compute_requirement_not_met"
                    )

                candidates.append(
                    (node, tuple(degraded_violations), True)
                )

        if not candidates:
            raise NoSchedulableNodeError(task.task_id, rejected_nodes)

        max_compute_score = max(
            node.compute_score
            for node, _, _ in candidates
        )

        scored_candidates = []

        for node, violations, degraded in candidates:
            score, score_breakdown = self._score_node(
                node,
                task,
                max_compute_score,
            )
            scored_candidates.append(
                (
                    score,
                    node,
                    score_breakdown,
                    violations,
                    degraded,
                )
            )

        (
            score,
            selected_node,
            score_breakdown,
            violations,
            degraded,
        ) = min(
            scored_candidates,
            key=lambda item: (
                -item[0],
                item[1].latency_ms,
                item[1].current_load,
                item[1].node_id,
            ),
        )

        return SchedulingDecision(
            task_id=task.task_id,
            node_id=selected_node.node_id,
            layer=selected_node.layer,
            score=score,
            score_breakdown=score_breakdown,
            degraded=degraded,
            violations=violations,
            rejected_nodes=rejected_nodes,
        )

    def select_node(
        self,
        task: TaskRequirement | Mapping[str, Any],
        *,
        excluded_node_ids: Iterable[str] = (),
    ) -> SchedulingDecision:
        """Select a node without changing load or recording an assignment."""

        requirement = self._as_requirement(task)

        with self._lock:
            return self._select_locked(
                requirement,
                set(excluded_node_ids),
            )

    def allocate_task(
        self,
        task: TaskRequirement | Mapping[str, Any],
        *,
        excluded_node_ids: Iterable[str] = (),
    ) -> SchedulingDecision:
        """Select, reserve estimated load, and persist the assignment."""

        requirement = self._as_requirement(task)

        with self._lock:
            if requirement.task_id in self._assignments:
                raise ValueError(
                    f"任务{requirement.task_id}已经分配资源"
                )

            decision = self._select_locked(
                requirement,
                set(excluded_node_ids),
            )
            node = self._nodes[decision.node_id]
            node.current_load = round(
                node.current_load + requirement.estimated_load,
                9,
            )
            self._assignments[requirement.task_id] = (
                decision.node_id,
                requirement.estimated_load,
            )

            return decision

    def release_task(self, task_id: str) -> str | None:
        """Release a prior allocation and return its node ID."""

        with self._lock:
            assignment = self._assignments.pop(task_id, None)

            if assignment is None:
                return None

            node_id, reserved_load = assignment
            node = self._nodes.get(node_id)

            if node is not None:
                node.current_load = round(
                    max(0, node.current_load - reserved_load),
                    9,
                )

            return node_id

    def migrate_task(
        self,
        task: TaskRequirement | Mapping[str, Any],
        *,
        failed_node_id: str | None = None,
        mark_failed_offline: bool = False,
    ) -> SchedulingDecision:
        """Release an old allocation and assign the task to another node."""

        requirement = self._as_requirement(task)

        with self._lock:
            previous_node_id = self.release_task(requirement.task_id)
            excluded_node_ids = {
                node_id
                for node_id in (previous_node_id, failed_node_id)
                if node_id is not None
            }

            if mark_failed_offline and failed_node_id is not None:
                node = self._nodes.get(failed_node_id)

                if node is not None:
                    node.online = False

            return self.allocate_task(
                requirement,
                excluded_node_ids=excluded_node_ids,
            )

    def get_assignment(self, task_id: str) -> str | None:
        with self._lock:
            assignment = self._assignments.get(task_id)
            return assignment[0] if assignment is not None else None

    def schedule_ready_tasks(
        self,
        plan: list[Mapping[str, Any]],
        completed_task_ids: Iterable[str],
        *,
        reserve: bool = False,
    ) -> dict[str, SchedulingDecision]:
        """Schedule all DAG nodes whose dependencies have completed."""

        completed = set(completed_task_ids)
        ready_tasks = [
            task for task in plan
            if str(task["task_id"]) not in completed
            and all(str(parent) in completed for parent in task.get("depends_on", ()))
        ]
        ready_tasks.sort(key=lambda task: (-int(task.get("priority", 5)), str(task["task_id"])))
        decisions: dict[str, SchedulingDecision] = {}
        allocated_task_ids = []

        try:
            for task in ready_tasks:
                if reserve:
                    decision = self.allocate_task(task)
                    allocated_task_ids.append(task["task_id"])
                else:
                    decision = self.select_node(task)

                decisions[task["task_id"]] = decision
        except Exception:
            for task_id in allocated_task_ids:
                self.release_task(task_id)
            raise

        return decisions

    def schedule_task_payload(
        self,
        task: Mapping[str, Any],
        *,
        reserve: bool = False,
    ) -> dict[str, Any]:
        """Coordinator-facing convenience API with a JSON-only result."""

        decision = (
            self.allocate_task(task)
            if reserve
            else self.select_node(task)
        )

        return {
            "schema_version": "1.0",
            "status": "scheduled",
            "decision": decision.to_dict(),
        }
