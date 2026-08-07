"""Coordinator and Agent caches for the fixed task-message schema."""

from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass, field
import copy
import hashlib
import json
import time
import uuid
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from .tasks import TaskSpec

from .task_messages import ConstraintDelta, DeltaOperation, EvidenceRef, FactDelta, TaskMessage


@dataclass
class TaskContext:
    """Local cache state. ``revision`` remains internal to the authority/cache."""

    task_id: str
    revision: int = 0
    summary: str | None = None
    facts: dict[str, str] = field(default_factory=dict)
    constraints: dict[str, str] = field(default_factory=dict)
    evidence_refs: dict[str, EvidenceRef] = field(default_factory=dict)
    updated_at: float = field(default_factory=time.time)

    def to_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "summary": self.summary,
            "facts": [{"id": item_id, "text": text} for item_id, text in sorted(self.facts.items())],
            "constraints": [
                {"id": item_id, "text": text} for item_id, text in sorted(self.constraints.items())
            ],
            "evidence_refs": [item.to_dict() for _, item in sorted(self.evidence_refs.items())],
        }


class TaskContextStore:
    """In-memory cache keyed only by ``task_id``.

    The ten-field wire protocol remains unchanged. All coordinator-to-agent
    context frames, including snapshots, are now emitted on the same MQTT topic
    so QoS-1 ordering fences snapshot replacement against ordinary deltas.
    Duplicate messages remain idempotent by ``message_id``.
    """

    def __init__(self, dedup_capacity: int = 4_096) -> None:
        if dedup_capacity < 1:
            raise ValueError("dedup_capacity must be positive")
        self._contexts: dict[str, TaskContext] = {}
        self._dedup_capacity = dedup_capacity
        self._applied_message_ids: OrderedDict[str, str] = OrderedDict()

    def ensure(self, task_id: str) -> TaskContext:
        if task_id not in self._contexts:
            self._contexts[task_id] = TaskContext(task_id=task_id)
        return self._contexts[task_id]

    def get(self, task_id: str) -> TaskContext:
        return self._contexts[task_id]


    def remove(self, task_id: str) -> TaskContext | None:
        """Remove cached task context, used when a planner removes a node."""
        return self._contexts.pop(task_id, None)

    @staticmethod
    def _clip(value: Any, maximum: int = 1_024) -> str:
        text = str(value).strip()
        return text if len(text) <= maximum else text[: maximum - 1] + "…"

    def initialize_from_spec(
        self,
        spec: "TaskSpec",
        *,
        sender: str = "planner",
        replace: bool = False,
    ) -> TaskContext:
        """Seed authoritative context from a Planner/ToDAG ``TaskSpec``.

        This closes the former integration gap where ToDAG metadata reached
        the Coordinator but never entered the receiver-conditioned context
        cache. Hard constraints and prohibitions are emitted as lossless
        constraint deltas; preferences, acceptance conditions, budget, risk
        and evidence dependencies become compact facts.
        """
        metadata = dict(spec.metadata or {})
        constraints: list[ConstraintDelta] = []
        facts: list[FactDelta] = []

        for index, item in enumerate(metadata.get("hard_constraints") or []):
            constraints.append(ConstraintDelta(
                id=f"hard:{index}",
                op=DeltaOperation.REPLACE,
                text=self._clip(item),
            ))
        for index, item in enumerate(metadata.get("prohibitions") or []):
            constraints.append(ConstraintDelta(
                id=f"prohibition:{index}",
                op=DeltaOperation.REPLACE,
                text=self._clip(item),
            ))
        for index, item in enumerate(metadata.get("soft_preferences") or []):
            facts.append(FactDelta(
                id=f"preference:{index}",
                op=DeltaOperation.REPLACE,
                text=self._clip(item),
            ))
        for index, item in enumerate(metadata.get("acceptance_conditions") or []):
            facts.append(FactDelta(
                id=f"acceptance:{index}",
                op=DeltaOperation.REPLACE,
                text=self._clip(item),
            ))
        for dep in metadata.get("evidence_dependencies") or []:
            facts.append(FactDelta(
                id=f"evidence_dependency:{dep}",
                op=DeltaOperation.REPLACE,
                text=self._clip(f"requires evidence from task {dep}"),
            ))

        budget = metadata.get("budget")
        if isinstance(budget, dict):
            for key, value in sorted(budget.items()):
                if value is not None:
                    facts.append(FactDelta(
                        id=f"budget:{key}",
                        op=DeltaOperation.REPLACE,
                        text=self._clip(f"{key}={value}"),
                    ))

        risk = metadata.get("risk")
        if risk:
            risk_text = json.dumps(risk, ensure_ascii=False, sort_keys=True) if isinstance(risk, (dict, list)) else str(risk)
            facts.append(FactDelta(
                id="risk",
                op=DeltaOperation.REPLACE,
                text=self._clip(risk_text),
            ))

        signature_doc = {
            "task_id": spec.task_id,
            "title": spec.title,
            "constraints": [item.to_dict() for item in constraints],
            "facts": [item.to_dict() for item in facts],
            "version": metadata.get("todag_node_version"),
            "revision": metadata.get("todag_revision"),
        }
        raw = json.dumps(signature_doc, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        signature = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]
        message = TaskMessage.create(
            message_id=f"bootstrap:{spec.task_id}:{signature}",
            sender=sender,
            receiver="coordinator",
            task_id=spec.task_id,
            summary=self._clip(spec.title, 240),
            facts=tuple(facts),
            constraints=tuple(constraints),
            evidence_refs=(),
            priority=max(1, min(10, int(spec.priority))),
            ttl=86_400,
        )
        return self.apply(message, replace=replace)

    def snapshot(self, task_id: str) -> dict[str, Any]:
        return copy.deepcopy(self.get(task_id).to_dict())

    def build_snapshot_message(
        self,
        *,
        sender: str,
        receiver: str,
        task_id: str,
        priority: int = 5,
        ttl: int = 300,
    ) -> TaskMessage:
        """Build a complete replacement message using only the agreed fields."""
        context = self.get(task_id)
        return TaskMessage.create(
            message_id=f"snapshot:{context.revision}:{uuid.uuid4().hex}",
            sender=sender,
            receiver=receiver,
            task_id=task_id,
            summary=context.summary,
            facts=tuple(
                FactDelta(id=item_id, op=DeltaOperation.REPLACE, text=text)
                for item_id, text in sorted(context.facts.items())
            ),
            constraints=tuple(
                ConstraintDelta(id=item_id, op=DeltaOperation.REPLACE, text=text)
                for item_id, text in sorted(context.constraints.items())
            ),
            evidence_refs=tuple(item for _, item in sorted(context.evidence_refs.items())),
            priority=priority,
            ttl=ttl,
        )

    def apply(self, message: TaskMessage, *, replace: bool = False) -> TaskContext:
        """Merge a delta or replace state for a coordinator-issued snapshot."""
        message.validate()
        seen_task_id = self._applied_message_ids.get(message.message_id)
        if seen_task_id is not None:
            if seen_task_id != message.task_id:
                raise ValueError(f"task message ID {message.message_id} was reused for another task")
            self._applied_message_ids.move_to_end(message.message_id)
            return self.ensure(message.task_id)

        context = self.ensure(message.task_id)
        if replace:
            context.summary = None
            context.facts.clear()
            context.constraints.clear()
            context.evidence_refs.clear()
        if message.summary is not None:
            context.summary = message.summary
        self._apply_deltas(context.facts, message.facts)
        self._apply_deltas(context.constraints, message.constraints)
        for evidence in message.evidence_refs:
            if evidence.op is DeltaOperation.REMOVE:
                context.evidence_refs.pop(evidence.id, None)
            else:
                context.evidence_refs[evidence.id] = evidence
        context.revision += 1
        context.updated_at = time.time()
        self._applied_message_ids[message.message_id] = message.task_id
        while len(self._applied_message_ids) > self._dedup_capacity:
            self._applied_message_ids.popitem(last=False)
        return context

    @staticmethod
    def _apply_deltas(target: dict[str, str], deltas: tuple[FactDelta, ...] | tuple[ConstraintDelta, ...]) -> None:
        for delta in deltas:
            if delta.op is DeltaOperation.REMOVE:
                target.pop(delta.id, None)
            elif delta.text is not None:
                target[delta.id] = delta.text
