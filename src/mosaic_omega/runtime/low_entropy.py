"""Receiver-conditioned low-entropy communication primitives.

The public ``TaskMessage`` wire body stays fixed at ten top-level fields.  The
additional mechanisms in this module are coordinator-side state: receiver
knowledge digests, semantic de-duplication, decision-impact gating, deferred
merge queues, causal ledgers, feedback calibration and communication metrics.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
import hashlib
import json
import math
import time
import uuid
from typing import Any

from .task_context import TaskContext
from .task_messages import ConstraintDelta, DeltaOperation, EvidenceRef, FactDelta, TaskMessage


def _stable_hash(value: Any) -> str:
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()[:20]


def text_hash(text: str | None) -> str | None:
    if text is None:
        return None
    return _stable_hash(text)


def evidence_hash(evidence: EvidenceRef) -> str:
    return _stable_hash({"artifact_id": evidence.artifact_id, "note": evidence.note})


def semantic_fingerprint(message: TaskMessage) -> str:
    """Content hash that deliberately ignores the transport-level message ID."""
    return _stable_hash(
        {
            "sender": message.sender,
            "receiver": message.receiver,
            "task_id": message.task_id,
            "summary": message.summary,
            "facts": [item.to_dict() for item in message.facts],
            "constraints": [item.to_dict() for item in message.constraints],
            "evidence_refs": [item.to_dict() for item in message.evidence_refs],
            "priority": message.priority,
            "ttl": message.ttl,
        }
    )


def encoded_size(value: object) -> int:
    return len(json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))


def estimate_tokens(value: object) -> int:
    """Dependency-free token estimate used only for comparative telemetry.

    This intentionally does not claim model-exact token accounting.  For ASCII
    text roughly four characters form a token; CJK characters are counted more
    conservatively as one token each.
    """
    text = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    ascii_chars = sum(ord(char) < 128 for char in text)
    non_ascii_chars = len(text) - ascii_chars
    return max(1, math.ceil(ascii_chars / 4) + non_ascii_chars)


@dataclass
class KnowledgeDigest:
    """Coordinator's conservative model of one receiver's acknowledged state."""

    task_id: str
    summary_hash: str | None = None
    fact_hashes: dict[str, str] = field(default_factory=dict)
    constraint_hashes: dict[str, str] = field(default_factory=dict)
    evidence_hashes: dict[str, str] = field(default_factory=dict)
    acknowledged_messages: int = 0
    updated_at: float = field(default_factory=time.monotonic)

    def acknowledge(self, message: TaskMessage) -> None:
        if message.summary is not None:
            self.summary_hash = text_hash(message.summary)
        for delta in message.facts:
            if delta.op is DeltaOperation.REMOVE:
                self.fact_hashes.pop(delta.id, None)
            elif delta.text is not None:
                self.fact_hashes[delta.id] = text_hash(delta.text) or ""
        for delta in message.constraints:
            if delta.op is DeltaOperation.REMOVE:
                self.constraint_hashes.pop(delta.id, None)
            elif delta.text is not None:
                self.constraint_hashes[delta.id] = text_hash(delta.text) or ""
        for evidence in message.evidence_refs:
            if evidence.op is DeltaOperation.REMOVE:
                self.evidence_hashes.pop(evidence.id, None)
            else:
                self.evidence_hashes[evidence.id] = evidence_hash(evidence)
        self.acknowledged_messages += 1
        self.updated_at = time.monotonic()


class ReceiverKnowledgeStore:
    """Knowledge digests are advanced only after receiver acknowledgement.

    If an ACK is lost, the model remains conservative and may resend a fact; it
    never suppresses information merely because the coordinator *attempted* to
    send it.
    """

    def __init__(self) -> None:
        self._digests: dict[tuple[str, str], KnowledgeDigest] = {}

    def get(self, receiver: str, task_id: str) -> KnowledgeDigest:
        key = (receiver, task_id)
        if key not in self._digests:
            self._digests[key] = KnowledgeDigest(task_id=task_id)
        return self._digests[key]

    def acknowledge(self, receiver: str, message: TaskMessage) -> KnowledgeDigest:
        digest = self.get(receiver, message.task_id)
        digest.acknowledge(message)
        return digest

    def reset_agent(self, receiver: str) -> None:
        for key in [key for key in self._digests if key[0] == receiver]:
            self._digests.pop(key, None)


class ReceiverConditionedCompressor:
    """Build only the state difference that the receiver has not ACKed."""

    @staticmethod
    def tailor(
        original: TaskMessage,
        authoritative: TaskContext,
        digest: KnowledgeDigest,
        *,
        receiver: str,
    ) -> TaskMessage | None:
        summary: str | None = None
        if original.summary is not None and text_hash(authoritative.summary) != digest.summary_hash:
            summary = authoritative.summary

        facts: list[FactDelta] = []
        for delta in original.facts:
            if delta.op is DeltaOperation.REMOVE:
                if delta.id in digest.fact_hashes:
                    facts.append(delta)
                continue
            current = authoritative.facts.get(delta.id)
            if current is None:
                continue
            if digest.fact_hashes.get(delta.id) != text_hash(current):
                facts.append(FactDelta(delta.id, delta.op, current))

        constraints: list[ConstraintDelta] = []
        for delta in original.constraints:
            if delta.op is DeltaOperation.REMOVE:
                if delta.id in digest.constraint_hashes:
                    constraints.append(delta)
                continue
            current = authoritative.constraints.get(delta.id)
            if current is None:
                continue
            if digest.constraint_hashes.get(delta.id) != text_hash(current):
                constraints.append(ConstraintDelta(delta.id, delta.op, current))

        evidence_refs: list[EvidenceRef] = []
        for candidate in original.evidence_refs:
            if candidate.op is DeltaOperation.REMOVE:
                if candidate.id in digest.evidence_hashes:
                    evidence_refs.append(candidate)
                continue
            current = authoritative.evidence_refs.get(candidate.id)
            if current is None:
                continue
            if digest.evidence_hashes.get(candidate.id) != evidence_hash(current):
                evidence_refs.append(current)

        if summary is None and not facts and not constraints and not evidence_refs:
            return None
        return TaskMessage.create(
            message_id=uuid.uuid4().hex,
            sender=original.sender,
            receiver=receiver,
            task_id=original.task_id,
            summary=summary,
            facts=tuple(facts),
            constraints=tuple(constraints),
            evidence_refs=tuple(evidence_refs),
            priority=original.priority,
            ttl=original.ttl,
        )


class PolicyAction(str, Enum):
    SEND = "send"
    DEFER = "defer"
    DROP = "drop"


@dataclass(frozen=True)
class PolicyDecision:
    action: PolicyAction
    impact_score: float
    reason: str
    defer_s: float = 0.0


class DecisionImpactEstimator:
    """Small deterministic baseline for online decision-impact estimation.

    Safety/permission constraints and evidence are never lossily suppressed.
    The scorer is intentionally explainable so it can later be replaced by a
    lightweight learned classifier without changing the surrounding API.
    """

    CRITICAL_TERMS = (
        "must", "forbid", "deny", "privacy", "permission", "security", "deadline",
        "必须", "不得", "禁止", "隐私", "权限", "安全", "截止", "故障", "失败",
    )

    def score(self, message: TaskMessage) -> float:
        score = 0.15 + 0.055 * message.priority
        if message.constraints:
            score = max(score, 0.95)
        if message.evidence_refs:
            score = max(score, 0.85)
        searchable = " ".join(
            [message.summary or ""]
            + [item.text or "" for item in message.facts]
            + [item.text or "" for item in message.constraints]
        ).lower()
        if any(term in searchable for term in self.CRITICAL_TERMS):
            score = max(score, 0.95)
        if any(item.op is DeltaOperation.REMOVE for item in (*message.facts, *message.constraints, *message.evidence_refs)):
            score = max(score, 0.85)
        return min(1.0, score)

    def decide(self, message: TaskMessage) -> PolicyDecision:
        score = self.score(message)
        if score >= 0.72:
            return PolicyDecision(PolicyAction.SEND, score, "high_decision_impact")
        # Unique low-impact information is deferred and merged, not discarded.
        # Actual drops are reserved for duplicates/expiry at the queue layer.
        defer_s = 0.05 if message.priority >= 5 else 0.15
        return PolicyDecision(PolicyAction.DEFER, score, "low_impact_merge_window", defer_s)


@dataclass
class DeferredMessage:
    message: TaskMessage
    enqueued_at: float
    send_after: float
    expires_at: float


class LowEntropyOutbox:
    """Priority-aware defer/merge queue for low-impact messages."""

    def __init__(self) -> None:
        self._pending: dict[tuple[str, str, str], DeferredMessage] = {}

    @staticmethod
    def _key(message: TaskMessage) -> tuple[str, str, str]:
        return (message.sender, message.receiver, message.task_id)

    def enqueue(self, message: TaskMessage, *, defer_s: float, now: float | None = None) -> bool:
        now = time.monotonic() if now is None else now
        key = self._key(message)
        existing = self._pending.get(key)
        merged = existing is not None
        if existing is not None:
            message = merge_messages(existing.message, message)
            enqueued_at = existing.enqueued_at
            expires_at = min(existing.expires_at, now + message.ttl)
        else:
            enqueued_at = now
            expires_at = now + message.ttl
        # Higher priority shortens the merge window.
        priority_delay = max(0.0, defer_s * (11 - message.priority) / 10)
        self._pending[key] = DeferredMessage(
            message=message,
            enqueued_at=enqueued_at,
            send_after=now + priority_delay,
            expires_at=expires_at,
        )
        return merged

    def pop_ready(self, *, now: float | None = None) -> tuple[list[DeferredMessage], list[DeferredMessage]]:
        now = time.monotonic() if now is None else now
        ready: list[DeferredMessage] = []
        expired: list[DeferredMessage] = []
        for key, item in list(self._pending.items()):
            if item.expires_at <= now:
                expired.append(item)
                self._pending.pop(key, None)
            elif item.send_after <= now:
                ready.append(item)
                self._pending.pop(key, None)
        ready.sort(key=lambda item: (-item.message.priority, item.enqueued_at))
        return ready, expired


def _merge_delta_items(items: tuple[Any, ...], newer: tuple[Any, ...]) -> tuple[Any, ...]:
    merged: dict[str, Any] = {item.id: item for item in items}
    for item in newer:
        merged[item.id] = item
    return tuple(merged[key] for key in sorted(merged))


def merge_messages(older: TaskMessage, newer: TaskMessage) -> TaskMessage:
    if (older.sender, older.receiver, older.task_id) != (newer.sender, newer.receiver, newer.task_id):
        raise ValueError("only messages for the same sender/receiver/task can be merged")
    return TaskMessage.create(
        message_id=uuid.uuid4().hex,
        sender=newer.sender,
        receiver=newer.receiver,
        task_id=newer.task_id,
        summary=newer.summary if newer.summary is not None else older.summary,
        facts=_merge_delta_items(older.facts, newer.facts),
        constraints=_merge_delta_items(older.constraints, newer.constraints),
        evidence_refs=_merge_delta_items(older.evidence_refs, newer.evidence_refs),
        priority=max(older.priority, newer.priority),
        ttl=min(older.ttl, newer.ttl),
    )


@dataclass(frozen=True)
class CausalRecord:
    message_id: str
    task_id: str
    receiver: str
    causal_parent: str | None
    semantic_hash: str
    predicted_impact: float
    created_at: float


class CausalLedger:
    """Central causal chain without adding bytes to the fixed TaskMessage body."""

    def __init__(self) -> None:
        self._last_by_task_receiver: dict[tuple[str, str], str] = {}
        self._records: dict[str, CausalRecord] = {}

    def record(self, message: TaskMessage, predicted_impact: float) -> CausalRecord:
        key = (message.task_id, message.receiver)
        record = CausalRecord(
            message_id=message.message_id,
            task_id=message.task_id,
            receiver=message.receiver,
            causal_parent=self._last_by_task_receiver.get(key),
            semantic_hash=semantic_fingerprint(message),
            predicted_impact=predicted_impact,
            created_at=time.time(),
        )
        self._records[message.message_id] = record
        self._last_by_task_receiver[key] = message.message_id
        return record

    def get(self, message_id: str) -> CausalRecord | None:
        return self._records.get(message_id)


@dataclass
class LowEntropyMetrics:
    candidate_messages: int = 0
    sent_messages: int = 0
    deferred_messages: int = 0
    merged_messages: int = 0
    duplicate_drops: int = 0
    expired_drops: int = 0
    candidate_bytes: int = 0
    sent_bytes: int = 0
    candidate_tokens_est: int = 0
    sent_tokens_est: int = 0
    acked_messages: int = 0
    feedback_positive: int = 0
    feedback_negative: int = 0
    queue_wait_ms: list[float] = field(default_factory=list)

    def observe_candidate(self, message: TaskMessage) -> None:
        body = message.to_dict()
        self.candidate_messages += 1
        self.candidate_bytes += encoded_size(body)
        self.candidate_tokens_est += estimate_tokens(body)

    def observe_sent(self, message: TaskMessage, queue_wait_s: float = 0.0) -> None:
        body = message.to_dict()
        self.sent_messages += 1
        self.sent_bytes += encoded_size(body)
        self.sent_tokens_est += estimate_tokens(body)
        self.queue_wait_ms.append(max(0.0, queue_wait_s) * 1000)

    def snapshot(self) -> dict[str, Any]:
        byte_reduction = 0.0 if not self.candidate_bytes else 1 - self.sent_bytes / self.candidate_bytes
        token_reduction = 0.0 if not self.candidate_tokens_est else 1 - self.sent_tokens_est / self.candidate_tokens_est
        waits = sorted(self.queue_wait_ms)
        p95 = 0.0 if not waits else waits[min(len(waits) - 1, math.ceil(0.95 * len(waits)) - 1)]
        return {
            "candidate_messages": self.candidate_messages,
            "sent_messages": self.sent_messages,
            "deferred_messages": self.deferred_messages,
            "merged_messages": self.merged_messages,
            "duplicate_drops": self.duplicate_drops,
            "expired_drops": self.expired_drops,
            "candidate_bytes": self.candidate_bytes,
            "sent_bytes": self.sent_bytes,
            "byte_reduction_ratio": round(byte_reduction, 6),
            "candidate_tokens_est": self.candidate_tokens_est,
            "sent_tokens_est": self.sent_tokens_est,
            "token_reduction_est_ratio": round(token_reduction, 6),
            "acked_messages": self.acked_messages,
            "feedback_positive": self.feedback_positive,
            "feedback_negative": self.feedback_negative,
            "queue_wait_p95_ms": round(p95, 3),
        }


def critical_fact_fidelity(expected: dict[str, str], reconstructed: dict[str, str]) -> float:
    """Exact critical-fact preservation ratio for experiments and regression tests."""
    if not expected:
        return 1.0
    preserved = sum(reconstructed.get(key) == value for key, value in expected.items())
    return preserved / len(expected)
