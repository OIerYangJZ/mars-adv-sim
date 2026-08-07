from __future__ import annotations

import unittest

from mosaic_omega.runtime.low_entropy import (
    DecisionImpactEstimator,
    LowEntropyOutbox,
    ReceiverConditionedCompressor,
    ReceiverKnowledgeStore,
    critical_fact_fidelity,
    semantic_fingerprint,
)
from mosaic_omega.runtime.task_context import TaskContextStore
from mosaic_omega.runtime.task_messages import (
    ConstraintDelta,
    DeltaOperation,
    EvidenceRef,
    FactDelta,
    TaskMessage,
    TaskMessageValidationError,
)


def make_message(message_id: str = "m1", **kwargs) -> TaskMessage:
    values = dict(
        message_id=message_id,
        sender="planner",
        receiver="analyst",
        task_id="task-a",
        summary=None,
        facts=(),
        constraints=(),
        evidence_refs=(),
        priority=5,
        ttl=300,
    )
    values.update(kwargs)
    return TaskMessage.create(**values)


class LowEntropyTests(unittest.TestCase):
    def test_task_message_rejects_extra_top_level_field(self) -> None:
        raw = make_message().to_dict()
        raw["unexpected"] = 1
        with self.assertRaises(TaskMessageValidationError):
            TaskMessage.from_dict(raw)

    def test_evidence_can_be_removed_without_snapshot(self) -> None:
        store = TaskContextStore()
        store.apply(
            make_message(
                "m1",
                evidence_refs=(EvidenceRef("e1", "artifact://e1", "source"),),
            )
        )
        store.apply(
            make_message(
                "m2",
                evidence_refs=(EvidenceRef("e1", None, None, DeltaOperation.REMOVE),),
            )
        )
        self.assertEqual(store.get("task-a").evidence_refs, {})

    def test_receiver_digest_suppresses_acknowledged_fact(self) -> None:
        authority = TaskContextStore()
        original = make_message(
            "m1",
            facts=(FactDelta("progress", DeltaOperation.ADD, "50%"),),
            priority=8,
        )
        context = authority.apply(original)
        knowledge = ReceiverKnowledgeStore()
        first = ReceiverConditionedCompressor.tailor(
            original, context, knowledge.get("analyst", "task-a"), receiver="analyst"
        )
        self.assertIsNotNone(first)
        knowledge.acknowledge("analyst", first)
        duplicate = ReceiverConditionedCompressor.tailor(
            original, context, knowledge.get("analyst", "task-a"), receiver="analyst"
        )
        self.assertIsNone(duplicate)

    def test_constraints_are_high_impact(self) -> None:
        message = make_message(
            constraints=(ConstraintDelta("privacy", DeltaOperation.ADD, "不得上传原始数据"),),
            priority=1,
        )
        decision = DecisionImpactEstimator().decide(message)
        self.assertEqual(decision.action.value, "send")
        self.assertGreaterEqual(decision.impact_score, 0.95)

    def test_low_impact_messages_merge_and_keep_latest_delta(self) -> None:
        outbox = LowEntropyOutbox()
        first = make_message(
            "m1", facts=(FactDelta("progress", DeltaOperation.REPLACE, "10%"),), priority=2, ttl=100
        )
        second = make_message(
            "m2", facts=(FactDelta("progress", DeltaOperation.REPLACE, "20%"),), priority=3, ttl=100
        )
        self.assertFalse(outbox.enqueue(first, defer_s=1.0, now=10.0))
        self.assertTrue(outbox.enqueue(second, defer_s=1.0, now=10.1))
        ready, expired = outbox.pop_ready(now=11.0)
        self.assertEqual(expired, [])
        self.assertEqual(len(ready), 1)
        self.assertEqual(ready[0].message.facts[0].text, "20%")
        self.assertEqual(ready[0].message.priority, 3)

    def test_ttl_expiry_drops_deferred_message(self) -> None:
        outbox = LowEntropyOutbox()
        message = make_message("m1", priority=1, ttl=1)
        outbox.enqueue(message, defer_s=10.0, now=0.0)
        ready, expired = outbox.pop_ready(now=1.1)
        self.assertEqual(ready, [])
        self.assertEqual(len(expired), 1)

    def test_semantic_hash_ignores_message_id(self) -> None:
        a = make_message("a", facts=(FactDelta("x", DeltaOperation.ADD, "v"),))
        b = make_message("b", facts=(FactDelta("x", DeltaOperation.ADD, "v"),))
        self.assertEqual(semantic_fingerprint(a), semantic_fingerprint(b))

    def test_critical_fact_fidelity(self) -> None:
        expected = {"privacy": "local-only", "deadline": "18:00"}
        self.assertEqual(critical_fact_fidelity(expected, dict(expected)), 1.0)
        self.assertEqual(critical_fact_fidelity(expected, {"privacy": "local-only"}), 0.5)

    def test_snapshot_message_is_orderable_on_same_context_topic(self) -> None:
        store = TaskContextStore()
        store.apply(make_message("m1", summary="state"))
        snapshot = store.build_snapshot_message(sender="coordinator", receiver="analyst", task_id="task-a")
        self.assertTrue(snapshot.message_id.startswith("snapshot:"))
        self.assertEqual(len(snapshot.to_dict()), 10)


if __name__ == "__main__":
    unittest.main()

class _RecordingTransport:
    def __init__(self) -> None:
        self.published: list[tuple[str, dict]] = []
        self.is_connected = True

    async def publish(self, topic: str, message: dict) -> None:
        self.published.append((topic, message))

    def add_connection_listener(self, listener) -> None:
        return

    async def start(self, handler) -> None:
        return

    async def stop(self) -> None:
        return


class CoordinatorReceiverConditioningTests(unittest.IsolatedAsyncioTestCase):
    async def test_acknowledged_constraint_is_not_retransmitted(self) -> None:
        from mosaic_omega.runtime.coordinator import CONTEXT_ACK_TOPIC, Coordinator, TASK_CONTEXT_UPDATE_TOPIC
        from mosaic_omega.runtime.protocol import envelope
        from mosaic_omega.runtime.tasks import TaskSpec

        transport = _RecordingTransport()
        coordinator = Coordinator(transport)
        coordinator.tasks.add(TaskSpec("task-a", "task", frozenset()))
        coordinator.task_contexts.ensure("task-a")
        update = make_message(
            "first",
            sender="planner",
            receiver="analyst",
            constraints=(ConstraintDelta("privacy", DeltaOperation.ADD, "不得上传原始数据"),),
            priority=9,
        )
        await coordinator._handle_task_context_update(update.to_dict())
        context_frames = [(topic, body) for topic, body in transport.published if topic == "agent/analyst/context"]
        self.assertEqual(len(context_frames), 1)
        delivered = TaskMessage.from_dict(context_frames[0][1])
        await coordinator._handle_context_ack(
            envelope(
                "TASK_CONTEXT_ACK",
                "analyst",
                "coordinator",
                agent_id="analyst",
                task_id="task-a",
                message_id=delivered.message_id,
            )
        )
        duplicate = make_message(
            "second",
            sender="planner",
            receiver="analyst",
            constraints=(ConstraintDelta("privacy", DeltaOperation.REPLACE, "不得上传原始数据"),),
            priority=9,
        )
        await coordinator._handle_task_context_update(duplicate.to_dict())
        context_frames = [(topic, body) for topic, body in transport.published if topic == "agent/analyst/context"]
        self.assertEqual(len(context_frames), 1)
        self.assertGreaterEqual(coordinator.low_entropy_metrics()["duplicate_drops"], 1)
