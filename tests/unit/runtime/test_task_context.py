from __future__ import annotations

import unittest

from mosaic_omega.runtime.task_context import TaskContextStore
from mosaic_omega.runtime.task_messages import DeltaOperation, FactDelta, TaskMessage


def message(
    message_id: str,
    *,
    summary: str | None = None,
    facts: tuple[FactDelta, ...] = (),
) -> TaskMessage:
    return TaskMessage.create(
        message_id=message_id,
        sender="sender",
        receiver="receiver",
        task_id="task-a",
        summary=summary,
        facts=facts,
    )


class TaskContextStoreTests(unittest.TestCase):
    def test_message_is_idempotent_by_message_id(self) -> None:
        store = TaskContextStore()
        update = message(
            "m1",
            summary="initial",
            facts=(FactDelta("progress", DeltaOperation.ADD, "50%"),),
        )
        self.assertEqual(store.apply(update).revision, 1)
        self.assertEqual(store.apply(update).revision, 1)
        self.assertEqual(store.get("task-a").facts["progress"], "50%")

    def test_snapshot_replaces_a_stale_cache(self) -> None:
        authority = TaskContextStore()
        authority.apply(message("m1", summary="initial"))
        authority.apply(
            message(
                "m2",
                facts=(FactDelta("progress", DeltaOperation.ADD, "50%"),),
            )
        )
        snapshot = authority.build_snapshot_message(
            sender="coordinator",
            receiver="receiver",
            task_id="task-a",
        )
        receiver = TaskContextStore()
        receiver.apply(
            TaskMessage.create(
                message_id="old",
                sender="coordinator",
                receiver="receiver",
                task_id="task-a",
                facts=(FactDelta("obsolete", DeltaOperation.ADD, "discard me"),),
            )
        )
        recovered = receiver.apply(snapshot, replace=True)
        self.assertEqual(recovered.summary, "initial")
        self.assertEqual(recovered.facts, {"progress": "50%"})
