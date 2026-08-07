"""Dependency-free offline benchmark for receiver-conditioned communication.

This does not replace real MQTT telemetry.  It provides a deterministic
regression benchmark that compares repeatedly sending full context snapshots
with receiver-conditioned incremental messages.
"""

from __future__ import annotations

import json
import uuid

from .low_entropy import (
    ReceiverConditionedCompressor,
    ReceiverKnowledgeStore,
    critical_fact_fidelity,
    encoded_size,
    estimate_tokens,
)
from .task_context import TaskContextStore
from .task_messages import ConstraintDelta, DeltaOperation, EvidenceRef, FactDelta, TaskMessage


def run() -> dict[str, float | int]:
    authority = TaskContextStore()
    receiver = ReceiverKnowledgeStore()
    compressor = ReceiverConditionedCompressor()
    task_id = "benchmark-task"
    baseline_bytes = 0
    actual_bytes = 0
    baseline_tokens = 0
    actual_tokens = 0

    seed = TaskMessage.create(
        message_id=uuid.uuid4().hex,
        sender="planner",
        receiver="analyst",
        task_id=task_id,
        summary="生成最终分析报告",
        facts=(
            FactDelta("goal", DeltaOperation.ADD, "形成最终结论"),
            FactDelta("progress", DeltaOperation.ADD, "0%"),
            FactDelta("source", DeltaOperation.ADD, "数据已准备"),
        ),
        constraints=(ConstraintDelta("privacy", DeltaOperation.ADD, "原始敏感数据不得离开本地"),),
        evidence_refs=(EvidenceRef("dataset", "artifact://dataset/1", "输入数据"),),
        priority=9,
    )
    context = authority.apply(seed)

    updates = [seed]
    for step in range(1, 21):
        updates.append(
            TaskMessage.create(
                message_id=uuid.uuid4().hex,
                sender="planner",
                receiver="analyst",
                task_id=task_id,
                facts=(FactDelta("progress", DeltaOperation.REPLACE, f"{step * 5}%"),),
                priority=4,
            )
        )

    for update in updates:
        if update is not seed:
            context = authority.apply(update)
        full = authority.build_snapshot_message(sender="coordinator", receiver="analyst", task_id=task_id)
        baseline_bytes += encoded_size(full.to_dict())
        baseline_tokens += estimate_tokens(full.to_dict())
        tailored = compressor.tailor(update, context, receiver.get("analyst", task_id), receiver="analyst")
        if tailored is not None:
            actual_bytes += encoded_size(tailored.to_dict())
            actual_tokens += estimate_tokens(tailored.to_dict())
            receiver.acknowledge("analyst", tailored)

    digest = receiver.get("analyst", task_id)
    reconstructed = {
        "privacy": context.constraints["privacy"],
        "goal": context.facts["goal"],
    }
    expected = dict(reconstructed)
    return {
        "messages": len(updates),
        "baseline_bytes": baseline_bytes,
        "receiver_conditioned_bytes": actual_bytes,
        "byte_reduction_ratio": round(1 - actual_bytes / baseline_bytes, 6),
        "baseline_tokens_est": baseline_tokens,
        "receiver_conditioned_tokens_est": actual_tokens,
        "token_reduction_est_ratio": round(1 - actual_tokens / baseline_tokens, 6),
        "critical_fact_fidelity": critical_fact_fidelity(expected, reconstructed),
        "acked_messages": digest.acknowledged_messages,
    }


if __name__ == "__main__":
    print(json.dumps(run(), ensure_ascii=False, indent=2))
