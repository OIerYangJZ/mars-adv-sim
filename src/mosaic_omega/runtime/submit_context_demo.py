"""Publish one complete task context followed by a small delta for measurement."""

from __future__ import annotations

import argparse
import asyncio
import uuid

from .coordinator import TASK_CONTEXT_UPDATE_TOPIC
from .settings import add_transport_arguments, transport_settings_from_args
from .task_messages import ConstraintDelta, DeltaOperation, EvidenceRef, FactDelta, TaskMessage
from .transport import create_transport


async def run(args: argparse.Namespace) -> None:
    settings = transport_settings_from_args(args)
    transport = create_transport(settings, "context-delta-submitter", ())
    await transport.start(lambda topic, message: asyncio.sleep(0))
    try:
        # The first message establishes the complete working context once.
        seed = TaskMessage.create(
            message_id=uuid.uuid4().hex,
            sender="planner-01",
            receiver="analyst-01",
            task_id="T1-analysis",
            summary="调研与代码任务已完成，开始汇总分析。",
            facts=(
                FactDelta("goal", DeltaOperation.ADD, "形成最终分析结论"),
                FactDelta("research", DeltaOperation.ADD, "检索结果已整理"),
                FactDelta("code", DeltaOperation.ADD, "数据处理脚本已完成"),
                FactDelta("progress", DeltaOperation.ADD, "等待分析输出"),
            ),
            constraints=(ConstraintDelta("format", DeltaOperation.ADD, "输出中文简要报告"),),
            evidence_refs=(EvidenceRef("source-list", "artifact://sources/T1", "检索来源清单"),),
            priority=8,
        )
        await transport.publish(TASK_CONTEXT_UPDATE_TOPIC, seed.to_dict())
        # The second message deliberately contains only the changed fact.
        delta = TaskMessage.create(
            message_id=uuid.uuid4().hex,
            sender="planner-01",
            receiver="analyst-01",
            task_id="T1-analysis",
            summary=None,
            facts=(FactDelta("progress", DeltaOperation.REPLACE, "开始生成分析结论"),),
            priority=8,
        )
        await transport.publish(TASK_CONTEXT_UPDATE_TOPIC, delta.to_dict())
        print("[context-submitter] seed context and one-fact delta submitted")
        await asyncio.sleep(0.2)
    finally:
        await transport.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="submit an actual low-entropy task-context delta")
    add_transport_arguments(parser)
    asyncio.run(run(parser.parse_args()))
