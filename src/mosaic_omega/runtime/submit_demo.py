"""Publish a simple dependency DAG to the coordinator."""

from __future__ import annotations

import argparse
import asyncio

from .coordinator import TASK_NEW_TOPIC
from .protocol import envelope
from .settings import add_transport_arguments, transport_settings_from_args
from .transport import create_transport


DEMO_TASKS = [
    {
        "task_id": "T1-plan", "title": "拆解调研任务", "required_skills": ["plan"],
        "priority": 9, "simulated_duration_s": 0.8,
    },
    {
        "task_id": "T1-search", "title": "检索资料", "required_skills": ["search"],
        "dependencies": ["T1-plan"], "requested_by": "planner-01", "simulated_duration_s": 0.8,
    },
    {
        "task_id": "T1-code", "title": "编写数据处理脚本", "required_skills": ["code"],
        "dependencies": ["T1-plan"], "requested_by": "planner-01", "simulated_duration_s": 1.0,
    },
    {
        "task_id": "T1-analysis", "title": "汇总分析", "required_skills": ["analysis"],
        "dependencies": ["T1-search", "T1-code"], "requested_by": "planner-01", "simulated_duration_s": 0.8,
    },
]


async def run(args: argparse.Namespace) -> None:
    settings = transport_settings_from_args(args)
    transport = create_transport(settings, "task-submitter", ())
    await transport.start(lambda topic, message: asyncio.sleep(0))
    await transport.publish(TASK_NEW_TOPIC, envelope("TASK_BATCH", "task-submitter", tasks=DEMO_TASKS))
    print("[submitter] demo task DAG submitted")
    await asyncio.sleep(0.1)
    await transport.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="提交演示任务依赖图")
    add_transport_arguments(parser)
    asyncio.run(run(parser.parse_args()))
