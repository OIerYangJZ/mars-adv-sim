"""CLI entrypoint for one independently running simulated agent."""

from __future__ import annotations

import argparse
import asyncio

from .agent_runtime import AgentRuntime
from .edge_cloud import ExecutionTier, ResourceDescriptor
from .models import AgentProfile
from .settings import add_transport_arguments, transport_settings_from_args
from .transport import create_transport


async def run(args: argparse.Namespace) -> None:
    settings = transport_settings_from_args(args)
    profile = AgentProfile(
        agent_id=args.agent_id,
        name=args.name,
        skills=tuple(item.strip() for item in args.skills.split(",") if item.strip()),
        endpoint=args.endpoint,
        max_load=args.max_load,
        reliability=args.reliability,
        tier=ExecutionTier(args.tier),
        resources=ResourceDescriptor(
            cpu_cores=args.cpu_cores,
            memory_mb=args.memory_mb,
            gpu_count=args.gpu_count,
            accelerator_tags=tuple(item.strip() for item in args.accelerator_tags.split(",") if item.strip()),
        ),
        labels=tuple(item.strip() for item in args.labels.split(",") if item.strip()),
    )
    transport = create_transport(settings, profile.agent_id, AgentRuntime.subscriptions(profile.agent_id))
    agent = AgentRuntime(profile, transport, heartbeat_interval=args.heartbeat_interval)
    await agent.start()
    try:
        await asyncio.Event().wait()
    finally:
        await agent.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="独立运行的模拟智能体")
    add_transport_arguments(parser)
    parser.add_argument("--agent-id", required=True)
    parser.add_argument("--name", required=True)
    parser.add_argument("--skills", required=True, help="逗号分隔，例如 code,test")
    parser.add_argument("--endpoint", default="", help="实机部署时填 Agent 服务地址")
    parser.add_argument("--max-load", type=int, default=1)
    parser.add_argument("--reliability", type=float, default=0.95)
    parser.add_argument("--tier", choices=tuple(tier.value for tier in ExecutionTier), default=ExecutionTier.DEVICE.value)
    parser.add_argument("--cpu-cores", type=int, default=0)
    parser.add_argument("--memory-mb", type=int, default=0)
    parser.add_argument("--gpu-count", type=int, default=0)
    parser.add_argument("--accelerator-tags", default="", help="comma-separated accelerator capabilities")
    parser.add_argument("--labels", default="", help="comma-separated deployment/data locality labels")
    parser.add_argument("--heartbeat-interval", type=float, default=3.0)
    asyncio.run(run(parser.parse_args()))
